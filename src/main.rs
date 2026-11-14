use gateway_plugin_sdk::{
    ErrorCode, Manifest, PluginFault,
    call::{
        data::QuotaFactsQuery,
        host::{
            AuthListRequest, AuthListResult, AuthRuntimeAccount, HttpRequest, StateGetRequest,
            StateGetResult, StatePutRequest, StatePutResult,
        },
    },
    client::{
        ComposedPlugin, Empty, HostClient, PluginBuilder, PluginSession, SessionConfig,
        SessionError, TypedCall, TypedReply, methods,
    },
};
use chrono::{FixedOffset, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{collections::BTreeMap, sync::Arc, time::{SystemTime, UNIX_EPOCH}};

const PLUGIN_ID: &str = "luck7x.wxpusher-account-monitor";
const STATE_NAMESPACE: &str = "monitor";
const STATE_KEY: &str = "monitor";
const WXPUSHER_STANDARD_URL: &str = "https://wxpusher.zjiecode.com/api/send/message";
const WXPUSHER_SIMPLE_URL: &str = "https://wxpusher.zjiecode.com/api/send/message/simple-push";
const SECOND_REMINDER_SECONDS: u64 = 2 * 60 * 60;

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Configuration {
    app_token: String,
    uid: String,
    #[serde(default = "enabled")]
    daily_probe_enabled: bool,
    #[serde(default = "enabled")]
    send_startup_message: bool,
    #[serde(default)]
    account_labels: BTreeMap<String, String>,
}

const fn enabled() -> bool { true }

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MonitorState {
    initialized: bool,
    accounts: Vec<AccountState>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AccountState {
    account_id: String,
    state: String,
    notified_state: String,
    last_probe_day: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WxMessage<'a> {
    app_token: &'a str,
    content: &'a str,
    summary: &'a str,
    content_type: u8,
    uids: [&'a str; 1],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WxSimpleMessage<'a> {
    spt: &'a str,
    content: &'a str,
    summary: &'a str,
    content_type: u8,
}

fn manifest() -> Result<Manifest, gateway_plugin_sdk::ManifestError> {
    Manifest::from_author_slice(include_bytes!("../plugin.json"))
}

fn plugin(configuration: Arc<Configuration>) -> Result<ComposedPlugin, gateway_plugin_sdk::client::AuthorError> {
    PluginBuilder::from_json(include_bytes!("../plugin.json"))?
        .on(methods::RECONCILE, move |call| {
            let configuration = Arc::clone(&configuration);
            async move { reconcile(configuration, call).await }
        })?
        .build()
}

async fn reconcile(
    configuration: Arc<Configuration>,
    call: TypedCall<Empty>,
) -> Result<TypedReply<Empty>, PluginFault> {
    let accounts = list_openai_accounts(&call.host).await?;
    let stored = get_state(&call.host).await?;
    let version = stored.as_ref().map(|record| record.version);
    let mut state = match stored {
        Some(record) => serde_json::from_value(record.value).map_err(|_| fault("插件状态无法解析"))?,
        None => MonitorState::default(),
    };

    let active = accounts
        .iter()
        .filter(|account| account.enabled && account.authentication_kind == "oauth")
        .map(|account| (account.account_id.clone(), account))
        .collect::<BTreeMap<_, _>>();
    let pool = active.values().copied().collect::<Vec<_>>();
    state.accounts.retain(|entry| active.contains_key(&entry.account_id));

    for account in active.values() {
        let current = effective_state(account);
        let entry = match state.accounts.iter_mut().find(|entry| entry.account_id == account.account_id) {
            Some(entry) => entry,
            None => {
                let notified = if is_healthy(&current) { current.clone() } else { String::new() };
                state.accounts.push(AccountState {
                    account_id: account.account_id.clone(),
                    state: current.clone(),
                    notified_state: notified,
                    last_probe_day: -1,
                });
                state.accounts.last_mut().expect("just inserted")
            }
        };
        let previous = entry.state.clone();
        let now = epoch_seconds();
        let marker = parse_alert_marker(&entry.notified_state);
        if !is_healthy(&current) {
            match marker {
                None => {
                    let title = format!("【OAuth 授权失效 1/2】{}", account_label(account, &configuration));
                    let content = failure_content(account, &current, 1, &pool, &configuration);
                    send_wxpusher(&call.host, &configuration, &title, &content).await?;
                    entry.notified_state = format!("alert:1:{now}");
                }
                Some(marker) if marker.count == 1
                    && now.saturating_sub(marker.first_at) >= SECOND_REMINDER_SECONDS =>
                {
                    let title = format!("【仍需重新授权 2/2】{}", account_label(account, &configuration));
                    let content = failure_content(account, &current, 2, &pool, &configuration);
                    send_wxpusher(&call.host, &configuration, &title, &content).await?;
                    entry.notified_state = format!("alert:2:{}", marker.first_at);
                }
                _ => {}
            }
        } else if current == "ready" && marker.is_some() && previous == "ready" {
            let title = format!("【OAuth 已恢复】{}", account_label(account, &configuration));
            let content = recovery_content(account, &pool, &configuration);
            send_wxpusher(&call.host, &configuration, &title, &content).await?;
            entry.notified_state = "ready".to_owned();
        } else if marker.is_none() && entry.notified_state.is_empty() {
            entry.notified_state.clone_from(&current);
        }
        entry.state.clone_from(&current);
    }

    if !state.initialized {
        if configuration.send_startup_message {
            let content = format!(
                "WxPusher 账号授权监控已启用。\n当前监控 {} 个 OpenAI OAuth 账号。\n异常状态只在变化时推送；每日检测不发送模型对话。",
                active.len()
            );
            send_wxpusher(&call.host, &configuration, "Codex 账号监控已启用", &content).await?;
        }
        state.initialized = true;
    }

    if configuration.daily_probe_enabled {
        let today = epoch_day();
        if let Some(entry) = state.accounts.iter_mut().find(|entry| {
            entry.last_probe_day != today
                && active.get(&entry.account_id).is_some_and(|account| is_healthy(&effective_state(account)))
        }) {
            let _ = call.host.refresh_account_quota(QuotaFactsQuery {
                account_id: entry.account_id.clone(),
            }).await;
            entry.last_probe_day = today;
        }
    }

    put_state(&call.host, &state, version).await?;
    Ok(TypedReply::new(Empty {}))
}

fn effective_state(account: &AuthRuntimeAccount) -> String {
    if account.has_refresh_token {
        account.credential_state.clone()
    } else {
        "missing_refresh_token".to_owned()
    }
}

fn is_healthy(state: &str) -> bool {
    matches!(state, "ready" | "unknown")
}

#[derive(Clone, Copy)]
struct AlertMarker {
    count: u8,
    first_at: u64,
}

fn parse_alert_marker(value: &str) -> Option<AlertMarker> {
    let mut parts = value.split(':');
    if parts.next()? != "alert" {
        return None;
    }
    let count = parts.next()?.parse().ok()?;
    let first_at = parts.next()?.parse().ok()?;
    Some(AlertMarker { count, first_at })
}

fn account_label<'a>(account: &'a AuthRuntimeAccount, configuration: &'a Configuration) -> &'a str {
    configuration
        .account_labels
        .get(&account.account_id)
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
        .or(account.email.as_deref())
        .unwrap_or(&account.name)
}

fn pool_status(accounts: &[&AuthRuntimeAccount], configuration: &Configuration) -> String {
    let healthy = accounts
        .iter()
        .filter(|account| is_healthy(&effective_state(account)))
        .count();
    let abnormal = accounts.len().saturating_sub(healthy);
    let mut output = format!("账号池状态：{healthy} 正常 / {abnormal} 异常 / 共 {} 个\n\n", accounts.len());
    for account in accounts {
        let state = effective_state(account);
        let label = account_label(account, configuration);
        if is_healthy(&state) {
            output.push_str(&format!("✅ {label}\n"));
        } else {
            output.push_str(&format!("❌ {label}（{state}）\n"));
        }
    }
    output.trim_end().to_owned()
}

fn failure_content(
    account: &AuthRuntimeAccount,
    state: &str,
    reminder: u8,
    pool: &[&AuthRuntimeAccount],
    configuration: &Configuration,
) -> String {
    let label = account_label(account, configuration);
    let email = account.email.as_deref().unwrap_or("未记录");
    let action = match state {
        "expired" | "invalid" | "missing_refresh_token" => "请进入账号管理完成 OAuth 重新授权。",
        "banned" => "账号可能已被上游封禁，请人工检查。",
        _ => "请进入账号管理检查凭据。",
    };
    let reminder_text = if reminder == 1 {
        "若 2 小时后仍未恢复，将发送第 2 次也是最后一次提醒。"
    } else {
        "这是本次故障的最后一次提醒，之后不会重复推送。"
    };
    format!(
        "发现时间：{}\n账号备注：{label}\n登录邮箱：{email}\n当前状态：{state}\n处理建议：{action}\n\n{}\n\n{reminder_text}",
        beijing_time(),
        pool_status(pool, configuration)
    )
}

fn recovery_content(
    account: &AuthRuntimeAccount,
    pool: &[&AuthRuntimeAccount],
    configuration: &Configuration,
) -> String {
    let label = account_label(account, configuration);
    let email = account.email.as_deref().unwrap_or("未记录");
    format!(
        "恢复时间：{}\n账号备注：{label}\n登录邮箱：{email}\n当前状态：ready\n结果：重新授权成功，账号已重新加入账号池。\n\n{}",
        beijing_time(),
        pool_status(pool, configuration)
    )
}

fn beijing_time() -> String {
    let offset = FixedOffset::east_opt(8 * 60 * 60).expect("valid UTC+8 offset");
    Utc::now().with_timezone(&offset).format("%Y-%m-%d %H:%M").to_string()
}

fn epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn epoch_day() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| i64::try_from(duration.as_secs() / 86_400).unwrap_or(i64::MAX))
}

async fn list_openai_accounts(host: &HostClient) -> Result<Vec<AuthRuntimeAccount>, PluginFault> {
    let mut cursor = None;
    let mut accounts = Vec::new();
    loop {
        let page: AuthListResult = payload_call(host, "host.auth.list", &AuthListRequest {
            provider_id: Some("openai".to_owned()),
            cursor,
            limit: 200,
        }).await?;
        accounts.extend(page.accounts);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(accounts),
        }
    }
}

async fn get_state(host: &HostClient) -> Result<Option<gateway_plugin_sdk::call::host::StateRecord>, PluginFault> {
    let reply = host.call(
        "host.state.get",
        serde_json::to_value(StateGetRequest {
            namespace: STATE_NAMESPACE.to_owned(),
            key: STATE_KEY.to_owned(),
        }).map_err(|_| fault("无法编码状态读取请求"))?,
        Vec::new(),
    ).await.map_err(SessionError::into_plugin_fault)?;
    let result: StateGetResult = serde_json::from_value(reply.result).map_err(|_| fault("状态读取响应无效"))?;
    Ok(result.record)
}

async fn put_state(host: &HostClient, state: &MonitorState, version: Option<u64>) -> Result<(), PluginFault> {
    let reply = host.call(
        "host.state.put",
        serde_json::to_value(StatePutRequest {
            namespace: STATE_NAMESPACE.to_owned(),
            key: STATE_KEY.to_owned(),
            value: serde_json::to_value(state).map_err(|_| fault("无法编码插件状态"))?,
            expected_version: version,
        }).map_err(|_| fault("无法编码状态写入请求"))?,
        Vec::new(),
    ).await.map_err(SessionError::into_plugin_fault)?;
    let _: StatePutResult = serde_json::from_value(reply.result).map_err(|_| fault("状态写入响应无效"))?;
    Ok(())
}

async fn payload_call<I: Serialize, O: DeserializeOwned>(
    host: &HostClient,
    method: &str,
    input: &I,
) -> Result<O, PluginFault> {
    let reply = host.call(
        method,
        serde_json::json!({}),
        serde_json::to_vec(input).map_err(|_| fault("无法编码宿主请求"))?,
    ).await.map_err(SessionError::into_plugin_fault)?;
    serde_json::from_slice(&reply.payload).map_err(|_| fault("宿主响应无效"))
}

async fn send_wxpusher(
    host: &HostClient,
    configuration: &Configuration,
    summary: &str,
    content: &str,
) -> Result<(), PluginFault> {
    let simple = configuration.app_token.starts_with("SPT_");
    let (url, body) = if simple {
        (
            WXPUSHER_SIMPLE_URL,
            serde_json::to_vec(&WxSimpleMessage {
                spt: &configuration.app_token,
                content,
                summary,
                content_type: 1,
            }),
        )
    } else {
        (
            WXPUSHER_STANDARD_URL,
            serde_json::to_vec(&WxMessage {
                app_token: &configuration.app_token,
                content,
                summary,
                content_type: 1,
                uids: [&configuration.uid],
            }),
        )
    };
    let body = body.map_err(|_| fault("无法编码 WxPusher 请求"))?;
    let response = host.http(HttpRequest {
        method: "POST".to_owned(),
        url: url.to_owned(),
        headers: vec![("content-type".to_owned(), b"application/json".to_vec())],
    }, body).await?;
    let status = response.status;
    let body = response.body.collect(64 * 1024).await?;
    if !(200..300).contains(&status) {
        return Err(fault("WxPusher 返回非成功 HTTP 状态"));
    }
    let value: serde_json::Value = serde_json::from_slice(&body).map_err(|_| fault("WxPusher 响应无法解析"))?;
    if value.get("code").and_then(serde_json::Value::as_i64) != Some(1000) {
        return Err(fault("WxPusher 拒绝了推送请求"));
    }
    Ok(())
}

fn fault(message: &'static str) -> PluginFault {
    PluginFault::new(ErrorCode::Fault, message)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = PluginSession::accept(tokio::io::stdin(), tokio::io::stdout(), SessionConfig::default()).await?;
    let manifest = manifest()?;
    if session.handshake().plugin_id != PLUGIN_ID || session.handshake().contributes != manifest.contributes {
        return Err(SessionError::Handshake.into());
    }
    let configuration: Configuration = serde_json::from_value(session.handshake().configuration.clone())?;
    session.run(plugin(Arc::new(configuration))?).await?;
    Ok(())
}

