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
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{collections::BTreeMap, sync::Arc, time::{SystemTime, UNIX_EPOCH}};

const PLUGIN_ID: &str = "luck7x.wxpusher-account-monitor";
const STATE_NAMESPACE: &str = "monitor";
const STATE_KEY: &str = "monitor";
const WXPUSHER_STANDARD_URL: &str = "https://wxpusher.zjiecode.com/api/send/message";
const WXPUSHER_SIMPLE_URL: &str = "https://wxpusher.zjiecode.com/api/send/message/simple-push";

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Configuration {
    app_token: String,
    uid: String,
    #[serde(default = "enabled")]
    daily_probe_enabled: bool,
    #[serde(default = "enabled")]
    send_startup_message: bool,
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
        entry.state.clone_from(&current);
        let changed_to_error = !is_healthy(&current) && entry.notified_state != current;
        let recovered = is_healthy(&current)
            && !entry.notified_state.is_empty()
            && !is_healthy(&entry.notified_state);
        if changed_to_error || recovered {
            let content = notification_content(account, &current, recovered);
            send_wxpusher(
                &call.host,
                &configuration,
                if recovered { "Codex 账号已恢复" } else { "Codex 账号授权告警" },
                &content,
            ).await?;
            entry.notified_state.clone_from(&current);
        } else if is_healthy(&current) && entry.notified_state.is_empty() {
            entry.notified_state.clone_from(&current);
        }
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

fn notification_content(account: &AuthRuntimeAccount, state: &str, recovered: bool) -> String {
    let identity = account.email.as_deref().unwrap_or(&account.name);
    let suffix = account.account_id.rsplit('_').next().unwrap_or(&account.account_id);
    if recovered {
        format!("账号：{identity}\nID：…{}\n状态：已恢复，可正常使用。", tail(suffix, 8))
    } else {
        let action = match state {
            "expired" | "invalid" | "missing_refresh_token" => "请进入账号管理完成重新授权。",
            "banned" => "账号可能已被上游封禁，请人工检查。",
            _ => "请进入账号管理检查凭据。",
        };
        format!("账号：{identity}\nID：…{}\n异常：{state}\n{action}", tail(suffix, 8))
    }
}

fn tail(value: &str, count: usize) -> &str {
    let start = value.char_indices().rev().nth(count.saturating_sub(1)).map_or(0, |(index, _)| index);
    &value[start..]
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

