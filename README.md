# WxPusher Account Monitor for Codex Proxy RS

Codex Proxy RS maintenance plugin that monitors OpenAI OAuth credential state and sends state-change notifications through WxPusher.

The plugin package contains no WxPusher credentials. Configure an SPT (simple push token), or a standard `appToken` plus `uid`, in the Codex Proxy RS plugin settings after installation. The token field is declared as a secret field and is stored separately by the host.

Tokens beginning with `SPT_` automatically use WxPusher's simple-push endpoint. Other tokens use the standard app endpoint and the configured UID.

Features:

- checks account credential state every maintenance cycle;
- sends an immediate failure notification and one final reminder after two hours if the account is still unhealthy;
- waits for two consecutive `ready` observations before sending one recovery notification;
- includes the complete account-pool status and uses locally configured account notes when available;
- probes one account per cycle through the non-conversation quota endpoint, at most once per UTC day;
- stores notification and probe state in the host-managed private plugin state.

