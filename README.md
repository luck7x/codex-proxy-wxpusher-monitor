# WxPusher Account Monitor for Codex Proxy RS

Codex Proxy RS maintenance plugin that monitors OpenAI OAuth credential state and sends state-change notifications through WxPusher.

The plugin package contains no WxPusher credentials. Configure `appToken` and `uid` in the Codex Proxy RS plugin settings after installation. `appToken` is declared as a secret field and is stored separately by the host.

Features:

- checks account credential state every maintenance cycle;
- notifies once when an account becomes expired, invalid, banned, or loses its refresh token;
- notifies once when an account recovers;
- probes one account per cycle through the non-conversation quota endpoint, at most once per UTC day;
- stores notification and probe state in the host-managed private plugin state.

