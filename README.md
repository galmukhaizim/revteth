# revteth

The relay for the thin client: it lets the phone use the Chromebook's internet.
The page sends the phone's connections over one WebSocket; this relay makes the
real connections and sends the data back.

**It runs itself.** Every time the Codespace starts, it pulls the latest code from
this repo and starts the relay and the Cloudflare tunnel in the background.

## One-time setup

1. Create a Codespace from this repo (**Code → Codespaces → Create codespace on main**).
2. GitHub asks for two secrets (or add them first under **Settings → Codespaces →
   Secrets** and give this repo access):
   - `RELAY_TOKEN`: a password you choose (16+ characters). Put the same value in
     the thin client's **Token** box.
   - `CF_TUNNEL_TOKEN`: your Cloudflare tunnel token (the long string after
     `--token` in Cloudflare's connector command).
3. In Cloudflare, the tunnel's published route should point to `http://localhost:8080`.

Tokens live only in your GitHub secrets, never in this repo.

## Everyday use

Just open the Codespace. Check that everything is up with:

```bash
bash status.sh
```

Restart both (for example after changing a secret) with:

```bash
bash start.sh
```

Logs are in `/tmp/revteth/relay.log` and `/tmp/revteth/tunnel.log`.

## Files

| File | What it is |
|---|---|
| `server.js` | The relay |
| `start.sh` | Pulls updates, then starts the relay and tunnel in the background |
| `status.sh` | Shows whether both are running |
| `.devcontainer/devcontainer.json` | Runs `start.sh` when the Codespace starts |
