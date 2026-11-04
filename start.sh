#!/usr/bin/env bash
# Starts (or restarts) the relay and the Cloudflare tunnel in the background.
# Runs automatically every time the Codespace starts. Safe to run again by hand:
#   bash start.sh
# Tokens come from Codespaces secrets RELAY_TOKEN and CF_TUNNEL_TOKEN, never from this repo.
set -u
cd "$(dirname "$0")" || exit 1
RUN=/tmp/revteth
mkdir -p "$RUN"
CLOUDFLARED="$HOME/cloudflared"

# 1. Pick up the latest relay code from GitHub (my edits land here).
git pull --ff-only -q 2>/dev/null && echo "✔ code up to date ($(git log -1 --format=%h))" ||
  echo "• couldn't pull updates (offline or local changes); using the current code"

# 2. Install the relay's one library, and download cloudflared once.
[ -d node_modules/ws ] || npm install --silent --no-audit --no-fund
if [ ! -x "$CLOUDFLARED" ]; then
  echo "• downloading cloudflared"
  curl -fsSL -o "$CLOUDFLARED" \
    https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 &&
    chmod +x "$CLOUDFLARED"
fi

# 3. Stop old copies (from an earlier run) so this is a clean restart.
for name in relay tunnel; do
  if [ -f "$RUN/$name.pid" ]; then
    kill "$(cat "$RUN/$name.pid")" 2>/dev/null
    rm -f "$RUN/$name.pid"
  fi
done
sleep 1

# 4. Start the relay.
if [ -z "${RELAY_TOKEN:-}" ]; then
  echo "✘ RELAY_TOKEN secret is missing. Add it in GitHub → Settings → Codespaces → Secrets,"
  echo "  give this repo access, then rebuild/restart the Codespace."
  exit 1
fi
TOKEN="$RELAY_TOKEN" PORT=8080 setsid nohup node server.js > "$RUN/relay.log" 2>&1 < /dev/null &
echo $! > "$RUN/relay.pid"

# 5. Start the tunnel (token passed by environment, so it never shows up in `ps`).
if [ -n "${CF_TUNNEL_TOKEN:-}" ]; then
  TUNNEL_TOKEN="$CF_TUNNEL_TOKEN" setsid nohup "$CLOUDFLARED" tunnel run > "$RUN/tunnel.log" 2>&1 < /dev/null &
  echo $! > "$RUN/tunnel.pid"
else
  echo "✘ CF_TUNNEL_TOKEN secret is missing, so the tunnel wasn't started."
fi

sleep 4
bash ./status.sh
