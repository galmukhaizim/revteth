#!/usr/bin/env bash
# Shows whether the relay and tunnel are running:   bash status.sh
RUN=/tmp/revteth
alive() { [ -f "$RUN/$1.pid" ] && kill -0 "$(cat "$RUN/$1.pid")" 2>/dev/null; }

echo "── relay ──"
if alive relay && health=$(curl -fsS --max-time 3 http://localhost:8080/ 2>/dev/null); then
  echo "✔ running: $health"
else
  echo "✘ not running. Last log lines:"
  tail -n 5 "$RUN/relay.log" 2>/dev/null
fi

echo "── tunnel ──"
if alive tunnel; then
  n=$(grep -c "Registered tunnel connection" "$RUN/tunnel.log" 2>/dev/null || true)
  host=$(grep -o '"hostname":"[^"]*"' "$RUN/tunnel.log" 2>/dev/null | tail -1 | cut -d'"' -f4)
  if [ "${n:-0}" -gt 0 ]; then
    echo "✔ connected to Cloudflare ($n connections)${host:+ → https://$host}"
  else
    echo "• starting… (run this again in a few seconds)"
  fi
else
  echo "✘ not running. Last log lines:"
  tail -n 5 "$RUN/tunnel.log" 2>/dev/null
fi
