// Thin-client relay: turns WebSocket connections from the browser tab into
// real TCP connections. Each WebSocket = one TCP connection.
//
//   wss://<relay>/?host=example.com&port=443&token=<TOKEN>
//
// Binary frames in both directions are the raw TCP bytes.

import http from "node:http";
import net from "node:net";
import dns from "node:dns/promises";
import crypto from "node:crypto";
import { WebSocketServer } from "ws";

const PORT = Number(process.env.PORT || 8080);
const TOKEN = process.env.TOKEN;
if (!TOKEN || TOKEN.length < 16) {
  console.error("Set TOKEN to a random secret of at least 16 characters, e.g.\n" +
    "  TOKEN=$(openssl rand -hex 16) npm start");
  process.exit(1);
}

// Never let the relay be used to reach private/internal networks.
const blocked = new net.BlockList();
for (const [net4, bits] of [
  ["0.0.0.0", 8], ["10.0.0.0", 8], ["100.64.0.0", 10], ["127.0.0.0", 8],
  ["169.254.0.0", 16], ["172.16.0.0", 12], ["192.168.0.0", 16], ["224.0.0.0", 4],
]) blocked.addSubnet(net4, bits, "ipv4");
for (const [net6, bits] of [["::1", 128], ["::", 128], ["fc00::", 7], ["fe80::", 10]])
  blocked.addSubnet(net6, bits, "ipv6");

const BLOCKED_PORTS = new Set([25]); // no spam relay

const tokenOk = (t) => {
  const a = Buffer.from(String(t ?? ""));
  const b = Buffer.from(TOKEN);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
};

let active = 0;

const server = http.createServer((req, res) => {
  res.writeHead(200, {
    "content-type": "text/plain",
    // Lets the page (opened from a local file) read this health check.
    "access-control-allow-origin": "*",
  });
  // "(mux)" tells the page this relay supports the faster shared-link protocol.
  res.end(`thin-client relay OK (mux) — ${active} active connection(s)\n`);
});

const wss = new WebSocketServer({
  server,
  maxPayload: 1 << 20,
  perMessageDeflate: false,
  // Refuse a wrong token before the WebSocket even opens.
  verifyClient: ({ req }, done) => {
    const t = new URL(req.url, "http://relay").searchParams.get("token");
    if (tokenOk(t)) done(true);
    else done(false, 401, "bad token");
  },
});

// Look up a target and refuse private/internal addresses and blocked ports.
async function resolveTarget(host, port) {
  if (!host || !Number.isInteger(port) || port < 1 || port > 65535 || BLOCKED_PORTS.has(port))
    throw new Error("bad target");
  let r;
  try {
    r = await dns.lookup(host);
  } catch {
    throw new Error("dns failed");
  }
  const allowPrivate = process.env.ALLOW_PRIVATE === "1"; // local testing only
  if (!allowPrivate && blocked.check(r.address, r.family === 6 ? "ipv6" : "ipv4"))
    throw new Error("private address");
  return r.address;
}

// ---------------------------------------------------------------------------
// Multiplexed protocol: one WebSocket carries many TCP connections.
// Frames: [type u8][stream id u32 BE][payload]. See the page's src/mux.js.
// Each stream may have at most WINDOW bytes in flight per direction; the
// receiver returns CREDIT once bytes are handed on. That keeps every queue
// small, so one big download can't make other connections lag.
// ---------------------------------------------------------------------------
const T_OPEN = 1, T_DATA = 2, T_CLOSE = 3, T_CREDIT = 4;
const WINDOW = 512 * 1024;
const MAX_STREAMS = 1024;

function frame(type, id, payload = Buffer.alloc(0)) {
  const b = Buffer.allocUnsafe(5 + payload.length);
  b[0] = type;
  b.writeUInt32BE(id, 1);
  payload.copy(b, 5);
  return b;
}
const u32 = (n) => {
  const b = Buffer.allocUnsafe(4);
  b.writeUInt32BE(n, 0);
  return b;
};

function parseTarget(s) {
  const m = s.match(/^\[([^\]]+)\]:(\d+)$/) || s.match(/^([^:]+):(\d+)$/);
  return m ? { host: m[1], port: Number(m[2]) } : { host: "", port: 0 };
}

// Overall limit across ALL streams, so many parallel downloads can't queue up
// seconds of data in front of a slow link. It tracks how fast the page is
// actually taking data and allows ~TARGET_QUEUE_S worth of it in flight.
const TARGET_QUEUE_S = 0.15;
const CHUNK = 64 * 1024; // the most a TCP socket hands us in one read
const SMALL_STREAM = 256 * 1024; // below this, a stream counts as interactive
const MIN_TOTAL = 256 * 1024;
const MAX_TOTAL = 8 * 1024 * 1024;

function handleMux(ws) {
  const streams = new Map();
  const paused = []; // streams waiting for room, resumed round-robin
  let total = 0; // bytes sent to the page and not yet credited, all streams
  let limit = 1024 * 1024;
  let delivered = 0; // credited bytes since the last rate sample
  let rate = 0; // smoothed bytes/second the page is consuming

  const send = (b) => {
    if (ws.readyState === ws.OPEN) ws.send(b);
  };

  const sampler = setInterval(() => {
    const busy = total > 0 || paused.length > 0;
    const sample = delivered / 0.25;
    delivered = 0;
    if (!busy) return; // idle: keep the last estimate
    rate = rate ? 0.7 * rate + 0.3 * sample : sample;
    limit = Math.min(MAX_TOTAL, Math.max(MIN_TOTAL, rate * TARGET_QUEUE_S));
    resumeSome();
  }, 250);

  function pauseStream(s) {
    if (s.paused) return;
    s.paused = true;
    s.sock.pause();
    paused.push(s);
  }

  // A resumed socket can deliver up to one CHUNK before we can pause it again,
  // so only resume as many streams as there is room for a full chunk each.
  let armed = 0; // resumed streams that haven't delivered their next chunk yet
  function resumeSome() {
    for (let k = paused.length; k > 0 && total + (armed + 1) * CHUNK <= limit; k--) {
      const s = paused.shift();
      if (streams.get(s.id) !== s || !s.sock) continue; // closed meanwhile
      if (s.inflight + CHUNK <= WINDOW) {
        s.paused = false;
        s.armed = true;
        armed++;
        s.sock.resume();
      } else {
        paused.push(s); // still over its own window; check again later
      }
    }
  }
  function disarm(s) {
    if (s.armed) {
      s.armed = false;
      armed--;
    }
  }

  const closeStream = (id, reason, notify) => {
    const s = streams.get(id);
    if (!s) return;
    streams.delete(id);
    active--;
    disarm(s);
    total -= s.inflight; // credits for this stream will never come back
    s.inflight = 0;
    s.sock?.destroy();
    if (notify) send(frame(T_CLOSE, id, Buffer.from(reason || "")));
    resumeSome();
  };

  const writeToTarget = (id, s, payload) => {
    // Credit goes back only once the bytes are handed to the target's socket.
    s.sock.write(payload, () => {
      if (streams.get(id) === s) send(frame(T_CREDIT, id, u32(payload.length)));
    });
  };

  async function openStream(id, target) {
    const s = { id, sock: null, pending: [], inflight: 0, sent: 0, paused: false, armed: false };
    streams.set(id, s);
    active++;
    const { host, port } = parseTarget(target);
    let address;
    try {
      address = await resolveTarget(host, port);
    } catch (e) {
      return closeStream(id, e.message, true);
    }
    if (streams.get(id) !== s) return; // closed while resolving

    const sock = net.connect({ host: address, port });
    s.sock = sock;
    sock.setNoDelay(true);
    sock.setTimeout(10 * 60 * 1000, () => sock.destroy());
    for (const p of s.pending) writeToTarget(id, s, p);
    s.pending = null;

    sock.on("data", (chunk) => {
      disarm(s);
      s.inflight += chunk.length;
      s.sent += chunk.length;
      total += chunk.length;
      send(frame(T_DATA, id, chunk));
      // Pause at this stream's own window, or, for bulk transfers only, when
      // there isn't room for another full chunk in the overall budget.
      // Small/new connections (page loads, clicks) skip the overall budget, so
      // they never wait behind big downloads.
      const bulk = s.sent > SMALL_STREAM;
      if (s.inflight + CHUNK > WINDOW || (bulk && total + (armed + 1) * CHUNK > limit)) pauseStream(s);
    });
    sock.on("error", (e) => closeStream(id, e.code || "connection error", true));
    sock.on("close", () => closeStream(id, "closed", true));
  }

  ws.on("message", (data) => {
    const b = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (b.length < 5) return;
    const type = b[0];
    const id = b.readUInt32BE(1);
    const payload = b.subarray(5);
    const s = streams.get(id);

    if (type === T_OPEN) {
      if (s || streams.size >= MAX_STREAMS) return send(frame(T_CLOSE, id, Buffer.from("too many streams")));
      openStream(id, payload.toString());
    } else if (type === T_DATA && s) {
      if (s.sock) writeToTarget(id, s, payload);
      else s.pending.push(payload); // still connecting
    } else if (type === T_CREDIT && s && payload.length >= 4) {
      const n = Math.min(payload.readUInt32BE(0), s.inflight);
      s.inflight -= n;
      total -= n;
      delivered += n;
      resumeSome();
    } else if (type === T_CLOSE && s) {
      closeStream(id, "", false);
    }
  });

  ws.on("close", () => {
    clearInterval(sampler);
    for (const id of [...streams.keys()]) closeStream(id, "", false);
  });
}

// ---------------------------------------------------------------------------
// Original protocol (one WebSocket per TCP connection), kept so older copies
// of the page still work.
// ---------------------------------------------------------------------------
wss.on("connection", async (ws, req) => {
  const url = new URL(req.url, "http://relay");
  if (url.searchParams.get("mux") === "1") return handleMux(ws);
  const host = url.searchParams.get("host");
  const port = Number(url.searchParams.get("port"));

  if (!tokenOk(url.searchParams.get("token"))) return ws.close(4001, "bad token");
  if (!host || !Number.isInteger(port) || port < 1 || port > 65535 || BLOCKED_PORTS.has(port))
    return ws.close(4002, "bad target");

  let address;
  try {
    const r = await dns.lookup(host);
    const allowPrivate = process.env.ALLOW_PRIVATE === "1"; // local testing only
    if (!allowPrivate && blocked.check(r.address, r.family === 6 ? "ipv6" : "ipv4"))
      return ws.close(4003, "private address");
    address = r.address;
  } catch {
    return ws.close(4004, "dns failed");
  }

  active++;
  const sock = net.connect({ host: address, port });
  sock.setNoDelay(true);
  sock.setTimeout(10 * 60 * 1000, () => sock.destroy());

  let closed = false;
  const shutdown = () => {
    if (closed) return;
    closed = true;
    active--;
    sock.destroy();
    if (ws.readyState === ws.OPEN) ws.close(1000);
  };

  // Browser → target (with backpressure).
  ws.on("message", (data) => {
    if (!sock.write(data)) ws._socket?.pause();
  });
  sock.on("drain", () => ws._socket?.resume());

  // Target → browser (with backpressure).
  sock.on("data", (chunk) => {
    ws.send(chunk, () => {
      if (ws.bufferedAmount < 1 << 20) sock.resume();
    });
    if (ws.bufferedAmount > 4 << 20) sock.pause();
  });

  // The first frame from us tells the tab the TCP connection is up.
  sock.once("connect", () => ws.send(Buffer.alloc(0)));

  sock.on("end", shutdown);
  sock.on("error", shutdown);
  sock.on("close", shutdown);
  ws.on("close", shutdown);
  ws.on("error", shutdown);
});

// Keep idle WebSockets alive through proxies like Codespaces port forwarding.
setInterval(() => {
  for (const ws of wss.clients) if (ws.readyState === ws.OPEN) ws.ping();
}, 25_000);

server.listen(PORT, () => console.log(`relay listening on :${PORT}`));
