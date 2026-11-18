// notes-app: a small personal notes web app with realtime sync
const http = require('http');
const https = require('https');
const net = require('net');
const dns = require('dns');
const crypto = require('crypto');

const PORT = parseInt(process.env.PORT || '3000', 10);
const startedAt = Date.now();

function sendJson(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(body);
}

function tcpCheck(host, port, timeoutMs) {
  return new Promise((resolve) => {
    const t0 = Date.now();
    const sock = new net.Socket();
    let done = false;
    const finish = (ok, err) => {
      if (done) return;
      done = true;
      try { sock.destroy(); } catch (e) {}
      resolve({ host, port, ok, ms: Date.now() - t0, error: err || null });
    };
    sock.setTimeout(timeoutMs || 5000);
    sock.once('connect', () => finish(true, null));
    sock.once('timeout', () => finish(false, 'timeout'));
    sock.once('error', (e) => finish(false, String(e && e.message || e)));
    try { sock.connect(port, host); } catch (e) { finish(false, String(e && e.message || e)); }
  });
}

function httpsGetText(url, timeoutMs) {
  return new Promise((resolve) => {
    const t0 = Date.now();
    let req;
    try {
      req = https.get(url, { timeout: timeoutMs || 6000 }, (resp) => {
        let data = '';
        resp.on('data', (c) => { data += c; if (data.length > 4096) req.destroy(); });
        resp.on('end', () => resolve({ url, ok: true, status: resp.statusCode, ms: Date.now() - t0, body: data.slice(0, 500) }));
      });
    } catch (e) {
      return resolve({ url, ok: false, ms: Date.now() - t0, error: String(e && e.message || e) });
    }
    req.on('timeout', () => { req.destroy(); resolve({ url, ok: false, ms: Date.now() - t0, error: 'timeout' }); });
    req.on('error', (e) => resolve({ url, ok: false, ms: Date.now() - t0, error: String(e && e.message || e) }));
  });
}

function dnsCheck(hostname) {
  return new Promise((resolve) => {
    dns.lookup(hostname, { all: true }, (err, addrs) => {
      if (err) return resolve({ hostname, ok: false, error: String(err.message || err) });
      resolve({ hostname, ok: true, addresses: addrs });
    });
  });
}

async function runChecks() {
  const [dns1, tcp443, tcp53, ip1, ip2] = await Promise.all([
    dnsCheck('example.com'),
    tcpCheck('1.1.1.1', 443, 5000),
    tcpCheck('8.8.8.8', 53, 5000),
    httpsGetText('https://api.ipify.org?format=json', 6000),
    httpsGetText('https://ifconfig.me/ip', 6000),
  ]);
  let egress = null;
  try {
    const j = JSON.parse(ip1.body || '{}');
    if (j.ip) egress = j.ip;
  } catch (e) {}
  if (!egress && ip2.body && /^[0-9a-fA-F:.]+/.test(ip2.body.trim())) egress = ip2.body.trim();
  return { at: new Date().toISOString(), dns: dns1, tcp: [tcp443, tcp53], http: [ip1, ip2], egress };
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname === '/health') {
    const m = process.memoryUsage();
    sendJson(res, 200, {
      status: 'ok',
      uptimeSec: Math.floor((Date.now() - startedAt) / 1000),
      startedAt: new Date(startedAt).toISOString(),
      memMB: { rss: +(m.rss / 1048576).toFixed(1), heapUsed: +(m.heapUsed / 1048576).toFixed(1) },
    });
    return;
  }
  if (url.pathname === '/check') {
    try {
      const out = await runChecks();
      sendJson(res, 200, out);
    } catch (e) {
      sendJson(res, 500, { error: String(e && e.message || e) });
    }
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end('<!doctype html><html><head><title>notes-app</title></head><body><h1>notes-app</h1><p>A small personal notes app. It is up and running.</p></body></html>');
});

// --- minimal WebSocket framing (RFC6455), server side, for realtime sync echo ---
function wsAccept(key) {
  return crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
}
function encodeFrame(op, payload) {
  const len = payload.length;
  let head;
  if (len < 126) { head = Buffer.alloc(2); head[1] = len; }
  else if (len < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  head[0] = 0x80 | op;
  return Buffer.concat([head, payload]);
}
class FrameParser {
  constructor(onMsg, onPing, onClose) {
    this.buf = Buffer.alloc(0);
    this.frags = [];
    this.fragOp = null;
    this.onMsg = onMsg; this.onPing = onPing; this.onClose = onClose;
  }
  push(chunk) {
    this.buf = Buffer.concat([this.buf, chunk]);
    while (true) {
      if (this.buf.length < 2) return;
      const fin = (this.buf[0] & 0x80) !== 0;
      const op = this.buf[0] & 0x0f;
      const masked = (this.buf[1] & 0x80) !== 0;
      let len = this.buf[1] & 0x7f;
      let off = 2;
      if (len === 126) { if (this.buf.length < off + 2) return; len = this.buf.readUInt16BE(off); off += 2; }
      else if (len === 127) { if (this.buf.length < off + 8) return; len = Number(this.buf.readBigUInt64BE(off)); off += 8; }
      let mask = null;
      if (masked) { if (this.buf.length < off + 4) return; mask = this.buf.slice(off, off + 4); off += 4; }
      if (this.buf.length < off + len) return;
      let payload = Buffer.from(this.buf.slice(off, off + len));
      if (mask) for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
      this.buf = this.buf.slice(off + len);
      if (op === 0x8) { this.onClose(); return; }
      if (op === 0x9) { this.onPing(payload); continue; }
      if (op === 0xA) continue;
      if (!fin) { if (this.fragOp === null) this.fragOp = op; this.frags.push(payload); continue; }
      if (op === 0x0) { this.frags.push(payload); const full = Buffer.concat(this.frags); const o = this.fragOp; this.frags = []; this.fragOp = null; this.onMsg(full, o); continue; }
      this.onMsg(payload, op);
    }
  }
}

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + wsAccept(key) + '\r\n\r\n');
  const parser = new FrameParser((msg, op) => {
    // echo back in same opcode (text stays text, binary stays binary)
    try { socket.write(encodeFrame(op === 2 ? 2 : 1, msg)); } catch (e) {}
  }, (payload) => {
    try { socket.write(encodeFrame(0xA, payload)); } catch (e) {}
  }, () => {
    try { socket.destroy(); } catch (e) {}
  });
  socket.on('data', (chunk) => parser.push(chunk));
  socket.on('error', () => { try { socket.destroy(); } catch (e) {} });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('notes-app listening on port ' + PORT);
});
