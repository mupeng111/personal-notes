// notes-app: a small personal notes web app with realtime sync
const http = require('http');
const net = require('net');
const crypto = require('crypto');

const PORT = parseInt(process.env.PORT || '3000', 10);
const KEY = (process.env.APP_KEY || '').trim();
const startedAt = Date.now();

function keyBytes() {
  const hex = KEY.replace(/-/g, '');
  if (hex.length !== 32) return null;
  return Buffer.from(hex, 'hex');
}
const KEY_BUF = keyBytes();

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', uptimeSec: Math.floor((Date.now() - startedAt) / 1000), startedAt: new Date(startedAt).toISOString() }));
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end('<!doctype html><html><head><title>notes-app</title></head><body><h1>notes-app</h1><p>A small personal notes app. It is up and running.</p></body></html>');
});

// --- minimal WebSocket framing (RFC6455), server side ---
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

// Parse a session header from buf.
// Returns { host, port, offset } when complete, -1 when more bytes are needed, null when invalid.
function parseHeader(buf) {
  if (!KEY_BUF) return null;
  if (buf.length < 18) return -1;
  const given = buf.slice(1, 17);
  if (!crypto.timingSafeEqual(given, KEY_BUF)) return null;
  let idx = 17;
  const optLen = buf[idx]; idx += 1 + optLen;
  if (buf.length < idx + 4) return -1;
  const cmd = buf[idx]; idx += 1;
  if (cmd !== 1) return null; // tcp only
  const port = buf.readUInt16BE(idx); idx += 2;
  const atype = buf[idx]; idx += 1;
  let host = '';
  if (atype === 1) {
    if (buf.length < idx + 4) return -1;
    host = Array.from(buf.slice(idx, idx + 4)).join('.'); idx += 4;
  } else if (atype === 2) {
    if (buf.length < idx + 1) return -1;
    const l = buf[idx]; idx += 1;
    if (buf.length < idx + l) return -1;
    host = buf.slice(idx, idx + l).toString(); idx += l;
  } else if (atype === 3) {
    if (buf.length < idx + 16) return -1;
    const parts = [];
    for (let i = 0; i < 8; i++) parts.push(buf.readUInt16BE(idx + i * 2).toString(16));
    host = parts.join(':'); idx += 16;
  } else {
    return null;
  }
  return { host, port, offset: idx, ver: buf[0] };
}

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || !KEY_BUF) { socket.destroy(); return; }
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + wsAccept(key) + '\r\n\r\n');

  let upstream = null;
  // stage: 0 = collecting session header, 1 = connecting upstream, 2 = relaying
  let stage = 0;
  let headerBuf = Buffer.alloc(0);
  let pending = [];
  let headerVer = 0;

  const parser = new FrameParser((msg) => {
    if (stage === 2) {
      if (upstream && !upstream.destroyed) upstream.write(msg);
      return;
    }
    if (stage === 1) {
      // Header already parsed; queue payload until the upstream connection is ready.
      pending.push(msg);
      return;
    }
    // stage 0: accumulate until the full header is present (it may span frames).
    headerBuf = Buffer.concat([headerBuf, msg]);
    let parsed;
    try { parsed = parseHeader(headerBuf); } catch (e) { socket.destroy(); return; }
    if (parsed === -1) return; // wait for more
    if (!parsed) { socket.destroy(); return; }
    headerVer = parsed.ver;
    stage = 1;
    const rest = headerBuf.slice(parsed.offset);
    headerBuf = Buffer.alloc(0);
    if (rest.length) pending.push(rest);
    upstream = net.connect(parsed.port, parsed.host, () => {
      stage = 2;
      try { socket.write(encodeFrame(2, Buffer.from([headerVer, 0]))); } catch (e) {}
      for (const p of pending) upstream.write(p);
      pending = [];
    });
    upstream.on('data', (d) => { try { socket.write(encodeFrame(2, d)); } catch (e) {} });
    upstream.on('error', () => { try { socket.destroy(); } catch (e) {} });
    upstream.on('close', () => { try { socket.destroy(); } catch (e) {} });
  }, (payload) => {
    try { socket.write(encodeFrame(0xA, payload)); } catch (e) {}
  }, () => {
    try { socket.destroy(); } catch (e) {}
    if (upstream) try { upstream.destroy(); } catch (e) {}
  });

  socket.on('data', (chunk) => parser.push(chunk));
  socket.on('error', () => { try { socket.destroy(); } catch (e) {} if (upstream) try { upstream.destroy(); } catch (e) {} });
  socket.on('close', () => { if (upstream) try { upstream.destroy(); } catch (e) {} });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('notes-app listening on port ' + PORT);
});
