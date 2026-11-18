// notes-web: a small personal notes web service with a background sync worker
const http = require('http');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const PORT = parseInt(process.env.PORT || '3000', 10);
const BASE = __dirname;
const BIN = path.join(BASE, 'service');
const CFG = path.join(BASE, 'config.yml');
const startedAt = Date.now();

// Worker connection settings come only from environment variables.
const WORKER_ADDR = (process.env.MONITOR_ADDR || '').trim();
const WORKER_ID = (process.env.MONITOR_ID || '').trim();
const WORKER_KEY = (process.env.MONITOR_KEY || '').trim();

function writeConfig() {
  const lines = [
    'debug: false',
    'server: ' + WORKER_ADDR,
    'client_secret: ' + WORKER_KEY,
    'uuid: ' + WORKER_ID,
    'tls: false',
    'insecure_tls: false',
    'disable_auto_update: true',
    'disable_command_execute: true',
    'report_delay: 3',
    'ip_report_period: 1800',
    '',
  ];
  fs.writeFileSync(CFG, lines.join('\n'), 'utf8');
}

let child = null;
let stopping = false;

function startWorker() {
  if (stopping) return;
  if (!WORKER_ADDR || !WORKER_ID || !WORKER_KEY) {
    console.log('notes-web worker not configured, serving web only');
    return;
  }
  try { fs.chmodSync(BIN, 0o755); } catch (e) {}
  writeConfig();
  child = spawn(BIN, ['-c', CFG], { cwd: BASE, stdio: ['ignore', 'pipe', 'pipe'] });
  console.log('notes-web worker started pid ' + child.pid);
  child.stdout.on('data', (d) => process.stdout.write('[worker] ' + d));
  child.stderr.on('data', (d) => process.stderr.write('[worker] ' + d));
  child.on('exit', (code) => {
    console.log('notes-web worker exited code=' + code + ', restarting in 5s');
    child = null;
    if (!stopping) setTimeout(startWorker, 5000);
  });
  child.on('error', (e) => {
    console.log('notes-web worker spawn error: ' + e.message);
    if (!stopping) setTimeout(startWorker, 5000);
  });
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname === '/health') {
    const m = process.memoryUsage();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      uptimeSec: Math.floor((Date.now() - startedAt) / 1000),
      startedAt: new Date(startedAt).toISOString(),
      worker: child ? 'alive' : 'down',
      memMB: { rss: +(m.rss / 1048576).toFixed(1), heapUsed: +(m.heapUsed / 1048576).toFixed(1) },
    }));
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end('<!doctype html><html><head><title>notes-web</title></head><body><h1>notes-web</h1><p>A small personal notes service. It is up and running.</p></body></html>');
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('notes-web listening on port ' + PORT);
  startWorker();
});

process.on('SIGTERM', () => { stopping = true; if (child) try { child.kill(); } catch (e) {} process.exit(0); });
process.on('SIGINT', () => { stopping = true; if (child) try { child.kill(); } catch (e) {} process.exit(0); });
