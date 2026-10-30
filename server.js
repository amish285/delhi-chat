const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const REGIONS = new Set(['delhi-ncr', 'india', 'world']);

const REPORTS_TO_BAN = 2;
const REPORT_WINDOW_MS = 60 * 60 * 1000;
const BAN_MS = 24 * 60 * 60 * 1000;
const MSG_LIMIT = 150;
const MSG_WINDOW_MS = 10 * 1000;

const app = express();
app.set('trust proxy', 1);
app.use(express.static(path.join(__dirname, 'public')));
app.get('/healthz', (_req, res) => res.send('ok'));

app.get('/config', (_req, res) => {
  const iceServers = [{ urls: 'stun:stun.l.google.com:19302' }];
  if (process.env.TURN_URLS) {
    iceServers.push({
      urls: process.env.TURN_URLS.split(',').map((s) => s.trim()),
      username: process.env.TURN_USERNAME || '',
      credential: process.env.TURN_CREDENTIAL || '',
    });
  }
  res.json({ iceServers });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 });

const queues = new Map();
const bans = new Map();
const reportLog = new Map();

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function getIp(req) {
  const xff = req.headers['x-forwarded-for'];
  if (xff) return xff.split(',')[0].trim();
  return req.socket.remoteAddress || 'unknown';
}

function isBanned(ip) {
  const until = bans.get(ip);
  if (!until) return false;
  if (Date.now() > until) {
    bans.delete(ip);
    return false;
  }
  return true;
}

function unqueue(c) {
  const q = queues.get(c.region);
  if (!q) return;
  const i = q.indexOf(c);
  if (i > -1) q.splice(i, 1);
}

function unpair(c, notify = true) {
  const p = c.partner;
  if (!p) return;
  c.partner = null;
  p.partner = null;
  if (notify) send(p.ws, { type: 'partner-left' });
}

function enqueue(c) {
  unqueue(c);
  if (!c.region) return;
  let q = queues.get(c.region);
  if (!q) {
    q = [];
    queues.set(c.region, q);
  }
  const i = q.findIndex(
    (o) => o !== c && !c.blocked.has(o.ip) && !o.blocked.has(c.ip)
  );
  if (i === -1) {
    q.push(c);
    send(c.ws, { type: 'waiting' });
    return;
  }
  const other = q.splice(i, 1)[0];
  c.partner = other;
  other.partner = c;
  send(c.ws, { type: 'matched', initiator: true });
  send(other.ws, { type: 'matched', initiator: false });
}

function recordReport(reportedIp, reporterIp) {
  const now = Date.now();
  let m = reportLog.get(reportedIp);
  if (!m) {
    m = new Map();
    reportLog.set(reportedIp, m);
  }
  m.set(reporterIp, now);
  for (const [ip, t] of m) if (now - t > REPORT_WINDOW_MS) m.delete(ip);
  if (m.size >= REPORTS_TO_BAN) {
    bans.set(reportedIp, now + BAN_MS);
    reportLog.delete(reportedIp);
    for (const ws of wss.clients) {
      if (ws._client && ws._client.ip === reportedIp) {
        send(ws, { type: 'banned' });
        ws.close();
      }
    }
  }
}

wss.on('connection', (ws, req) => {
  const ip = getIp(req);
  if (isBanned(ip)) {
    send(ws, { type: 'banned' });
    ws.close();
    return;
  }

  const c = {
    ws,
    ip,
    region: null,
    partner: null,
    blocked: new Set(),
    msgCount: 0,
    windowStart: Date.now(),
  };
  ws._client = c;
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));

  ws.on('message', (raw) => {
    const now = Date.now();
    if (now - c.windowStart > MSG_WINDOW_MS) {
      c.windowStart = now;
      c.msgCount = 0;
    }
    if (++c.msgCount > MSG_LIMIT) return;

    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    switch (msg.type) {
      case 'join': {
        if (msg.ageConfirmed !== true) {
          send(ws, { type: 'error', message: 'You must confirm you are 18+.' });
          return;
        }
        if (!REGIONS.has(msg.region)) return;
        unpair(c);
        unqueue(c);
        c.region = msg.region;
        enqueue(c);
        break;
      }
      case 'signal': {
        if (c.partner && msg.data) send(c.partner.ws, { type: 'signal', data: msg.data });
        break;
      }
      case 'next': {
        if (!c.region) return;
        unpair(c);
        enqueue(c);
        break;
      }
      case 'report': {
        const p = c.partner;
        if (p) {
          c.blocked.add(p.ip);
          recordReport(p.ip, c.ip);
          unpair(c);
        }
        if (c.region && ws.readyState === 1) enqueue(c);
        break;
      }
      case 'stop': {
        unpair(c);
        unqueue(c);
        break;
      }
    }
  });

  ws.on('close', () => {
    unpair(c);
    unqueue(c);
  });
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => console.log(`Listening on port ${PORT}`));
