const http = require('http');
const fs   = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;

// ── STATIC ───────────────────────────────────────────────────
const httpServer = http.createServer((req, res) => {
  let file = 'index.html';
  if (req.url === '/sitemap.xml') file = 'sitemap.xml';
  else if (req.url === '/robots.txt') file = 'robots.txt';
  const fp = path.join(__dirname, file);
  fs.readFile(fp, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    const mime = fp.endsWith('.xml') ? 'application/xml'
               : fp.endsWith('.txt') ? 'text/plain' : 'text/html';
    res.writeHead(200, { 'Content-Type': mime, 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});

// ── WEBSOCKET (raw, no socket.io — ultra-fast) ────────────────
const { WebSocketServer } = require('ws');
const wss = new WebSocketServer({ server: httpServer });

// rooms: { code: { p1: ws, p2: ws, state: {...} } }
const rooms = new Map();

function genCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let c = '';
  for (let i = 0; i < 6; i++) c += chars[Math.floor(Math.random() * chars.length)];
  return c;
}

function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function broadcast(room, obj, except) {
  const msg = JSON.stringify(obj);
  if (room.p1 && room.p1 !== except && room.p1.readyState === 1) room.p1.send(msg);
  if (room.p2 && room.p2 !== except && room.p2.readyState === 1) room.p2.send(msg);
}

wss.on('connection', ws => {
  ws.roomCode = null;
  ws.role     = null; // 'p1' or 'p2'

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'create') {
      // Generate unique code
      let code;
      do { code = genCode(); } while (rooms.has(code));
      const room = { p1: ws, p2: null };
      rooms.set(code, room);
      ws.roomCode = code;
      ws.role = 'p1';
      send(ws, { type: 'created', code });
    }

    else if (msg.type === 'join') {
      const code = (msg.code || '').toUpperCase().trim();
      const room = rooms.get(code);
      if (!room) { send(ws, { type: 'error', msg: 'Room not found. Check the code.' }); return; }
      if (room.p2)  { send(ws, { type: 'error', msg: 'Room is full.' }); return; }
      room.p2 = ws;
      ws.roomCode = code;
      ws.role = 'p2';
      // Tell p1 someone joined — game can start
      send(room.p1, { type: 'start', role: 'p1' });
      send(room.p2, { type: 'start', role: 'p2' });
    }

    else if (msg.type === 'state') {
      // Player sends their game state — relay to partner instantly
      const room = rooms.get(ws.roomCode);
      if (!room) return;
      broadcast(room, { type: 'state', role: ws.role, data: msg.data }, ws);
    }

    else if (msg.type === 'event') {
      // Relay game events (kill, loot, chest open etc)
      const room = rooms.get(ws.roomCode);
      if (!room) return;
      broadcast(room, { type: 'event', role: ws.role, ev: msg.ev }, ws);
    }
  });

  ws.on('close', () => {
    if (!ws.roomCode) return;
    const room = rooms.get(ws.roomCode);
    if (!room) return;
    // Notify partner
    broadcast(room, { type: 'partner_left' }, ws);
    rooms.delete(ws.roomCode);
  });

  ws.on('error', () => {});
});

httpServer.listen(PORT, () => console.log('🐼 Panda Wars running on port ' + PORT));
