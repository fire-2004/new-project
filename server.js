// Server realtime sederhana: menyiarkan posisi, nama, dan skin pemain dalam satu dunia.
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const MAX_PLAYERS = 30;
const TICK_MS = 100;          // siaran posisi 10x per detik
const WORLD_MAX = 100000;     // batas koordinat (anti nilai aneh)

const server = http.createServer((req, res) => {
  // Endpoint cek hidup (health check)
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("RPG server OK. Pemain online: " + players.size);
});

const wss = new WebSocketServer({ server, maxPayload: 1024 });
const players = new Map();    // id -> { id, name, skin, x, y, f, a, ws, dirty, lastMsg }
let nextId = 1;

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function broadcast(obj, exceptId) {
  const msg = JSON.stringify(obj);
  for (const p of players.values()) {
    if (p.id !== exceptId && p.ws.readyState === 1) p.ws.send(msg);
  }
}

function cleanName(s) {
  s = String(s || "").replace(/[^A-Za-z0-9_]/g, "").slice(0, 14);
  return s || "Petualang";
}

function num(v) {
  v = Number(v);
  if (!Number.isFinite(v)) return 0;
  return Math.max(-WORLD_MAX, Math.min(WORLD_MAX, v));
}

wss.on("connection", (ws) => {
  if (players.size >= MAX_PLAYERS) {
    send(ws, { t: "full" });
    ws.close();
    return;
  }

  const me = { id: nextId++, name: "Petualang", skin: 0, x: 0, y: 0, f: 0, a: 0,
               ws, dirty: false, lastMsg: 0, joined: false };
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (data) => {
    let m;
    try { m = JSON.parse(data.toString()); } catch (e) { return; }
    if (!m || typeof m.t !== "string") return;

    if (m.t === "join" && !me.joined) {
      me.name = cleanName(m.name);
      me.skin = Math.max(0, Math.min(20, Math.floor(num(m.skin))));
      me.x = num(m.x); me.y = num(m.y);
      me.joined = true;
      players.set(me.id, me);
      // Kirim daftar pemain yang sudah ada ke pemain baru
      const others = [];
      for (const p of players.values()) {
        if (p.id !== me.id) others.push({ id: p.id, name: p.name, skin: p.skin, x: p.x, y: p.y, f: p.f, a: p.a });
      }
      send(ws, { t: "welcome", id: me.id, players: others });
      broadcast({ t: "join", id: me.id, name: me.name, skin: me.skin, x: me.x, y: me.y }, me.id);
    } else if (m.t === "pos" && me.joined) {
      const now = Date.now();
      if (now - me.lastMsg < 40) return;   // maks ~25 pesan/detik per pemain
      me.lastMsg = now;
      me.x = num(m.x); me.y = num(m.y);
      me.f = Math.max(0, Math.min(7, Math.floor(num(m.f))));
      me.a = Math.max(0, Math.min(7, Math.floor(num(m.a))));
      me.dirty = true;
    } else if (m.t === "skin" && me.joined) {
      me.skin = Math.max(0, Math.min(20, Math.floor(num(m.skin))));
      broadcast({ t: "skin", id: me.id, skin: me.skin }, me.id);
    }
  });

  ws.on("close", () => {
    if (me.joined) {
      players.delete(me.id);
      broadcast({ t: "leave", id: me.id });
    }
  });
  ws.on("error", () => {});
});

// Siaran posisi: hanya pemain yang bergerak sejak tick terakhir
setInterval(() => {
  const changed = [];
  for (const p of players.values()) {
    if (p.dirty) { changed.push([p.id, Math.round(p.x * 10) / 10, Math.round(p.y * 10) / 10, p.f, p.a]); p.dirty = false; }
  }
  if (changed.length) broadcast({ t: "state", p: changed });
}, TICK_MS);

// Putuskan koneksi mati (tidak membalas ping)
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => console.log("RPG server jalan di port " + PORT));
