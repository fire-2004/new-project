// Server realtime RPG (WebSocket). Jalankan: npm i ws && node server.js
// Fitur: pemain lain, host monster (pemain dengan id terkecil), sinkron pohon/batu.
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

let nextId = 1;
let hostId = -1;
const players = new Map();      // id -> {ws, name, skin, x, y, f, a, dirty}
const deadNodes = new Map();    // "x,y" -> waktu (ms epoch) pohon/batu hidup lagi

const send = (ws, obj) => { if (ws.readyState === 1) ws.send(typeof obj === "string" ? obj : JSON.stringify(obj)); };
const broadcast = (obj, exceptId) => {
  const s = JSON.stringify(obj);
  for (const [id, p] of players) if (id !== exceptId) send(p.ws, s);
};

function pickHost() {
  let best = -1;
  for (const id of players.keys()) if (best < 0 || id < best) best = id;
  if (best !== hostId) {
    hostId = best;
    if (hostId >= 0) broadcast({ t: "host", id: hostId });
  }
}

function aliveDeadNodes() {
  const now = Date.now(), out = [];
  for (const [k, until] of deadNodes) {
    if (until <= now) { deadNodes.delete(k); continue; }
    const [x, y] = k.split(",").map(Number);
    out.push([x, y, until - now]);
  }
  return out;
}

wss.on("connection", (ws) => {
  let id = -1;

  ws.on("message", (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    if (m.t === "join" && id < 0) {
      id = nextId++;
      const name = String(m.name || "?").slice(0, 14);
      players.set(id, { ws, name, skin: m.skin | 0, x: +m.x || 0, y: +m.y || 0, f: 0, a: 0, dirty: false });
      pickHost();
      const others = [];
      for (const [pid, p] of players) if (pid !== id) others.push({ id: pid, name: p.name, skin: p.skin, x: p.x, y: p.y });
      send(ws, { t: "welcome", you: id, host: hostId, players: others, dead: aliveDeadNodes() });
      broadcast({ t: "join", id, name, skin: m.skin | 0, x: +m.x || 0, y: +m.y || 0 }, id);
      return;
    }
    if (id < 0) return;
    const me = players.get(id);

    switch (m.t) {
      case "pos":
        me.x = +m.x || 0; me.y = +m.y || 0; me.f = m.f | 0; me.a = m.a | 0; me.dirty = true;
        break;
      case "skin":
        me.skin = m.skin | 0;
        broadcast({ t: "skin", id, skin: me.skin }, id);
        break;
      case "nd": {                       // pohon/batu habis
        const x = m.x | 0, y = m.y | 0;
        const ms = Math.max(1000, Math.min(120000, m.ms | 0));
        deadNodes.set(x + "," + y, Date.now() + ms);
        broadcast({ t: "nd", x, y, ms }, id);
        break;
      }
      case "mon":                        // snapshot monster, hanya dari host
        if (id === hostId) broadcast({ t: "mon", m: m.m }, id);
        break;
      case "hit": {                      // pemain non-host memukul monster -> teruskan ke host
        const h = players.get(hostId);
        if (id !== hostId && h) send(h.ws, { t: "hit", m: m.m, d: m.d, x: m.x, y: m.y, from: id });
        break;
      }
      case "kill": {                     // hadiah dari host ke pembunuh
        if (id !== hostId) break;
        const target = players.get(m.to | 0);
        if (target) send(target.ws, { t: "kill", x: m.x, y: m.y, lv: m.lv, el: m.el, xp: m.xp });
        break;
      }
    }
  });

  ws.on("close", () => {
    if (id < 0) return;
    players.delete(id);
    broadcast({ t: "leave", id });
    pickHost();
  });
  ws.on("error", () => {});
});

// Posisi semua pemain 10x/detik (hanya jika ada yang berubah)
setInterval(() => {
  const p = [];
  for (const [id, pl] of players) if (pl.dirty) { p.push([id, pl.x, pl.y, pl.f, pl.a]); pl.dirty = false; }
  if (p.length) broadcast({ t: "state", p });
}, 100);

console.log("Server jalan di port " + PORT);
