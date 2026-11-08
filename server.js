// Server realtime RPG (WebSocket). Jalankan: npm i ws && node server.js
// Fitur: pemain lain, host monster (pemain dengan id terkecil), sinkron pohon/batu, chat, trade.
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

let nextId = 1;
let hostId = -1;
const players = new Map();      // id -> {ws, name, skin, x, y, f, a, dirty}
const deadNodes = new Map();    // "x,y" -> waktu (ms epoch) pohon/batu hidup lagi
const requests = new Map();     // idPengaju -> {to, at}  permintaan trade yang menunggu jawaban
const REQ_TTL = 30000;

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


// ---------------- Trade ----------------
// Dua fase supaya tidak ada barang hilang/ganda:
//  1) kedua pihak menekan "setuju" -> server kirim tr_go (tawaran lawan)
//  2) tiap klien menahan barangnya lalu kirim tr_ready -> setelah keduanya siap server kirim tr_fin
// Batal kapan saja -> tr_end(ok:false); klien yang sudah menahan barang mengembalikannya.
const cleanOffer = (items, coins) => {
  const out = [];
  if (Array.isArray(items)) {
    const seen = new Set();
    for (const it of items) {
      if (!Array.isArray(it) || out.length >= 8) break;
      const id = it[0] | 0, n = it[1] | 0;
      if (id < 1 || id > 63 || n < 1 || n > 999 || seen.has(id)) continue;
      seen.add(id);
      out.push([id, n]);
    }
  }
  let c = Math.floor(+coins || 0);
  if (!(c >= 0)) c = 0;
  return { items: out, coins: Math.min(c, 1000000000) };
};

const tradeOf = (id) => { const p = players.get(id); return p ? p.trade : null; };
const partnerId = (t, id) => (t.a === id ? t.b : t.a);
const side = (t, id) => (t.a === id ? t.sa : t.sb);

function endTrade(t, why, exceptId) {
  for (const pid of [t.a, t.b]) {
    const p = players.get(pid);
    if (!p) continue;
    p.trade = null;
    if (pid !== exceptId) send(p.ws, { t: "tr_end", ok: false, why });
  }
}

function sendOks(t) {
  const A = players.get(t.a), B = players.get(t.b);
  if (A) send(A.ws, { t: "tr_ok", me: t.sa.ok, them: t.sb.ok });
  if (B) send(B.ws, { t: "tr_ok", me: t.sb.ok, them: t.sa.ok });
}

function handleTrade(id, me, m) {
  switch (m.t) {
    case "tr_req": {
      const to = m.to | 0, target = players.get(to);
      if (!target || to === id) return send(me.ws, { t: "tr_no", why: "gone" });
      if (me.trade || target.trade) return send(me.ws, { t: "tr_no", why: "busy" });
      for (const [from, r] of requests) if (from === id || r.to === to || (from === to && r.to === id)) {
        if (Date.now() - r.at < REQ_TTL) return send(me.ws, { t: "tr_no", why: "busy" });
        requests.delete(from);
      }
      requests.set(id, { to, at: Date.now() });
      send(target.ws, { t: "tr_req", from: id, name: me.name });
      break;
    }
    case "tr_ans": {                    // jawaban untuk permintaan dari "from"
      const from = m.from | 0, r = requests.get(from);
      if (!r || r.to !== id) return;
      requests.delete(from);
      const asker = players.get(from);
      if (!asker) return;
      if (!m.ok) return send(asker.ws, { t: "tr_no", why: "declined" });
      if (me.trade || asker.trade) return send(me.ws, { t: "tr_no", why: "busy" });
      const t = { a: from, b: id, sa: { items: [], coins: 0, ok: false, ready: false },
                  sb: { items: [], coins: 0, ok: false, ready: false }, phase: "open" };
      asker.trade = me.trade = t;
      send(asker.ws, { t: "tr_open", with: id, name: me.name });
      send(me.ws, { t: "tr_open", with: from, name: asker.name });
      break;
    }
    case "tr_offer": {
      const t = me.trade;
      if (!t || t.phase !== "open") return;
      const o = cleanOffer(m.items, m.coins), s = side(t, id);
      s.items = o.items; s.coins = o.coins;
      t.sa.ok = t.sb.ok = false;
      const other = players.get(partnerId(t, id));
      if (other) send(other.ws, { t: "tr_offer", items: o.items, coins: o.coins });
      sendOks(t);
      break;
    }
    case "tr_ok": {
      const t = me.trade;
      if (!t || t.phase !== "open") return;
      side(t, id).ok = !!m.v;
      sendOks(t);
      if (t.sa.ok && t.sb.ok) {
        t.phase = "commit";
        const A = players.get(t.a), B = players.get(t.b);
        if (A) send(A.ws, { t: "tr_go", items: t.sb.items, coins: t.sb.coins });
        if (B) send(B.ws, { t: "tr_go", items: t.sa.items, coins: t.sa.coins });
      }
      break;
    }
    case "tr_ready": {
      const t = me.trade;
      if (!t || t.phase !== "commit") return;
      side(t, id).ready = true;
      if (t.sa.ready && t.sb.ready) {
        for (const pid of [t.a, t.b]) {
          const p = players.get(pid);
          if (p) { p.trade = null; send(p.ws, { t: "tr_fin" }); }
        }
      }
      break;
    }
    case "tr_cancel": {
      const t = me.trade;
      if (t) endTrade(t, "cancel", id);
      else {                            // membatalkan permintaan yang belum dijawab
        const r = requests.get(id);
        if (r) { requests.delete(id); const o = players.get(r.to); if (o) send(o.ws, { t: "tr_gone", from: id }); }
      }
      send(me.ws, { t: "tr_end", ok: false, why: "cancel", self: true });
      break;
    }
  }
}

function dropPlayer(id) {
  const me = players.get(id);
  if (me && me.trade) endTrade(me.trade, "left", id);
  for (const [from, r] of requests) {
    if (from === id) { const o = players.get(r.to); if (o) send(o.ws, { t: "tr_gone", from: id }); requests.delete(from); }
    else if (r.to === id) { const o = players.get(from); if (o) send(o.ws, { t: "tr_no", why: "gone" }); requests.delete(from); }
  }
}

wss.on("connection", (ws) => {
  let id = -1;

  ws.on("message", (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    if (m.t === "join" && id < 0) {
      id = nextId++;
      const name = String(m.name || "?").slice(0, 14);
      players.set(id, { ws, name, skin: m.skin | 0, x: +m.x || 0, y: +m.y || 0, f: 0, a: 0, dirty: false, trade: null, lastChat: 0 });
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
      case "chat": {
        const now = Date.now();
        if (now - me.lastChat < 600) break;                       // batas kecepatan
        const text = String(m.m || "").replace(/[^\x20-\x7E]/g, "").trim().slice(0, 80);
        if (!text) break;
        me.lastChat = now;
        broadcast({ t: "chat", id, name: me.name, m: text }, id);
        break;
      }
      case "tr_req": case "tr_ans": case "tr_offer": case "tr_ok": case "tr_ready": case "tr_cancel":
        handleTrade(id, me, m);
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
    dropPlayer(id);
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

setInterval(() => {
  const now = Date.now();
  for (const [from, r] of requests) if (now - r.at > REQ_TTL) {
    requests.delete(from);
    const a = players.get(from), b = players.get(r.to);
    if (a) send(a.ws, { t: "tr_no", why: "timeout" });
    if (b) send(b.ws, { t: "tr_gone", from });
  }
}, 5000);

console.log("Server jalan di port " + PORT);
