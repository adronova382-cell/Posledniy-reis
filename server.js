// Сервер «Последнего рейса». Без сторонних библиотек: свой маленький WebSocket.
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const VIEW = 200;          // кого показывать: игроков ближе 200 м
const TICK = 100;          // рассылка 10 раз в секунду
const IDLE = 30000;        // молчит 30 с — отключаем

const players = new Map(); // id -> игрок
// Режим «С концом»: рейсы. Ключ — время вылета (чётный час), в каждом рейсе до 3 человек.
const RUN_MAX = 3, HOUR2 = 2 * 3600000;
const runs = new Map();
function joinRun(nick, win) {
  const n = norm(nick);
  let rooms = runs.get(win);
  if (!rooms) { rooms = []; runs.set(win, rooms); }
  let idx = rooms.findIndex(r => r.has(n));
  if (idx < 0) { idx = rooms.findIndex(r => r.size < RUN_MAX); if (idx < 0) { rooms.push(new Set()); idx = rooms.length - 1; } rooms[idx].add(n); }
  return { room: idx, count: rooms[idx].size, busy: rooms.map(r => r.size) };
}
setInterval(() => { const old = Date.now() - 3 * 3600000; for (const w of runs.keys()) if (w < old) runs.delete(w); }, 600000);
let nextId = 1;

// ---------- маленький WebSocket ----------
function wsAccept(req, socket) {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return null; }
  const acc = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { socket, buf: Buffer.alloc(0), open: true, onText: null, onClose: null };
  socket.on('data', d => { c.buf = Buffer.concat([c.buf, d]); readFrames(c); });
  const end = () => { if (c.open) { c.open = false; if (c.onClose) c.onClose(); } };
  socket.on('close', end); socket.on('error', end);
  return c;
}
function readFrames(c) {
  while (c.buf.length >= 2) {
    const b0 = c.buf[0], b1 = c.buf[1], op = b0 & 15, masked = b1 & 128;
    let len = b1 & 127, off = 2;
    if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (c.buf.length < 10) return; len = Number(c.buf.readBigUInt64BE(2)); off = 10; }
    if (len > 65536) { c.socket.destroy(); return; }
    const need = off + (masked ? 4 : 0) + len;
    if (c.buf.length < need) return;
    let data = c.buf.subarray(off + (masked ? 4 : 0), need);
    if (masked) { const m = c.buf.subarray(off, off + 4); data = Buffer.from(data); for (let i = 0; i < data.length; i++) data[i] ^= m[i & 3]; }
    c.buf = c.buf.subarray(need);
    if (op === 1 && c.onText) c.onText(data.toString('utf8'));
    else if (op === 8) { wsSendRaw(c, 8, Buffer.alloc(0)); c.socket.end(); }
    else if (op === 9) wsSendRaw(c, 10, data);
  }
}
function wsSendRaw(c, op, payload) {
  if (!c.open) return;
  const len = payload.length;
  let head;
  if (len < 126) { head = Buffer.from([128 | op, len]); }
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 128 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 128 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  try { c.socket.write(Buffer.concat([head, payload])); } catch (e) {}
}
const send = (c, obj) => wsSendRaw(c, 1, Buffer.from(JSON.stringify(obj)));

// ---------- правила ----------
const norm = n => String(n || '').toLowerCase().replace(/ё/g, 'е').replace(/\s+/g, '');
const nickOk = n => typeof n === 'string' && n.length >= 3 && n.length <= 14 && /^[A-Za-zА-Яа-яЁё0-9_ \-]+$/.test(n);
const num = (v, a, b) => { v = +v; return Number.isFinite(v) ? Math.min(b, Math.max(a, v)) : 0; };
const WEAPONS = ['bat', 'hammer', 'crowbar', 'axe', 'shovel', 'spear', 'crossbow', 'shotgun'];
const ATKS = ['', 'chop', 'side', 'head', 'throw', 'thrust', 'shoot'];
const EMO = ['', 'wave', 'dance', 'thumb', 'point', 'kneel', 'scare'];
function cleanState(m) {
  return {
    x: num(m.x, -250, 250), y: num(m.y, -5, 30), z: num(m.z, -250, 250), yaw: num(m.yaw, -7, 7),
    cr: m.cr ? 1 : 0, air: m.air ? 1 : 0, dead: m.dead ? 1 : 0, car: m.car ? 1 : 0, cy: num(m.cy, -1e3, 1e3),
    w: WEAPONS.includes(m.w) ? m.w : 'bat', a: ATKS.includes(m.a) ? m.a : '', an: num(m.an, 0, 1e9) | 0,
    e: EMO.includes(m.e) ? m.e : '', top: num(m.top, 0, 0xffffff) | 0,
  };
}

// ---------- сервер ----------
const srv = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
  res.end('Последний рейс — сервер работает. Игроков онлайн: ' + players.size);
});
srv.on('upgrade', (req, socket) => {
  const c = wsAccept(req, socket);
  if (!c) return;
  let me = null;
  c.onText = txt => {
    let m; try { m = JSON.parse(txt); } catch (e) { return; }
    if (m.t === 'hello') {
      if (me) return;
      if (!nickOk(m.nick)) { send(c, { t: 'err', code: 'nick_bad' }); return; }
      for (const p of players.values()) if (norm(p.nick) === norm(m.nick)) { send(c, { t: 'err', code: 'nick_taken' }); return; }
      me = { id: 'p' + (nextId++), c, nick: m.nick, rank: num(m.rank, 0, 999) | 0, world: String(m.world || '').slice(0, 20), st: null, last: Date.now() };
      players.set(me.id, me);
      send(c, { t: 'welcome', id: me.id, online: players.size });
      console.log('+', me.nick, me.world, 'онлайн', players.size);
      return;
    }
    if (m.t === 'join_run') {
      if (!nickOk(m.nick)) { send(c, { t: 'err', code: 'nick_bad' }); return; }
      for (const p of players.values()) if (norm(p.nick) === norm(m.nick)) { send(c, { t: 'err', code: 'nick_taken' }); return; }
      const MSK = 3 * 3600000, win = Math.round((num(m.win, 0, 9e15) + MSK) / HOUR2) * HOUR2 - MSK;
      if (!process.env.ANYTIME && Math.abs(win - Date.now()) > 15 * 60000) { send(c, { t: 'err', code: 'no_heli' }); return; }
      send(c, Object.assign({ t: 'run', win }, joinRun(m.nick, win)));
      return;
    }
    if (!me) return;
    me.last = Date.now();
    if (m.t === 'st') me.st = cleanState(m);
    else if (m.t === 'ping') send(c, { t: 'pong', c: m.c });
  };
  c.onClose = () => {
    if (!me) return;
    players.delete(me.id);
    for (const p of players.values()) send(p.c, { t: 'leave', id: me.id });
    console.log('-', me.nick, 'онлайн', players.size);
  };
});
setInterval(() => {
  const now = Date.now();
  for (const p of players.values()) {
    if (now - p.last > IDLE) { p.c.socket.destroy(); continue; }
    if (!p.st) continue;
    const ps = [];
    for (const o of players.values()) {
      if (o === p || !o.st || o.world !== p.world) continue;
      if (Math.hypot(o.st.x - p.st.x, o.st.z - p.st.z) > VIEW) continue;
      ps.push(Object.assign({ id: o.id, n: o.nick, r: o.rank }, o.st));
    }
    send(p.c, { t: 'snap', ps, on: players.size });
  }
}, TICK);
srv.listen(PORT, () => console.log('Сервер «Последнего рейса» на порту', PORT));
