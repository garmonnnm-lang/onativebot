/**
 * Комнаты, игроки и авторитетное состояние кооперативного мира.
 * Сервер решает: кто в комнате, кто хозяин, какие ресурсы срублены, какие тайники вскрыты,
 * какое сейчас время и погода, кто и какую награду получил. Клиент только отправляет намерения.
 */
import { createHash, randomBytes } from 'node:crypto';
import type { WsConn } from './ws';
import {
  DAY_CYCLE_SECONDS, MAX_PLAYERS_LIMIT, MAX_SPEED, PROTOCOL_VERSION, RECONNECT_GRACE_MS, REACH_SERVER, ROOM_CODE_CHARS,
  ROOM_NAME_MAX, SERVER_TICK_HZ, START_TIME_SECONDS, cleanName, cleanText, parseClientMsg, validRoomCode,
  type ClientMsg, type ErrorCode, type HeldNet, type Privacy, type PlayerPublic, type PlayerState, type RoomInfo,
  type RoomMode, type RoomStatus, type ServerMsg, type WeatherKindNet, type WorldSnapshot,
} from '../shared/net';
import { LOOT_TABLES, STASH_TABLES, hashString, rollLoot } from '../shared/loot';
import type { RoomStore, RoomFile } from './persist';

const COLORS = [0xe8774a, 0x4aa3e8, 0x6fcf6a, 0xe8c84a, 0xc46ae8, 0xe84a8f, 0x4ae8d0, 0xb0b0b0];
const MAX_ROOMS = 100;
const RES_HP = { tree: 4, rock: 5 } as const;
const RES_RESPAWN_S = { tree: 720, rock: 480 } as const;

interface Bucket { tokens: number; at: number }

class Player {
  room: Room | null = null;
  conn: WsConn | null = null;
  online = true;
  dropTimer: NodeJS.Timeout | null = null;
  state: PlayerState;
  hasPos = false;
  lastMoveAt = 0;
  lastHitAt = 0;
  lastSwingAt = 0;
  color = 0xffffff;
  rateAll: Bucket = { tokens: 80, at: Date.now() };
  rateAct: Bucket = { tokens: 10, at: Date.now() };
  strikes = 0;
  strikeAt = 0;

  constructor(readonly id: string, readonly token: string, public name: string) {
    this.state = { id, x: 0, y: 0, z: 0, yaw: 0, pitch: 0, speed: 0, held: 'none', swim: false, swingAt: 0 };
  }

  pub(isHost: boolean): PlayerPublic {
    return { id: this.id, name: this.name, color: this.color, host: isHost, online: this.online };
  }
}

interface ResState { hp: number; kind: 'tree' | 'rock'; respawnAt: number }

export class Room {
  status: RoomStatus = 'lobby';
  hostId = '';
  hostTokenHash = '';
  players = new Map<string, Player>();
  resources = new Map<string, ResState>();
  opened = new Set<string>();
  timeSeconds = START_TIME_SECONDS;
  day = 1;
  weather: WeatherKindNet = 'clear';
  nextWeatherAt = 0;
  dirty = false;
  emptySince = 0;
  createdAt = Date.now();

  constructor(
    readonly code: string, public name: string, readonly mode: RoomMode, public max: number, public privacy: Privacy,
  ) {
    this.scheduleWeather();
  }

  scheduleWeather(): void {
    this.nextWeatherAt = Date.now() + (150 + Math.random() * 210) * 1000;
  }

  info(): RoomInfo {
    let n = 0;
    for (const p of this.players.values()) if (p.online) n++;
    return { code: this.code, name: this.name, mode: this.mode, players: n, max: this.max, status: this.status, privacy: this.privacy };
  }

  publicPlayers(): PlayerPublic[] {
    return [...this.players.values()].map((p) => p.pub(p.id === this.hostId));
  }

  snapshot(): WorldSnapshot {
    const resources: WorldSnapshot['resources'] = [];
    for (const [key, r] of this.resources) resources.push({ key, hp: r.hp, alive: r.respawnAt === 0 });
    return { time: { seconds: this.timeSeconds, day: this.day }, weather: this.weather, resources, opened: [...this.opened] };
  }

  send(msg: ServerMsg, except?: string): void {
    const text = JSON.stringify(msg);
    for (const p of this.players.values()) if (p.online && p.conn && p.id !== except) p.conn.send(text);
  }
}

const rnd = (min: number, max: number): number => min + Math.floor(Math.random() * (max - min + 1));
const r2 = (n: number): number => Math.round(n * 100) / 100;

export class RoomManager {
  readonly rooms = new Map<string, Room>();
  private byToken = new Map<string, Player>();
  private lastSave = Date.now();
  private lastTimeSync = 0;
  private timer: NodeJS.Timeout | null = null;
  private lastTick = Date.now();

  constructor(private store: RoomStore | null, private log: (m: string) => void = () => {}) {}

  start(): void {
    this.timer = setInterval(() => this.tick(), 1000 / SERVER_TICK_HZ);
  }

  stats(): { rooms: number; players: number } {
    let players = 0;
    for (const r of this.rooms.values()) for (const p of r.players.values()) if (p.online) players++;
    return { rooms: this.rooms.size, players };
  }

  listOpen(): RoomInfo[] {
    const out: RoomInfo[] = [];
    for (const r of this.rooms.values()) if (r.privacy === 'open' && r.info().players > 0) out.push(r.info());
    return out.slice(0, 50);
  }

  // ───────────── соединения ─────────────

  attach(conn: WsConn): void {
    let me: Player | null = null;
    const helloTimer = setTimeout(() => { if (!me) conn.close(4000, 'hello timeout'); }, 10_000);
    conn.on('message', (raw: string) => {
      const msg = parseClientMsg(raw);
      if (!msg) { if (me) this.strike(me, conn, 'bad_message', 'Некорректное сообщение.'); else conn.close(4001, 'bad message'); return; }
      if (!me) {
        if (msg.t !== 'hello') { conn.close(4002, 'hello first'); return; }
        clearTimeout(helloTimer);
        me = this.hello(conn, msg);
        return;
      }
      if (me.conn !== conn) return; // устаревшее соединение
      this.handle(me, conn, msg);
    });
    conn.on('close', () => {
      clearTimeout(helloTimer);
      if (me && me.conn === conn) this.disconnected(me);
    });
  }

  private hello(conn: WsConn, msg: Extract<ClientMsg, { t: 'hello' }>): Player | null {
    if (msg.v !== PROTOCOL_VERSION) {
      this.sendTo(conn, { t: 'error', code: 'bad_version', msg: 'Версия клиента не совпадает с версией сервера. Обновите страницу.' });
      conn.close(4003, 'version');
      return null;
    }
    let p = msg.token ? this.byToken.get(msg.token) : undefined;
    if (p) {
      // переподключение: старое соединение вытесняется
      const old = p.conn;
      p.conn = conn;
      p.online = true;
      if (p.dropTimer) { clearTimeout(p.dropTimer); p.dropTimer = null; }
      if (old && old !== conn) { old.removeAllListeners('message'); old.close(4004, 'replaced'); }
      this.sendTo(conn, { t: 'welcome', id: p.id, token: p.token, name: p.name, v: PROTOCOL_VERSION });
      if (p.room) {
        this.sendJoined(p, p.room);
        p.room.send({ t: 'players', players: p.room.publicPlayers(), room: p.room.info() });
        this.log(`reconnect ${p.name} → ${p.room.code}`);
      }
      return p;
    }
    const token = randomBytes(16).toString('hex');
    const id = randomBytes(4).toString('hex');
    p = new Player(id, token, cleanName(msg.name));
    p.conn = conn;
    this.byToken.set(token, p);
    this.sendTo(conn, { t: 'welcome', id, token, name: p.name, v: PROTOCOL_VERSION });
    return p;
  }

  private disconnected(p: Player): void {
    p.online = false;
    p.conn = null;
    const room = p.room;
    if (!room) { this.byToken.delete(p.token); return; }
    room.send({ t: 'players', players: room.publicPlayers(), room: room.info() });
    this.log(`disconnect ${p.name} (${room.code})`);
    p.dropTimer = setTimeout(() => {
      p.dropTimer = null;
      if (!p.online) { this.removeFromRoom(p, 'timeout'); this.byToken.delete(p.token); }
    }, RECONNECT_GRACE_MS);
  }

  private sendTo(conn: WsConn | null, msg: ServerMsg): void {
    conn?.send(JSON.stringify(msg));
  }

  private err(p: Player, code: ErrorCode, msg: string): void {
    this.sendTo(p.conn, { t: 'error', code, msg });
  }

  private strike(p: Player, conn: WsConn, code: ErrorCode, msg: string): void {
    this.err(p, code, msg);
    const now = Date.now();
    if (now - p.strikeAt > 10_000) p.strikes = 0;
    p.strikeAt = now;
    if (++p.strikes >= 8) conn.close(4008, 'abuse');
  }

  private take(b: Bucket, rate: number, burst: number): boolean {
    const now = Date.now();
    b.tokens = Math.min(burst, b.tokens + ((now - b.at) / 1000) * rate);
    b.at = now;
    if (b.tokens < 1) return false;
    b.tokens -= 1;
    return true;
  }

  // ───────────── обработка сообщений ─────────────

  private handle(p: Player, conn: WsConn, msg: ClientMsg): void {
    if (!this.take(p.rateAll, 60, 80)) { this.strike(p, conn, 'rate_limit', 'Слишком много сообщений.'); return; }
    if (msg.t !== 'move' && msg.t !== 'ping' && !this.take(p.rateAct, 6, 10)) { this.strike(p, conn, 'rate_limit', 'Слишком частые действия.'); return; }

    switch (msg.t) {
      case 'hello': return;
      case 'ping': this.sendTo(conn, { t: 'pong', c: msg.c }); return;
      case 'create': this.create(p, msg); return;
      case 'join': this.join(p, msg.code); return;
      case 'leave': this.removeFromRoom(p, 'left'); this.sendTo(p.conn, { t: 'left' }); return;
      case 'start': this.startGame(p); return;
      case 'move': this.move(p, msg); return;
      case 'swing': this.swing(p); return;
      case 'hit': this.hit(p, msg); return;
      case 'open': this.open(p, msg); return;
      case 'chat': this.chat(p, msg.text); return;
    }
  }

  private newCode(): string {
    for (let tries = 0; tries < 50; tries++) {
      let c = '';
      const bytes = randomBytes(5);
      for (let i = 0; i < 5; i++) c += ROOM_CODE_CHARS[bytes[i] % ROOM_CODE_CHARS.length];
      if (!this.rooms.has(c) && !this.store?.exists(c)) return c;
    }
    throw new Error('no free code');
  }

  private create(p: Player, msg: Extract<ClientMsg, { t: 'create' }>): void {
    if (p.room) { this.err(p, 'already_in_room', 'Вы уже находитесь в комнате.'); return; }
    if (this.rooms.size >= MAX_ROOMS) { this.err(p, 'unknown', 'Сервер переполнен: слишком много комнат.'); return; }
    const max = Math.min(MAX_PLAYERS_LIMIT, Math.max(1, Math.floor(msg.max)));
    const name = cleanText(msg.name, ROOM_NAME_MAX) || 'Комната';
    const room = new Room(this.newCode(), name, msg.mode, max, msg.privacy);
    room.hostTokenHash = hashToken(p.token);
    this.rooms.set(room.code, room);
    this.log(`room ${room.code} "${room.name}" создана (${p.name})`);
    this.addPlayer(room, p);
    room.dirty = true;
  }

  private join(p: Player, rawCode: string): void {
    if (p.room) { this.err(p, 'already_in_room', 'Вы уже находитесь в комнате.'); return; }
    const code = rawCode.trim().toUpperCase();
    if (!validRoomCode(code)) { this.err(p, 'bad_message', 'Некорректный код комнаты.'); return; }
    let room = this.rooms.get(code);
    if (!room && this.store) {
      const f = this.store.load(code);
      if (f) room = this.restore(f);
    }
    if (!room) { this.err(p, 'no_room', `Комната ${code} не найдена.`); return; }
    if (room.players.size >= room.max) { this.err(p, 'room_full', `Комната ${code} заполнена.`); return; }
    this.addPlayer(room, p);
  }

  private addPlayer(room: Room, p: Player): void {
    // уникальное имя в комнате
    let name = p.name;
    const taken = new Set([...room.players.values()].map((x) => x.name));
    for (let i = 2; taken.has(name); i++) name = `${p.name.slice(0, 16)} (${i})`;
    p.name = name;
    const used = new Set([...room.players.values()].map((x) => x.color));
    p.color = COLORS.find((c) => !used.has(c)) ?? COLORS[room.players.size % COLORS.length];
    p.room = room;
    p.hasPos = false;
    room.players.set(p.id, p);
    room.emptySince = 0;
    if (!room.hostId || !room.players.has(room.hostId)) {
      if (!room.hostTokenHash || room.hostTokenHash === hashToken(p.token) || room.players.size === 1) room.hostId = p.id;
    }
    if (room.hostTokenHash === hashToken(p.token)) room.hostId = p.id;
    this.sendJoined(p, room);
    room.send({ t: 'players', players: room.publicPlayers(), room: room.info() }, p.id);
    this.log(`join ${p.name} → ${room.code} (${room.info().players}/${room.max})`);
  }

  private sendJoined(p: Player, room: Room): void {
    this.sendTo(p.conn, {
      t: 'joined', room: room.info(), you: p.id, players: room.publicPlayers(), world: room.snapshot(), link: `?room=${room.code}`,
    });
  }

  private removeFromRoom(p: Player, why: string): void {
    const room = p.room;
    if (!room) return;
    if (p.dropTimer) { clearTimeout(p.dropTimer); p.dropTimer = null; }
    room.players.delete(p.id);
    p.room = null;
    this.log(`leave ${p.name} ← ${room.code} (${why})`);
    if (room.hostId === p.id) {
      const next = [...room.players.values()].find((x) => x.online) ?? [...room.players.values()][0];
      room.hostId = next?.id ?? '';
    }
    if (room.players.size === 0) { this.unload(room); return; }
    room.send({ t: 'players', players: room.publicPlayers(), room: room.info() });
  }

  private unload(room: Room): void {
    this.rooms.delete(room.code);
    if (room.status === 'playing') this.store?.save(this.toFile(room));
    else this.store?.remove(room.code);
    this.log(`room ${room.code} выгружена`);
  }

  private startGame(p: Player): void {
    const room = p.room;
    if (!room) { this.err(p, 'not_in_room', 'Вы не в комнате.'); return; }
    if (room.hostId !== p.id) { this.err(p, 'not_host', 'Начать игру может только хозяин комнаты.'); return; }
    if (room.status === 'playing') return;
    room.status = 'playing';
    room.dirty = true;
    room.send({ t: 'started' });
    room.send({ t: 'players', players: room.publicPlayers(), room: room.info() });
  }

  private move(p: Player, m: Extract<ClientMsg, { t: 'move' }>): void {
    const room = p.room;
    if (!room) return;
    const now = Date.now();
    if (Math.abs(m.x) > 260 || Math.abs(m.z) > 260 || m.y < -30 || m.y > 90) { this.sendTo(p.conn, { t: 'correct', x: p.state.x, y: p.state.y, z: p.state.z }); return; }
    if (p.hasPos) {
      const dt = Math.max(0.05, Math.min(2, (now - p.lastMoveAt) / 1000));
      const dist = Math.hypot(m.x - p.state.x, m.z - p.state.z);
      if (dist > MAX_SPEED * dt + 4) {
        this.sendTo(p.conn, { t: 'correct', x: p.state.x, y: p.state.y, z: p.state.z });
        p.lastMoveAt = now;
        return;
      }
    }
    p.hasPos = true;
    p.lastMoveAt = now;
    const s = p.state;
    s.x = m.x; s.y = m.y; s.z = m.z; s.yaw = m.yaw; s.pitch = m.pitch;
    s.speed = Math.min(MAX_SPEED, Math.max(0, m.speed));
    s.held = m.held as HeldNet; s.swim = m.swim;
  }

  private swing(p: Player): void {
    const room = p.room;
    if (!room) return;
    const now = Date.now();
    if (now - p.lastSwingAt < 250) return;
    p.lastSwingAt = now;
    p.state.swingAt = now;
    room.send({ t: 'swing', id: p.id }, p.id);
  }

  private hit(p: Player, m: Extract<ClientMsg, { t: 'hit' }>): void {
    const room = p.room;
    if (!room || room.status !== 'playing') return;
    const now = Date.now();
    if (now - p.lastHitAt < 380) return;
    const kx = Number(m.key.split(':')[1]), kz = Number(m.key.split(':')[2]);
    if (!m.key.startsWith(m.kind + ':') || Math.abs(Math.round(m.x * 2) - kx) > 1 || Math.abs(Math.round(m.z * 2) - kz) > 1) { this.err(p, 'bad_message', 'Некорректный ресурс.'); return; }
    if (m.kind === 'tree' && p.state.held !== 'axe') return;
    if (m.kind === 'rock' && p.state.held !== 'hammer') return;
    if (Math.hypot(p.state.x - m.x, p.state.z - m.z) > REACH_SERVER + 2) { this.err(p, 'too_far', 'Слишком далеко.'); return; }
    p.lastHitAt = now;
    let r = room.resources.get(m.key);
    if (!r) {
      if (room.resources.size > 5000) return;
      r = { hp: RES_HP[m.kind], kind: m.kind, respawnAt: 0 };
      room.resources.set(m.key, r);
    }
    if (r.respawnAt !== 0) return; // уже срублен — награду повторно не выдаём
    r.hp -= 1;
    room.dirty = true;
    if (r.hp > 0) {
      room.send({ t: 'resource', key: m.key, hp: r.hp, alive: true, by: p.id });
      return;
    }
    r.respawnAt = now + RES_RESPAWN_S[m.kind] * 1000;
    room.send({ t: 'resource', key: m.key, hp: 0, alive: false, by: p.id });
    const item = m.kind === 'tree' ? { id: 'wood', count: rnd(3, 5) } : { id: 'stone', count: rnd(2, 4) };
    this.sendTo(p.conn, { t: 'reward', items: [item], xp: m.kind === 'tree' ? 5 : 6, reason: 'сбор ресурсов' });
  }

  private open(p: Player, m: Extract<ClientMsg, { t: 'open' }>): void {
    const room = p.room;
    if (!room || room.status !== 'playing') return;
    const table = STASH_TABLES[m.id];
    if (!table || !LOOT_TABLES[table]) { this.err(p, 'bad_message', 'Неизвестный тайник.'); return; }
    if (room.opened.has(m.id)) { this.sendTo(p.conn, { t: 'opened', id: m.id, by: '' }); return; }
    room.opened.add(m.id);
    room.dirty = true;
    const loot = rollLoot(table, hashString(m.id));
    this.sendTo(p.conn, { t: 'opened', id: m.id, by: p.id, loot });
    room.send({ t: 'opened', id: m.id, by: p.id }, p.id);
  }

  private chat(p: Player, text: string): void {
    const room = p.room;
    if (!room) return;
    const t = cleanText(text, 160);
    if (t) room.send({ t: 'chat', from: p.id, name: p.name, text: t });
  }

  // ───────────── тик ─────────────

  private tick(): void {
    const now = Date.now();
    const dt = Math.min(1, (now - this.lastTick) / 1000);
    this.lastTick = now;
    const syncTime = now - this.lastTimeSync > 5000;
    if (syncTime) this.lastTimeSync = now;

    for (const room of this.rooms.values()) {
      room.timeSeconds += dt;
      while (room.timeSeconds >= DAY_CYCLE_SECONDS) { room.timeSeconds -= DAY_CYCLE_SECONDS; room.day++; room.dirty = true; }

      if (now > room.nextWeatherAt) {
        const r = Math.random();
        room.weather = r < 0.42 ? 'clear' : r < 0.67 ? 'cloudy' : r < 0.87 ? 'rain' : r < 0.94 ? 'fog' : 'storm';
        room.scheduleWeather();
        room.dirty = true;
        room.send({ t: 'time', seconds: r2(room.timeSeconds), day: room.day, weather: room.weather });
      } else if (syncTime) {
        room.send({ t: 'time', seconds: r2(room.timeSeconds), day: room.day, weather: room.weather });
      }

      for (const [key, r] of room.resources) {
        if (r.respawnAt !== 0 && now >= r.respawnAt) {
          r.respawnAt = 0; r.hp = RES_HP[r.kind];
          room.send({ t: 'resource', key, hp: r.hp, alive: true, by: '' });
          room.resources.delete(key);
          room.dirty = true;
        }
      }

      const online: PlayerState[] = [];
      for (const p of room.players.values()) {
        if (p.online && p.hasPos) {
          const s = p.state;
          online.push({ ...s, x: r2(s.x), y: r2(s.y), z: r2(s.z), yaw: r2(s.yaw), pitch: r2(s.pitch), speed: r2(s.speed) });
        }
      }
      if (online.length) room.send({ t: 'state', at: now, players: online });

      if (room.info().players === 0) {
        if (!room.emptySince) room.emptySince = now;
      } else room.emptySince = 0;
    }

    if (now - this.lastSave > 30_000) { this.lastSave = now; this.saveAll(false); }
  }

  // ───────────── сохранение ─────────────

  private toFile(room: Room): RoomFile {
    return {
      v: 1, code: room.code, name: room.name, mode: room.mode, max: room.max, privacy: room.privacy, status: room.status,
      hostTokenHash: room.hostTokenHash, createdAt: room.createdAt, savedAt: Date.now(),
      time: { seconds: room.timeSeconds, day: room.day }, weather: room.weather,
      resources: [...room.resources].map(([key, r]) => ({ key, hp: r.hp, kind: r.kind, respawnIn: r.respawnAt ? Math.max(0, r.respawnAt - Date.now()) : 0 })),
      opened: [...room.opened],
    };
  }

  private restore(f: RoomFile): Room {
    const room = new Room(f.code, f.name, f.mode, f.max, f.privacy);
    room.status = f.status;
    room.hostTokenHash = f.hostTokenHash;
    room.createdAt = f.createdAt;
    room.timeSeconds = f.time.seconds;
    room.day = f.time.day;
    room.weather = f.weather;
    for (const r of f.resources) room.resources.set(r.key, { hp: r.hp, kind: r.kind, respawnAt: r.respawnIn > 0 ? Date.now() + r.respawnIn : 0 });
    for (const id of f.opened) room.opened.add(id);
    this.rooms.set(room.code, room);
    this.log(`room ${room.code} восстановлена с диска`);
    return room;
  }

  saveAll(force: boolean): void {
    if (!this.store) return;
    for (const room of this.rooms.values()) {
      if (room.status !== 'playing') continue;
      if (room.dirty || force) { this.store.save(this.toFile(room)); room.dirty = false; }
    }
  }

  shutdown(): void {
    if (this.timer) clearInterval(this.timer);
    this.saveAll(true);
    for (const room of this.rooms.values()) {
      room.send({ t: 'closed', reason: 'Сервер перезапускается. Мир сохранён — зайдите по тому же коду.' });
      for (const p of room.players.values()) { if (p.dropTimer) clearTimeout(p.dropTimer); p.conn?.close(1001, 'shutdown'); }
    }
  }
}

function hashToken(t: string): string {
  return createHash('sha256').update(t).digest('hex');
}
