import { NetClient, checkServer } from '../network/client';
import { RemotePlayers } from '../network/remotePlayers';
import { cleanName, type Privacy, type PlayerPublic, type RoomInfo, type ServerMsg, type WorldSnapshot } from '../../shared/net';
import { getItem } from '../../shared/items';
import type { WeatherKind } from '../weather/weather';
import type { Game } from './game';
import type { ResourceNode } from '../world/vegetation';

type Reply = Extract<ServerMsg, { t: 'joined' } | { t: 'error' }>;

/**
 * Кооператив на стороне клиента: комнаты, синхронизация игроков, общие ресурсы и тайники,
 * серверное время и погода. Всё «авторитетное» решает сервер (server/rooms.ts).
 */
export class Multiplayer {
  readonly net = new NetClient();
  readonly remote: RemotePlayers;
  /** Идёт кооперативная игра (мир загружен). */
  active = false;
  /** Игрок состоит в комнате (лобби или игра). */
  inRoom = false;
  room: RoomInfo | null = null;
  players: PlayerPublic[] = [];
  myId = '';
  link = '';
  private snapshot: WorldSnapshot | null = null;
  private pending: { resolve: (m: Reply) => void; timer: number } | null = null;
  private wasOnline = false;

  constructor(private g: Game) {
    this.remote = new RemotePlayers(() => this.myId);
    this.net.onMessage = (m) => this.onMsg(m);
    this.net.onState = (s) => this.onNetState(s);
    window.setInterval(() => this.sendMove(), 66);
  }

  get isHost(): boolean { return !!this.players.find((p) => p.id === this.myId)?.host; }

  absoluteLink(code: string): string {
    return `${location.origin}${location.pathname}?room=${code}`;
  }

  // ───────────── запросы ─────────────

  private request(send: () => void): Promise<Reply> {
    return new Promise((resolve) => {
      if (this.pending) window.clearTimeout(this.pending.timer);
      const timer = window.setTimeout(() => {
        this.pending = null;
        resolve({ t: 'error', code: 'unknown', msg: 'Сервер не ответил. Попробуйте ещё раз.' });
      }, 6000);
      this.pending = { resolve, timer };
      send();
    });
  }

  /** Подключается к серверу. Возвращает текст ошибки или null. */
  async connect(name: string): Promise<string | null> {
    const st = await checkServer();
    if (!st.ok) return st.error ?? 'Сервер недоступен.';
    try {
      await this.net.connect(cleanName(name));
      return null;
    } catch (e) {
      return (e as Error).message;
    }
  }

  async createRoom(name: string, roomName: string, max: number, privacy: Privacy): Promise<string | null> {
    const err = await this.connect(name);
    if (err) return err;
    const r = await this.request(() => this.net.send({ t: 'create', name: roomName, max, privacy, mode: 'coop' }));
    if (r.t === 'error') { this.net.disconnect(); return r.msg; }
    return null;
  }

  async joinRoom(name: string, code: string): Promise<string | null> {
    const err = await this.connect(name);
    if (err) return err;
    const r = await this.request(() => this.net.send({ t: 'join', code }));
    if (r.t === 'error') { this.net.disconnect(); return r.msg; }
    return null;
  }

  startRoom(): void { this.net.send({ t: 'start' }); }

  /** Выход из комнаты и закрытие соединения. */
  leave(): void {
    if (this.net.state === 'online') this.net.send({ t: 'leave' });
    this.net.disconnect();
    this.resetLocal();
  }

  private resetLocal(): void {
    const was = this.active;
    this.active = false;
    this.inRoom = false;
    this.room = null;
    this.players = [];
    this.snapshot = null;
    this.myId = '';
    this.remote.clear();
    if (was) this.g.onCoopEnded();
  }

  // ───────────── действия игрока ─────────────

  sendSwing(): void { if (this.active) this.net.send({ t: 'swing' }); }

  sendHit(node: ResourceNode): void {
    this.net.send({ t: 'hit', key: node.key, kind: node.kind, x: node.x, z: node.z });
  }

  requestOpen(id: string): void {
    const table = this.g.world.stashTable(id);
    if (table) this.net.send({ t: 'open', id, table });
  }

  sendChat(text: string): void { this.net.send({ t: 'chat', text }); }

  // ───────────── обновление ─────────────

  update(dt: number): void {
    this.remote.update(dt);
  }

  /** Отправка позиции по таймеру (а не по кадрам): работает и при низком FPS, и во фоновой вкладке. */
  private sendMove(): void {
    if (!this.active) return;
    const p = this.g.player;
    this.net.send({
      t: 'move', x: p.pos.x, y: p.pos.y, z: p.pos.z, yaw: this.g.input.yaw, pitch: this.g.input.pitch,
      speed: p.speed, held: this.g.held.current, swim: p.swimming,
    });
  }

  // ───────────── входящие сообщения ─────────────

  private resolvePending(m: Reply): boolean {
    if (!this.pending) return false;
    window.clearTimeout(this.pending.timer);
    const p = this.pending;
    this.pending = null;
    p.resolve(m);
    return true;
  }

  private onNetState(s: string): void {
    const g = this.g;
    if (s === 'reconnecting' && this.inRoom) g.ui.netBanner('Соединение потеряно. Переподключение…');
    if (s === 'online') {
      g.ui.netBanner(null);
      if (this.wasOnline && this.inRoom) g.ui.toast('Соединение восстановлено.', 'good');
    }
    this.wasOnline = s === 'online' || (this.wasOnline && s === 'reconnecting');
  }

  private onMsg(m: ServerMsg): void {
    const g = this.g;
    switch (m.t) {
      case 'welcome': this.myId = m.id; break;
      case 'joined': {
        const first = !this.inRoom;
        this.inRoom = true;
        this.room = m.room;
        this.players = m.players;
        this.myId = m.you;
        this.link = this.absoluteLink(m.room.code);
        this.snapshot = m.world;
        this.remote.setPublic(m.players);
        this.resolvePending(m);
        if (m.room.status === 'playing') {
          if (!this.active) g.startCoop(m.world);
          else g.world.applySnapshot(m.world);
        } else if (first) g.ui.showRoomLobby();
        else g.ui.refreshRoomLobby();
        break;
      }
      case 'players': {
        const prev = this.players;
        this.players = m.players;
        this.room = m.room;
        this.remote.setPublic(m.players);
        if (this.active) {
          for (const p of m.players) {
            const old = prev.find((x) => x.id === p.id);
            if (!old && p.id !== this.myId) g.ui.feed(`${p.name} присоединился к игре`);
            else if (old && old.online && !p.online) g.ui.feed(`${p.name}: соединение потеряно`);
            else if (old && !old.online && p.online) g.ui.feed(`${p.name} снова в игре`);
          }
          for (const old of prev) if (!m.players.some((p) => p.id === old.id)) g.ui.feed(`${old.name} покинул игру`);
        }
        g.ui.refreshRoomLobby();
        g.ui.refreshPlayers();
        break;
      }
      case 'started': if (!this.active && this.snapshot) g.startCoop(this.snapshot); break;
      case 'state': this.remote.applyState(m.players); break;
      case 'swing': this.remote.swing(m.id); break;
      case 'correct': g.player.teleport(m.x, m.z); g.player.pos.y = Math.max(m.y, g.player.pos.y); break;
      case 'resource': {
        if (!this.active) break;
        g.world.applyResource(m.key, m.hp, m.alive);
        if (m.by && m.by !== this.myId) {
          const n = g.world.findByKey(m.key);
          if (n && Math.hypot(n.x - g.player.pos.x, n.z - g.player.pos.z) < 30) { if (n.kind === 'tree') g.audio.chop(); else g.audio.hit(); }
        }
        break;
      }
      case 'reward': {
        for (const it of m.items) { g.giveOrDrop(it.id, it.count); g.ui.feed(`+${it.count} ${getItem(it.id).name}`); }
        g.addXp(m.xp, m.reason);
        break;
      }
      case 'opened': {
        g.world.markStashOpened(m.id);
        if (m.loot && m.by === this.myId) {
          for (const l of m.loot) g.giveOrDrop(l.id, l.count);
          g.audio.pickup();
          g.ui.toast(`Найдено: ${m.loot.map((l) => `${g.itemName(l.id)} ×${l.count}`).join(', ')}`, 'good');
          g.addXp(6, 'исследование');
        } else if (m.by && m.by !== this.myId) {
          const who = this.players.find((p) => p.id === m.by)?.name ?? 'Игрок';
          g.ui.feed(`${who} вскрыл тайник`);
        }
        break;
      }
      case 'time': {
        if (!this.active) break;
        const d = g.daynight;
        const diff = Math.abs(d.seconds - m.seconds);
        if (diff > 1.5 || d.dayNumber !== m.day) d.setTime(m.seconds, m.day);
        g.setWeatherNet(m.weather as WeatherKind);
        break;
      }
      case 'chat': g.ui.feed(`${m.name}: ${m.text}`); break;
      case 'closed': {
        const text = m.reason;
        this.net.disconnect();
        this.resetLocal();
        g.ui.toast(text, 'warn');
        g.leaveToMenuNoSave();
        break;
      }
      case 'left': break;
      case 'error': {
        if (this.resolvePending(m)) break;
        g.ui.toast(m.msg, m.code === 'too_far' ? 'info' : 'warn');
        break;
      }
      case 'pong': break;
    }
  }
}

