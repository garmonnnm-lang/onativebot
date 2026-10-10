/**
 * Сетевой протокол TIDEBOUND (JSON поверх WebSocket). Общий для клиента и сервера.
 * Архитектура: авторитетный сервер комнат на Node.js. «Хозяин» комнаты — роль в интерфейсе
 * (может начать игру), реальные проверки делает сервер.
 */

export const PROTOCOL_VERSION = 1;
export const ROOM_CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
export const NAME_MAX = 20;
export const ROOM_NAME_MAX = 32;
export const MAX_PLAYERS_LIMIT = 8;
export const MAX_MESSAGE_BYTES = 4096;
export const SERVER_TICK_HZ = 10;
/** Длительность суток (минуты) — должна совпадать с CONFIG.day на клиенте (клиент импортирует отсюда). */
export const DAY_MINUTES = { day: 16, dusk: 4, night: 4 } as const;
export const DAY_CYCLE_SECONDS = (DAY_MINUTES.day + DAY_MINUTES.dusk + DAY_MINUTES.night) * 60;
export const START_TIME_SECONDS = DAY_MINUTES.day * 60 * 0.3;
export const RECONNECT_GRACE_MS = 45_000;
/** Максимальная допустимая скорость игрока (м/с) с запасом на рывки/лаги. */
export const MAX_SPEED = 11;
export const REACH_SERVER = 6;

export type RoomMode = 'coop';
export type Privacy = 'open' | 'code' | 'private';
export type RoomStatus = 'lobby' | 'playing';
export type WeatherKindNet = 'clear' | 'cloudy' | 'fog' | 'rain' | 'storm';
export type HeldNet = 'none' | 'rod' | 'axe' | 'hammer' | 'food' | 'drink';

export interface PlayerPublic {
  id: string;
  name: string;
  color: number;
  host: boolean;
  online: boolean;
}

export interface PlayerState {
  id: string;
  x: number; y: number; z: number;
  yaw: number; pitch: number;
  /** Скорость (м/с) — для анимации ходьбы. */
  speed: number;
  held: HeldNet;
  swim: boolean;
  /** Время последнего взмаха инструментом (мс на сервере) — для анимации у других. */
  swingAt: number;
}

export interface RoomInfo {
  code: string;
  name: string;
  mode: RoomMode;
  players: number;
  max: number;
  status: RoomStatus;
  privacy: Privacy;
}

export interface WorldSnapshot {
  /** Секунды внутри суточного цикла и номер дня. */
  time: { seconds: number; day: number };
  weather: WeatherKindNet;
  /** Повреждённые/срубленные ресурсы. alive=false — срублен. */
  resources: { key: string; hp: number; alive: boolean }[];
  opened: string[];
}

// ───────────── клиент → сервер ─────────────
export type ClientMsg =
  | { t: 'hello'; v: number; name: string; token?: string }
  | { t: 'create'; name: string; max: number; privacy: Privacy; mode: RoomMode }
  | { t: 'join'; code: string }
  | { t: 'leave' }
  | { t: 'start' }
  | { t: 'move'; x: number; y: number; z: number; yaw: number; pitch: number; speed: number; held: HeldNet; swim: boolean }
  | { t: 'swing' }
  | { t: 'hit'; key: string; kind: 'tree' | 'rock'; x: number; z: number }
  | { t: 'open'; id: string; table: string }
  | { t: 'chat'; text: string }
  | { t: 'ping'; c: number };

// ───────────── сервер → клиент ─────────────
export type ErrorCode =
  | 'bad_message' | 'rate_limit' | 'no_room' | 'room_full' | 'not_host' | 'bad_version'
  | 'not_in_room' | 'already_in_room' | 'too_far' | 'unknown';

export type ServerMsg =
  | { t: 'welcome'; id: string; token: string; name: string; v: number }
  | { t: 'joined'; room: RoomInfo; you: string; players: PlayerPublic[]; world: WorldSnapshot; link: string }
  | { t: 'players'; players: PlayerPublic[]; room: RoomInfo }
  | { t: 'started' }
  | { t: 'state'; at: number; players: PlayerState[] }
  | { t: 'correct'; x: number; y: number; z: number }
  | { t: 'swing'; id: string }
  | { t: 'resource'; key: string; hp: number; alive: boolean; by: string }
  | { t: 'reward'; items: { id: string; count: number }[]; xp: number; reason: string }
  | { t: 'opened'; id: string; by: string; loot?: { id: string; count: number }[] }
  | { t: 'time'; seconds: number; day: number; weather: WeatherKindNet }
  | { t: 'chat'; from: string; name: string; text: string }
  | { t: 'left' }
  | { t: 'closed'; reason: string }
  | { t: 'error'; code: ErrorCode; msg: string }
  | { t: 'pong'; c: number };

// ───────────── валидация ─────────────
const isNum = (v: unknown, lim = 1e5): v is number => typeof v === 'number' && Number.isFinite(v) && Math.abs(v) <= lim;
const isStr = (v: unknown, max: number): v is string => typeof v === 'string' && v.length <= max;

export function validRoomCode(code: string): boolean {
  return /^[A-Z0-9]{4,8}$/.test(code);
}

export function cleanName(raw: unknown, fallback = 'Игрок'): string {
  if (typeof raw !== 'string') return fallback;
  const s = raw.replace(/[\u0000-\u001f<>&"'`]/g, '').trim().slice(0, NAME_MAX);
  return s || fallback;
}

export function cleanText(raw: string, max: number): string {
  return raw.replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, max);
}

const HELD = new Set(['none', 'rod', 'axe', 'hammer', 'food', 'drink']);

/** Проверяет структуру входящего сообщения. null — сообщение некорректно. */
export function parseClientMsg(raw: string): ClientMsg | null {
  if (raw.length > MAX_MESSAGE_BYTES) return null;
  let m: unknown;
  try { m = JSON.parse(raw); } catch { return null; }
  if (!m || typeof m !== 'object') return null;
  const o = m as Record<string, unknown>;
  switch (o.t) {
    case 'hello':
      if (!isNum(o.v, 1000) || !isStr(o.name, 64)) return null;
      if (o.token !== undefined && !isStr(o.token, 64)) return null;
      return { t: 'hello', v: o.v, name: o.name, token: o.token as string | undefined };
    case 'create':
      if (!isStr(o.name, 64) || !isNum(o.max, 100)) return null;
      if (o.privacy !== 'open' && o.privacy !== 'code' && o.privacy !== 'private') return null;
      return { t: 'create', name: o.name, max: o.max, privacy: o.privacy, mode: 'coop' };
    case 'join':
      if (!isStr(o.code, 16)) return null;
      return { t: 'join', code: o.code.toUpperCase() };
    case 'leave': return { t: 'leave' };
    case 'start': return { t: 'start' };
    case 'swing': return { t: 'swing' };
    case 'ping': return isNum(o.c) ? { t: 'ping', c: o.c } : null;
    case 'chat': return isStr(o.text, 200) ? { t: 'chat', text: o.text } : null;
    case 'move':
      if (![o.x, o.y, o.z].every((n) => isNum(n, 1000)) || !isNum(o.yaw, 100) || !isNum(o.pitch, 10) || !isNum(o.speed, 100)) return null;
      if (typeof o.held !== 'string' || !HELD.has(o.held) || typeof o.swim !== 'boolean') return null;
      return { t: 'move', x: o.x as number, y: o.y as number, z: o.z as number, yaw: o.yaw, pitch: o.pitch, speed: o.speed, held: o.held as HeldNet, swim: o.swim };
    case 'hit':
      if (!isStr(o.key, 40) || !/^(tree|rock):-?\d+:-?\d+$/.test(o.key)) return null;
      if (o.kind !== 'tree' && o.kind !== 'rock') return null;
      if (!isNum(o.x, 1000) || !isNum(o.z, 1000)) return null;
      return { t: 'hit', key: o.key, kind: o.kind, x: o.x, z: o.z };
    case 'open':
      if (!isStr(o.id, 40) || !/^[a-z0-9-]+$/.test(o.id) || !isStr(o.table, 40)) return null;
      return { t: 'open', id: o.id, table: o.table };
    default: return null;
  }
}

/** Стабильный ключ ресурса: не зависит от качества графики и порядка генерации. */
export function resourceKey(kind: 'tree' | 'rock', x: number, z: number): string {
  return `${kind}:${Math.round(x * 2)}:${Math.round(z * 2)}`;
}
