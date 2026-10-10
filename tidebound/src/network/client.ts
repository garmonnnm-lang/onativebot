/**
 * Сетевой слой клиента: HTTP-проверка сервера, список комнат и WebSocket-соединение
 * с авто-переподключением. Протокол описан в shared/net.ts.
 */
import {
  PROTOCOL_VERSION, validRoomCode as validCode, type ClientMsg, type RoomInfo, type ServerMsg,
} from '../../shared/net';

export interface ServerStatus { ok: boolean; error?: string }

/** Базовый адрес сервера. VITE_SERVER_URL → dev: тот же хост, порт 3001 → прод: тот же адрес, откуда загружена страница. */
export function httpBase(): string {
  const env = import.meta.env.VITE_SERVER_URL as string | undefined;
  if (env) return env.replace(/\/$/, '');
  if (import.meta.env.DEV) return `${location.protocol}//${location.hostname}:3001`;
  return location.origin;
}

export function wsUrl(): string {
  return `${httpBase().replace(/^http/, 'ws')}/ws`;
}

async function fetchJson<T>(path: string, timeoutMs = 3500): Promise<T> {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const r = await fetch(`${httpBase()}${path}`, { signal: ctrl.signal, cache: 'no-store' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return (await r.json()) as T;
  } finally {
    clearTimeout(t);
  }
}

export async function checkServer(): Promise<ServerStatus> {
  try {
    const h = await fetchJson<{ ok: boolean; protocol: number }>('/health');
    if (!h.ok) return { ok: false, error: 'Сервер сообщил о неисправности.' };
    if (h.protocol !== PROTOCOL_VERSION) return { ok: false, error: 'Версия сервера не совпадает с версией клиента. Обновите страницу.' };
    return { ok: true };
  } catch {
    return { ok: false, error: 'Сервер комнат недоступен. Запустите его командой «npm run server» или проверьте подключение.' };
  }
}

export async function fetchRooms(): Promise<RoomInfo[]> {
  const r = await fetchJson<{ rooms: RoomInfo[] }>('/api/rooms');
  return Array.isArray(r.rooms) ? r.rooms : [];
}

/** Допустимый код комнаты: 4–8 латинских букв и цифр. */
export function validRoomCode(code: string): boolean {
  return validCode(code);
}

export type NetState = 'idle' | 'connecting' | 'online' | 'reconnecting' | 'failed';

/** WebSocket-клиент: hello → welcome, авто-переподключение с тем же токеном, измерение пинга. */
export class NetClient {
  state: NetState = 'idle';
  playerId = '';
  ping = 0;
  onMessage: (m: ServerMsg) => void = () => {};
  onState: (s: NetState) => void = () => {};

  private ws: WebSocket | null = null;
  private name = 'Игрок';
  private token = '';
  private wantOnline = false;
  private retry = 0;
  private retryTimer: number | null = null;
  private pingTimer: number | null = null;
  private pingSent = 0;
  private firstConnectReject: ((e: Error) => void) | null = null;
  private lostAt = 0;

  /** Открывает соединение и ждёт welcome. */
  connect(name: string): Promise<void> {
    this.name = name;
    this.wantOnline = true;
    this.token = ''; // новое подключение — новая личность; токен нужен только для автопереподключения в этой сессии
    if (this.state === 'online' && this.ws?.readyState === WebSocket.OPEN) return Promise.resolve();
    return new Promise((resolve, reject) => {
      this.firstConnectReject = reject;
      const prev = this.onMessage;
      this.onMessage = (m) => {
        if (m.t === 'welcome') { this.onMessage = prev; this.firstConnectReject = null; resolve(); }
        prev(m);
      };
      this.open();
    });
  }

  private setState(s: NetState): void {
    if (this.state === s) return;
    this.state = s;
    this.onState(s);
  }

  private open(): void {
    this.setState(this.retry > 0 ? 'reconnecting' : 'connecting');
    let ws: WebSocket;
    try { ws = new WebSocket(wsUrl()); } catch { this.failed('Не удалось открыть соединение.'); return; }
    this.ws = ws;
    ws.onopen = () => {
      this.raw({ t: 'hello', v: PROTOCOL_VERSION, name: this.name, token: this.token || undefined });
    };
    ws.onmessage = (e) => {
      let m: ServerMsg;
      try { m = JSON.parse(String(e.data)) as ServerMsg; } catch { return; }
      if (m.t === 'welcome') {
        this.playerId = m.id;
        this.token = m.token;
        this.retry = 0;
        this.lostAt = 0;
        this.setState('online');
        this.startPing();
      } else if (m.t === 'pong') {
        this.ping = Math.round(performance.now() - this.pingSent);
        return;
      }
      this.onMessage(m);
    };
    ws.onclose = () => {
      this.stopPing();
      if (this.ws !== ws) return;
      this.ws = null;
      if (!this.wantOnline) { this.setState('idle'); return; }
      if (!this.lostAt) this.lostAt = Date.now();
      if (this.firstConnectReject && this.retry === 0) { this.failed('Не удалось подключиться к серверу.'); return; }
      this.scheduleRetry();
    };
    ws.onerror = () => { /* подробности придут в onclose */ };
  }

  private scheduleRetry(): void {
    if (Date.now() - this.lostAt > 40_000) { this.failed('Соединение потеряно и не удалось восстановить.'); return; }
    this.retry++;
    this.setState('reconnecting');
    const delay = Math.min(4000, 500 * this.retry);
    this.retryTimer = window.setTimeout(() => { this.retryTimer = null; this.open(); }, delay);
  }

  private failed(msg: string): void {
    this.setState('failed');
    this.wantOnline = false;
    const rej = this.firstConnectReject;
    this.firstConnectReject = null;
    if (rej) rej(new Error(msg));
    else this.onMessage({ t: 'closed', reason: msg });
  }

  private startPing(): void {
    this.stopPing();
    this.pingTimer = window.setInterval(() => {
      this.pingSent = performance.now();
      this.raw({ t: 'ping', c: 1 });
    }, 5000);
  }

  private stopPing(): void {
    if (this.pingTimer !== null) { clearInterval(this.pingTimer); this.pingTimer = null; }
  }

  private raw(m: ClientMsg): void {
    if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(m));
  }

  send(m: ClientMsg): boolean {
    if (this.state !== 'online' || this.ws?.readyState !== WebSocket.OPEN) return false;
    this.ws.send(JSON.stringify(m));
    return true;
  }

  /** Полное закрытие: забываем токен, чтобы следующий вход был новым игроком. */
  disconnect(): void {
    this.wantOnline = false;
    if (this.retryTimer !== null) { clearTimeout(this.retryTimer); this.retryTimer = null; }
    this.stopPing();
    const ws = this.ws;
    this.ws = null;
    ws?.close();
    this.setState('idle');
    this.token = '';
  }
}
