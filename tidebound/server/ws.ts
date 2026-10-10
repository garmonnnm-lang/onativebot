/**
 * Минимальная реализация WebSocket-сервера (RFC 6455) без внешних зависимостей:
 * текстовые кадры, фрагментация, ping/pong, close, ограничение размера кадра.
 */
import { createHash } from 'node:crypto';
import { EventEmitter } from 'node:events';
import type { IncomingMessage } from 'node:http';
import type { Duplex } from 'node:stream';

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_PAYLOAD = 16 * 1024;

export class WsConn extends EventEmitter {
  private buf: Buffer = Buffer.alloc(0);
  private frag: Buffer[] = [];
  private fragLen = 0;
  closed = false;
  lastSeen = Date.now();
  readonly ip: string;

  constructor(private socket: Duplex, req: IncomingMessage) {
    super();
    this.ip = (req.headers['x-forwarded-for'] as string | undefined)?.split(',')[0].trim() ?? req.socket.remoteAddress ?? '?';
    socket.on('data', (d: Buffer) => this.onData(d));
    socket.on('close', () => this.finish());
    socket.on('error', () => this.finish());
  }

  send(text: string): void {
    if (this.closed) return;
    this.writeFrame(0x1, Buffer.from(text, 'utf8'));
  }

  ping(): void {
    if (!this.closed) this.writeFrame(0x9, Buffer.alloc(0));
  }

  close(code = 1000, reason = ''): void {
    if (this.closed) return;
    const r = Buffer.from(reason, 'utf8').subarray(0, 100);
    const p = Buffer.alloc(2 + r.length);
    p.writeUInt16BE(code, 0);
    r.copy(p, 2);
    this.writeFrame(0x8, p);
    this.socket.end();
    setTimeout(() => this.socket.destroy(), 1500).unref();
    this.finish(code);
  }

  terminate(): void {
    this.socket.destroy();
    this.finish(1006);
  }

  private finish(code = 1006): void {
    if (this.closed) return;
    this.closed = true;
    this.emit('close', code);
  }

  private writeFrame(op: number, payload: Buffer): void {
    const len = payload.length;
    let head: Buffer;
    if (len < 126) head = Buffer.from([0x80 | op, len]);
    else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
    else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
    try { this.socket.write(Buffer.concat([head, payload])); } catch { /* сокет закрыт */ }
  }

  private onData(d: Buffer): void {
    this.lastSeen = Date.now();
    this.buf = this.buf.length ? Buffer.concat([this.buf, d]) : d;
    while (!this.closed) {
      const b = this.buf;
      if (b.length < 2) return;
      const fin = (b[0] & 0x80) !== 0;
      const op = b[0] & 0x0f;
      const masked = (b[1] & 0x80) !== 0;
      let len = b[1] & 0x7f;
      let off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) {
        if (b.length < 10) return;
        const big = b.readBigUInt64BE(2);
        if (big > BigInt(MAX_PAYLOAD)) { this.close(1009, 'too big'); return; }
        len = Number(big); off = 10;
      }
      if (!masked) { this.close(1002, 'unmasked'); return; }
      if (len > MAX_PAYLOAD) { this.close(1009, 'too big'); return; }
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4);
      const payload = Buffer.from(b.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      this.buf = b.subarray(off + 4 + len);

      if (op >= 0x8) {
        if (op === 0x8) { this.close(1000); return; }
        if (op === 0x9) this.writeFrame(0xa, payload);
        continue; // pong игнорируем (lastSeen уже обновлён)
      }
      if (op === 0x1 || op === 0x2 || op === 0x0) {
        if (op !== 0x0) { this.frag = []; this.fragLen = 0; }
        this.frag.push(payload);
        this.fragLen += payload.length;
        if (this.fragLen > MAX_PAYLOAD) { this.close(1009, 'too big'); return; }
        if (fin) {
          const full = Buffer.concat(this.frag);
          this.frag = []; this.fragLen = 0;
          if (op !== 0x2) this.emit('message', full.toString('utf8'));
        }
      }
    }
  }
}

/** Выполняет рукопожатие. Возвращает соединение или null (ответ об ошибке уже отправлен). */
export function acceptUpgrade(req: IncomingMessage, socket: Duplex, allowedOrigins: string[]): WsConn | null {
  const key = req.headers['sec-websocket-key'];
  const upgrade = String(req.headers.upgrade ?? '').toLowerCase();
  if (typeof key !== 'string' || upgrade !== 'websocket') {
    socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
    return null;
  }
  const origin = req.headers.origin;
  if (allowedOrigins.length && origin && !allowedOrigins.includes(origin)) {
    socket.end('HTTP/1.1 403 Forbidden\r\n\r\n');
    return null;
  }
  const accept = createHash('sha1').update(key + GUID).digest('base64');
  socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
  (socket as Duplex & { setNoDelay?: (v: boolean) => void }).setNoDelay?.(true);
  return new WsConn(socket, req);
}
