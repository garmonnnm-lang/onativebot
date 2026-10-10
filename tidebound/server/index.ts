/**
 * TIDEBOUND — сервер комнат.
 * Запуск: npm run server   (переменные: PORT, HOST, DATA_DIR, ALLOWED_ORIGINS, SERVE_DIST)
 * Эндпоинты: GET /health, GET /api/rooms, WebSocket на любом пути (по умолчанию /ws).
 * Если рядом есть папка dist/ (npm run build), сервер отдаёт и сам клиент — один процесс на всё.
 */
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { existsSync, readFileSync, statSync } from 'node:fs';
import { gzipSync } from 'node:zlib';
import { dirname, extname, join, normalize, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { acceptUpgrade, type WsConn } from './ws';
import { RoomManager } from './rooms';
import { RoomStore } from './persist';
import { PROTOCOL_VERSION } from '../shared/net';

const here = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT ?? 3001);
const HOST = process.env.HOST ?? '0.0.0.0';
const DATA_DIR = resolve(process.env.DATA_DIR ?? join(here, 'data', 'rooms'));
const ALLOWED = (process.env.ALLOWED_ORIGINS ?? '').split(',').map((s) => s.trim()).filter(Boolean);
const DIST = resolve(process.env.SERVE_DIST ?? join(here, '..', 'dist'));
const log = (m: string): void => console.log(`[${new Date().toISOString().slice(11, 19)}] ${m}`);

const store = new RoomStore(DATA_DIR);
const removed = store.cleanup(14);
if (removed) log(`удалено устаревших сохранений комнат: ${removed}`);
const manager = new RoomManager(store, log);
manager.start();

const MIME: Record<string, string> = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.svg': 'image/svg+xml', '.glb': 'model/gltf-binary', '.gltf': 'model/gltf+json',
  '.fbx': 'application/octet-stream', '.ico': 'image/x-icon', '.woff2': 'font/woff2', '.map': 'application/json', '.wasm': 'application/wasm', '.webmanifest': 'application/manifest+json',
};

function cors(req: IncomingMessage, res: ServerResponse): void {
  const origin = req.headers.origin;
  if (!origin) return;
  if (!ALLOWED.length || ALLOWED.includes(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
  }
}

function json(res: ServerResponse, code: number, body: unknown): void {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(body));
}

function serveStatic(req: IncomingMessage, res: ServerResponse): boolean {
  if (!existsSync(DIST)) return false;
  const url = new URL(req.url ?? '/', 'http://x');
  let rel = normalize(decodeURIComponent(url.pathname)).replace(/^(\.\.[/\\])+/, '');
  if (rel.endsWith('/') || rel === '') rel = join(rel, 'index.html');
  let file = join(DIST, rel);
  if (!file.startsWith(DIST)) { res.writeHead(403).end(); return true; }
  if (!existsSync(file) || statSync(file).isDirectory()) file = join(DIST, 'index.html');
  try {
    let data = readFileSync(file);
    const long = /\.(glb|png|js|css|woff2)$/.test(file) && file.includes('assets') ;
    const headers: Record<string, string> = { 'Content-Type': MIME[extname(file)] ?? 'application/octet-stream', 'Cache-Control': long ? 'public, max-age=31536000, immutable' : 'no-cache' };
    // сжатие текстовых файлов (JS/CSS/HTML/JSON) — важно для мобильного интернета
    if (/\.(js|css|html|json|svg|webmanifest)$/.test(file) && String(req.headers['accept-encoding'] ?? '').includes('gzip')) {
      data = gzipSync(data); headers['Content-Encoding'] = 'gzip'; headers['Vary'] = 'Accept-Encoding';
    }
    res.writeHead(200, headers);
    res.end(data);
  } catch { res.writeHead(404).end(); }
  return true;
}

const server = createServer((req, res) => {
  cors(req, res);
  if (req.method === 'OPTIONS') { res.writeHead(204, { 'Access-Control-Allow-Methods': 'GET, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type' }).end(); return; }
  const path = (req.url ?? '/').split('?')[0];
  if (path === '/health') { json(res, 200, { ok: true, protocol: PROTOCOL_VERSION, ...manager.stats() }); return; }
  if (path === '/api/rooms') { json(res, 200, { rooms: manager.listOpen() }); return; }
  if (req.method === 'GET' && serveStatic(req, res)) return;
  json(res, 404, { error: 'not found' });
});

const conns = new Set<WsConn>();
server.on('upgrade', (req, socket) => {
  const conn = acceptUpgrade(req, socket, ALLOWED);
  if (!conn) return;
  conns.add(conn);
  conn.on('close', () => conns.delete(conn));
  manager.attach(conn);
});

// проверка «живости» соединений: ping каждые 15 с, обрыв после 45 с тишины
setInterval(() => {
  for (const c of conns) {
    if (Date.now() - c.lastSeen > 45_000) c.terminate(); else c.ping();
  }
}, 15_000).unref();

server.listen(PORT, HOST, () => {
  log(`TIDEBOUND server: http://${HOST}:${PORT}  (данные: ${DATA_DIR})`);
  if (existsSync(DIST)) log(`клиент из ${DIST} доступен по тому же адресу`);
});

function shutdown(sig: string): void {
  log(`${sig}: сохраняю комнаты и закрываю соединения…`);
  manager.shutdown();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2500).unref();
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('uncaughtException', (e) => { console.error('uncaughtException', e); });
process.on('unhandledRejection', (e) => { console.error('unhandledRejection', e); });
