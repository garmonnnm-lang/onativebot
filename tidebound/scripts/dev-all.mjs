// Запускает сервер комнат (порт 3001) и Vite (порт 5173) одной командой: npm run dev:all
import { spawn } from 'node:child_process';

const isWin = process.platform === 'win32';
const run = (name, args) => {
  const p = spawn(isWin ? 'npm.cmd' : 'npm', args, { stdio: 'inherit', shell: isWin });
  p.on('exit', (code) => { console.log(`[${name}] завершился (${code})`); shutdown(); });
  return p;
};
const procs = [run('server', ['run', 'server:dev']), run('client', ['run', 'dev'])];
let closing = false;
function shutdown() {
  if (closing) return;
  closing = true;
  for (const p of procs) { try { p.kill(); } catch { /* уже завершён */ } }
  setTimeout(() => process.exit(0), 300);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
