/** Сохранение комнат в JSON-файлы (DATA_DIR). Запись атомарная: tmp-файл + rename. */
import { existsSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { Privacy, RoomMode, RoomStatus, WeatherKindNet } from '../shared/net';
import { validRoomCode } from '../shared/net';

export interface RoomFile {
  v: 1;
  code: string; name: string; mode: RoomMode; max: number; privacy: Privacy; status: RoomStatus;
  hostTokenHash: string; createdAt: number; savedAt: number;
  time: { seconds: number; day: number };
  weather: WeatherKindNet;
  resources: { key: string; hp: number; kind: 'tree' | 'rock'; respawnIn: number }[];
  opened: string[];
}

export class RoomStore {
  constructor(private dir: string) {
    mkdirSync(dir, { recursive: true });
  }

  private path(code: string): string {
    return join(this.dir, `${code}.json`);
  }

  exists(code: string): boolean {
    return validRoomCode(code) && existsSync(this.path(code));
  }

  load(code: string): RoomFile | null {
    if (!validRoomCode(code)) return null;
    try {
      const f = JSON.parse(readFileSync(this.path(code), 'utf8')) as RoomFile;
      if (f.v !== 1 || f.code !== code || !f.time || !Array.isArray(f.resources)) return null;
      return f;
    } catch { return null; }
  }

  save(f: RoomFile): void {
    try {
      const tmp = this.path(f.code) + '.tmp';
      writeFileSync(tmp, JSON.stringify(f));
      renameSync(tmp, this.path(f.code));
    } catch (e) { console.error('[persist] не удалось сохранить комнату', f.code, e); }
  }

  remove(code: string): void {
    try { rmSync(this.path(code), { force: true }); } catch { /* ignore */ }
  }

  /** Удаляет файлы комнат, к которым не обращались дольше maxAgeDays. */
  cleanup(maxAgeDays: number): number {
    let n = 0;
    for (const name of readdirSync(this.dir)) {
      if (!name.endsWith('.json')) continue;
      const p = join(this.dir, name);
      if (Date.now() - statSync(p).mtimeMs > maxAgeDays * 86_400_000) { rmSync(p, { force: true }); n++; }
    }
    return n;
  }
}
