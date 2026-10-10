import { CONFIG } from '../core/config';

/** Формат сохранения одиночной игры. `version` меняется при несовместимых изменениях. */
export interface SaveData {
  version: number;
  savedAt: number;
  player: { x: number; y: number; z: number; yaw: number; pitch: number };
  vitals: unknown;
  inventory: unknown;
  hotbarIndex: number;
  time: { seconds: number; day: number };
  weather: string;
  world: unknown;
}

const C = CONFIG.save;

function openDb(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    if (typeof indexedDB === 'undefined') { reject(new Error('IndexedDB недоступна')); return; }
    const req = indexedDB.open(C.dbName, 1);
    req.onupgradeneeded = () => req.result.createObjectStore(C.store);
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error ?? new Error('IndexedDB error'));
  });
}

/** Сохранения в IndexedDB; при недоступности — резервно в localStorage. */
export class SaveManager {
  async save(data: SaveData): Promise<'indexeddb' | 'localstorage'> {
    try {
      const db = await openDb();
      await new Promise<void>((res, rej) => {
        const tx = db.transaction(C.store, 'readwrite');
        tx.objectStore(C.store).put(data, C.slot);
        tx.oncomplete = () => res();
        tx.onerror = () => rej(tx.error);
        tx.onabort = () => rej(tx.error);
      });
      db.close();
      return 'indexeddb';
    } catch (e) {
      console.warn('[save] IndexedDB не сработала, пробуем localStorage', e);
      localStorage.setItem(C.fallbackKey, JSON.stringify(data));
      return 'localstorage';
    }
  }

  async load(): Promise<SaveData | null> {
    let raw: unknown = null;
    try {
      const db = await openDb();
      raw = await new Promise<unknown>((res, rej) => {
        const r = db.transaction(C.store, 'readonly').objectStore(C.store).get(C.slot);
        r.onsuccess = () => res(r.result);
        r.onerror = () => rej(r.error);
      });
      db.close();
    } catch { /* пробуем резерв */ }
    if (!raw) {
      try {
        const s = localStorage.getItem(C.fallbackKey);
        if (s) raw = JSON.parse(s);
      } catch { raw = null; }
    }
    return this.validate(raw);
  }

  /** Проверка структуры: повреждённое сохранение не должно ронять игру. */
  validate(raw: unknown): SaveData | null {
    if (!raw || typeof raw !== 'object') return null;
    const d = raw as Partial<SaveData>;
    if (d.version !== C.version) return null;
    const p = d.player;
    if (!p || ![p.x, p.y, p.z, p.yaw].every((n) => typeof n === 'number' && Number.isFinite(n))) return null;
    if (!d.time || typeof d.time.seconds !== 'number' || typeof d.time.day !== 'number') return null;
    if (!d.inventory || typeof d.inventory !== 'object') return null;
    return d as SaveData;
  }

  async clear(): Promise<void> {
    try {
      const db = await openDb();
      db.transaction(C.store, 'readwrite').objectStore(C.store).delete(C.slot);
      db.close();
    } catch { /* ignore */ }
    try { localStorage.removeItem(C.fallbackKey); } catch { /* ignore */ }
  }
}
