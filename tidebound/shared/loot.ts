import { makeRng } from './terrain';

export interface LootEntry { id: string; min: number; max: number; chance: number }

export const LOOT_TABLES: Record<string, LootEntry[]> = {
  stash_small: [
    { id: 'bread', min: 1, max: 3, chance: 0.8 }, { id: 'water', min: 1, max: 2, chance: 0.7 },
    { id: 'rope', min: 1, max: 2, chance: 0.5 }, { id: 'bait_worm', min: 3, max: 8, chance: 0.6 },
    { id: 'nails', min: 4, max: 12, chance: 0.5 },
  ],
  stash_tools: [
    { id: 'scrap', min: 2, max: 6, chance: 0.9 }, { id: 'metal_part', min: 1, max: 3, chance: 0.6 },
    { id: 'nails', min: 8, max: 20, chance: 0.8 }, { id: 'cloth', min: 1, max: 3, chance: 0.5 },
    { id: 'line', min: 1, max: 2, chance: 0.5 },
  ],
  stash_food: [
    { id: 'fish_dried', min: 2, max: 4, chance: 0.9 }, { id: 'water', min: 1, max: 3, chance: 0.8 },
    { id: 'bread', min: 2, max: 4, chance: 0.7 }, { id: 'bait_lure', min: 1, max: 2, chance: 0.4 },
  ],
  stash_rare: [
    { id: 'metal_part', min: 2, max: 5, chance: 0.9 }, { id: 'bait_lure', min: 1, max: 3, chance: 0.8 },
    { id: 'rod_good', min: 1, max: 1, chance: 0.25 }, { id: 'pearl', min: 1, max: 1, chance: 0.3 },
    { id: 'fuel', min: 1, max: 1, chance: 0.4 },
  ],
};

/** Детерминированно (по seed) бросает лут из таблицы. */
export function rollLoot(table: string, seed: number): { id: string; count: number }[] {
  const rng = makeRng(seed);
  const out: { id: string; count: number }[] = [];
  for (const e of LOOT_TABLES[table] ?? []) {
    if (rng() <= e.chance) out.push({ id: e.id, count: e.min + Math.floor(rng() * (e.max - e.min + 1)) });
  }
  if (!out.length) out.push({ id: 'bread', count: 1 });
  return out;
}

/** Канонический список тайников мира: id → таблица лута (сервер проверяет по нему). */
export const STASH_TABLES: Record<string, string> = {
  'stash-garage': 'stash_tools',
  'stash-lighthouse': 'stash_rare',
  'stash-workshop': 'stash_tools',
  'stash-dock': 'stash_small',
  'stash-hill': 'stash_rare',
  'stash-quarry': 'stash_tools',
  'stash-forest': 'stash_food',
  'stash-beach': 'stash_food',
  'stash-north': 'stash_small',
};

export function hashString(s: string): number {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}
