/** Таблицы улова (общие для клиента и будущего серверного расчёта). */

export type CatchKind = 'fish' | 'junk' | 'container';

export interface CatchEntry {
  id: string; // id предмета
  kind: CatchKind;
  weight: number; // базовый вес выбора
  /** Минимальный/максимальный размер в кг для отображения. */
  kg: [number, number];
  /** Сложность вываживания 0.5..2. */
  fight: number;
  /** Предпочтение по глубине: 0 — мелко, 1 — глубоко. */
  depth: number;
  night?: number; // множитель ночью
  storm?: number; // множитель в шторм
  bait?: Partial<Record<string, number>>;
  xp: number;
  /** Для 'container' — таблица лута. */
  lootTable?: string;
}

export const CATCH_TABLE: CatchEntry[] = [
  { id: 'fish_common', kind: 'fish', weight: 40, kg: [0.3, 1.8], fight: 0.7, depth: 0.3, xp: 8, bait: { bait_worm: 1.4, bait_crumb: 1.3 } },
  { id: 'fish_flat', kind: 'fish', weight: 22, kg: [0.4, 2.2], fight: 0.8, depth: 0.2, xp: 10, bait: { bait_worm: 1.3 } },
  { id: 'fish_tang', kind: 'fish', weight: 14, kg: [0.2, 0.9], fight: 0.9, depth: 0.3, xp: 14, bait: { bait_crumb: 1.4, bait_lure: 1.2 } },
  { id: 'fish_tuna', kind: 'fish', weight: 7, kg: [4, 14], fight: 1.5, depth: 0.9, xp: 30, night: 1.3, bait: { bait_lure: 2.2 } },
  { id: 'fish_lion', kind: 'fish', weight: 4, kg: [0.6, 1.4], fight: 1.2, depth: 0.6, xp: 28, night: 1.5, bait: { bait_lure: 2 } },
  { id: 'fish_puffer', kind: 'fish', weight: 3, kg: [0.5, 1.2], fight: 1.1, depth: 0.5, xp: 24, bait: { bait_lure: 1.5 } },
  { id: 'fish_angler', kind: 'fish', weight: 1.2, kg: [2, 6], fight: 1.7, depth: 1, xp: 55, night: 2.4, storm: 1.5, bait: { bait_lure: 2 } },
  { id: 'fish_shark', kind: 'fish', weight: 0.8, kg: [8, 30], fight: 2, depth: 1, xp: 80, storm: 2, bait: { bait_lure: 2.5 } },
  { id: 'junk_boot', kind: 'junk', weight: 7, kg: [0.3, 0.8], fight: 0.4, depth: 0.2, xp: 2 },
  { id: 'scrap', kind: 'junk', weight: 9, kg: [0.5, 3], fight: 0.6, depth: 0.4, xp: 4, storm: 1.8 },
  { id: 'wood', kind: 'junk', weight: 8, kg: [1, 4], fight: 0.5, depth: 0.2, xp: 3, storm: 2 },
  { id: 'bottle_note', kind: 'junk', weight: 1.5, kg: [0.2, 0.3], fight: 0.3, depth: 0.3, xp: 12 },
  { id: 'pearl', kind: 'junk', weight: 0.4, kg: [0.05, 0.05], fight: 0.5, depth: 0.8, xp: 40 },
  { id: 'crate', kind: 'container', weight: 2.5, kg: [6, 12], fight: 1.1, depth: 0.5, xp: 20, storm: 2.5, lootTable: 'stash_tools' },
];

export const BAIT_BITE: Record<string, number> = { none: 0.55, bait_worm: 1, bait_crumb: 0.9, bait_lure: 1.25 };

/** Модификатор частоты поклёвок по времени суток (рассвет/закат — лучше). */
export function biteTimeFactor(clock: number): number {
  if ((clock >= 5 && clock <= 8) || (clock >= 18 && clock <= 22)) return 1.35;
  if (clock >= 22 || clock < 4) return 0.9;
  return 1;
}
