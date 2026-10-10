/** Рецепты крафта (клиент + будущий сервер: сервер проверяет те же правила). */
import type { ItemStack } from './inventory';

export type Station = 'hand' | 'workbench';

export interface Recipe {
  id: string;
  name: string;
  station: Station;
  inputs: ItemStack[];
  output: ItemStack;
  xp: number;
  minLevel?: number;
  group: 'Материалы' | 'Инструменты' | 'Рыбалка';
}

export const STATION_NAMES: Record<Station, string> = { hand: 'Руками', workbench: 'Верстак' };

const R = (r: Recipe): Recipe => r;

export const RECIPES: Recipe[] = [
  // руками
  R({ id: 'plank_hand', name: 'Доски (вручную)', station: 'hand', group: 'Материалы', inputs: [{ id: 'wood', count: 2 }], output: { id: 'plank', count: 1 }, xp: 1 }),
  R({ id: 'line_hand', name: 'Леска', station: 'hand', group: 'Рыбалка', inputs: [{ id: 'cloth', count: 1 }], output: { id: 'line', count: 2 }, xp: 1 }),
  R({ id: 'rope_hand', name: 'Верёвка', station: 'hand', group: 'Материалы', inputs: [{ id: 'cloth', count: 3 }], output: { id: 'rope', count: 1 }, xp: 1 }),
  R({ id: 'crumb_hand', name: 'Хлебный мякиш', station: 'hand', group: 'Рыбалка', inputs: [{ id: 'bread', count: 1 }], output: { id: 'bait_crumb', count: 3 }, xp: 1 }),
  // верстак
  R({ id: 'plank', name: 'Доски', station: 'workbench', group: 'Материалы', inputs: [{ id: 'wood', count: 2 }], output: { id: 'plank', count: 3 }, xp: 2 }),
  R({ id: 'nails', name: 'Гвозди', station: 'workbench', group: 'Материалы', inputs: [{ id: 'scrap', count: 1 }], output: { id: 'nails', count: 10 }, xp: 2 }),
  R({ id: 'metal_part', name: 'Металлические детали', station: 'workbench', group: 'Материалы', inputs: [{ id: 'scrap', count: 3 }], output: { id: 'metal_part', count: 1 }, xp: 3 }),
  R({ id: 'rod_basic', name: 'Простая удочка', station: 'workbench', group: 'Инструменты', inputs: [{ id: 'plank', count: 2 }, { id: 'line', count: 1 }, { id: 'rope', count: 1 }], output: { id: 'rod_basic', count: 1 }, xp: 6 }),
  R({ id: 'rod_good', name: 'Улучшенная удочка', station: 'workbench', group: 'Инструменты', inputs: [{ id: 'plank', count: 3 }, { id: 'line', count: 2 }, { id: 'metal_part', count: 2 }, { id: 'rope', count: 1 }], output: { id: 'rod_good', count: 1 }, xp: 15, minLevel: 2 }),
  R({ id: 'axe', name: 'Топор', station: 'workbench', group: 'Инструменты', inputs: [{ id: 'plank', count: 2 }, { id: 'metal_part', count: 1 }, { id: 'rope', count: 1 }], output: { id: 'axe', count: 1 }, xp: 6 }),
  R({ id: 'hammer', name: 'Молот', station: 'workbench', group: 'Инструменты', inputs: [{ id: 'plank', count: 2 }, { id: 'metal_part', count: 1 }, { id: 'nails', count: 4 }], output: { id: 'hammer', count: 1 }, xp: 6 }),
  R({ id: 'lure', name: 'Блесна', station: 'workbench', group: 'Рыбалка', inputs: [{ id: 'metal_part', count: 1 }, { id: 'line', count: 1 }], output: { id: 'bait_lure', count: 3 }, xp: 4 }),
];

export function recipesFor(station: Station): Recipe[] {
  return RECIPES.filter((r) => r.station === 'hand' || r.station === station);
}

export function getRecipe(id: string): Recipe | undefined {
  return RECIPES.find((r) => r.id === id);
}
