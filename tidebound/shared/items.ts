/** Общая база предметов (клиент + будущий сервер). */

export type ItemCategory =
  | 'material' | 'food' | 'drink' | 'bait' | 'tool' | 'weapon'
  | 'building' | 'vehicle' | 'rare' | 'quest' | 'junk';
export type Rarity = 'common' | 'uncommon' | 'rare' | 'epic';
export type HeldKind = 'none' | 'rod' | 'axe' | 'hammer' | 'food' | 'drink';

export interface ItemDef {
  id: string;
  name: string;
  description: string;
  category: ItemCategory;
  /** Временная иконка интерфейса (эмодзи). В игровом мире эмодзи не используются. */
  icon: string;
  stack: number;
  weight: number;
  rarity: Rarity;
  held?: HeldKind;
  food?: { hunger: number; thirst?: number; health?: number };
  drink?: { thirst: number; health?: number };
  tool?: { power: number; kind: 'axe' | 'hammer' | 'rod'; rodTier?: number };
  /** Рыба: имя модели из набора Cute Fish Pack. */
  fishModel?: string;
}

const defs: ItemDef[] = [
  { id: 'wood', name: 'Дерево', description: 'Брёвна и ветки. Основа для досок и костров.', category: 'material', icon: '🪵', stack: 50, weight: 0.5, rarity: 'common' },
  { id: 'plank', name: 'Доски', description: 'Обработанные доски для построек и инструментов.', category: 'material', icon: '🟫', stack: 50, weight: 0.4, rarity: 'common' },
  { id: 'stone', name: 'Камень', description: 'Прочный камень из карьера и с побережья.', category: 'material', icon: '🪨', stack: 50, weight: 0.8, rarity: 'common' },
  { id: 'scrap', name: 'Металлолом', description: 'Ржавые обломки. Можно переплавить на детали.', category: 'material', icon: '🔩', stack: 40, weight: 0.7, rarity: 'common' },
  { id: 'metal_part', name: 'Металлические детали', description: 'Годные детали для инструментов и машины.', category: 'material', icon: '⚙️', stack: 30, weight: 0.6, rarity: 'uncommon' },
  { id: 'nails', name: 'Гвозди', description: 'Пригодятся при строительстве.', category: 'material', icon: '📌', stack: 100, weight: 0.05, rarity: 'common' },
  { id: 'rope', name: 'Верёвка', description: 'Прочная пеньковая верёвка.', category: 'material', icon: '🧵', stack: 30, weight: 0.2, rarity: 'common' },
  { id: 'line', name: 'Леска', description: 'Запасная рыболовная леска.', category: 'material', icon: '〰️', stack: 20, weight: 0.05, rarity: 'common' },
  { id: 'cloth', name: 'Ткань', description: 'Обрывки парусины и одежды.', category: 'material', icon: '🧶', stack: 30, weight: 0.1, rarity: 'common' },
  { id: 'hide', name: 'Шкура', description: 'Шкура животного.', category: 'material', icon: '🟤', stack: 20, weight: 0.8, rarity: 'common' },
  { id: 'fuel', name: 'Топливо', description: 'Канистра топлива для будущей машины.', category: 'vehicle', icon: '⛽', stack: 10, weight: 2, rarity: 'uncommon' },

  { id: 'fish_raw', name: 'Сырая рыба', description: 'Лучше приготовить на костре.', category: 'food', icon: '🐟', stack: 20, weight: 0.6, rarity: 'common', held: 'food', food: { hunger: 8, health: -3 } },
  { id: 'fish_cooked', name: 'Жареная рыба', description: 'Сытная и вкусная.', category: 'food', icon: '🍣', stack: 20, weight: 0.5, rarity: 'common', held: 'food', food: { hunger: 30, health: 4 } },
  { id: 'fish_dried', name: 'Вяленая рыба', description: 'Лёгкий запас еды в дорогу.', category: 'food', icon: '🐠', stack: 20, weight: 0.3, rarity: 'common', held: 'food', food: { hunger: 22 } },
  { id: 'meat_raw', name: 'Сырое мясо', description: 'Лучше приготовить на костре.', category: 'food', icon: '🥩', stack: 20, weight: 0.7, rarity: 'common', held: 'food', food: { hunger: 10, health: -5 } },
  { id: 'meat_cooked', name: 'Жареное мясо', description: 'Горячее и сытное.', category: 'food', icon: '🍖', stack: 20, weight: 0.6, rarity: 'common', held: 'food', food: { hunger: 38, health: 5 } },
  { id: 'bread', name: 'Сухарь', description: 'Чёрствый, но съедобный.', category: 'food', icon: '🍞', stack: 20, weight: 0.2, rarity: 'common', held: 'food', food: { hunger: 14 } },
  { id: 'water', name: 'Бутылка воды', description: 'Чистая питьевая вода.', category: 'drink', icon: '💧', stack: 10, weight: 0.5, rarity: 'common', held: 'drink', drink: { thirst: 35 } },
  { id: 'water_dirty', name: 'Мутная вода', description: 'Лучше вскипятить на костре.', category: 'drink', icon: '🫗', stack: 10, weight: 0.5, rarity: 'common', held: 'drink', drink: { thirst: 20, health: -6 } },

  { id: 'bait_worm', name: 'Червь', description: 'Классическая наживка. Рыба берёт охотно.', category: 'bait', icon: '🪱', stack: 50, weight: 0.02, rarity: 'common' },
  { id: 'bait_crumb', name: 'Хлебный мякиш', description: 'Хорошо работает у берега.', category: 'bait', icon: '🥖', stack: 50, weight: 0.02, rarity: 'common' },
  { id: 'bait_lure', name: 'Блесна', description: 'Привлекает хищную и редкую рыбу.', category: 'bait', icon: '🎣', stack: 20, weight: 0.05, rarity: 'uncommon' },

  { id: 'rod_basic', name: 'Простая удочка', description: 'Лёгкая удочка для начинающих.', category: 'tool', icon: '🎣', stack: 1, weight: 1, rarity: 'common', held: 'rod', tool: { power: 1, kind: 'rod', rodTier: 1 } },
  { id: 'rod_good', name: 'Улучшенная удочка', description: 'Прочнее и точнее. Лучше для глубины.', category: 'tool', icon: '🎣', stack: 1, weight: 1.2, rarity: 'uncommon', held: 'rod', tool: { power: 2, kind: 'rod', rodTier: 3 } },
  { id: 'axe', name: 'Топор', description: 'Рубит деревья.', category: 'tool', icon: '🪓', stack: 1, weight: 2, rarity: 'common', held: 'axe', tool: { power: 1, kind: 'axe' } },
  { id: 'hammer', name: 'Молот', description: 'Дробит камни и нужен для строительства.', category: 'tool', icon: '🔨', stack: 1, weight: 2.5, rarity: 'common', held: 'hammer', tool: { power: 1, kind: 'hammer' } },

  { id: 'fish_common', name: 'Морской окунь', description: 'Обычная морская рыба.', category: 'food', icon: '🐟', stack: 20, weight: 0.8, rarity: 'common', held: 'food', food: { hunger: 9, health: -3 }, fishModel: 'RedSnapper' },
  { id: 'fish_flat', name: 'Камбала', description: 'Плоская донная рыба.', category: 'food', icon: '🐟', stack: 20, weight: 0.9, rarity: 'common', held: 'food', food: { hunger: 10, health: -3 }, fishModel: 'Flatfish' },
  { id: 'fish_tang', name: 'Голубой хирург', description: 'Яркая рыба с прибрежных скал.', category: 'food', icon: '🐠', stack: 20, weight: 0.5, rarity: 'uncommon', held: 'food', food: { hunger: 9, health: -2 }, fishModel: 'BlueTang' },
  { id: 'fish_tuna', name: 'Тунец', description: 'Крупная рыба с глубины.', category: 'food', icon: '🐟', stack: 10, weight: 3, rarity: 'rare', held: 'food', food: { hunger: 18, health: -3 }, fishModel: 'Tuna' },
  { id: 'fish_lion', name: 'Крылатка', description: 'Редкая и красивая рыба.', category: 'rare', icon: '🐠', stack: 10, weight: 1, rarity: 'rare', fishModel: 'Lionfish' },
  { id: 'fish_angler', name: 'Удильщик', description: 'Глубоководный хищник. Настоящий трофей.', category: 'rare', icon: '🐡', stack: 5, weight: 2, rarity: 'epic', fishModel: 'Anglerfish' },
  { id: 'fish_puffer', name: 'Рыба-фугу', description: 'Необычный трофей.', category: 'rare', icon: '🐡', stack: 5, weight: 1, rarity: 'rare', fishModel: 'Puffer' },
  { id: 'fish_shark', name: 'Малая акула', description: 'Опасный и очень редкий улов.', category: 'rare', icon: '🦈', stack: 3, weight: 6, rarity: 'epic', fishModel: 'Shark' },

  { id: 'bottle_note', name: 'Бутылка с запиской', description: 'Кто-то просил о помощи… или шутил.', category: 'quest', icon: '🍾', stack: 5, weight: 0.3, rarity: 'rare' },
  { id: 'junk_boot', name: 'Старый сапог', description: 'Бесполезный мусор.', category: 'junk', icon: '🥾', stack: 10, weight: 0.4, rarity: 'common' },
  { id: 'pearl', name: 'Жемчужина', description: 'Редкая находка.', category: 'rare', icon: '🦪', stack: 10, weight: 0.05, rarity: 'epic' },
];

export const ITEMS: Record<string, ItemDef> = Object.fromEntries(defs.map((d) => [d.id, d]));

export function getItem(id: string): ItemDef {
  const d = ITEMS[id];
  if (!d) throw new Error(`Неизвестный предмет: ${id}`);
  return d;
}

export const RARITY_COLORS: Record<Rarity, string> = {
  common: '#9db4d6', uncommon: '#58d68d', rare: '#5dade2', epic: '#c39bd3',
};
export const RARITY_NAMES: Record<Rarity, string> = {
  common: 'Обычный', uncommon: 'Необычный', rare: 'Редкий', epic: 'Эпический',
};
export const CATEGORY_NAMES: Record<ItemCategory, string> = {
  material: 'Материал', food: 'Еда', drink: 'Напиток', bait: 'Наживка', tool: 'Инструмент',
  weapon: 'Оружие', building: 'Строительство', vehicle: 'Компонент машины', rare: 'Редкая находка',
  quest: 'Квестовый предмет', junk: 'Мусор',
};
