export type Quality = 'low' | 'medium' | 'high';
export type Difficulty = 'easy' | 'normal' | 'hard';

export interface Settings {
  language: 'ru';
  mouseSensitivity: number;
  cameraBob: boolean;
  wheelSwitch: boolean;
  showHints: boolean;
  difficulty: Difficulty;
  autosaveMinutes: number;
  quality: Quality;
  fov: number;
  shadows: boolean;
  masterVolume: number;
  effectsVolume: number;
  ambientVolume: number;
  muted: boolean;
  hudScale: number;
  showMinimap: boolean;
  showFps: boolean;
  playerName: string;
}

export const DEFAULT_SETTINGS: Settings = {
  language: 'ru',
  mouseSensitivity: 5,
  cameraBob: true,
  wheelSwitch: true,
  showHints: true,
  difficulty: 'normal',
  autosaveMinutes: 5,
  quality: 'medium',
  fov: 75,
  shadows: true,
  masterVolume: 0.7,
  effectsVolume: 0.9,
  ambientVolume: 0.8,
  muted: false,
  hudScale: 1,
  showMinimap: true,
  showFps: false,
  playerName: '',
};

import { isTouchDevice } from '../input/device';

const KEY = 'tidebound.settings.v1';

class SettingsStore {
  value: Settings = { ...DEFAULT_SETTINGS };
  private listeners = new Set<(s: Settings) => void>();

  constructor() {
    try {
      const raw = localStorage.getItem(KEY);
      if (raw) this.value = { ...DEFAULT_SETTINGS, ...(JSON.parse(raw) as Partial<Settings>) };
      else if (isTouchDevice) this.value = { ...DEFAULT_SETTINGS, quality: 'low', shadows: false, showMinimap: true, wheelSwitch: false };
    } catch { /* повреждённые настройки — берём значения по умолчанию */ }
  }

  subscribe(fn: (s: Settings) => void): void { this.listeners.add(fn); }

  set(patch: Partial<Settings>): void {
    this.value = { ...this.value, ...patch };
    this.persist();
    for (const fn of this.listeners) fn(this.value);
  }

  reset(): void {
    this.value = { ...DEFAULT_SETTINGS };
    this.persist();
    for (const fn of this.listeners) fn(this.value);
  }

  private persist(): void {
    try { localStorage.setItem(KEY, JSON.stringify(this.value)); } catch { /* ignore */ }
  }
}

export const settings = new SettingsStore();

export const QUALITY_PRESETS: Record<Quality, { shadowMap: number; pixelRatio: number; rain: number; trees: number; grass: number; waterSeg: number; lights: number }> = {
  low: { shadowMap: 0, pixelRatio: 0.75, rain: 800, trees: 0.55, grass: 0.3, waterSeg: 110, lights: 2 },
  medium: { shadowMap: 1024, pixelRatio: 1, rain: 2500, trees: 0.8, grass: 0.65, waterSeg: 150, lights: 4 },
  high: { shadowMap: 2048, pixelRatio: 1.25, rain: 5000, trees: 1, grass: 1, waterSeg: 220, lights: 6 },
};

export const DIFFICULTY: Record<Difficulty, { hunger: number; thirst: number; damage: number }> = {
  easy: { hunger: 0.6, thirst: 0.6, damage: 0.7 },
  normal: { hunger: 1, thirst: 1, damage: 1 },
  hard: { hunger: 1.5, thirst: 1.5, damage: 1.4 },
};
