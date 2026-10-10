import { CONFIG } from '../core/config';

export type DayPhase = 'day' | 'dusk' | 'night';

export interface TimeState {
  day: number;
  clock: number; // 0..24
  phase: DayPhase;
  cycle: number; // 0..1
  sunDir: [number, number, number];
  elevation: number;
  daylight: number; // 0 ночь … 1 день
  twilight: number; // 0..1 оранжевый оттенок
}

function smooth(a: number, b: number, x: number): number {
  const t = Math.min(1, Math.max(0, (x - a) / (b - a)));
  return t * t * (3 - 2 * t);
}

/**
 * Время мира. В мультиплеере авторитетным источником станет сервер — клиент вызовет setTime().
 */
export class DayNight {
  seconds = CONFIG.day.dayMinutes * 60 * 0.3;
  dayNumber = 1;
  paused = false;
  timeScale = 1;

  get cycleSeconds(): number {
    const d = CONFIG.day;
    return (d.dayMinutes + d.duskMinutes + d.nightMinutes) * 60;
  }

  update(dt: number): void {
    if (this.paused) return;
    this.seconds += dt * this.timeScale;
    while (this.seconds >= this.cycleSeconds) {
      this.seconds -= this.cycleSeconds;
      this.dayNumber++;
    }
  }

  setTime(seconds: number, day: number): void {
    const c = this.cycleSeconds;
    this.seconds = ((seconds % c) + c) % c;
    this.dayNumber = Math.max(1, Math.floor(day) || 1);
  }

  /** Пропуск ночи (сон). false — сейчас день. */
  skipToMorning(): boolean {
    if (this.get().phase === 'day') return false;
    this.seconds = 0;
    this.dayNumber++;
    return true;
  }

  get(): TimeState {
    const { dayMinutes, duskMinutes, nightMinutes } = CONFIG.day;
    const t = this.seconds / 60;
    const duskEnd = dayMinutes + duskMinutes;
    const total = dayMinutes + duskMinutes + nightMinutes;

    let theta: number;
    let phase: DayPhase;
    let clock: number;
    if (t < dayMinutes) {
      phase = 'day';
      const k = t / dayMinutes;
      theta = Math.PI * k;
      clock = 6 + 13 * k;
    } else if (t < duskEnd) {
      phase = 'dusk';
      const k = (t - dayMinutes) / duskMinutes;
      theta = Math.PI + 0.5 * k;
      clock = 19 + 2 * k;
    } else {
      phase = 'night';
      const k = (t - duskEnd) / nightMinutes;
      theta = Math.PI + 0.5 + (Math.PI - 0.5) * k;
      clock = (21 + 9 * k) % 24;
    }

    const elevation = Math.sin(theta);
    const sunDir: [number, number, number] = [Math.cos(theta), elevation, 0.28];
    const len = Math.hypot(sunDir[0], sunDir[1], sunDir[2]);
    sunDir[0] /= len; sunDir[1] /= len; sunDir[2] /= len;

    const daylight = smooth(-0.12, 0.28, elevation);
    const twilight = Math.max(0, 1 - Math.abs(elevation - 0.02) / 0.3);
    return { day: this.dayNumber, clock, phase, cycle: t / total, sunDir, elevation, daylight, twilight };
  }
}

export function formatClock(clock: number): string {
  const h = Math.floor(clock) % 24;
  const m = Math.floor((clock - Math.floor(clock)) * 60);
  return `${h.toString().padStart(2, '0')}:${m.toString().padStart(2, '0')}`;
}
