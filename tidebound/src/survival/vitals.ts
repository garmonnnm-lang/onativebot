import { CONFIG } from '../core/config';
import { DIFFICULTY, settings } from '../core/settings';

export interface VitalsSave {
  health: number; hunger: number; thirst: number; stamina: number; level: number; xp: number;
}

export function xpForLevel(level: number): number {
  return Math.round(60 * Math.pow(level, 1.35));
}

/** Здоровье, голод, жажда, выносливость, уровень и опыт. Значения 0..100. */
export class Vitals {
  health = 100;
  hunger = 100;
  thirst = 100;
  stamina = 100;
  level = 1;
  xp = 0;
  private regenDelay = 0;

  get dead(): boolean { return this.health <= 0; }

  reset(): void {
    this.health = 100; this.hunger = 80; this.thirst = 80; this.stamina = 100;
  }

  spendStamina(n: number): void {
    this.stamina = Math.max(0, this.stamina - n);
    this.regenDelay = 1.2;
  }

  /** Возвращает причину смерти (если игрок погиб на этом шаге). */
  update(dt: number, ctx: { sprinting: boolean; cold: number }): string | null {
    const S = CONFIG.survival;
    const d = DIFFICULTY[settings.value.difficulty];
    this.hunger = Math.max(0, this.hunger - (S.hungerPerMin / 60) * d.hunger * dt);
    this.thirst = Math.max(0, this.thirst - (S.thirstPerMin / 60) * d.thirst * dt);

    this.regenDelay = Math.max(0, this.regenDelay - dt);
    if (!ctx.sprinting && this.regenDelay <= 0) {
      const slow = this.hunger < 10 || this.thirst < 10 ? 0.4 : 1;
      this.stamina = Math.min(100, this.stamina + S.staminaRegen * slow * dt);
    }

    let cause: string | null = null;
    if (this.hunger <= 0) { this.health -= S.starvingDamagePerSec * d.damage * dt; cause = 'Вы умерли от голода.'; }
    if (this.thirst <= 0) { this.health -= S.starvingDamagePerSec * 1.3 * d.damage * dt; cause = 'Вы умерли от обезвоживания.'; }
    if (!cause && this.hunger > 40 && this.thirst > 40 && this.health < 100) {
      this.health = Math.min(100, this.health + S.healthRegenPerSec * dt);
    }
    if (this.health <= 0) { this.health = 0; return cause ?? 'Вы погибли.'; }
    return null;
  }

  damage(n: number): void {
    this.health = Math.max(0, this.health - n * DIFFICULTY[settings.value.difficulty].damage);
  }

  heal(n: number): void { this.health = Math.min(100, this.health + n); }

  /** Добавляет опыт; возвращает число полученных уровней. */
  addXp(n: number): number {
    this.xp += Math.max(0, Math.round(n));
    let gained = 0;
    while (this.xp >= xpForLevel(this.level)) {
      this.xp -= xpForLevel(this.level);
      this.level++;
      gained++;
    }
    return gained;
  }

  serialize(): VitalsSave {
    return { health: this.health, hunger: this.hunger, thirst: this.thirst, stamina: this.stamina, level: this.level, xp: this.xp };
  }

  restore(v: Partial<VitalsSave> | undefined): void {
    if (!v) return;
    const n = (x: unknown, d: number, lo: number, hi: number) => (typeof x === 'number' && Number.isFinite(x) ? Math.min(hi, Math.max(lo, x)) : d);
    this.health = n(v.health, 100, 1, 100);
    this.hunger = n(v.hunger, 80, 0, 100);
    this.thirst = n(v.thirst, 80, 0, 100);
    this.stamina = n(v.stamina, 100, 0, 100);
    this.level = Math.floor(n(v.level, 1, 1, 99));
    this.xp = Math.floor(n(v.xp, 0, 0, 1e7));
  }
}
