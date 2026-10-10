import { DAY_MINUTES } from '../../shared/net';

/** Игровые константы. Все значения настраиваемые. */
export const CONFIG = {
  day: { dayMinutes: DAY_MINUTES.day, duskMinutes: DAY_MINUTES.dusk, nightMinutes: DAY_MINUTES.night },
  player: {
    eyeHeight: 1.65,
    crouchEye: 1.15,
    radius: 0.42,
    walk: 4.2,
    sprint: 7.0,
    crouch: 2.1,
    jump: 7.0,
    gravity: 22,
    reach: 3.4,
    swimDepth: 1.2,
  },
  survival: {
    hungerPerMin: 1.6,
    thirstPerMin: 2.2,
    staminaDrain: 16,
    staminaRegen: 14,
    healthRegenPerSec: 0.6,
    starvingDamagePerSec: 0.8,
  },
  save: { dbName: 'tidebound', store: 'saves', slot: 'single', version: 1, fallbackKey: 'tidebound-save-v1' },
};
