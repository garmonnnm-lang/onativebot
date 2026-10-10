/**
 * Детерминированная карта Грейхейвена.
 * Модуль не зависит от Three.js: его используют и клиент, и (в этапе 4) сервер
 * для проверки перемещений, спавна животных и размещения построек.
 */

export const WORLD = {
  radius: 150,
  size: 460,
  seaLevel: 0,
  floor: -14,
  seed: 20260,
} as const;

export function hash2(ix: number, iz: number, seed: number = WORLD.seed): number {
  let h = (Math.imul(ix, 374761393) + Math.imul(iz, 668265263) + Math.imul(seed, 2147483647)) | 0;
  h = Math.imul(h ^ (h >>> 13), 1274126177);
  h ^= h >>> 16;
  return (h >>> 0) / 4294967296;
}

function smooth(t: number): number {
  return t * t * (3 - 2 * t);
}

export function valueNoise(x: number, z: number, seed: number = WORLD.seed): number {
  const ix = Math.floor(x);
  const iz = Math.floor(z);
  const fx = smooth(x - ix);
  const fz = smooth(z - iz);
  const a = hash2(ix, iz, seed);
  const b = hash2(ix + 1, iz, seed);
  const c = hash2(ix, iz + 1, seed);
  const d = hash2(ix + 1, iz + 1, seed);
  return a + (b - a) * fx + (c - a) * fz + (a - b - c + d) * fx * fz;
}

export function fbm(x: number, z: number, octaves = 4, seed: number = WORLD.seed): number {
  let amp = 0.5;
  let freq = 1;
  let sum = 0;
  let norm = 0;
  for (let i = 0; i < octaves; i++) {
    sum += valueNoise(x * freq, z * freq, seed + i * 17) * amp;
    norm += amp;
    amp *= 0.5;
    freq *= 2.03;
  }
  return sum / norm;
}

export function smoothstep(a: number, b: number, x: number): number {
  const t = Math.min(1, Math.max(0, (x - a) / (b - a)));
  return t * t * (3 - 2 * t);
}

export function islandRadius(theta: number): number {
  return (
    WORLD.radius +
    22 * Math.sin(3 * theta + 1.1) +
    12 * Math.sin(5 * theta + 2.3) +
    6 * Math.sin(9 * theta + 0.4)
  );
}

interface Bump {
  x: number;
  z: number;
  r: number;
  h: number;
}

const BUMPS: Bump[] = [
  { x: -70, z: -55, r: 52, h: 24 }, // холм
  { x: 78, z: -50, r: 38, h: 9 }, // скальный участок
  { x: 28, z: -168, r: 32, h: 7 }, // мыс маяка
];

function baseHeight(x: number, z: number): number {
  const r = Math.hypot(x, z);
  const theta = Math.atan2(z, x);
  let R = islandRadius(theta);

  const dl = Math.hypot(x - 28, z + 168);
  R += 38 * Math.exp(-(dl * dl) / (2 * 22 * 22));

  const s = R - r;
  let h: number;
  if (s < 0) h = Math.max(WORLD.floor, s * 0.2 - 0.4);
  else if (s < 16) h = -0.4 + (s / 16) * 1.9;
  else h = 1.5 + Math.min(s - 16, 40) * 0.025;

  const n = fbm(x * 0.02, z * 0.02, 4) - 0.5;
  const detail = fbm(x * 0.08, z * 0.08, 2, WORLD.seed + 9) - 0.5;
  if (s > 8) h += n * 6 * smoothstep(8, 40, s) + detail * 0.8;

  for (const b of BUMPS) {
    const d = Math.hypot(x - b.x, z - b.z) / b.r;
    if (d < 1) {
      const k = Math.cos(d * Math.PI * 0.5);
      h += b.h * k * k;
    }
  }

  const dq = Math.hypot(x - 80, z + 25);
  if (dq < 16) h -= 3.2 * (1 - smoothstep(0, 16, dq));

  return Math.max(WORLD.floor, h);
}

export interface Pad {
  x: number;
  z: number;
  r: number;
  blend: number;
  h: number;
}

export interface Landmarks {
  dockStart: { x: number; z: number };
  dockDir: { x: number; z: number };
  dockLength: number;
  dockWidth: number;
  dockDeck: number;
  spawn: { x: number; z: number; yaw: number };
  square: { x: number; z: number };
  workshop: { x: number; z: number; yaw: number };
  garage: { x: number; z: number; yaw: number };
  lighthouse: { x: number; z: number };
  hill: { x: number; z: number };
  quarry: { x: number; z: number };
  beachCamp: { x: number; z: number };
  buildPlots: { x: number; z: number; r: number }[];
  houses: { x: number; z: number; yaw: number; kind: number }[];
  fishingSpots: { x: number; z: number; name: string }[];
}

function findShore(dx: number, dz: number): { x: number; z: number } {
  const len = Math.hypot(dx, dz);
  const ux = dx / len;
  const uz = dz / len;
  let last = { x: 0, z: 0 };
  for (let d = 20; d < 320; d += 0.5) {
    const x = ux * d;
    const z = uz * d;
    if (baseHeight(x, z) < 0.35) return last;
    last = { x, z };
  }
  return last;
}

function buildLandmarks(): { lm: Landmarks; pads: Pad[] } {
  const dockDirRaw = { x: 0.12, z: 1 };
  const dl = Math.hypot(dockDirRaw.x, dockDirRaw.z);
  const dockDir = { x: dockDirRaw.x / dl, z: dockDirRaw.z / dl };
  const shore = findShore(dockDir.x, dockDir.z);
  const dockStart = { x: shore.x - dockDir.x * 4, z: shore.z - dockDir.z * 4 };
  const yawOut = Math.atan2(dockDir.x, dockDir.z);

  const square = { x: shore.x - dockDir.x * 40, z: shore.z - dockDir.z * 40 };
  const perp = { x: dockDir.z, z: -dockDir.x };

  const workshop = { x: square.x - perp.x * 62 - dockDir.x * 8, z: square.z - perp.z * 62 - dockDir.z * 8, yaw: 0 };
  const garage = { x: square.x + perp.x * 62 - dockDir.x * 6, z: square.z + perp.z * 62 - dockDir.z * 6, yaw: 0 };

  const houses: Landmarks['houses'] = [];
  const ring = [
    [-34, -14, 0], [-30, 20, 1], [30, -16, 2], [34, 18, 0],
    [-8, -38, 1], [20, -40, 2], [-52, -34, 2], [52, -36, 1],
  ];
  for (const [ox, oz, kind] of ring) {
    const hx = square.x + ox;
    const hz = square.z + oz;
    houses.push({ x: hx, z: hz, yaw: Math.atan2(square.x - hx, square.z - hz), kind });
  }

  const plots = [
    { x: square.x - 70, z: square.z - 55, r: 14 },
    { x: square.x + 72, z: square.z - 52, r: 14 },
    { x: square.x, z: square.z - 80, r: 16 },
  ];

  const lm: Landmarks = {
    dockStart, dockDir, dockLength: 34, dockWidth: 3.6, dockDeck: 0.85,
    spawn: { x: dockStart.x + dockDir.x * 6, z: dockStart.z + dockDir.z * 6, yaw: yawOut },
    square, workshop, garage,
    lighthouse: { x: 28, z: -178 },
    hill: { x: -70, z: -55 },
    quarry: { x: 80, z: -25 },
    beachCamp: { x: shore.x + perp.x * 26 - dockDir.x * 4, z: shore.z + perp.z * 26 - dockDir.z * 4 },
    buildPlots: plots,
    houses,
    fishingSpots: [],
  };

  const pads: Pad[] = [];
  const addPad = (x: number, z: number, r: number, blend: number, minH = 1.7) => {
    pads.push({ x, z, r, blend, h: Math.max(minH, baseHeight(x, z)) });
  };
  addPad(square.x, square.z, 16, 14);
  for (const h of houses) addPad(h.x, h.z, 8, 8);
  addPad(workshop.x, workshop.z, 11, 9);
  addPad(garage.x, garage.z, 12, 9);
  for (const p of plots) addPad(p.x, p.z, p.r, 10);
  addPad(lm.beachCamp.x, lm.beachCamp.z, 5, 6, 1.1);
  addPad(lm.lighthouse.x, lm.lighthouse.z, 15, 10, 3);
  // двор смотрителя маяка (дом, сараи, ограда стоят к северо-западу от башни)
  addPad(lm.lighthouse.x - 25, lm.lighthouse.z - 16, 14, 9, 2.4);

  return { lm, pads };
}

const built = buildLandmarks();
export const LANDMARKS: Landmarks = built.lm;
export const PADS: Pad[] = built.pads;

export function heightAt(x: number, z: number): number {
  let h = baseHeight(x, z);
  for (const p of PADS) {
    const d = Math.hypot(x - p.x, z - p.z);
    if (d < p.r + p.blend) {
      const t = d <= p.r ? 1 : 1 - smoothstep(0, 1, (d - p.r) / p.blend);
      h = h + (p.h - h) * t;
    }
  }
  return h;
}

export function isLand(x: number, z: number, minHeight = 0.6): boolean {
  return heightAt(x, z) > minHeight;
}

export function slopeAt(x: number, z: number): number {
  const e = 1.2;
  const dx = heightAt(x + e, z) - heightAt(x - e, z);
  const dz = heightAt(x, z + e) - heightAt(x, z - e);
  return Math.hypot(dx, dz) / (2 * e);
}

export function makeRng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Высота волны — совпадает с шейдером воды (src/renderer/water.ts). */
export function waveHeight(x: number, z: number, t: number, amp: number): number {
  return (
    amp *
    (0.55 * Math.sin(x * 0.11 + t * 0.9) +
      0.35 * Math.sin(z * 0.15 - t * 1.1 + 1.7) +
      0.25 * Math.sin((x + z) * 0.07 + t * 0.6))
  );
}

// Места для рыбалки: конец пирса, по бокам от пирса и у мыса маяка.
{
  const L = LANDMARKS;
  const perp = { x: L.dockDir.z, z: -L.dockDir.x };
  const end = { x: L.dockStart.x + L.dockDir.x * L.dockLength, z: L.dockStart.z + L.dockDir.z * L.dockLength };
  L.fishingSpots.push(
    { x: end.x, z: end.z, name: 'Конец причала' },
    { x: L.beachCamp.x + L.dockDir.x * 8, z: L.beachCamp.z + L.dockDir.z * 8, name: 'Пляж' },
    { x: L.lighthouse.x, z: L.lighthouse.z + 40, name: 'Мыс маяка' },
  );
  void perp;
}
