import * as THREE from 'three';
import { LANDMARKS, WORLD, fbm, hash2, heightAt, makeRng, slopeAt, smoothstep, islandRadius } from '../../shared/terrain';
import { AssetManager, InstancedProp } from '../renderer/assets';
import { forestDensity } from './terrainMesh';
import { QUALITY_PRESETS, type Quality } from '../core/settings';
import type { ColliderGrid } from './colliders';
import { makeCircle } from './colliders';
import { resourceKey } from '../../shared/net';

export interface ResourceNode {
  id: number;
  /** Стабильный ключ для сети (не зависит от качества графики). */
  key: string;
  kind: 'tree' | 'rock';
  x: number;
  y: number;
  z: number;
  /** Радиус для проверки наведения. */
  r: number;
  height: number;
  hp: number;
  maxHp: number;
  prop: InstancedProp;
  index: number;
  matrix: THREE.Matrix4;
  collider: ReturnType<typeof makeCircle>;
  /** Секунды до возрождения (0 — цел). */
  respawn: number;
  gives: { id: string; min: number; max: number };
}

/** Зоны, где нельзя ставить растительность (постройки, площадь, пирс, лагерь). */
export function isReserved(x: number, z: number, margin = 0): boolean {
  const L = LANDMARKS;
  const d = (p: { x: number; z: number }) => Math.hypot(x - p.x, z - p.z);
  if (d(L.square) < 26 + margin) return true;
  for (const h of L.houses) if (d(h) < 11 + margin) return true;
  if (d(L.workshop) < 16 + margin || d(L.garage) < 17 + margin) return true;
  for (const p of L.buildPlots) if (d(p) < p.r + 5 + margin) return true;
  if (d(L.beachCamp) < 9 + margin) return true;
  if (d(L.lighthouse) < 14 + margin) return true;
  // коридор пирса и дорога от площади к пляжу
  const ax = L.dockStart.x - L.dockDir.x * 60, az = L.dockStart.z - L.dockDir.z * 60;
  const bx = L.dockStart.x + L.dockDir.x * 40, bz = L.dockStart.z + L.dockDir.z * 40;
  const dx = bx - ax, dz = bz - az;
  const t = Math.max(0, Math.min(1, ((x - ax) * dx + (z - az) * dz) / (dx * dx + dz * dz)));
  if (Math.hypot(x - (ax + dx * t), z - (az + dz * t)) < 7 + margin) return true;
  return false;
}

export interface VegetationResult {
  group: THREE.Group;
  resources: ResourceNode[];
  props: InstancedProp[];
}

interface Variant {
  path: string;
  height: number;
  /** Радиус ствола (коллайдер). */
  trunk: number;
  weight: number;
}

const PINES: Variant[] = [
  { path: 'nature/tree_pineTallA.glb', height: 10, trunk: 0.5, weight: 3 },
  { path: 'nature/tree_pineTallB.glb', height: 11, trunk: 0.5, weight: 3 },
  { path: 'nature/tree_pineTallC.glb', height: 9, trunk: 0.5, weight: 2 },
  { path: 'nature/tree_pineTallD.glb', height: 12, trunk: 0.5, weight: 2 },
  { path: 'nature/tree_pineRoundA.glb', height: 8, trunk: 0.45, weight: 2 },
  { path: 'nature/tree_pineDefaultA.glb', height: 7, trunk: 0.4, weight: 1 },
];
const BROAD: Variant[] = [
  { path: 'nature/tree_default.glb', height: 6.5, trunk: 0.45, weight: 3 },
  { path: 'nature/tree_oak.glb', height: 7.5, trunk: 0.55, weight: 2 },
  { path: 'nature/tree_thin.glb', height: 6, trunk: 0.3, weight: 1 },
];
const PALMS: Variant[] = [
  { path: 'pirate/palm-bend.glb', height: 6.5, trunk: 0.3, weight: 1 },
  { path: 'pirate/palm-straight.glb', height: 7, trunk: 0.3, weight: 1 },
  { path: 'pirate/palm-detailed-bend.glb', height: 6.5, trunk: 0.3, weight: 1 },
  { path: 'pirate/palm-detailed-straight.glb', height: 7.5, trunk: 0.3, weight: 1 },
];

/**
 * Расставляет деревья, пальмы, кусты, камни и траву через InstancedMesh.
 * Если модель не загрузилась, соответствующая группа просто пропускается.
 */
export async function buildVegetation(assets: AssetManager, colliders: ColliderGrid, quality: Quality): Promise<VegetationResult> {
  const q = QUALITY_PRESETS[quality];
  const group = new THREE.Group();
  group.name = 'vegetation';
  const resources: ResourceNode[] = [];
  const allProps: InstancedProp[] = [];
  const mk = (prop: ConstructorParameters<typeof InstancedProp>[0], cap: number, sh: boolean, dist = 0): InstancedProp => {
    const ip = new InstancedProp(prop, cap, sh, 48, dist);
    allProps.push(ip);
    return ip;
  };
  const rng = makeRng(WORLD.seed + 5);
  const m4 = new THREE.Matrix4();
  const quat = new THREE.Quaternion();
  const up = new THREE.Vector3(0, 1, 0);
  const pos = new THREE.Vector3();
  const scl = new THREE.Vector3();
  let nextId = 1;

  // --- какие места подходят для деревьев ---
  interface Spot { x: number; z: number; y: number; kind: 'pine' | 'broad' | 'palm'; rot: number; s: number; rv: number; vr: number }
  const spots: Spot[] = [];
  const R = WORLD.size / 2 - 20;
  for (let gx = -R; gx < R; gx += 5.2) {
    for (let gz = -R; gz < R; gz += 5.2) {
      const x = gx + (hash2(gx * 7 | 0, gz * 3 | 0, 11) - 0.5) * 4.6;
      const z = gz + (hash2(gx * 3 | 0, gz * 7 | 0, 17) - 0.5) * 4.6;
      const h = heightAt(x, z);
      if (h < 0.9 || h > 25) continue;
      if (isReserved(x, z)) continue;
      if (Math.hypot(x - LANDMARKS.quarry.x, z - LANDMARKS.quarry.z) < 24) continue;
      const slope = slopeAt(x, z);
      if (slope > 0.6) continue;
      const dens = forestDensity(x, z);
      const hillBoost = Math.exp(-(Math.hypot(x - LANDMARKS.hill.x, z - LANDMARKS.hill.z) ** 2) / (2 * 40 * 40)) * 0.35;
      let kind: Spot['kind'];
      let p: number;
      if (h < 2.1) { kind = 'palm'; p = 0.05 * smoothstep(0.9, 1.4, h); }
      else if (h < 4 && dens < 0.35) { kind = 'broad'; p = 0.035; }
      else { kind = rng() < 0.12 ? 'broad' : 'pine'; p = 0.04 + dens * 0.8 + hillBoost; }
      if (rng() > p * q.trees) continue;
      spots.push({ x, z, y: h, kind, rot: rng() * Math.PI * 2, s: 0.82 + rng() * 0.42, rv: rng(), vr: rng() });
    }
  }

  function pickLoaded<T extends { v: Variant }>(list: T[], r: number): T {
    const total = list.reduce((sum, l) => sum + l.v.weight, 0);
    let t = r * total;
    for (const l of list) {
      t -= l.v.weight;
      if (t <= 0) return l;
    }
    return list[list.length - 1];
  }

  async function plant(variants: Variant[], kind: Spot['kind'], gather: boolean): Promise<void> {
    const mine = spots.filter((s) => s.kind === kind);
    if (!mine.length) return;
    const loaded: { v: Variant; ip: InstancedProp }[] = [];
    for (const v of variants) {
      const prop = await assets.prop(v.path, { height: v.height });
      if (!prop) continue;
      const count = mine.length; // верхняя оценка для каждого варианта
      const ip = mk(prop, count + 4, true, 230);
      loaded.push({ v, ip });
      group.add(ip.group);
    }
    if (!loaded.length) return;
    for (const s of mine) {
      const { v, ip } = pickLoaded(loaded, s.vr);
      quat.setFromAxisAngle(up, s.rot);
      pos.set(s.x, s.y - 0.15, s.z);
      scl.setScalar(s.s);
      m4.compose(pos, quat, scl);
      const idx = ip.add(m4);
      if (idx < 0) continue;
      const col = makeCircle(s.x, s.z, v.trunk * s.s);
      colliders.add(col);
      if (gather) {
        resources.push({
          id: nextId++, key: resourceKey('tree', s.x, s.z), kind: 'tree', x: s.x, y: s.y, z: s.z, r: 1.1, height: v.height * s.s,
          hp: 4, maxHp: 4, prop: ip, index: idx, matrix: m4.clone(), collider: col, respawn: 0,
          gives: { id: 'wood', min: 2, max: 4 },
        });
      }
    }
  }

  await plant(PINES, 'pine', true);
  await plant(BROAD, 'broad', true);
  await plant(PALMS, 'palm', true);

  // --- кусты и цветы ---
  const bushPaths = ['nature/plant_bush.glb', 'nature/plant_bushLarge.glb', 'nature/plant_bushSmall.glb'];
  const bushProps = (await Promise.all(bushPaths.map((p) => assets.prop(p, { height: p.includes('Large') ? 1.7 : p.includes('Small') ? 0.8 : 1.2 })))).filter(Boolean);
  if (bushProps.length) {
    const ips = bushProps.map((p) => mk(p!, 420, true, 140));
    ips.forEach((ip) => group.add(ip.group));
    const target = Math.floor(260 * q.grass) + 40;
    let placed = 0, tries = 0;
    while (placed < target && tries < target * 12) {
      tries++;
      const ang = rng() * Math.PI * 2;
      const rr = Math.sqrt(rng()) * (WORLD.radius + 10);
      const x = Math.cos(ang) * rr, z = Math.sin(ang) * rr;
      const h = heightAt(x, z);
      if (h < 1.7 || h > 22 || isReserved(x, z, -4) || slopeAt(x, z) > 0.55) continue;
      if (rr > islandRadius(ang) - 14) continue;
      const ip = ips[Math.floor(rng() * ips.length)];
      quat.setFromAxisAngle(up, rng() * 6.28);
      pos.set(x, h - 0.05, z);
      scl.setScalar(0.8 + rng() * 0.6);
      m4.compose(pos, quat, scl);
      ip.add(m4);
      placed++;
    }
  }

  // --- трава (хвосты травы) ---
  const grassProp = await assets.prop('nature/grass_large.glb', { height: 0.55 });
  if (grassProp) {
    const cap = Math.floor(2600 * q.grass) + 200;
    const ip = mk(grassProp, cap, false, 70);
    group.add(ip.group);
    let tries = 0;
    while (ip.count < cap && tries < cap * 6) {
      tries++;
      const x = (rng() - 0.5) * (WORLD.size - 40), z = (rng() - 0.5) * (WORLD.size - 40);
      const h = heightAt(x, z);
      if (h < 1.9 || h > 20 || slopeAt(x, z) > 0.5 || isReserved(x, z, -6)) continue;
      if (fbm(x * 0.06, z * 0.06, 2, 3) < 0.35) continue;
      quat.setFromAxisAngle(up, rng() * 6.28);
      pos.set(x, h - 0.03, z);
      scl.setScalar(0.8 + rng() * 0.9);
      m4.compose(pos, quat, scl);
      ip.add(m4);
    }
  }

  // --- цветы ---
  const flowerPaths = ['nature/flower_redA.glb', 'nature/flower_yellowA.glb', 'nature/flower_purpleA.glb'];
  for (const fp of flowerPaths) {
    const prop = await assets.prop(fp, { height: 0.45 });
    if (!prop) continue;
    const ip = mk(prop, 90, false, 70);
    group.add(ip.group);
    let tries = 0;
    while (ip.count < Math.floor(70 * q.grass) + 10 && tries < 800) {
      tries++;
      const x = (rng() - 0.5) * (WORLD.size - 60), z = (rng() - 0.5) * (WORLD.size - 60);
      const h = heightAt(x, z);
      if (h < 1.9 || h > 12 || slopeAt(x, z) > 0.4 || isReserved(x, z, -8)) continue;
      quat.setFromAxisAngle(up, rng() * 6.28);
      pos.set(x, h, z);
      scl.setScalar(0.9 + rng() * 0.5);
      m4.compose(pos, quat, scl);
      ip.add(m4);
    }
  }

  // --- камни: карьер (добываемые), побережье (декор) ---
  const quarryPaths = ['nature/rock_largeA.glb', 'nature/rock_largeB.glb', 'nature/rock_largeC.glb', 'nature/rock_tallA.glb', 'nature/rock_tallB.glb', 'nature/stone_largeA.glb', 'nature/stone_largeB.glb'];
  const quarryProps = (await Promise.all(quarryPaths.map((p) => assets.prop(p, { height: p.includes('tall') ? 3.2 : p.includes('stone') ? 1.6 : 2.2 })))).filter(Boolean);
  if (quarryProps.length) {
    const ips = quarryProps.map((p) => mk(p!, 40, true, 0));
    ips.forEach((ip) => group.add(ip.group));
    const qrng = makeRng(WORLD.seed + 77); // отдельный генератор: позиции камней не зависят от качества графики
    let n = 0, tries = 0;
    while (n < 26 && tries < 400) {
      tries++;
      const a = qrng() * 6.28, rr = 4 + Math.sqrt(qrng()) * 24;
      const x = LANDMARKS.quarry.x + Math.cos(a) * rr, z = LANDMARKS.quarry.z + Math.sin(a) * rr;
      const h = heightAt(x, z);
      if (h < 1.2) continue;
      const k = Math.floor(qrng() * ips.length);
      const ip = ips[k];
      const s = 0.85 + qrng() * 0.7;
      quat.setFromAxisAngle(up, qrng() * 6.28);
      pos.set(x, h - 0.12, z);
      scl.setScalar(s);
      m4.compose(pos, quat, scl);
      const idx = ip.add(m4);
      if (idx < 0) continue;
      const hgt = quarryProps[k]!.size.y * s;
      const col = makeCircle(x, z, Math.max(0.7, Math.max(quarryProps[k]!.size.x, quarryProps[k]!.size.z) * 0.4 * s));
      colliders.add(col);
      resources.push({
        id: nextId++, key: resourceKey('rock', x, z), kind: 'rock', x, y: h, z, r: col.r + 0.6, height: hgt, hp: 5, maxHp: 5, prop: ip, index: idx,
        matrix: m4.clone(), collider: col, respawn: 0, gives: { id: 'stone', min: 2, max: 4 },
      });
      n++;
    }
  }

  const shorePaths = ['nature/rock_smallA.glb', 'nature/rock_smallB.glb', 'nature/stone_smallA.glb', 'pirate/rocks-sand-a.glb', 'pirate/rocks-sand-b.glb'];
  const shoreProps = (await Promise.all(shorePaths.map((p) => assets.prop(p, { width: p.startsWith('pirate') ? 2.4 : 1.2 })))).filter(Boolean);
  if (shoreProps.length) {
    const ips = shoreProps.map((p) => mk(p!, 120, true, 170));
    ips.forEach((ip) => group.add(ip.group));
    let n = 0, tries = 0;
    const want = Math.floor(120 * Math.max(0.5, q.grass));
    while (n < want && tries < 6000) {
      tries++;
      const a = rng() * 6.28;
      const rr = (WORLD.radius - 40) + rng() * 90;
      const x = Math.cos(a) * rr, z = Math.sin(a) * rr;
      const h = heightAt(x, z);
      if (h < 0.1 || h > 1.6 || isReserved(x, z, -2)) continue;
      const ip = ips[Math.floor(rng() * ips.length)];
      quat.setFromAxisAngle(up, rng() * 6.28);
      pos.set(x, h - 0.1, z);
      scl.setScalar(0.7 + rng() * 0.9);
      m4.compose(pos, quat, scl);
      ip.add(m4);
      n++;
    }
  }

  // --- скалы-ограничители в море (граница карты) ---
  const stackPaths = ['pirate/rocks-a.glb', 'pirate/rocks-b.glb', 'pirate/rocks-c.glb'];
  const stackProps = (await Promise.all(stackPaths.map((p) => assets.prop(p, { width: 9 })))).filter(Boolean);
  if (stackProps.length) {
    // текстура скал из набора даёт почти чёрный цвет — задаём собственный каменный материал
    const stoneMat = new THREE.MeshStandardMaterial({ color: 0x8b9199, roughness: 0.95, metalness: 0, side: THREE.DoubleSide, emissive: 0x2a3036 });
    for (const sp of stackProps) for (const part of sp!.parts) { part.material = stoneMat; part.geometry.computeVertexNormals(); }
    const ips = stackProps.map((p) => mk(p!, 80, false, 0));
    ips.forEach((ip) => group.add(ip.group));
    for (let i = 0; i < 64; i++) {
      const a = (i / 64) * Math.PI * 2 + rng() * 0.05;
      const rr = islandRadius(a) + 62 + rng() * 18;
      const x = Math.cos(a) * rr, z = Math.sin(a) * rr;
      if (Math.hypot(x - LANDMARKS.dockStart.x - LANDMARKS.dockDir.x * 30, z - LANDMARKS.dockStart.z - LANDMARKS.dockDir.z * 30) < 28) continue;
      const k = Math.floor(rng() * ips.length);
      quat.setFromAxisAngle(up, rng() * 6.28);
      pos.set(x, -1.4, z);
      scl.setScalar(1.0 + rng() * 1.4);
      m4.compose(pos, quat, scl);
      ips[k].add(m4);
    }
  }

  for (const ip of allProps) ip.build();
  return { group, resources, props: allProps };
}
