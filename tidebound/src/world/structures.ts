import * as THREE from 'three';
import { makeBox, makePlatform } from './colliders';
import type { BoxCollider, Interactable, LightSource, Platform } from './types';

const matCache = new Map<string, THREE.MeshStandardMaterial>();
/** Общие материалы (переиспользуются, чтобы не плодить шейдерные программы). */
export function stdMat(color: number, rough = 0.9, emissive?: number, emissiveIntensity = 0): THREE.MeshStandardMaterial {
  const key = `${color}|${rough}|${emissive ?? ''}|${emissiveIntensity}`;
  let m = matCache.get(key);
  if (!m) {
    m = new THREE.MeshStandardMaterial({ color, roughness: rough, metalness: 0.05 });
    if (emissive !== undefined) {
      m.emissive = new THREE.Color(emissive);
      m.emissiveIntensity = emissiveIntensity;
    }
    matCache.set(key, m);
  }
  return m;
}

export const COLORS = {
  plank: 0xb78a57, plankDark: 0x8a6038, wood: 0x6e4a2e, woodDark: 0x4b3220, metal: 0x6b7480,
  rust: 0x8a5a44, roofRed: 0xb4503f, roofBlue: 0x41709f, roofGrey: 0x6c7076, concrete: 0xbab5a6,
  wallTeal: 0x6fa3a0, wallSand: 0xd9c49a, wallRust: 0xa9795f,
};

export function box(w: number, h: number, d: number, mat: THREE.Material): THREE.Mesh {
  const m = new THREE.Mesh(new THREE.BoxGeometry(w, h, d), mat);
  m.castShadow = true;
  m.receiveShadow = true;
  return m;
}

export interface Structure {
  group: THREE.Group;
  colliders: BoxCollider[];
  platforms: Platform[];
  interactables: Interactable[];
  lights: LightSource[];
  update?: (dt: number) => void;
}

// ───────────────────────────── Пирс ─────────────────────────────

export interface DockOptions {
  start: { x: number; z: number };
  dir: { x: number; z: number };
  length: number;
  width: number;
  deckY: number;
  lampMat: THREE.Material;
}

/** Деревянный причал: настил, сваи, ограждение у основания и фонари. */
export function buildDock(o: DockOptions): Structure {
  const group = new THREE.Group();
  group.name = 'dock';
  const dl = Math.hypot(o.dir.x, o.dir.z);
  const yaw = Math.atan2(o.dir.x, o.dir.z);
  group.position.set(o.start.x, 0, o.start.z);
  group.rotation.y = yaw;

  const plankLen = 1.1;
  const nPlanks = Math.ceil(o.length / plankLen);
  const planks = new THREE.InstancedMesh(new THREE.BoxGeometry(o.width, 0.16, plankLen - 0.06), stdMat(0xffffff, 0.95), nPlanks);
  planks.castShadow = true;
  planks.receiveShadow = true;
  const m4 = new THREE.Matrix4();
  const col = new THREE.Color();
  const dark = new THREE.Color(COLORS.plankDark);
  for (let i = 0; i < nPlanks; i++) {
    const jitter = ((i * 37) % 7) / 7;
    m4.makeTranslation((jitter - 0.5) * 0.04, o.deckY - 0.08, i * plankLen + plankLen / 2);
    planks.setMatrixAt(i, m4);
    col.set(COLORS.plank).lerp(dark, (i % 3) * 0.28 + jitter * 0.2);
    planks.setColorAt(i, col);
  }
  group.add(planks);

  const beamMat = stdMat(COLORS.woodDark);
  for (const sx of [-1, 1]) {
    const beam = box(0.22, 0.24, o.length, beamMat);
    beam.position.set(sx * (o.width / 2 - 0.12), o.deckY - 0.3, o.length / 2);
    group.add(beam);
  }

  const postCount = Math.floor(o.length / 4) + 1;
  const posts = new THREE.InstancedMesh(new THREE.CylinderGeometry(0.17, 0.2, 7.5, 8), stdMat(COLORS.wood), postCount * 2);
  posts.castShadow = true;
  let pi = 0;
  for (let i = 0; i < postCount; i++) {
    for (const sx of [-1, 1]) {
      m4.makeTranslation(sx * (o.width / 2 - 0.12), o.deckY - 3.6, 0.6 + i * 4);
      posts.setMatrixAt(pi++, m4);
    }
  }
  group.add(posts);

  const railMat = stdMat(COLORS.plankDark);
  for (const sx of [-1, 1]) {
    const rail = box(0.1, 0.1, o.length * 0.55, railMat);
    rail.position.set(sx * (o.width / 2 - 0.06), o.deckY + 0.95, o.length * 0.275);
    group.add(rail);
    for (let i = 0; i <= 5; i++) {
      const p = box(0.12, 1.0, 0.12, railMat);
      p.position.set(sx * (o.width / 2 - 0.06), o.deckY + 0.45, 0.3 + (i * (o.length * 0.55 - 0.6)) / 5);
      group.add(p);
    }
  }

  const lights: LightSource[] = [];
  const toWorld = new THREE.Matrix4().makeRotationY(yaw).setPosition(o.start.x, 0, o.start.z);
  for (const lz of [o.length * 0.3, o.length * 0.98]) {
    const pole = box(0.12, 2.6, 0.12, stdMat(COLORS.woodDark));
    pole.position.set(o.width / 2 - 0.25, o.deckY + 1.3, lz);
    group.add(pole);
    const lamp = new THREE.Mesh(new THREE.SphereGeometry(0.2, 10, 8), o.lampMat);
    lamp.position.set(o.width / 2 - 0.25, o.deckY + 2.65, lz);
    group.add(lamp);
    const wp = new THREE.Vector3(o.width / 2 - 0.25, o.deckY + 2.65, lz).applyMatrix4(toWorld);
    lights.push({ position: wp, color: 0xffc271, intensity: 18, distance: 16, nightOnly: true });
  }

  const cx = o.start.x + (o.dir.x / dl) * (o.length / 2);
  const cz = o.start.z + (o.dir.z / dl) * (o.length / 2);
  const platform = makePlatform(cx, cz, o.width / 2, o.length / 2, yaw, o.deckY);
  return { group, colliders: [], platforms: [platform], interactables: [], lights };
}

// ───────────────────────────── Здание с дверью ─────────────────────────────

export interface ShedOptions {
  id: string;
  x: number; z: number; y: number; yaw: number;
  width: number; depth: number; height: number;
  wall: number; roof: number;
  doorWidth: number; doorHeight: number;
  garage?: boolean;
  doorName: string;
  windowMat: THREE.Material;
  onDoor?: (open: boolean) => void;
}

/**
 * Простое здание с рабочей дверью. Стены — отдельные коробки, поэтому коллизии (OBB)
 * точно совпадают с геометрией. Временная процедурная модель.
 */
export function buildShed(o: ShedOptions): Structure & { door: { open: boolean }; interior: THREE.Vector3 } {
  const group = new THREE.Group();
  group.position.set(o.x, o.y, o.z);
  group.rotation.y = o.yaw;
  const t = 0.28;
  const wallMat = stdMat(o.wall, 0.95);
  const colliders: BoxCollider[] = [];
  const hw = o.width / 2;
  const hd = o.depth / 2;

  const toWorld = (lx: number, lz: number) => ({
    x: o.x + lx * Math.cos(o.yaw) + lz * Math.sin(o.yaw),
    z: o.z - lx * Math.sin(o.yaw) + lz * Math.cos(o.yaw),
  });
  const addWall = (w: number, h: number, d: number, lx: number, ly: number, lz: number, collide = true) => {
    const m = box(w, h, d, wallMat);
    m.position.set(lx, ly, lz);
    group.add(m);
    if (collide) {
      const c = toWorld(lx, lz);
      colliders.push(makeBox(c.x, c.z, w / 2, d / 2, o.yaw));
    }
  };

  const floor = box(o.width - 0.1, 0.2, o.depth - 0.1, stdMat(COLORS.plankDark));
  floor.position.set(0, 0.06, 0);
  group.add(floor);
  const base = box(o.width + 0.5, 0.9, o.depth + 0.5, stdMat(COLORS.concrete));
  base.position.set(0, -0.35, 0);
  group.add(base);

  const wy = o.height / 2 + 0.16;
  addWall(o.width, o.height, t, 0, wy, -hd + t / 2);
  addWall(t, o.height, o.depth - t * 2, -hw + t / 2, wy, 0);
  addWall(t, o.height, o.depth - t * 2, hw - t / 2, wy, 0);
  const frontZ = hd - t / 2;
  const sideW = (o.width - o.doorWidth) / 2;
  addWall(sideW, o.height, t, -(o.doorWidth / 2 + sideW / 2), wy, frontZ);
  addWall(sideW, o.height, t, o.doorWidth / 2 + sideW / 2, wy, frontZ);
  const lintelH = o.height - o.doorHeight;
  addWall(o.doorWidth, lintelH, t, 0, o.doorHeight + lintelH / 2 + 0.16, frontZ, false);

  const winGeo = new THREE.PlaneGeometry(1.1, 0.8);
  for (const sx of [-1, 1]) {
    for (const lz of [-hd * 0.45, hd * 0.35]) {
      const wn = new THREE.Mesh(winGeo, o.windowMat);
      wn.position.set(sx * (hw + 0.01), 1.7, lz);
      wn.rotation.y = (sx * Math.PI) / 2;
      group.add(wn);
    }
  }

  // двускатная крыша: два наклонных листа + два фронтона
  const roofMat = stdMat(o.roof, 0.85);
  const pitch = 0.42;
  const slopeLen = (hd + 0.6) / Math.cos(pitch);
  const ridgeY = o.height + 0.16 + Math.tan(pitch) * hd;
  for (const side of [-1, 1]) {
    const r = box(o.width + 0.9, 0.16, slopeLen, roofMat);
    r.position.set(0, ridgeY - Math.sin(pitch) * slopeLen * 0.5 + 0.06, side * (hd + 0.6) * 0.5);
    r.rotation.x = side * pitch;
    group.add(r);
  }
  const gableShape = new THREE.Shape();
  gableShape.moveTo(-hd, 0);
  gableShape.lineTo(hd, 0);
  gableShape.lineTo(0, Math.tan(pitch) * hd);
  const gableGeo = new THREE.ShapeGeometry(gableShape);
  const gableMat = stdMat(o.wall, 0.95);
  gableMat.side = THREE.DoubleSide;
  for (const sx of [-1, 1]) {
    const g = new THREE.Mesh(gableGeo, gableMat);
    g.position.set(sx * (hw - t / 2), o.height + 0.16, 0);
    g.rotation.y = Math.PI / 2;
    g.castShadow = true;
    group.add(g);
  }

  // дверь на петле (гараж — подъёмные ворота)
  const pivot = new THREE.Group();
  const dW = o.garage ? o.doorWidth : o.doorWidth - 0.04;
  pivot.position.set(-o.doorWidth / 2, 0.16, frontZ);
  const doorMesh = box(dW, o.doorHeight - 0.04, 0.12, stdMat(o.garage ? COLORS.metal : COLORS.wood, 0.8));
  doorMesh.position.set(dW / 2, (o.doorHeight - 0.04) / 2, 0);
  pivot.add(doorMesh);
  if (!o.garage) {
    const handle = box(0.06, 0.06, 0.18, stdMat(0x2a2a2a));
    handle.position.set(dW - 0.2, 1.0, 0.08);
    pivot.add(handle);
  } else {
    for (let i = 1; i < 6; i++) {
      const rib = box(dW, 0.04, 0.16, stdMat(0x4c535c));
      rib.position.set(dW / 2, i * (o.doorHeight / 6), 0.02);
      pivot.add(rib);
    }
  }
  group.add(pivot);

  const doorC = toWorld(0, frontZ);
  const doorCollider = makeBox(doorC.x, doorC.z, o.doorWidth / 2, 0.1, o.yaw);
  colliders.push(doorCollider);

  const door = { open: false };
  let target = 0;
  const interactables: Interactable[] = [{
    id: `${o.id}-door`,
    position: new THREE.Vector3(doorC.x, o.y + 1.2, doorC.z),
    radius: 3.6,
    label: () => (door.open ? `Закрыть: ${o.doorName}` : `Открыть: ${o.doorName}`),
    onUse: (game) => {
      door.open = !door.open;
      target = door.open ? (o.garage ? 0 : -Math.PI * 0.55) : 0;
      doorCollider.enabled = !door.open;
      game.audio.door(door.open);
      o.onDoor?.(door.open);
    },
  }];

  const update = (dt: number) => {
    if (o.garage) {
      const ty = door.open ? o.doorHeight - 0.1 : 0.16;
      pivot.position.y += (ty - pivot.position.y) * Math.min(1, dt * 3);
    } else {
      pivot.rotation.y += (target - pivot.rotation.y) * Math.min(1, dt * 6);
    }
  };

  const inner = toWorld(0, 0);
  const lights: LightSource[] = [
    { position: new THREE.Vector3(inner.x, o.y + o.height - 0.5, inner.z), color: 0xffc680, intensity: 14, distance: 12, nightOnly: true },
  ];
  const bulb = new THREE.Mesh(new THREE.SphereGeometry(0.12, 8, 6), o.windowMat);
  bulb.position.set(0, o.height - 0.3, 0);
  group.add(bulb);

  const floorPlatform = makePlatform(o.x, o.z, hw - 0.25, hd - 0.25, o.yaw, o.y + 0.16);
  return { group, colliders, platforms: [floorPlatform], interactables, lights, door, interior: new THREE.Vector3(inner.x, o.y, inner.z), update };
}

// ───────────────────────────── Мелкие объекты ─────────────────────────────

export function buildWorkbenchModel(): THREE.Group {
  const g = new THREE.Group();
  const top = box(2.0, 0.12, 0.9, stdMat(COLORS.plank));
  top.position.y = 0.95;
  g.add(top);
  for (const sx of [-1, 1]) for (const sz of [-1, 1]) {
    const leg = box(0.12, 0.95, 0.12, stdMat(COLORS.woodDark));
    leg.position.set(sx * 0.9, 0.47, sz * 0.38);
    g.add(leg);
  }
  const shelf = box(1.8, 0.08, 0.7, stdMat(COLORS.plankDark));
  shelf.position.y = 0.35;
  g.add(shelf);
  const vise = box(0.22, 0.2, 0.2, stdMat(COLORS.metal, 0.5));
  vise.position.set(0.7, 1.1, 0.25);
  g.add(vise);
  return g;
}

export function buildBedModel(): THREE.Group {
  const g = new THREE.Group();
  const frame = box(1.1, 0.35, 2.1, stdMat(COLORS.wood));
  frame.position.y = 0.3;
  g.add(frame);
  const mattress = box(1.0, 0.2, 1.95, stdMat(0xd9d3c0));
  mattress.position.y = 0.58;
  g.add(mattress);
  const pillow = box(0.7, 0.14, 0.4, stdMat(0xf2eddc));
  pillow.position.set(0, 0.74, -0.7);
  g.add(pillow);
  const blanket = box(1.02, 0.1, 1.2, stdMat(0x4a78a8));
  blanket.position.set(0, 0.7, 0.3);
  g.add(blanket);
  return g;
}

export function buildLampPost(mat: THREE.Material): THREE.Group {
  const g = new THREE.Group();
  const pole = box(0.12, 3.2, 0.12, stdMat(COLORS.woodDark));
  pole.position.y = 1.6;
  g.add(pole);
  const lamp = new THREE.Mesh(new THREE.SphereGeometry(0.22, 10, 8), mat);
  lamp.position.y = 3.3;
  g.add(lamp);
  return g;
}

/** Костёр: камни, брёвна и анимированные язычки пламени (процедурная заглушка). */
export function buildCampfire(): { group: THREE.Group; flames: THREE.Mesh[] } {
  const g = new THREE.Group();
  const stoneMat = stdMat(0x7d8088);
  for (let i = 0; i < 8; i++) {
    const a = (i / 8) * Math.PI * 2;
    const s = new THREE.Mesh(new THREE.DodecahedronGeometry(0.17, 0), stoneMat);
    s.position.set(Math.cos(a) * 0.55, 0.1, Math.sin(a) * 0.55);
    s.castShadow = true;
    g.add(s);
  }
  const logMat = stdMat(COLORS.woodDark);
  for (let i = 0; i < 3; i++) {
    const l = new THREE.Mesh(new THREE.CylinderGeometry(0.07, 0.08, 0.9, 6), logMat);
    l.position.y = 0.16;
    l.rotation.z = Math.PI / 2;
    l.rotation.y = (i / 3) * Math.PI;
    g.add(l);
  }
  const flames: THREE.Mesh[] = [];
  const flameMat = new THREE.MeshBasicMaterial({ color: 0xffa23a, transparent: true, opacity: 0.85, depthWrite: false });
  const coreMat = new THREE.MeshBasicMaterial({ color: 0xfff1a0, transparent: true, opacity: 0.9, depthWrite: false });
  for (let i = 0; i < 4; i++) {
    const f = new THREE.Mesh(new THREE.ConeGeometry(0.2 - i * 0.03, 0.7 + i * 0.1, 6), i === 3 ? coreMat : flameMat);
    f.position.set((i - 1.5) * 0.08, 0.5, ((i % 2) - 0.5) * 0.12);
    flames.push(f);
    g.add(f);
  }
  return { group: g, flames };
}
