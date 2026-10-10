import * as THREE from 'three';
import { LANDMARKS, WORLD, heightAt, isLand, makeRng, waveHeight } from '../../shared/terrain';
import { AssetManager, propToGroup } from '../renderer/assets';
import { ColliderGrid, insidePlatform, makeBox, makeCircle } from './colliders';
import { buildTerrainMesh, forestDensity } from './terrainMesh';
import { buildVegetation, isReserved, type ResourceNode } from './vegetation';
import { buildLighthouse } from './lighthouse';
import {
  COLORS, buildBedModel, buildCampfire, buildDock, buildLampPost, buildShed, buildWorkbenchModel, stdMat, box,
  type Structure,
} from './structures';
import type { Interactable, LightSource, Platform } from './types';
import { rollLoot, hashString } from '../../shared/loot';
import { QUALITY_PRESETS, type Quality } from '../core/settings';
import type { Game } from '../core/game';

export interface WorldSave {
  doors: Record<string, boolean>;
  opened: string[];
  resources: { id: number; respawn: number }[];
}

interface Stash {
  id: string;
  table: string;
  opened: boolean;
}

interface Bobber {
  obj: THREE.Object3D;
  x: number;
  z: number;
  y: number;
  phase: number;
  roll: number;
}

const ZONE_NAMES: [string, (x: number, z: number) => boolean][] = [];

/** Мир Грейхейвена: рельеф, постройки, растительность, интерактивные объекты, освещение. */
export class World {
  readonly group = new THREE.Group();
  readonly colliders = new ColliderGrid();
  readonly platforms: Platform[] = [];
  readonly interactables: Interactable[] = [];
  readonly lights: LightSource[] = [];
  resources: ResourceNode[] = [];
  readonly windowMat = stdMat(0xffd27a, 0.4, 0xffb347, 0);
  readonly lampMat = stdMat(0xffe2a0, 0.4, 0xffc271, 0);
  lighthouseBeam: THREE.Object3D | null = null;
  lighthouseFromModel = false;
  readonly doors = new Map<string, { open: boolean }>();
  private stashes = new Map<string, Stash>();
  private updaters: ((dt: number, t: number) => void)[] = [];
  private bobbers: Bobber[] = [];
  private campfires: { flames: THREE.Mesh[]; light: LightSource }[] = [];
  private pool: THREE.PointLight[] = [];
  private poolCount = 4;
  private nightValue = 0;
  private lastZone = '';
  terrain!: THREE.Mesh;

  static async create(assets: AssetManager, quality: Quality, progress: (text: string, k: number) => void): Promise<World> {
    const w = new World();
    await w.build(assets, quality, progress);
    return w;
  }

  private async build(assets: AssetManager, quality: Quality, progress: (text: string, k: number) => void): Promise<void> {
    const L = LANDMARKS;
    const q = QUALITY_PRESETS[quality];
    this.poolCount = q.lights;

    progress('Строим рельеф…', 0.08);
    this.terrain = buildTerrainMesh(Math.round(q.waterSeg * 1.1));
    this.group.add(this.terrain);

    progress('Причал…', 0.2);
    const dock = buildDock({ start: L.dockStart, dir: L.dockDir, length: L.dockLength, width: L.dockWidth, deckY: L.dockDeck, lampMat: this.lampMat });
    this.addStructure(dock);

    progress('Мастерская и гараж…', 0.3);
    const ws = L.workshop;
    const wsYaw = Math.atan2(L.square.x - ws.x, L.square.z - ws.z);
    const workshop = buildShed({
      id: 'workshop', x: ws.x, z: ws.z, y: heightAt(ws.x, ws.z), yaw: wsYaw, width: 9, depth: 8, height: 3.4,
      wall: COLORS.wallRust, roof: COLORS.roofGrey, doorWidth: 1.4, doorHeight: 2.3, doorName: 'мастерскую', windowMat: this.windowMat,
    });
    this.addStructure(workshop);
    this.doors.set('workshop-door', workshop.door);
    this.furnishWorkshop(workshop, ws.x, ws.z, workshop.group.position.y, wsYaw);

    const gg = L.garage;
    const ggYaw = Math.atan2(L.square.x - gg.x, L.square.z - gg.z);
    const garage = buildShed({
      id: 'garage', x: gg.x, z: gg.z, y: heightAt(gg.x, gg.z), yaw: ggYaw, width: 10, depth: 9, height: 3.8,
      wall: COLORS.wallTeal, roof: COLORS.roofBlue, doorWidth: 4, doorHeight: 3, garage: true, doorName: 'гараж', windowMat: this.windowMat,
    });
    this.addStructure(garage);
    this.doors.set('garage-door', garage.door);
    this.addChest('stash-garage', 'stash_tools', garage.group.position.y, gg.x, gg.z, ggYaw, -3.3, -3.3, assets, 'chest');

    progress('Жилые дома…', 0.42);
    await this.buildHouses(assets);

    progress('Старый маяк…', 0.55);
    const lh = await buildLighthouse(assets, L.lighthouse.x, heightAt(L.lighthouse.x, L.lighthouse.z), L.lighthouse.z, 26);
    this.group.add(lh.group);
    for (const c of lh.colliders) this.colliders.add(c);
    this.lighthouseBeam = lh.beam;
    this.lighthouseFromModel = lh.fromModel;
    this.lights.push({ position: lh.lamp, color: 0xffe9a8, intensity: 60, distance: 70, nightOnly: true });
    this.addStash('stash-lighthouse', 'stash_rare', L.lighthouse.x + 7, L.lighthouse.z + 9, assets, 'chest');

    progress('Растительность…', 0.7);
    const veg = await buildVegetation(assets, this.colliders, quality);
    this.group.add(veg.group);
    this.resources = veg.resources;
    this.cullProps = veg.props;

    progress('Детали и интерактив…', 0.86);
    await this.buildDockProps(assets);
    await this.buildSquareAndCamp(assets);
    await this.buildStashes(assets);
    await this.buildSea(assets);

    for (let i = 0; i < this.poolCount; i++) {
      const pl = new THREE.PointLight(0xffc271, 0, 20, 1.6);
      pl.castShadow = false;
      this.pool.push(pl);
      this.group.add(pl);
    }
    this.setupZones();
    progress('Готово', 1);
  }

  private addStructure(s: Structure): void {
    this.group.add(s.group);
    for (const c of s.colliders) this.colliders.add(c);
    this.platforms.push(...s.platforms);
    this.interactables.push(...s.interactables);
    this.lights.push(...s.lights);
    if (s.update) this.updaters.push(s.update);
  }

  private localToWorld(x: number, z: number, yaw: number, lx: number, lz: number): { x: number; z: number } {
    return { x: x + lx * Math.cos(yaw) + lz * Math.sin(yaw), z: z - lx * Math.sin(yaw) + lz * Math.cos(yaw) };
  }

  private furnishWorkshop(shed: Structure, x: number, z: number, y: number, yaw: number): void {
    const bench = buildWorkbenchModel();
    bench.position.set(-2.6, 0.16, -2.7);
    shed.group.add(bench);
    const bw = this.localToWorld(x, z, yaw, -2.6, -2.7);
    this.colliders.add(makeBox(bw.x, bw.z, 1.05, 0.5, yaw, 1.2));
    this.interactables.push({
      id: 'workbench', position: new THREE.Vector3(bw.x, y + 1.1, bw.z), radius: 3.2,
      label: () => 'Верстак — крафт',
      onUse: (g) => g.openCraft('workbench'),
    });

    const bed = buildBedModel();
    bed.position.set(2.8, 0.16, -2.4);
    shed.group.add(bed);
    const bp = this.localToWorld(x, z, yaw, 2.8, -2.4);
    this.colliders.add(makeBox(bp.x, bp.z, 0.6, 1.1, yaw, 0.9));
    this.addBedInteractable('bed-workshop', bp.x, y + 0.9, bp.z);

    this.addChestAt('stash-workshop', 'stash_tools', new THREE.Vector3(...[this.localToWorld(x, z, yaw, 3.3, 2.6).x, y + 0.16, this.localToWorld(x, z, yaw, 3.3, 2.6).z]), yaw, shed.group, 3.3, 2.6);
  }

  private addBedInteractable(id: string, x: number, y: number, z: number): void {
    this.interactables.push({
      id, position: new THREE.Vector3(x, y, z), radius: 3.2,
      label: () => 'Лечь спать',
      onUse: (g) => g.sleep(),
    });
  }

  /** Сундук внутри здания (локальные координаты группы). */
  private addChestAt(id: string, table: string, worldPos: THREE.Vector3, yaw: number, parent: THREE.Group, lx: number, lz: number): void {
    const c = box(0.9, 0.6, 0.6, stdMat(COLORS.wood));
    c.position.set(lx, 0.46, lz);
    const lid = box(0.96, 0.18, 0.66, stdMat(COLORS.woodDark));
    lid.position.set(lx, 0.85, lz);
    parent.add(c, lid);
    this.colliders.add(makeBox(worldPos.x, worldPos.z, 0.5, 0.35, yaw, 0.8));
    this.registerStash(id, table, new THREE.Vector3(worldPos.x, worldPos.y + 0.6, worldPos.z), 'Открыть сундук');
  }

  private addChest(id: string, table: string, y: number, x: number, z: number, yaw: number, lx: number, lz: number, _assets: AssetManager, _kind: string): void {
    const p = this.localToWorld(x, z, yaw, lx, lz);
    const group = new THREE.Group();
    group.position.set(x, y, z);
    group.rotation.y = yaw;
    this.group.add(group);
    this.addChestAt(id, table, new THREE.Vector3(p.x, y + 0.16, p.z), yaw, group, lx, lz);
  }

  private registerStash(id: string, table: string, pos: THREE.Vector3, verb: string): void {
    const st: Stash = { id, table, opened: false };
    this.stashes.set(id, st);
    this.interactables.push({
      id, position: pos, radius: 3.0,
      label: () => (st.opened ? null : verb),
      onUse: (g) => {
        if (st.opened) return;
        if (g.mp.active) { g.mp.requestOpen(id); return; }
        const loot = rollLoot(st.table, hashString(id));
        if (!g.inventory.hasRoomFor(loot)) {
          g.ui.toast('Инвентарь полон — освободите место.', 'warn');
          return;
        }
        for (const l of loot) g.inventory.add(l.id, l.count);
        st.opened = true;
        g.audio.pickup();
        g.ui.toast(`Найдено: ${loot.map((l) => `${g.itemName(l.id)} ×${l.count}`).join(', ')}`, 'good');
        g.addXp(6, 'исследование');
      },
    });
  }

  /** Тайник снаружи: модель из пирата + коллайдер. */
  private async addStash(id: string, table: string, x: number, z: number, assets: AssetManager, model: 'chest' | 'crate' | 'barrel'): Promise<void> {
    const h = heightAt(x, z);
    const prop = await assets.prop(`pirate/${model}.glb`, { height: model === 'barrel' ? 1.0 : 0.9 });
    const g = new THREE.Group();
    g.position.set(x, h - 0.02, z);
    g.rotation.y = hashString(id) % 6.28;
    if (prop) g.add(propToGroup(prop));
    else g.add(box(0.9, 0.7, 0.6, stdMat(COLORS.wood)));
    this.group.add(g);
    this.colliders.add(makeCircle(x, z, 0.55, h + 1.2));
    this.registerStash(id, table, new THREE.Vector3(x, h + 0.7, z), model === 'chest' ? 'Открыть сундук' : model === 'crate' ? 'Осмотреть ящик' : 'Осмотреть бочку');
  }

  private async buildHouses(assets: AssetManager): Promise<void> {
    const kinds = ['building-sample-house-a', 'building-sample-house-b', 'building-sample-house-c'];
    for (const h of LANDMARKS.houses) {
      const prop = await assets.prop(`buildings/${kinds[h.kind % kinds.length]}.glb`, { width: 8.5 });
      const y = heightAt(h.x, h.z);
      if (prop) {
        const g = propToGroup(prop);
        g.position.set(h.x, y - 0.05, h.z);
        g.rotation.y = h.yaw;
        this.group.add(g);
        this.colliders.add(makeBox(h.x, h.z, prop.size.x * 0.46, prop.size.z * 0.46, 0));
      } else {
        // временная замена: коробка с крышей
        const g = new THREE.Group();
        g.position.set(h.x, y, h.z);
        g.rotation.y = h.yaw;
        g.add(Object.assign(box(7, 4, 6, stdMat(0xd9c49a)), { position: new THREE.Vector3(0, 2, 0) }));
        g.add(Object.assign(box(7.6, 0.4, 6.6, stdMat(COLORS.roofRed)), { position: new THREE.Vector3(0, 4.2, 0) }));
        this.group.add(g);
        this.colliders.add(makeBox(h.x, h.z, 3.5, 3, h.yaw));
      }
      // крыльцо-фонарь
      const lp = this.localToWorld(h.x, h.z, h.yaw, 3.2, 4.6);
      const post = buildLampPost(this.lampMat);
      post.position.set(lp.x, heightAt(lp.x, lp.z), lp.z);
      this.group.add(post);
      this.lights.push({ position: new THREE.Vector3(lp.x, heightAt(lp.x, lp.z) + 3.3, lp.z), color: 0xffc271, intensity: 16, distance: 15, nightOnly: true });
      this.colliders.add(makeCircle(lp.x, lp.z, 0.2));
    }
  }

  private async buildDockProps(assets: AssetManager): Promise<void> {
    const L = LANDMARKS;
    const perp = { x: L.dockDir.z, z: -L.dockDir.x };
    const at = (s: number, lat: number) => ({
      x: L.dockStart.x + L.dockDir.x * s + perp.x * lat,
      z: L.dockStart.z + L.dockDir.z * s + perp.z * lat,
    });
    const yDeck = L.dockDeck;
    const place = async (path: string, s: number, lat: number, size: { height?: number; width?: number }, y: number, rot: number, collide = 0.5) => {
      const prop = await assets.prop(path, size);
      if (!prop) return;
      const p = at(s, lat);
      const g = propToGroup(prop);
      g.position.set(p.x, y, p.z);
      g.rotation.y = rot;
      this.group.add(g);
      if (collide > 0) this.colliders.add(makeCircle(p.x, p.z, collide, y + 1.4));
    };
    await place('pirate/barrel.glb', 1.4, 1.2, { height: 1.0 }, yDeck, 0.4);
    await place('pirate/barrel.glb', 2.4, 1.3, { height: 1.0 }, yDeck, 1.4);
    await place('pirate/crate.glb', 1.6, -1.2, { height: 0.9 }, yDeck, 0.2, 0.55);
    await place('pirate/crate-bottles.glb', 3.8, -1.3, { height: 0.9 }, yDeck, 1.2, 0.55);
    await place('pirate/barrel.glb', L.dockLength - 1.2, -1.2, { height: 1.0 }, yDeck, 2.1);
    // тайник у основания причала
    const sp = at(5.5, 3.2);
    await this.addStash('stash-dock', 'stash_small', sp.x, sp.z, assets, 'crate');

    // лодки у причала
    const boats: [string, number, number, number][] = [
      ['boats/boat-row-small.glb', 12, 4.3, 1.2],
      ['boats/boat-fishing-small.glb', 24, -5.2, 1.6],
    ];
    for (const [path, s, lat, rot] of boats) {
      const prop = await assets.prop(path, { width: 4.2 });
      if (!prop) continue;
      const p = at(s, lat);
      const g = propToGroup(prop);
      const holder = new THREE.Group();
      holder.add(g);
      holder.position.set(p.x, 0, p.z);
      holder.rotation.y = Math.atan2(L.dockDir.x, L.dockDir.z) + rot * 0.1 + Math.PI / 2;
      this.group.add(holder);
      this.bobbers.push({ obj: holder, x: p.x, z: p.z, y: -0.15, phase: Math.random() * 6, roll: 0.05 });
    }
  }

  private async buildSquareAndCamp(assets: AssetManager): Promise<void> {
    const L = LANDMARKS;
    const sq = L.square;
    const sy = heightAt(sq.x, sq.z);

    // колодец
    const well = new THREE.Group();
    well.position.set(sq.x, sy, sq.z);
    const ring = new THREE.Mesh(new THREE.CylinderGeometry(1.0, 1.1, 0.9, 14), stdMat(0x8d9199));
    ring.position.y = 0.45;
    ring.castShadow = true;
    const water = new THREE.Mesh(new THREE.CylinderGeometry(0.85, 0.85, 0.05, 14), stdMat(0x2c7fb8, 0.2));
    water.position.y = 0.78;
    well.add(ring, water);
    for (const sx of [-1, 1]) {
      const p = box(0.14, 2.2, 0.14, stdMat(COLORS.woodDark));
      p.position.set(sx * 0.95, 1.4, 0);
      well.add(p);
    }
    const roofL = box(2.6, 0.12, 1.5, stdMat(COLORS.roofRed));
    roofL.position.set(0, 2.6, 0.45);
    roofL.rotation.x = 0.5;
    const roofR = roofL.clone();
    roofR.position.z = -0.45;
    roofR.rotation.x = -0.5;
    well.add(roofL, roofR);
    this.group.add(well);
    this.colliders.add(makeCircle(sq.x, sq.z, 1.15));
    let wellCool = 0;
    this.updaters.push((dt) => { wellCool = Math.max(0, wellCool - dt); });
    this.interactables.push({
      id: 'well', position: new THREE.Vector3(sq.x, sy + 1, sq.z), radius: 3.4,
      label: () => 'Набрать воды из колодца',
      onUse: (g) => {
        if (wellCool > 0) return;
        if (!g.inventory.add('water', 1)) { g.ui.toast('Инвентарь полон.', 'warn'); return; }
        wellCool = 1.2;
        g.audio.drink();
        g.ui.toast('Вы набрали бутылку воды.', 'good');
      },
    });

    // фонари на площади
    for (const [ox, oz] of [[10, 10], [-10, 10], [10, -10], [-10, -10]]) {
      const x = sq.x + ox, z = sq.z + oz;
      const p = buildLampPost(this.lampMat);
      p.position.set(x, heightAt(x, z), z);
      this.group.add(p);
      this.colliders.add(makeCircle(x, z, 0.2));
      this.lights.push({ position: new THREE.Vector3(x, heightAt(x, z) + 3.3, z), color: 0xffc271, intensity: 20, distance: 18, nightOnly: true });
    }

    // лагерь на пляже: палатка, кровать, костёр
    const bc = L.beachCamp;
    const by = heightAt(bc.x, bc.z);
    const tent = await assets.prop('nature/tent_smallOpen.glb', { width: 3.6 });
    if (tent) {
      const g = propToGroup(tent);
      g.position.set(bc.x - 2.6, by, bc.z);
      g.rotation.y = Math.atan2(L.dockDir.x, L.dockDir.z) + Math.PI;
      this.group.add(g);
      this.colliders.add(makeCircle(bc.x - 2.6, bc.z, 1.3, by + 1));
    }
    const bed = buildBedModel();
    bed.position.set(bc.x + 0.3, by, bc.z + 2.4);
    bed.rotation.y = 0.4;
    this.group.add(bed);
    this.colliders.add(makeCircle(bc.x + 0.3, bc.z + 2.4, 0.9, by + 0.8));
    this.addBedInteractable('bed-beach', bc.x + 0.3, by + 0.9, bc.z + 2.4);

    const fire = buildCampfire();
    fire.group.position.set(bc.x + 3.2, by, bc.z - 0.5);
    this.group.add(fire.group);
    this.colliders.add(makeCircle(bc.x + 3.2, bc.z - 0.5, 0.75, by + 0.6));
    const fl: LightSource = { position: new THREE.Vector3(bc.x + 3.2, by + 1, bc.z - 0.5), color: 0xff9a40, intensity: 26, distance: 20, nightOnly: false, flicker: true };
    this.lights.push(fl);
    this.campfires.push({ flames: fire.flames, light: fl });
    this.interactables.push({
      id: 'campfire', position: new THREE.Vector3(bc.x + 3.2, by + 0.6, bc.z - 0.5), radius: 3.2,
      label: () => 'Приготовить еду на костре',
      onUse: (g) => g.cookAll(),
    });

    // заброшенная машина на площадке рядом с гаражом (площадка для будущего транспорта)
    const gg = L.garage;
    const ggYaw = Math.atan2(L.square.x - gg.x, L.square.z - gg.z);
    const cp = this.localToWorld(gg.x, gg.z, ggYaw, 0, 11.5);
    const cy = heightAt(cp.x, cp.z);
    const car = await assets.prop('cars/jeep.glb', { width: 4.6 });
    if (car) {
      const g = propToGroup(car);
      g.position.set(cp.x, cy, cp.z);
      g.rotation.y = ggYaw + Math.PI / 2;
      this.group.add(g);
      this.colliders.add(makeBox(cp.x, cp.z, 1.1, 2.3, ggYaw + Math.PI / 2, 1.8));
      this.interactables.push({
        id: 'car', position: new THREE.Vector3(cp.x, cy + 1.3, cp.z), radius: 4.2,
        label: () => 'Старый джип',
        onUse: (gm) => gm.ui.toast('Транспорт (сборка, ремонт, вождение) появится на этапе 7. Сейчас джип — декорация.', 'info'),
      });
    }
  }

  private async buildStashes(assets: AssetManager): Promise<void> {
    const L = LANDMARKS;
    const find = (x: number, z: number, minH = 2.4): { x: number; z: number } => {
      for (let r = 0; r < 60; r += 3) {
        for (let a = 0; a < 6.28; a += 0.7) {
          const px = x + Math.cos(a) * r, pz = z + Math.sin(a) * r;
          if (isLand(px, pz, minH) && !isReserved(px, pz, -3)) return { x: px, z: pz };
        }
      }
      return { x, z };
    };
    const spots: [string, string, number, number, 'chest' | 'crate' | 'barrel', number?][] = [
      ['stash-hill', 'stash_rare', L.hill.x + 5, L.hill.z + 3, 'chest', 18],
      ['stash-quarry', 'stash_tools', L.quarry.x - 16, L.quarry.z + 8, 'crate'],
      ['stash-forest', 'stash_food', -105, 15, 'chest', 3],
      ['stash-beach', 'stash_food', L.beachCamp.x + 5, L.beachCamp.z + 6, 'barrel', 1.2],
      ['stash-north', 'stash_small', 30, -110, 'crate'],
    ];
    for (const [id, table, x, z, model, minH] of spots) {
      const p = find(x, z, minH ?? 2.4);
      await this.addStash(id, table, p.x, p.z, assets, model);
    }
  }

  private async buildSea(assets: AssetManager): Promise<void> {
    const L = LANDMARKS;
    const bobProp = await assets.prop('boats/buoy.glb', { height: 1.3 });
    for (const sp of L.fishingSpots) {
      if (!bobProp) break;
      const g = propToGroup(bobProp);
      const holder = new THREE.Group();
      holder.add(g);
      // буй чуть в стороне от точки лова
      holder.position.set(sp.x + 3, 0, sp.z + 3);
      this.group.add(holder);
      this.bobbers.push({ obj: holder, x: sp.x + 3, z: sp.z + 3, y: -0.2, phase: Math.random() * 6, roll: 0.12 });
    }
    // остов корабля у берега
    const wreck = await assets.prop('pirate/ship-wreck.glb', { width: 13 });
    if (wreck) {
      const perp = { x: L.dockDir.z, z: -L.dockDir.x };
      let x = L.beachCamp.x + perp.x * 42, z = L.beachCamp.z + perp.z * 42;
      for (let d = 0; d < 80; d += 2) {
        if (heightAt(x, z) < 0.2) break;
        x += L.dockDir.x * 2; z += L.dockDir.z * 2;
      }
      const g = propToGroup(wreck);
      g.position.set(x, -0.5, z);
      g.rotation.y = 0.9;
      this.group.add(g);
      this.colliders.add(makeCircle(x, z, 3.2));
    }
  }

  // ───────────────────────────── Зоны ─────────────────────────────

  private setupZones(): void {
    const L = LANDMARKS;
    const near = (p: { x: number; z: number }, r: number) => (x: number, z: number) => Math.hypot(x - p.x, z - p.z) < r;
    ZONE_NAMES.length = 0;
    ZONE_NAMES.push(
      ['Старый маяк', near(L.lighthouse, 30)],
      ['Заброшенная мастерская', near(L.workshop, 17)],
      ['Гараж', near(L.garage, 18)],
      ['Центральная площадь', near(L.square, 22)],
      ['Скальный карьер', near(L.quarry, 28)],
      ['Холм', near(L.hill, 40)],
      ['Рыбацкий причал', (x, z) => {
        const dx = x - L.dockStart.x, dz = z - L.dockStart.z;
        const s = dx * L.dockDir.x + dz * L.dockDir.z;
        const lat = Math.abs(dx * L.dockDir.z - dz * L.dockDir.x);
        return s > -6 && s < L.dockLength + 4 && lat < 7;
      }],
      ['Песчаный пляж', (x, z) => heightAt(x, z) < 1.7 && heightAt(x, z) > -0.5],
      ['Лесная зона', (x, z) => heightAt(x, z) > 1.7 && forestDensity(x, z) > 0.5],
    );
  }

  zoneAt(x: number, z: number): string {
    for (const [name, test] of ZONE_NAMES) if (test(x, z)) return name;
    return heightAt(x, z) < -0.5 ? 'Открытое море' : 'Грейхейвен';
  }

  // ───────────────────────────── Физика/запросы ─────────────────────────────

  /** Высота опоры под точкой: рельеф или плита (пирс, пол) в пределах шага. */
  groundAt(x: number, z: number, feetY: number): { y: number; platform: boolean } {
    let y = heightAt(x, z);
    let platform = false;
    for (const p of this.platforms) {
      if (p.top > y - 0.02 && p.top <= feetY + 0.7 && insidePlatform(p, x, z)) {
        if (p.top >= y) { y = p.top; platform = true; }
      }
    }
    return { y, platform };
  }

  /** Ближайший добываемый ресурс в поле зрения. */
  findResource(origin: THREE.Vector3, dir: THREE.Vector3, reach: number, kind?: 'tree' | 'rock'): ResourceNode | null {
    let best: ResourceNode | null = null;
    let bestScore = -1;
    const to = new THREE.Vector3();
    for (const r of this.resources) {
      if (r.respawn > 0) continue;
      if (kind && r.kind !== kind) continue;
      const dx = r.x - origin.x, dz = r.z - origin.z;
      const dist = Math.hypot(dx, dz) - r.r * 0.5;
      if (dist > reach) continue;
      to.set(dx, Math.min(r.height * 0.4, 2.2) + r.y - origin.y, dz).normalize();
      const dot = to.dot(dir);
      if (dot < 0.84) continue;
      const score = dot - dist * 0.02;
      if (score > bestScore) { bestScore = score; best = r; }
    }
    return best;
  }

  /** Наносит урон ресурсу. Возвращает true, если он разрушен. */
  damageResource(r: ResourceNode, dmg: number): boolean {
    r.hp -= dmg;
    if (r.hp > 0) return false;
    r.respawn = r.kind === 'tree' ? 720 : 480;
    r.collider.enabled = false;
    this.setResourceVisible(r, false);
    return true;
  }

  private cullProps: import('../renderer/assets').InstancedProp[] = [];
  private cullX = 1e9;
  private cullZ = 1e9;

  private setResourceVisible(r: ResourceNode, vis: boolean): void {
    if (vis) r.prop.setMatrix(r.index, r.matrix);
    else r.prop.setMatrix(r.index, new THREE.Matrix4().makeScale(0, 0, 0));
  }

  // ───────────────────────────── Обновление ─────────────────────────────

  update(dt: number, elapsed: number, night: number, waveAmp: number, playerPos: THREE.Vector3): void {
    this.nightValue = night;
    this.windowMat.emissiveIntensity = night * 1.8;
    this.lampMat.emissiveIntensity = night * 2.2;
    for (const u of this.updaters) u(dt, elapsed);
    if (Math.abs(playerPos.x - this.cullX) + Math.abs(playerPos.z - this.cullZ) > 6) {
      this.cullX = playerPos.x; this.cullZ = playerPos.z;
      for (const ip of this.cullProps) ip.updateCulling(playerPos.x, playerPos.z);
    }

    for (const r of this.resources) {
      if (r.respawn > 0) {
        r.respawn -= dt;
        if (r.respawn <= 0) {
          r.respawn = 0;
          r.hp = r.maxHp;
          r.collider.enabled = true;
          this.setResourceVisible(r, true);
        }
      }
    }

    for (const b of this.bobbers) {
      const y = waveHeight(b.x, b.z, elapsed, waveAmp) + b.y;
      b.obj.position.y = y;
      b.obj.rotation.z = Math.sin(elapsed * 0.9 + b.phase) * b.roll * (0.5 + waveAmp);
      b.obj.rotation.x = Math.cos(elapsed * 0.7 + b.phase) * b.roll * 0.7 * (0.5 + waveAmp);
    }

    for (const c of this.campfires) {
      c.flames.forEach((f, i) => {
        const s = 0.85 + Math.sin(elapsed * (9 + i * 2.3) + i) * 0.18 + Math.random() * 0.06;
        f.scale.set(s, 0.9 + Math.sin(elapsed * (7 + i * 3)) * 0.25, s);
      });
    }

    if (this.lighthouseBeam) {
      this.lighthouseBeam.rotation.y = elapsed * 0.7;
      const m = this.lighthouseBeam.userData.mat as THREE.MeshBasicMaterial | undefined;
      if (m) m.opacity = 0.16 * night;
    }

    this.assignLights(playerPos, elapsed);
  }

  /** Ограниченный пул PointLight: ближайшие источники получают свет. */
  private assignLights(p: THREE.Vector3, t: number): void {
    const cand: { l: LightSource; d: number }[] = [];
    for (const l of this.lights) {
      const on = l.nightOnly ? this.nightValue > 0.05 : true;
      if (!on) continue;
      const d = l.position.distanceToSquared(p);
      if (d < (l.distance * 3.2) ** 2) cand.push({ l, d });
    }
    cand.sort((a, b) => a.d - b.d);
    for (let i = 0; i < this.pool.length; i++) {
      const pl = this.pool[i];
      const c = cand[i];
      if (!c) { pl.intensity = 0; continue; }
      pl.position.copy(c.l.position);
      pl.color.setHex(c.l.color);
      pl.distance = c.l.distance;
      const k = c.l.nightOnly ? this.nightValue : 1;
      const flick = c.l.flicker ? 0.85 + Math.sin(t * 11) * 0.08 + Math.sin(t * 23.7) * 0.05 : 1;
      pl.intensity = c.l.intensity * k * flick * (c.l.nightOnly ? 1 : 0.35 + 0.65 * this.nightValue);
    }
  }

  // ───────────────────────────── Сохранение ─────────────────────────────

  private byKey: Map<string, ResourceNode> | null = null;

  findByKey(key: string): ResourceNode | undefined {
    if (!this.byKey) this.byKey = new Map(this.resources.map((r) => [r.key, r]));
    return this.byKey.get(key);
  }

  /** Применяет авторитетное состояние ресурса от сервера (кооператив). */
  applyResource(key: string, hp: number, alive: boolean): void {
    const r = this.findByKey(key);
    if (!r) return;
    r.hp = hp;
    if (alive) {
      r.respawn = 0;
      r.collider.enabled = true;
      this.setResourceVisible(r, true);
    } else {
      r.respawn = 1e9; // возрождением управляет сервер
      r.collider.enabled = false;
      this.setResourceVisible(r, false);
    }
  }

  stashTable(id: string): string | undefined { return this.stashes.get(id)?.table; }

  markStashOpened(id: string): void {
    const s = this.stashes.get(id);
    if (s) s.opened = true;
  }

  /** Применяет снимок мира комнаты (после resetState). */
  applySnapshot(snap: { resources: { key: string; hp: number; alive: boolean }[]; opened: string[] }): void {
    for (const r of snap.resources) this.applyResource(r.key, r.hp, r.alive);
    for (const id of snap.opened) this.markStashOpened(id);
  }

  /** Сброс состояния мира для новой игры. */
  resetState(): void {
    for (const s of this.stashes.values()) s.opened = false;
    for (const r of this.resources) {
      r.respawn = 0; r.hp = r.maxHp; r.collider.enabled = true;
      this.setResourceVisible(r, true);
    }
    for (const d of this.doors.values()) d.open = false;
  }

  serialize(): WorldSave {
    const doors: Record<string, boolean> = {};
    for (const [k, v] of this.doors) doors[k] = v.open;
    return {
      doors,
      opened: [...this.stashes.values()].filter((s) => s.opened).map((s) => s.id),
      resources: this.resources.filter((r) => r.respawn > 0).map((r) => ({ id: r.id, respawn: r.respawn })),
    };
  }

  restore(data: WorldSave | undefined): void {
    if (!data) return;
    for (const id of data.opened ?? []) {
      const s = this.stashes.get(id);
      if (s) s.opened = true;
    }
    for (const r of data.resources ?? []) {
      const node = this.resources.find((n) => n.id === r.id);
      if (node && Number.isFinite(r.respawn) && r.respawn > 0) {
        node.respawn = r.respawn;
        node.hp = 0;
        node.collider.enabled = false;
        this.setResourceVisible(node, false);
      }
    }
  }

  dispose(): void {
    this.group.traverse((o) => {
      const m = o as THREE.Mesh;
      if (m.isMesh) m.geometry?.dispose();
    });
  }
}

export { hashString };

void WORLD;
void makeRng;
