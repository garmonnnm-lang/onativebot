import * as THREE from 'three';
import { getItem, RARITY_COLORS } from '../../shared/items';
import type { ItemStack } from '../../shared/inventory';
import { heightAt } from '../../shared/terrain';
import type { Interactable } from '../world/types';
import type { Game } from '../core/game';

export interface DroppedSave { x: number; z: number; stacks: ItemStack[]; ttl: number; bag: boolean }

interface Entry {
  mesh: THREE.Object3D;
  inter: Interactable;
  data: DroppedSave;
}

/** Выброшенные предметы и рюкзаки погибших игроков. Срок жизни настраивается (ttl, сек). */
export class DroppedItems {
  private entries: Entry[] = [];
  private group = new THREE.Group();
  static readonly TTL = 900;

  constructor(private game: Game) {
    game.renderer.scene.add(this.group);
  }

  spawn(x: number, z: number, stacks: ItemStack[], bag = false, ttl = DroppedItems.TTL): void {
    if (!stacks.length) return;
    const y = Math.max(0.05, this.game.world.groundAt(x, z, 99).y);
    let mesh: THREE.Object3D;
    if (bag) {
      const g = new THREE.Group();
      const body = new THREE.Mesh(new THREE.BoxGeometry(0.5, 0.55, 0.3), new THREE.MeshStandardMaterial({ color: 0x8a5a2b, roughness: 0.9 }));
      body.position.y = 0.3;
      const flap = new THREE.Mesh(new THREE.BoxGeometry(0.5, 0.2, 0.34), new THREE.MeshStandardMaterial({ color: 0x5b3a1b, roughness: 0.9 }));
      flap.position.y = 0.6;
      g.add(body, flap);
      mesh = g;
    } else {
      const def = getItem(stacks[0].id);
      mesh = new THREE.Mesh(new THREE.OctahedronGeometry(0.2, 0), new THREE.MeshStandardMaterial({
        color: RARITY_COLORS[def.rarity], roughness: 0.5, emissive: RARITY_COLORS[def.rarity], emissiveIntensity: 0.25,
      }));
      mesh.position.y = 0.3;
    }
    const holder = new THREE.Group();
    holder.position.set(x, y, z);
    holder.add(mesh);
    mesh.traverse((o) => { o.castShadow = true; });
    this.group.add(holder);

    const data: DroppedSave = { x, z, stacks, ttl, bag };
    const label = () => (bag
      ? 'Подобрать рюкзак'
      : `Подобрать: ${stacks.map((s) => `${getItem(s.id).name} ×${s.count}`).join(', ')}`);
    const inter: Interactable = {
      id: `drop-${Math.random().toString(36).slice(2)}`,
      position: new THREE.Vector3(x, y + 0.4, z),
      radius: 2.8,
      label,
      onUse: (g) => {
        if (!g.inventory.hasRoomFor(data.stacks)) { g.ui.toast('Инвентарь полон.', 'warn'); return; }
        for (const s of data.stacks) g.inventory.add(s.id, s.count);
        g.audio.pickup();
        g.ui.toast(bag ? 'Вы подобрали рюкзак.' : 'Подобрано.', 'good');
        this.remove(entry);
      },
    };
    const entry: Entry = { mesh: holder, inter, data };
    this.entries.push(entry);
    this.game.world.interactables.push(inter);
  }

  private remove(e: Entry): void {
    this.group.remove(e.mesh);
    const i = this.game.world.interactables.indexOf(e.inter);
    if (i >= 0) this.game.world.interactables.splice(i, 1);
    const k = this.entries.indexOf(e);
    if (k >= 0) this.entries.splice(k, 1);
  }

  update(dt: number, t: number): void {
    for (const e of [...this.entries]) {
      e.data.ttl -= dt;
      if (e.data.ttl <= 0) { this.remove(e); continue; }
      e.mesh.children[0].rotation.y = t * 1.2;
      if (!e.data.bag) e.mesh.children[0].position.y = 0.35 + Math.sin(t * 2 + e.data.x) * 0.06;
    }
  }

  clear(): void { for (const e of [...this.entries]) this.remove(e); }

  serialize(): DroppedSave[] {
    return this.entries.map((e) => ({ ...e.data, stacks: e.data.stacks.map((s) => ({ ...s })) }));
  }

  restore(list: DroppedSave[] | undefined): void {
    this.clear();
    for (const d of list ?? []) {
      if (!Number.isFinite(d.x) || !Number.isFinite(d.z) || !Array.isArray(d.stacks)) continue;
      try { for (const s of d.stacks) getItem(s.id); } catch { continue; }
      this.spawn(d.x, d.z, d.stacks, !!d.bag, Math.max(10, d.ttl));
    }
  }

  get count(): number { return this.entries.length; }
  ground(x: number, z: number): number { return heightAt(x, z); }
}
