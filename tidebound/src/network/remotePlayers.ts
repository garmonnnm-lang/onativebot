import * as THREE from 'three';
import type { HeldNet, PlayerPublic, PlayerState } from '../../shared/net';

/**
 * Другие игроки комнаты. Пока это процедурный низкополигональный человечек
 * (временная замена, пока не подключены анимированные модели из набора Animated Men):
 * ходьба, взмах инструментом, предмет в руке, имя над головой.
 */
const SKIN = 0xf0c6a0;

function boxMesh(w: number, h: number, d: number, color: number, yOffset = 0): THREE.Mesh {
  const g = new THREE.BoxGeometry(w, h, d);
  g.translate(0, yOffset, 0);
  const m = new THREE.Mesh(g, new THREE.MeshStandardMaterial({ color, roughness: 0.85, flatShading: true }));
  m.castShadow = true;
  return m;
}

function nameSprite(name: string, color: number): THREE.Sprite {
  const c = document.createElement('canvas');
  c.width = 256; c.height = 64;
  const x = c.getContext('2d')!;
  x.fillStyle = 'rgba(5,16,38,0.7)';
  x.beginPath();
  x.roundRect(4, 8, 248, 48, 12);
  x.fill();
  x.fillStyle = '#' + new THREE.Color(color).getHexString();
  x.fillRect(14, 22, 8, 20);
  x.fillStyle = '#fff';
  x.font = '600 26px system-ui, sans-serif';
  x.textBaseline = 'middle';
  x.fillText(name.length > 14 ? name.slice(0, 13) + '…' : name, 30, 33);
  const tex = new THREE.CanvasTexture(c);
  tex.colorSpace = THREE.SRGBColorSpace;
  const sp = new THREE.Sprite(new THREE.SpriteMaterial({ map: tex, transparent: true, depthWrite: false, fog: false }));
  sp.scale.set(1.6, 0.4, 1);
  sp.position.y = 2.15;
  sp.renderOrder = 10;
  return sp;
}

class Avatar {
  readonly group = new THREE.Group();
  private body: THREE.Group;
  private armR: THREE.Group;
  private armL: THREE.Group;
  private legR: THREE.Group;
  private legL: THREE.Group;
  private heldGroup = new THREE.Group();
  private tag: THREE.Sprite | null = null;
  private tagKey = '';
  private phase = 0;
  private swingT = 0;
  private held: HeldNet = 'none';
  pos = new THREE.Vector3();
  target = new THREE.Vector3();
  yaw = 0;
  targetYaw = 0;
  pitch = 0;
  speed = 0;
  swim = false;
  fresh = true;

  constructor(color: number) {
    this.body = new THREE.Group();
    this.group.add(this.body);
    const torso = boxMesh(0.5, 0.72, 0.28, color, 1.18);
    const head = new THREE.Mesh(new THREE.SphereGeometry(0.2, 12, 10), new THREE.MeshStandardMaterial({ color: SKIN, roughness: 0.8 }));
    head.position.y = 1.78;
    head.castShadow = true;
    const hair = boxMesh(0.4, 0.12, 0.4, 0x3a2a1c, 1.95);
    this.body.add(torso, head, hair);

    const limb = (w: number, h: number, c: number, x: number, y: number): THREE.Group => {
      const g = new THREE.Group();
      g.position.set(x, y, 0);
      g.add(boxMesh(w, h, w, c, -h / 2));
      this.body.add(g);
      return g;
    };
    this.legR = limb(0.2, 0.8, 0x2d3a55, 0.13, 0.82);
    this.legL = limb(0.2, 0.8, 0x2d3a55, -0.13, 0.82);
    this.armR = limb(0.16, 0.66, color, 0.34, 1.5);
    this.armL = limb(0.16, 0.66, color, -0.34, 1.5);
    const hand = boxMesh(0.14, 0.14, 0.14, SKIN, -0.68);
    this.armR.add(hand);
    this.heldGroup.position.set(0, -0.68, 0.08);
    this.armR.add(this.heldGroup);
  }

  setName(name: string, color: number): void {
    const key = `${name}|${color}`;
    if (key === this.tagKey) return;
    this.tagKey = key;
    if (this.tag) { this.group.remove(this.tag); (this.tag.material as THREE.SpriteMaterial).map?.dispose(); this.tag.material.dispose(); }
    this.tag = nameSprite(name, color);
    this.group.add(this.tag);
  }

  setHeld(h: HeldNet): void {
    if (h === this.held) return;
    this.held = h;
    this.heldGroup.clear();
    const wood = 0x7a5230, metal = 0x9aa4b0;
    if (h === 'rod') {
      const r = new THREE.Mesh(new THREE.CylinderGeometry(0.015, 0.025, 1.5, 6), new THREE.MeshStandardMaterial({ color: wood }));
      r.rotation.x = Math.PI / 2 - 0.5; r.position.set(0, 0.1, 0.55);
      this.heldGroup.add(r);
    } else if (h === 'axe') {
      const s = boxMesh(0.05, 0.6, 0.05, wood, 0.15);
      const hd = boxMesh(0.04, 0.16, 0.22, metal, 0.4);
      hd.position.z = 0.08;
      this.heldGroup.add(s, hd);
    } else if (h === 'hammer') {
      const s = boxMesh(0.05, 0.55, 0.05, wood, 0.12);
      const hd = boxMesh(0.12, 0.12, 0.26, 0x555d68, 0.4);
      this.heldGroup.add(s, hd);
    } else if (h === 'food' || h === 'drink') {
      this.heldGroup.add(boxMesh(0.1, h === 'drink' ? 0.2 : 0.08, 0.1, h === 'drink' ? 0x66b8ff : 0xd9a066, 0.05));
    }
    this.heldGroup.rotation.x = h === 'rod' ? -0.3 : 0.2;
  }

  swing(): void { this.swingT = 0.32; }

  update(dt: number): void {
    const k = 1 - Math.exp(-dt * 12);
    if (this.fresh) { this.pos.copy(this.target); this.yaw = this.targetYaw; this.fresh = false; }
    this.pos.lerp(this.target, k);
    let dy = this.targetYaw - this.yaw;
    dy = Math.atan2(Math.sin(dy), Math.cos(dy));
    this.yaw += dy * k;
    this.group.position.copy(this.pos);
    this.group.rotation.y = this.yaw + Math.PI; // модель смотрит в +Z, игрок — в -Z
    const mv = Math.min(1, this.speed / 4);
    this.phase += dt * (3 + this.speed * 1.4);
    const sw = Math.sin(this.phase) * 0.8 * mv;
    this.legR.rotation.x = sw; this.legL.rotation.x = -sw;
    this.armL.rotation.x = sw * 0.8;
    if (this.swingT > 0) {
      this.swingT = Math.max(0, this.swingT - dt);
      const p = 1 - this.swingT / 0.32;
      this.armR.rotation.x = -2.2 + Math.sin(p * Math.PI) * -0.4 + p * 2.0;
    } else if (this.held === 'rod') {
      this.armR.rotation.x = -1.1 - this.pitch * 0.4;
    } else {
      this.armR.rotation.x = -sw * 0.8;
    }
    this.body.rotation.x = this.swim ? 1.2 : 0;
    this.body.position.y = this.swim ? -0.5 : 0;
  }

  dispose(): void {
    this.group.traverse((o) => {
      const m = o as THREE.Mesh;
      if (m.geometry) m.geometry.dispose();
      const mat = (m as { material?: THREE.Material | THREE.Material[] }).material;
      if (Array.isArray(mat)) mat.forEach((x) => x.dispose()); else mat?.dispose();
    });
  }
}

export class RemotePlayers {
  private avatars = new Map<string, Avatar>();
  private info = new Map<string, PlayerPublic>();
  readonly group = new THREE.Group();

  constructor(private myId: () => string) {}

  setPublic(players: PlayerPublic[]): void {
    this.info.clear();
    for (const p of players) this.info.set(p.id, p);
    for (const [id, a] of this.avatars) {
      const p = this.info.get(id);
      if (!p) { this.group.remove(a.group); a.dispose(); this.avatars.delete(id); }
      else a.setName(p.name, p.color);
    }
  }

  applyState(states: PlayerState[]): void {
    const me = this.myId();
    for (const s of states) {
      if (s.id === me) continue;
      const pub = this.info.get(s.id);
      if (!pub) continue;
      let a = this.avatars.get(s.id);
      if (!a) {
        a = new Avatar(pub.color);
        a.setName(pub.name, pub.color);
        this.avatars.set(s.id, a);
        this.group.add(a.group);
      }
      a.target.set(s.x, s.y, s.z);
      a.targetYaw = s.yaw;
      a.pitch = s.pitch;
      a.speed = s.speed;
      a.swim = s.swim;
      a.setHeld(s.held);
    }
  }

  swing(id: string): void { this.avatars.get(id)?.swing(); }

  update(dt: number): void {
    for (const a of this.avatars.values()) a.update(dt);
  }

  clear(): void {
    for (const a of this.avatars.values()) { this.group.remove(a.group); a.dispose(); }
    this.avatars.clear();
    this.info.clear();
  }

  /** Позиции для миникарты. */
  positions(): { id: string; name: string; color: number; x: number; z: number }[] {
    const out: { id: string; name: string; color: number; x: number; z: number }[] = [];
    for (const [id, a] of this.avatars) {
      const p = this.info.get(id);
      if (p?.online) out.push({ id, name: p.name, color: p.color, x: a.pos.x, z: a.pos.z });
    }
    return out;
  }
}
