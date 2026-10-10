import * as THREE from 'three';
import type { HeldKind } from '../../shared/items';
import { stdMat, COLORS } from '../world/structures';

/**
 * Предмет в руках (вид от первого лица). Модели процедурные (заглушки) — их можно заменить GLB.
 * Анимации: появление, покачивание, взмах, заброс, ожидание и вываживание.
 */
export class HeldItem {
  readonly root = new THREE.Group();
  private models = new Map<HeldKind, THREE.Group>();
  private kind: HeldKind = 'none';
  private equip = 1; // 0..1 (1 = убрано)
  private swingT = -1;
  private castT = -1;
  private time = 0;
  /** 0 — спокойно, 1 — вываживание (удилище гнётся). */
  reelAmount = 0;
  waiting = false;
  readonly rodTip = new THREE.Object3D();

  constructor(camera: THREE.Camera) {
    camera.add(this.root);
    this.root.position.set(0.32, -0.3, -0.6);
    this.build();
    this.setKind('none');
  }

  private build(): void {
    const wood = stdMat(COLORS.wood), woodD = stdMat(COLORS.woodDark), metal = stdMat(0x9aa4b0, 0.4), skin = stdMat(0xe0b08a, 0.8);
    const hand = () => {
      const h = new THREE.Mesh(new THREE.BoxGeometry(0.1, 0.09, 0.14), skin);
      return h;
    };

    // удочка
    const rod = new THREE.Group();
    const handle = new THREE.Mesh(new THREE.CylinderGeometry(0.018, 0.022, 0.32, 8), woodD);
    handle.rotation.x = Math.PI / 2;
    const shaft = new THREE.Mesh(new THREE.CylinderGeometry(0.004, 0.014, 1.5, 6), stdMat(0xd8b46a));
    shaft.rotation.x = Math.PI / 2;
    shaft.position.z = -0.9;
    const reel = new THREE.Mesh(new THREE.CylinderGeometry(0.035, 0.035, 0.04, 12), metal);
    reel.rotation.z = Math.PI / 2;
    reel.position.set(0.03, -0.03, -0.02);
    rod.add(handle, shaft, reel);
    const hnd = hand(); hnd.position.set(0, -0.01, 0.02); rod.add(hnd);
    this.rodTip.position.set(0, 0, -1.65);
    rod.add(this.rodTip);
    rod.rotation.x = 0.55;
    this.models.set('rod', rod);

    // топор
    const axe = new THREE.Group();
    const ah = new THREE.Mesh(new THREE.CylinderGeometry(0.022, 0.026, 0.62, 8), wood);
    ah.rotation.x = Math.PI / 2 * 0.0;
    ah.position.y = 0.1;
    const head = new THREE.Mesh(new THREE.BoxGeometry(0.05, 0.16, 0.2), metal);
    head.position.set(0, 0.36, -0.05);
    const edge = new THREE.Mesh(new THREE.BoxGeometry(0.02, 0.2, 0.06), stdMat(0xdfe6ee, 0.3));
    edge.position.set(0, 0.36, -0.17);
    axe.add(ah, head, edge);
    const ah2 = hand(); ah2.position.set(0, 0.0, 0); axe.add(ah2);
    axe.rotation.set(0.2, 0.15, 0);
    this.models.set('axe', axe);

    // молот
    const ham = new THREE.Group();
    const hh = new THREE.Mesh(new THREE.CylinderGeometry(0.022, 0.026, 0.6, 8), wood);
    hh.position.y = 0.1;
    const hd = new THREE.Mesh(new THREE.BoxGeometry(0.12, 0.12, 0.2), metal);
    hd.position.set(0, 0.4, 0);
    ham.add(hh, hd);
    const h3 = hand(); h3.position.set(0, 0.0, 0); ham.add(h3);
    ham.rotation.set(0.2, 0.15, 0);
    this.models.set('hammer', ham);

    // еда
    const food = new THREE.Group();
    const fb = new THREE.Mesh(new THREE.SphereGeometry(0.09, 10, 8), stdMat(0xd9a05b));
    fb.scale.set(1.4, 0.8, 1);
    const fh = hand(); fh.position.set(0, -0.08, 0.02);
    food.add(fb, fh);
    this.models.set('food', food);

    // вода
    const drink = new THREE.Group();
    const bt = new THREE.Mesh(new THREE.CylinderGeometry(0.04, 0.04, 0.2, 10), stdMat(0x7fc4f2, 0.15));
    const cap = new THREE.Mesh(new THREE.CylinderGeometry(0.018, 0.018, 0.04, 8), stdMat(0xffffff));
    cap.position.y = 0.12;
    const dh = hand(); dh.position.set(0, -0.06, 0.02);
    drink.add(bt, cap, dh);
    this.models.set('drink', drink);

    for (const m of this.models.values()) {
      m.traverse((o) => { o.frustumCulled = false; });
      m.visible = false;
      this.root.add(m);
    }
  }

  setKind(kind: HeldKind): void {
    if (kind === this.kind) return;
    this.kind = kind;
    this.equip = 1;
    for (const [k, m] of this.models) m.visible = k === kind;
  }

  get current(): HeldKind { return this.kind; }

  swing(): void { if (this.swingT < 0) this.swingT = 0; }
  cast(): void { this.castT = 0; }

  tipWorld(out: THREE.Vector3): THREE.Vector3 {
    this.rodTip.getWorldPosition(out);
    return out;
  }

  update(dt: number, speed: number, bob: number, bobPhase: number, sprinting: boolean): void {
    this.time += dt;
    this.equip = Math.max(0, this.equip - dt * 4.2);
    const e = this.equip * this.equip;
    const r = this.root;
    const sway = Math.sin(this.time * 1.4) * 0.004;
    let px = 0.32 + Math.cos(bobPhase) * 0.012 * bob + sway;
    let py = -0.3 + Math.sin(bobPhase * 2) * 0.012 * bob - e * 0.45;
    let pz = -0.6;
    let rx = 0, ry = 0, rz = 0;
    if (sprinting) { py -= 0.03; rx += 0.25; }

    if (this.swingT >= 0) {
      this.swingT += dt / 0.38;
      const t = this.swingT;
      const k = t < 0.35 ? -(t / 0.35) : -1 + ((t - 0.35) / 0.65) * 1.0; // замах назад → удар
      const hit = t >= 0.35 ? Math.sin(((t - 0.35) / 0.65) * Math.PI) : 0;
      rx += k * 0.7 - hit * 1.1 * (t >= 0.35 ? 1 : 0) * -1;
      pz -= hit * 0.18;
      py -= hit * 0.12;
      if (t >= 1) this.swingT = -1;
    }
    if (this.castT >= 0) {
      this.castT += dt / 0.7;
      const t = this.castT;
      const back = t < 0.35 ? t / 0.35 : 0;
      const fwd = t >= 0.35 ? Math.min(1, (t - 0.35) / 0.3) : 0;
      rx += -back * 0.7 + fwd * 0.9 - (t > 0.65 ? (t - 0.65) * 2.2 : 0);
      if (t >= 1) this.castT = -1;
    }
    if (this.kind === 'rod') {
      if (this.waiting) { rx += -0.12 + Math.sin(this.time * 2.2) * 0.01; }
      rx += -this.reelAmount * 0.35 + Math.sin(this.time * 24) * 0.012 * this.reelAmount;
      rz += Math.sin(this.time * 17) * 0.02 * this.reelAmount;
      py += this.reelAmount * 0.03;
    }
    r.position.set(px, py, pz);
    r.rotation.set(rx, ry, rz);
    void speed;
  }
}
