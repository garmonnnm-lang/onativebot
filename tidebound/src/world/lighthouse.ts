import * as THREE from 'three';
import { AssetManager } from '../renderer/assets';
import { makeBox, makeCircle } from './colliders';
import { heightAt } from '../../shared/terrain';
import { COLORS, stdMat, box } from './structures';
import type { Collider } from './types';

const TOWER_RE = /^07[1-9]-/;
const EXTRA_RE = /^(080|081|082|083|084|087|088|091|092|093|094|095|096|097|098|099|105|107|108|109|110|111|112|113|115|118|119|120|145|146|147|148)-/;
const TOP_NAME = /^\d{3}-[a-z0-9-]+$/;

export interface LighthouseResult {
  group: THREE.Group;
  colliders: Collider[];
  /** Мировая позиция лампы (для света/луча). */
  lamp: THREE.Vector3;
  beam: THREE.Object3D;
  /** true, если использована загруженная модель пользователя, а не процедурная замена. */
  fromModel: boolean;
  height: number;
}

/**
 * Маяк из GLB пользователя (light-station-and-rocky-point): берутся только башня, дом смотрителя и мелкие
 * постройки, а рельеф/море модели не используются — их заменяет собственный ландшафт игры.
 * Если модель не загрузилась, строится процедурная замена.
 */
export async function buildLighthouse(assets: AssetManager, x: number, y: number, z: number, targetHeight = 26): Promise<LighthouseResult> {
  const out = new THREE.Group();
  out.name = 'lighthouse';
  out.position.set(x, y, z);
  const colliders: Collider[] = [];

  const root = await assets.loadScene('user/lighthouse_station.glb');
  const holder = new THREE.Group();
  const tower = new THREE.Group();
  const extra = new THREE.Group();
  holder.add(tower, extra);
  let ok = false;

  if (root) {
    root.updateMatrixWorld(true);
    const take = (obj: THREE.Object3D) => {
      if (TOP_NAME.test(obj.name) && (TOWER_RE.test(obj.name) || EXTRA_RE.test(obj.name))) {
        const c = obj.clone(true);
        c.matrixAutoUpdate = false;
        c.matrix.copy(obj.matrixWorld);
        (TOWER_RE.test(obj.name) ? tower : extra).add(c);
        return;
      }
      for (const ch of obj.children) take(ch);
    };
    take(root);
    ok = tower.children.length >= 4;
  }

  if (ok) {
    holder.updateMatrixWorld(true);
    const tb = new THREE.Box3().setFromObject(tower);
    const size = tb.getSize(new THREE.Vector3());
    const s = targetHeight / Math.max(size.y, 1e-6);
    holder.scale.setScalar(s);
    const c = tb.getCenter(new THREE.Vector3());
    holder.position.set(-c.x * s, -tb.min.y * s - 0.2, -c.z * s);
    holder.traverse((o) => {
      const m = o as THREE.Mesh;
      if (m.isMesh) { m.castShadow = true; m.receiveShadow = true; }
    });
    out.add(holder);
    out.updateMatrixWorld(true);

    // Окружение маяка в модели расставлено по её собственной скале, поэтому каждый предмет
    // опускается/поднимается на высоту нашего рельефа; всё, что оказалось в воде, убирается.
    const shift = new THREE.Matrix4();
    for (const child of [...extra.children]) {
      const bb = new THREE.Box3().setFromObject(child);
      const cc = bb.getCenter(new THREE.Vector3());
      const gy = heightAt(cc.x, cc.z);
      if (gy < 0.9) { extra.remove(child); continue; }
      const dyLocal = (gy - bb.min.y) / s;
      child.matrix.premultiply(shift.makeTranslation(0, dyLocal, 0));
      child.updateMatrixWorld(true);
    }
    out.updateMatrixWorld(true);

    // башня: окружность по основанию (плинт)
    const plinth = tower.children.find((c) => /^071-/.test(c.name));
    const pb = new THREE.Box3().setFromObject(plinth ?? tower);
    const psz = pb.getSize(new THREE.Vector3());
    const pc = pb.getCenter(new THREE.Vector3());
    colliders.push(makeCircle(pc.x, pc.z, Math.max(psz.x, psz.z) * 0.5 * 0.95));
    // дом смотрителя, сараи, ограда и крупный реквизит — коллайдеры по габаритам
    const SOLID = /^(080|081|082|083|084|087|088|091|092|094|095|096|097|098|099|105|107|108|109|110|145|146|147|148)-/;
    for (const child of extra.children) {
      if (!SOLID.test(child.name)) continue;
      const bb = new THREE.Box3().setFromObject(child);
      const sz = bb.getSize(new THREE.Vector3());
      const cc = bb.getCenter(new THREE.Vector3());
      colliders.push(makeBox(cc.x, cc.z, Math.max(0.2, sz.x * 0.5), Math.max(0.2, sz.z * 0.5), 0));
    }
    const lampPos = new THREE.Vector3(x, y + targetHeight * 0.9, z);
    const beam = makeBeam();
    beam.position.copy(lampPos);
    beam.position.sub(out.position);
    out.add(beam);
    return { group: out, colliders, lamp: lampPos, beam, fromModel: true, height: targetHeight };
  }

  // ── процедурная замена ──
  console.warn('[lighthouse] модель пользователя не найдена — используется временная процедурная башня');
  const white = stdMat(0xf1eee4);
  const red = stdMat(0xc8453a);
  const stages = 6;
  for (let i = 0; i < stages; i++) {
    const r0 = 3.4 - i * 0.28, r1 = r0 - 0.28;
    const seg = new THREE.Mesh(new THREE.CylinderGeometry(r1, r0, targetHeight / stages / 1.15, 20), i % 2 ? red : white);
    seg.position.y = 1 + i * (targetHeight / stages / 1.15) + targetHeight / stages / 2.3;
    seg.castShadow = true;
    out.add(seg);
  }
  const gallery = new THREE.Mesh(new THREE.CylinderGeometry(2.6, 2.4, 0.4, 20), stdMat(COLORS.metal));
  gallery.position.y = targetHeight * 0.88;
  out.add(gallery);
  const lantern = new THREE.Mesh(new THREE.CylinderGeometry(1.2, 1.2, 2, 16), stdMat(0xffe9a0, 0.3, 0xffd25a, 1.2));
  lantern.position.y = targetHeight * 0.88 + 1.2;
  out.add(lantern);
  const dome = new THREE.Mesh(new THREE.ConeGeometry(1.6, 1.6, 16), red);
  dome.position.y = targetHeight * 0.88 + 3;
  out.add(dome);
  const hut = box(5, 3, 4, stdMat(COLORS.wallSand));
  hut.position.set(7, 1.5, 3);
  out.add(hut);
  colliders.push(makeCircle(x, z, 3.2));
  colliders.push(makeBox(x + 7, z + 3, 2.5, 2));
  const lampPos = new THREE.Vector3(x, y + targetHeight * 0.88 + 1.2, z);
  const beam = makeBeam();
  beam.position.set(0, targetHeight * 0.88 + 1.2, 0);
  out.add(beam);
  return { group: out, colliders, lamp: lampPos, beam, fromModel: false, height: targetHeight };
}

/** Вращающийся световой луч маяка (аддитивный конус, виден ночью). */
function makeBeam(): THREE.Object3D {
  const g = new THREE.Group();
  const mat = new THREE.MeshBasicMaterial({
    color: 0xfff2b0, transparent: true, opacity: 0.0, blending: THREE.AdditiveBlending, depthWrite: false, side: THREE.DoubleSide, fog: false,
  });
  for (const dir of [1, -1]) {
    const cone = new THREE.Mesh(new THREE.ConeGeometry(5, 90, 16, 1, true), mat);
    cone.rotation.z = (dir * Math.PI) / 2;
    cone.position.x = dir * 45;
    g.add(cone);
  }
  g.userData.mat = mat;
  return g;
}
