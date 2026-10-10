import * as THREE from 'three';
import { GLTFLoader } from 'three/examples/jsm/loaders/GLTFLoader.js';
import { FBXLoader } from 'three/examples/jsm/loaders/FBXLoader.js';
import { clone as cloneSkinned } from 'three/examples/jsm/utils/SkeletonUtils.js';

export interface PropPart {
  geometry: THREE.BufferGeometry;
  material: THREE.Material | THREE.Material[];
}

export interface Prop {
  parts: PropPart[];
  size: THREE.Vector3;
}

export interface PropOptions {
  /** Нормализовать по высоте (м). */
  height?: number;
  /** Нормализовать по наибольшей горизонтальной стороне (м). */
  width?: number;
  /** Множитель масштаба. */
  scale?: number;
  /** Не центрировать по XZ и не опускать низ на y=0. */
  keepOrigin?: boolean;
}

/**
 * Загрузчик моделей с кэшем. Ошибка загрузки не ломает игру: возвращается null,
 * вызывающий код использует процедурную замену.
 */
export class AssetManager {
  readonly failed: string[] = [];
  loaded = 0;
  requested = 0;
  onProgress: ((loaded: number, total: number) => void) | null = null;
  private gltf = new GLTFLoader();
  private fbx = new FBXLoader();
  private scenes = new Map<string, Promise<THREE.Object3D | null>>();
  private props = new Map<string, Promise<Prop | null>>();
  private base = import.meta.env.BASE_URL;

  loadScene(path: string): Promise<THREE.Object3D | null> {
    let p = this.scenes.get(path);
    if (p) return p;
    this.requested++;
    const url = this.base + 'models/' + path;
    const isFbx = path.toLowerCase().endsWith('.fbx');
    p = new Promise<THREE.Object3D | null>((resolve) => {
      const done = (obj: THREE.Object3D | null) => {
        this.loaded++;
        this.onProgress?.(this.loaded, this.requested);
        resolve(obj);
      };
      const fail = (err: unknown) => {
        console.warn(`[assets] не удалось загрузить ${path}`, err);
        this.failed.push(path);
        done(null);
      };
      try {
        if (isFbx) this.fbx.load(url, (o) => done(o), undefined, fail);
        else this.gltf.load(url, (g) => { g.scene.animations = g.animations; done(g.scene); }, undefined, fail);
      } catch (e) {
        fail(e);
      }
    });
    this.scenes.set(path, p);
    return p;
  }

  /** Копия сцены (со скелетами) для динамических объектов. */
  async clone(path: string): Promise<THREE.Object3D | null> {
    const s = await this.loadScene(path);
    if (!s) return null;
    const c = cloneSkinned(s);
    c.animations = s.animations;
    return c;
  }

  /** Статичная модель как набор геометрий (для instancing и дешёвых копий). */
  prop(path: string, opts: PropOptions = {}): Promise<Prop | null> {
    const key = path + JSON.stringify(opts);
    let p = this.props.get(key);
    if (!p) {
      p = this.loadScene(path).then((root) => (root ? this.toProp(root, opts) : null));
      this.props.set(key, p);
    }
    return p;
  }

  private toProp(root: THREE.Object3D, opts: PropOptions): Prop | null {
    root.updateMatrixWorld(true);
    const parts: PropPart[] = [];
    root.traverse((o) => {
      const m = o as THREE.Mesh;
      if (!m.isMesh || !m.geometry) return;
      const g = m.geometry.clone();
      g.applyMatrix4(m.matrixWorld);
      parts.push({ geometry: g, material: m.material });
    });
    if (!parts.length) return null;

    const box = new THREE.Box3();
    for (const p of parts) {
      p.geometry.computeBoundingBox();
      if (p.geometry.boundingBox) box.union(p.geometry.boundingBox);
    }
    const size = box.getSize(new THREE.Vector3());
    let s = opts.scale ?? 1;
    if (opts.height && size.y > 1e-5) s = opts.height / size.y;
    else if (opts.width) s = opts.width / Math.max(size.x, size.z, 1e-5);

    const center = box.getCenter(new THREE.Vector3());
    const mtx = new THREE.Matrix4();
    if (opts.keepOrigin) mtx.makeScale(s, s, s);
    else mtx.makeScale(s, s, s).multiply(new THREE.Matrix4().makeTranslation(-center.x, -box.min.y, -center.z));

    const nb = new THREE.Box3();
    for (const p of parts) {
      p.geometry.applyMatrix4(mtx);
      p.geometry.computeBoundingBox();
      p.geometry.computeBoundingSphere();
      if (p.geometry.boundingBox) nb.union(p.geometry.boundingBox);
    }
    return { parts, size: nb.getSize(new THREE.Vector3()) };
  }
}

/** Group из prop: геометрия и материалы общие у всех копий. */
export function propToGroup(prop: Prop, shadows = true): THREE.Group {
  const g = new THREE.Group();
  for (const part of prop.parts) {
    const m = new THREE.Mesh(part.geometry, part.material);
    m.castShadow = shadows;
    m.receiveShadow = shadows;
    g.add(m);
  }
  return g;
}

/**
 * InstancedMesh-набор для многократно повторяющегося объекта.
 * Экземпляры собираются по пространственным ячейкам (чанкам): у каждой ячейки свой InstancedMesh
 * с корректной ограничивающей сферой, поэтому работают отсечение по камере (в т.ч. для теней)
 * и отсечение по дальности. Чанки строятся при первом обращении к build()/updateCulling().
 */
export class InstancedProp {
  readonly meshes: THREE.InstancedMesh[] = [];
  readonly group = new THREE.Group();
  private pending: THREE.Matrix4[] = [];
  private built = false;
  private chunks: { cx: number; cz: number; r: number; meshes: THREE.InstancedMesh[] }[] = [];
  private slot: { chunk: number; local: number }[] = [];

  constructor(
    private prop: Prop,
    readonly capacity: number,
    private shadows = true,
    /** Размер ячейки, м. */
    private cell = 48,
    /** Дальность прорисовки, м (0 — без ограничения). */
    public maxDist = 0,
  ) {}

  get count(): number { return this.pending.length; }

  add(matrix: THREE.Matrix4): number {
    if (this.built || this.pending.length >= this.capacity) return -1;
    this.pending.push(matrix.clone());
    return this.pending.length - 1;
  }

  build(): void {
    if (this.built) return;
    this.built = true;
    const cells = new Map<string, number[]>();
    this.pending.forEach((m, i) => {
      const key = `${Math.floor(m.elements[12] / this.cell)},${Math.floor(m.elements[14] / this.cell)}`;
      const arr = cells.get(key);
      if (arr) arr.push(i); else cells.set(key, [i]);
    });
    this.slot = new Array(this.pending.length);
    for (const ids of cells.values()) {
      const chunkIdx = this.chunks.length;
      const ms: THREE.InstancedMesh[] = [];
      for (const part of this.prop.parts) {
        const mesh = new THREE.InstancedMesh(part.geometry, part.material, ids.length);
        mesh.castShadow = this.shadows;
        mesh.receiveShadow = this.shadows;
        mesh.frustumCulled = true;
        ids.forEach((gi, li) => mesh.setMatrixAt(li, this.pending[gi]));
        mesh.instanceMatrix.needsUpdate = true;
        mesh.computeBoundingSphere();
        mesh.computeBoundingBox();
        this.group.add(mesh);
        this.meshes.push(mesh);
        ms.push(mesh);
      }
      ids.forEach((gi, li) => { this.slot[gi] = { chunk: chunkIdx, local: li }; });
      const sph = ms[0].boundingSphere ?? new THREE.Sphere();
      this.chunks.push({ cx: sph.center.x, cz: sph.center.z, r: sph.radius, meshes: ms });
    }
  }

  /** Скрывает чанки дальше maxDist (радиус чанка учитывается). */
  updateCulling(x: number, z: number): void {
    if (!this.built) this.build();
    if (this.maxDist <= 0) return;
    for (const c of this.chunks) {
      const vis = Math.hypot(c.cx - x, c.cz - z) - c.r < this.maxDist;
      for (const m of c.meshes) m.visible = vis;
    }
  }

  setMatrix(i: number, matrix: THREE.Matrix4): void {
    if (!this.built) { this.pending[i].copy(matrix); return; }
    const sl = this.slot[i];
    if (!sl) return;
    for (const m of this.chunks[sl.chunk].meshes) {
      m.setMatrixAt(sl.local, matrix);
      m.instanceMatrix.needsUpdate = true;
    }
  }

  dispose(): void {
    for (const m of this.meshes) m.dispose();
  }
}
