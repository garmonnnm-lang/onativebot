import * as THREE from 'three';
import { LANDMARKS, WORLD, fbm, heightAt, slopeAt, smoothstep } from '../../shared/terrain';

const SAND = new THREE.Color(0xe6d29a);
const SAND_WET = new THREE.Color(0xb59c68);
const SEABED = new THREE.Color(0x2c7377);
const GRASS_A = new THREE.Color(0x73b94f);
const GRASS_B = new THREE.Color(0x4f9440);
const FOREST = new THREE.Color(0x3c7a3a);
const ROCK = new THREE.Color(0x8d9199);
const ROCK_DARK = new THREE.Color(0x6a6e78);
const PATH = new THREE.Color(0xb8aa8b);
const DIRT = new THREE.Color(0x8a6d4b);

const tmpGrass = new THREE.Color();
const tmpRock = new THREE.Color();

/** Лесная «плотность» (0..1) — используется и для окраски земли, и для расстановки деревьев. */
export function forestDensity(x: number, z: number): number {
  return smoothstep(0.5, 0.68, fbm(x * 0.02 + 40, z * 0.02 - 40, 3, 5));
}

/** Цвет вершины рельефа по высоте, уклону и району. */
export function terrainColor(x: number, z: number, h: number, slope: number, out: THREE.Color): THREE.Color {
  const n = fbm(x * 0.05, z * 0.05, 3, 77);
  const n2 = fbm(x * 0.25, z * 0.25, 2, 91);
  if (h < 0) return out.copy(SAND_WET).lerp(SEABED, smoothstep(0.0, -5, h)).multiplyScalar(0.9 + n2 * 0.2);
  if (h < 1.45) {
    out.copy(SAND_WET).lerp(SAND, smoothstep(0.0, 0.7, h));
    return out.multiplyScalar(0.92 + n2 * 0.16);
  }
  tmpGrass.copy(GRASS_A).lerp(GRASS_B, n).lerp(FOREST, forestDensity(x, z) * 0.7);
  out.copy(tmpGrass);
  out.lerp(SAND, 1 - smoothstep(1.45, 2.4, h));
  const rockK = Math.max(smoothstep(0.55, 0.95, slope), smoothstep(17, 24, h));
  out.lerp(tmpRock.copy(ROCK).lerp(ROCK_DARK, n2), rockK);
  const dq = Math.hypot(x - LANDMARKS.quarry.x, z - LANDMARKS.quarry.z);
  out.lerp(ROCK, (1 - smoothstep(10, 26, dq)) * 0.85);
  const ds = Math.hypot(x - LANDMARKS.square.x, z - LANDMARKS.square.z);
  out.lerp(PATH, (1 - smoothstep(9, 15, ds)) * 0.9);
  for (const p of LANDMARKS.buildPlots) {
    const d = Math.hypot(x - p.x, z - p.z);
    out.lerp(DIRT, (1 - smoothstep(p.r * 0.7, p.r + 4, d)) * 0.55);
  }
  return out.multiplyScalar(0.94 + n2 * 0.12);
}

/** Меш рельефа с вершинными цветами. */
export function buildTerrainMesh(segments = 230): THREE.Mesh {
  const geo = new THREE.PlaneGeometry(WORLD.size, WORLD.size, segments, segments);
  geo.rotateX(-Math.PI / 2);
  const pos = geo.getAttribute('position') as THREE.BufferAttribute;
  const colors = new Float32Array(pos.count * 3);
  const c = new THREE.Color();
  for (let i = 0; i < pos.count; i++) {
    const x = pos.getX(i);
    const z = pos.getZ(i);
    const h = heightAt(x, z);
    pos.setY(i, h);
    terrainColor(x, z, h, slopeAt(x, z), c);
    colors[i * 3] = c.r;
    colors[i * 3 + 1] = c.g;
    colors[i * 3 + 2] = c.b;
  }
  geo.setAttribute('color', new THREE.BufferAttribute(colors, 3));
  geo.computeVertexNormals();
  const mesh = new THREE.Mesh(geo, new THREE.MeshStandardMaterial({ vertexColors: true, roughness: 1, metalness: 0 }));
  mesh.receiveShadow = true;
  mesh.name = 'terrain';
  return mesh;
}
