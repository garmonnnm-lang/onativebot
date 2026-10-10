import * as THREE from 'three';

const vert = /* glsl */ `
attribute vec2 aRand; // x: смещение по времени, y: длина
uniform float uTime, uSpeed, uHeight, uSlant;
uniform vec3 uCenter;
uniform float uSize;
varying float vA;
void main() {
  vec3 p = position;
  float fall = mod(aRand.x * uHeight - uTime * uSpeed, uHeight);
  vec3 w = vec3(
    uCenter.x + (mod(p.x - uCenter.x + uSize * 0.5, uSize) - uSize * 0.5),
    uCenter.y - 6.0 + fall,
    uCenter.z + (mod(p.z - uCenter.z + uSize * 0.5, uSize) - uSize * 0.5)
  );
  // нижний конец отрезка (position.y маркирует конец) сдвинут вниз и по ветру
  float end = p.y;
  w.y -= end * aRand.y;
  w.x -= end * aRand.y * uSlant;
  vA = 1.0 - end * 0.8;
  gl_Position = projectionMatrix * viewMatrix * vec4(w, 1.0);
}`;

const frag = /* glsl */ `
uniform float uOpacity;
uniform vec3 uColor;
varying float vA;
void main() { gl_FragColor = vec4(uColor, uOpacity * vA); }`;

/** Дождь: отрезки, падающие в шейдере вокруг камеры. Количество капель зависит от качества и силы дождя. */
export class Rain {
  readonly mesh: THREE.LineSegments;
  private uniforms = {
    uTime: { value: 0 },
    uSpeed: { value: 30 },
    uHeight: { value: 28 },
    uSlant: { value: 0.1 },
    uCenter: { value: new THREE.Vector3() },
    uSize: { value: 34 },
    uOpacity: { value: 0.0 },
    uColor: { value: new THREE.Color(0xbcd4ea) },
  };
  private maxCount: number;

  constructor(count: number) {
    this.maxCount = count;
    const pos = new Float32Array(count * 6);
    const rnd = new Float32Array(count * 4);
    for (let i = 0; i < count; i++) {
      const x = Math.random() * 34, z = Math.random() * 34;
      const r = Math.random();
      const len = 0.5 + Math.random() * 0.6;
      pos.set([x, 0, z, x, 1, z], i * 6);
      rnd.set([r, len, r, len], i * 4);
    }
    const g = new THREE.BufferGeometry();
    g.setAttribute('position', new THREE.BufferAttribute(pos, 3));
    g.setAttribute('aRand', new THREE.BufferAttribute(rnd, 2));
    this.mesh = new THREE.LineSegments(g, new THREE.ShaderMaterial({
      uniforms: this.uniforms, vertexShader: vert, fragmentShader: frag, transparent: true, depthWrite: false, fog: false,
    }));
    this.mesh.frustumCulled = false;
    this.mesh.renderOrder = 20;
    this.mesh.visible = false;
  }

  update(t: number, center: THREE.Vector3, intensity: number, wind: number, daylight: number): void {
    const u = this.uniforms;
    u.uTime.value = t;
    u.uCenter.value.copy(center);
    u.uSlant.value = 0.08 + wind * 0.35;
    u.uOpacity.value = 0.12 + 0.38 * Math.min(1, intensity);
    u.uColor.value.setRGB(0.55 + 0.3 * daylight, 0.65 + 0.25 * daylight, 0.78 + 0.17 * daylight);
    const n = Math.floor(this.maxCount * Math.min(1, intensity));
    this.mesh.geometry.setDrawRange(0, n * 2);
    this.mesh.visible = n > 20;
  }

  dispose(): void {
    this.mesh.geometry.dispose();
    (this.mesh.material as THREE.Material).dispose();
  }
}
