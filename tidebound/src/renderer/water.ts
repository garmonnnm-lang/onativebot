import * as THREE from 'three';
import { WORLD, heightAt } from '../../shared/terrain';

const HMIN = -14;
const HMAX = 30;

const vert = /* glsl */ `
uniform float uTime, uAmp;
varying vec3 vWorld;
varying float vWave;
#include <fog_pars_vertex>
void main() {
  vec3 p = position;
  float a1 = p.x * 0.11 + uTime * 0.9;
  float a2 = p.z * 0.15 - uTime * 1.1 + 1.7;
  float a3 = (p.x + p.z) * 0.07 + uTime * 0.6;
  float h = uAmp * (0.55 * sin(a1) + 0.35 * sin(a2) + 0.25 * sin(a3));
  p.y += h;
  vWave = h;
  vWorld = p;
  vec4 mvPosition = modelViewMatrix * vec4(p, 1.0);
  gl_Position = projectionMatrix * mvPosition;
  #include <fog_vertex>
}`;

const frag = /* glsl */ `
uniform float uTime, uAmp, uLight, uStorm;
uniform vec3 uSunDir, uSunColor, uSkyColor, uShallow, uDeep, uAmbient;
uniform sampler2D uHeightTex;
uniform float uTexSize, uHMin, uHMax;
varying vec3 vWorld;
varying float vWave;
#include <fog_pars_fragment>

float hash(vec2 p) { return fract(sin(dot(p, vec2(127.1, 311.7))) * 43758.5453); }
float noise(vec2 p) {
  vec2 i = floor(p), f = fract(p);
  f = f * f * (3.0 - 2.0 * f);
  return mix(mix(hash(i), hash(i + vec2(1, 0)), f.x), mix(hash(i + vec2(0, 1)), hash(i + vec2(1, 1)), f.x), f.y);
}

void main() {
  vec2 uv = vWorld.xz / uTexSize + 0.5;
  float hn = texture2D(uHeightTex, uv).r;
  float ground = mix(uHMin, uHMax, hn);
  float depth = max(0.0, -ground);
  if (abs(vWorld.x) > uTexSize * 0.5 || abs(vWorld.z) > uTexSize * 0.5) depth = 12.0;

  float a1 = vWorld.x * 0.11 + uTime * 0.9;
  float a2 = vWorld.z * 0.15 - uTime * 1.1 + 1.7;
  float a3 = (vWorld.x + vWorld.z) * 0.07 + uTime * 0.6;
  float dx = uAmp * (0.55 * 0.11 * cos(a1) + 0.25 * 0.07 * cos(a3));
  float dz = uAmp * (0.35 * 0.15 * cos(a2) + 0.25 * 0.07 * cos(a3));
  vec2 rp = vWorld.xz * 0.9;
  float r1 = noise(rp + vec2(uTime * 0.6, uTime * 0.3));
  float r2 = noise(rp * 1.9 - vec2(uTime * 0.4, -uTime * 0.5));
  vec2 ripple = (vec2(r1, r2) - 0.5) * (0.35 + uAmp * 0.25);
  vec3 N = normalize(vec3(-dx - ripple.x, 1.0, -dz - ripple.y));
  vec3 V = normalize(cameraPosition - vWorld);

  float fres = 0.03 + 0.97 * pow(1.0 - max(dot(N, V), 0.0), 4.0);
  float dd = smoothstep(0.0, 8.0, depth);
  vec3 body = mix(uShallow, uDeep, dd) * uAmbient;
  vec3 col = mix(body, uSkyColor * (0.55 + 0.45 * uLight), clamp(fres * 0.85, 0.0, 1.0));

  vec3 R = reflect(-V, N);
  float spec = pow(max(dot(R, normalize(uSunDir)), 0.0), 220.0);
  col += uSunColor * spec * 1.8 * uLight;

  float n = noise(vWorld.xz * 1.3 + uTime * 0.25);
  float shoreFoam = smoothstep(1.3, 0.0, depth) * (0.55 + 0.45 * sin(depth * 16.0 - uTime * 2.2 + n * 6.0));
  float crest = smoothstep(0.55, 1.0, vWave / max(uAmp, 0.01)) * (0.25 + uStorm * 0.9) * n;
  float foam = clamp(shoreFoam * 0.9 + crest, 0.0, 1.0);
  col = mix(col, vec3(0.92, 0.96, 1.0) * (0.35 + 0.65 * uLight), foam);

  float alpha = mix(0.0, 0.94, smoothstep(0.0, 0.7, depth));
  alpha = max(alpha, foam * 0.85);
  gl_FragColor = vec4(col, alpha);
  #include <tonemapping_fragment>
  #include <colorspace_fragment>
  #include <fog_fragment>
}`;

/** Анимированная поверхность моря. Волны считаются в вершинном шейдере (синхронно с shared/terrain.waveHeight). */
export class Water {
  readonly mesh: THREE.Mesh;
  readonly uniforms: Record<string, THREE.IUniform>;
  private heightTex: THREE.DataTexture;

  constructor(segments = 200) {
    this.heightTex = Water.bakeHeightTexture();
    this.uniforms = THREE.UniformsUtils.merge([
      THREE.UniformsLib.fog,
      {
        uTime: { value: 0 },
        uAmp: { value: 0.4 },
        uLight: { value: 1 },
        uStorm: { value: 0 },
        uSunDir: { value: new THREE.Vector3(0, 1, 0) },
        uSunColor: { value: new THREE.Color(0xfff2d0) },
        uSkyColor: { value: new THREE.Color(0x9ccbee) },
        uShallow: { value: new THREE.Color(0x35c8c0) },
        uDeep: { value: new THREE.Color(0x0a4f86) },
        uAmbient: { value: new THREE.Color(1, 1, 1) },
        uHeightTex: { value: this.heightTex },
        uTexSize: { value: WORLD.size },
        uHMin: { value: HMIN },
        uHMax: { value: HMAX },
      },
    ]);
    const mat = new THREE.ShaderMaterial({
      uniforms: this.uniforms,
      vertexShader: vert,
      fragmentShader: frag,
      transparent: true,
      depthWrite: false,
      fog: true,
    });
    const geo = new THREE.PlaneGeometry(1200, 1200, segments, segments);
    geo.rotateX(-Math.PI / 2);
    this.mesh = new THREE.Mesh(geo, mat);
    this.mesh.position.y = WORLD.seaLevel;
    this.mesh.frustumCulled = false;
    this.mesh.renderOrder = 10;
  }

  private static bakeHeightTexture(): THREE.DataTexture {
    const N = 256;
    const data = new Uint16Array(N * N);
    for (let j = 0; j < N; j++) {
      for (let i = 0; i < N; i++) {
        const x = (i / (N - 1) - 0.5) * WORLD.size;
        const z = (j / (N - 1) - 0.5) * WORLD.size;
        const n = Math.min(1, Math.max(0, (heightAt(x, z) - HMIN) / (HMAX - HMIN)));
        data[j * N + i] = THREE.DataUtils.toHalfFloat(n);
      }
    }
    const tex = new THREE.DataTexture(data, N, N, THREE.RedFormat, THREE.HalfFloatType);
    tex.minFilter = THREE.LinearFilter;
    tex.magFilter = THREE.LinearFilter;
    tex.wrapS = tex.wrapT = THREE.ClampToEdgeWrapping;
    tex.needsUpdate = true;
    return tex;
  }

  set time(t: number) { this.uniforms.uTime.value = t; }

  dispose(): void {
    this.mesh.geometry.dispose();
    (this.mesh.material as THREE.Material).dispose();
    this.heightTex.dispose();
  }
}
