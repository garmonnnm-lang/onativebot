import * as THREE from 'three';

const vert = /* glsl */ `
varying vec3 vDir;
void main() {
  vDir = position;
  vec4 p = modelViewMatrix * vec4(position, 1.0);
  gl_Position = projectionMatrix * p;
  gl_Position.z = gl_Position.w;
}`;

const frag = /* glsl */ `
varying vec3 vDir;
uniform vec3 uTop, uHorizon, uSunDir, uMoonDir, uSunColor, uCloudColor;
uniform float uTime, uStars, uCloud, uDark, uFlash;

float hash(vec2 p) { return fract(sin(dot(p, vec2(127.1, 311.7))) * 43758.5453); }
float noise(vec2 p) {
  vec2 i = floor(p), f = fract(p);
  f = f * f * (3.0 - 2.0 * f);
  return mix(mix(hash(i), hash(i + vec2(1, 0)), f.x), mix(hash(i + vec2(0, 1)), hash(i + vec2(1, 1)), f.x), f.y);
}
float fbm(vec2 p) {
  float s = 0.0, a = 0.5;
  for (int i = 0; i < 5; i++) { s += noise(p) * a; p *= 2.02; a *= 0.5; }
  return s;
}

void main() {
  vec3 d = normalize(vDir);
  float h = clamp(d.y, 0.0, 1.0);
  vec3 col = mix(uHorizon, uTop, pow(h, 0.5));
  if (d.y < 0.0) col = mix(uHorizon, uHorizon * 0.55, clamp(-d.y * 3.0, 0.0, 1.0));

  float sd = max(dot(d, uSunDir), 0.0);
  col += uSunColor * (pow(sd, 900.0) * 8.0 + pow(sd, 14.0) * 0.32 + pow(sd, 3.0) * 0.14) * (1.0 - uDark * 0.7);

  float md = max(dot(d, uMoonDir), 0.0);
  col += vec3(0.85, 0.9, 1.0) * smoothstep(0.9993, 0.9997, md) * uStars * 1.4;
  col += vec3(0.35, 0.42, 0.6) * pow(md, 60.0) * 0.25 * uStars;

  if (d.y > 0.0) {
    vec2 sp = d.xz / (d.y + 0.25) * 55.0;
    float s = hash(floor(sp));
    float tw = step(0.987, s) * (0.55 + 0.45 * sin(uTime * 2.0 + s * 60.0));
    col += vec3(tw) * uStars * smoothstep(0.02, 0.35, d.y) * (1.0 - uCloud * 0.8);
  }

  if (d.y > 0.01) {
    vec2 cp = d.xz / (d.y + 0.18) * 1.15 + vec2(uTime * 0.012, uTime * 0.006);
    float c = fbm(cp);
    float cov = smoothstep(1.02 - uCloud, 1.25 - uCloud * 0.85, c + uCloud * 0.12);
    cov *= smoothstep(0.01, 0.22, d.y);
    vec3 cc = mix(uCloudColor, uCloudColor * 0.28, uDark);
    cc += uSunColor * pow(sd, 6.0) * 0.35 * (1.0 - uDark);
    col = mix(col, cc, clamp(cov, 0.0, 1.0) * 0.92);
  }

  col += uFlash * vec3(0.75, 0.8, 1.0) * smoothstep(-0.1, 0.5, d.y) * 0.7;
  gl_FragColor = vec4(col, 1.0);
  #include <tonemapping_fragment>
  #include <colorspace_fragment>
}`;

export class Sky {
  readonly mesh: THREE.Mesh;
  readonly uniforms = {
    uTop: { value: new THREE.Color(0x3a86e8) },
    uHorizon: { value: new THREE.Color(0xbfe0f5) },
    uSunDir: { value: new THREE.Vector3(0, 1, 0) },
    uMoonDir: { value: new THREE.Vector3(0, -1, 0) },
    uSunColor: { value: new THREE.Color(0xfff2d0) },
    uCloudColor: { value: new THREE.Color(0xffffff) },
    uTime: { value: 0 },
    uStars: { value: 0 },
    uCloud: { value: 0.3 },
    uDark: { value: 0 },
    uFlash: { value: 0 },
  };

  constructor() {
    const mat = new THREE.ShaderMaterial({
      uniforms: this.uniforms,
      vertexShader: vert,
      fragmentShader: frag,
      side: THREE.BackSide,
      depthWrite: false,
      depthTest: false,
      fog: false,
    });
    this.mesh = new THREE.Mesh(new THREE.SphereGeometry(500, 32, 16), mat);
    this.mesh.frustumCulled = false;
    this.mesh.renderOrder = -1000;
  }
}
