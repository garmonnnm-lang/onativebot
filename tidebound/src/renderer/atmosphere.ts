import * as THREE from 'three';
import type { TimeState } from '../daynight/daynight';
import type { Weather } from '../weather/weather';
import type { Sky } from './sky';
import type { Water } from './water';

const C = (hex: number) => new THREE.Color(hex);

const NIGHT_TOP = C(0x050c22), NIGHT_HOR = C(0x142848);
const DAY_TOP = C(0x3d86ee), DAY_HOR = C(0xbfe3f8);
const DUSK_TOP = C(0x35407f), DUSK_HOR = C(0xf59a5e);
const STORM_TOP = C(0x3d4750), STORM_HOR = C(0x6f7c86);

/** Управляет светом, небом, туманом и параметрами воды по времени суток и погоде. */
export class Atmosphere {
  readonly sun = new THREE.DirectionalLight(0xfff0d8, 3);
  readonly moon = new THREE.DirectionalLight(0x8fa8e0, 0.4);
  readonly hemi = new THREE.HemisphereLight(0xa8d0ff, 0x5a4a38, 1);
  readonly fog = new THREE.FogExp2(0xbfe3f8, 0.002);
  shadowsEnabled = true;

  private tmpA = new THREE.Color();
  private tmpB = new THREE.Color();
  private top = new THREE.Color();
  private hor = new THREE.Color();

  constructor(private scene: THREE.Scene, private sky: Sky, private water: Water) {
    scene.add(this.sun, this.sun.target, this.moon, this.moon.target, this.hemi);
    scene.fog = this.fog;
    this.sun.castShadow = true;
    const cam = this.sun.shadow.camera;
    cam.left = -70; cam.right = 70; cam.top = 70; cam.bottom = -70; cam.near = 1; cam.far = 260;
    this.sun.shadow.bias = -0.0004;
    this.sun.shadow.normalBias = 0.05;
  }

  setShadowMapSize(size: number): void {
    this.shadowsEnabled = size > 0;
    if (size > 0 && this.sun.shadow.mapSize.x !== size) {
      this.sun.shadow.mapSize.set(size, size);
      this.sun.shadow.map?.dispose();
      this.sun.shadow.map = null;
    }
  }

  update(time: TimeState, weather: Weather, focus: THREE.Vector3, elapsed: number): void {
    const w = weather.cur;
    const day = time.daylight;
    const tw = time.twilight * (1 - w.dark * 0.6);

    this.top.copy(NIGHT_TOP).lerp(DAY_TOP, day);
    this.hor.copy(NIGHT_HOR).lerp(DAY_HOR, day);
    this.top.lerp(DUSK_TOP, tw * 0.75);
    this.hor.lerp(DUSK_HOR, tw * 0.9);
    const storm = Math.min(1, w.dark * 1.15);
    this.tmpA.copy(STORM_TOP).multiplyScalar(0.25 + 0.75 * day);
    this.tmpB.copy(STORM_HOR).multiplyScalar(0.18 + 0.82 * day);
    this.top.lerp(this.tmpA, storm);
    this.hor.lerp(this.tmpB, storm);

    const u = this.sky.uniforms;
    u.uTop.value.copy(this.top);
    u.uHorizon.value.copy(this.hor);
    u.uSunDir.value.set(time.sunDir[0], time.sunDir[1], time.sunDir[2]);
    u.uMoonDir.value.set(-time.sunDir[0], -time.sunDir[1], -time.sunDir[2] + 0.1).normalize();
    u.uSunColor.value.set(0xffe6b8).lerp(this.tmpA.set(0xff8a3c), Math.min(1, tw * 1.2));
    u.uCloudColor.value.copy(this.hor).lerp(this.tmpB.set(0xffffff), 0.5 * day).multiplyScalar(0.55 + 0.45 * day);
    u.uStars.value = Math.max(0, 1 - day * 2.2) * (1 - w.cloud * 0.75);
    u.uCloud.value = w.cloud;
    u.uDark.value = w.dark;
    u.uTime.value = elapsed;
    u.uFlash.value = weather.flash;

    this.fog.color.copy(this.hor).multiplyScalar(0.92);
    this.fog.density = w.fog * (1 + (1 - day) * 0.8);

    const above = Math.max(0, time.elevation);
    this.sun.color.set(0xff9f58).lerp(this.tmpA.set(0xfff4e4), Math.min(1, above * 2.5));
    this.sun.intensity = 3.1 * day * (1 - w.dark * 0.78);
    this.moon.intensity = 0.55 * (1 - day) * (1 - w.dark * 0.4);
    this.hemi.color.set(0x18274f).lerp(this.tmpA.set(0xaed3ff), day).lerp(this.tmpB.set(0xffb27a), tw * 0.3);
    this.hemi.groundColor.set(0x1a1612).lerp(this.tmpA.set(0x6b5a44), day);
    this.hemi.intensity = (0.28 + 0.9 * day) * (1 - w.dark * 0.45) + weather.flash * 1.6;

    const d = 120;
    this.sun.position.set(focus.x + time.sunDir[0] * d, Math.max(8, focus.y + time.sunDir[1] * d), focus.z + time.sunDir[2] * d);
    this.sun.target.position.copy(focus);
    this.moon.position.set(focus.x - time.sunDir[0] * d, focus.y + Math.abs(time.sunDir[1]) * d + 30, focus.z - time.sunDir[2] * d);
    this.moon.target.position.copy(focus);
    this.sun.castShadow = this.shadowsEnabled && time.elevation > 0.04 && day > 0.05;

    const wu = this.water.uniforms;
    wu.uSunDir.value.set(time.sunDir[0], time.sunDir[1], time.sunDir[2]);
    wu.uSunColor.value.copy(this.sun.color);
    wu.uSkyColor.value.copy(this.hor);
    wu.uLight.value = Math.max(0.08, day * (1 - w.dark * 0.5));
    wu.uStorm.value = Math.min(1, Math.max(0, (w.waves - 0.6) / 1.0));
    wu.uAmp.value = w.waves;
    wu.uShallow.value.set(0x35c8c0).lerp(this.tmpA.set(0x41807f), w.dark * 0.7);
    wu.uDeep.value.set(0x0a4f86).lerp(this.tmpA.set(0x16303e), w.dark * 0.8);
    const amb = 0.12 + 0.88 * day;
    wu.uAmbient.value.setRGB(amb * (1 - 0.1 * tw), amb * (1 - 0.25 * tw), amb * (1 - 0.1 * tw) + (1 - day) * 0.05);
    this.water.time = elapsed;
  }
}
