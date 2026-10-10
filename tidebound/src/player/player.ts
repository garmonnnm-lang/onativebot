import * as THREE from 'three';
import { CONFIG } from '../core/config';
import { WORLD, heightAt, slopeAt, waveHeight } from '../../shared/terrain';
import type { World } from '../world/world';
import type { Input } from '../input/input';
import type { AudioSystem } from '../audio/audio';
import { settings } from '../core/settings';

export type Surface = 'sand' | 'grass' | 'wood' | 'rock';

/** Игрок от первого лица: движение, прыжок, приседание, коллизии, плавание, покачивание камеры. */
export class Player {
  readonly pos = new THREE.Vector3();
  readonly vel = new THREE.Vector3();
  onGround = false;
  onPlatform = false;
  swimming = false;
  crouching = false;
  sprinting = false;
  moving = false;
  eye = CONFIG.player.eyeHeight;
  bobPhase = 0;
  bobAmount = 0;
  private stepDist = 0;
  private fallStart = 0;
  /** Горизонтальная скорость (для анимации рук). */
  speed = 0;
  /** Только что приземлился (для эффектов), секунды. */
  landed = 0;
  fallDamage = 0;

  constructor(private world: World, private input: Input, private audio: AudioSystem) {}

  teleport(x: number, z: number, yaw?: number): void {
    const g = this.world.groundAt(x, z, 99);
    this.pos.set(x, Math.max(g.y, 0.2) , z);
    this.vel.set(0, 0, 0);
    if (yaw !== undefined) { this.input.yaw = yaw; this.input.pitch = 0; }
    this.onGround = true;
  }

  get eyePos(): THREE.Vector3 {
    return new THREE.Vector3(this.pos.x, this.pos.y + this.eye, this.pos.z);
  }

  surface(): Surface {
    if (this.onPlatform) return 'wood';
    const h = heightAt(this.pos.x, this.pos.z);
    if (slopeAt(this.pos.x, this.pos.z) > 0.7 || h > 17) return 'rock';
    return h < 1.5 ? 'sand' : 'grass';
  }

  update(dt: number, canMove: boolean, stamina: { value: number; spend: (n: number) => void }, elapsed: number, waveAmp: number): void {
    const P = CONFIG.player;
    const inp = this.input;
    this.landed = Math.max(0, this.landed - dt);
    this.fallDamage = 0;

    const ax = canMove ? inp.axis() : { x: 0, z: 0 };
    this.crouching = canMove && inp.held.has('crouch') && !this.swimming;
    const wantsSprint = canMove && inp.held.has('sprint') && ax.z > 0 && !this.crouching && stamina.value > 1 && !this.swimming;
    this.sprinting = wantsSprint;
    this.moving = Math.hypot(ax.x, ax.z) > 0.05;

    let speed = this.crouching ? P.crouch : this.sprinting ? P.sprint : P.walk;
    if (this.swimming) speed = 2.4;

    // направление в мире (yaw: 0 → смотрим на -Z)
    const sin = Math.sin(inp.yaw), cos = Math.cos(inp.yaw);
    const wx = (ax.x * cos - ax.z * sin) * speed;
    const wz = (-ax.x * sin - ax.z * cos) * speed;
    // плавное ускорение
    const k = 1 - Math.exp(-dt * (this.onGround ? 14 : 4));
    this.vel.x += (wx - this.vel.x) * k;
    this.vel.z += (wz - this.vel.z) * k;

    if (this.sprinting && this.moving) stamina.spend(CONFIG.survival.staminaDrain * dt);

    // горизонтальное перемещение: по осям отдельно, чтобы скользить вдоль препятствий и берега
    const r = P.radius;
    const trial = { x: this.pos.x, z: this.pos.z };
    for (const axis of ['x', 'z'] as const) {
      const prev = trial[axis];
      trial[axis] += this.vel[axis] * dt;
      const g = this.world.groundAt(trial.x, trial.z, this.pos.y);
      const deep = g.y < -P.swimDepth && !g.platform;
      if (deep && !this.swimming) {
        // не заходим глубже границы бродa, если не плывём
        trial[axis] = prev;
        this.vel[axis] = 0;
      }
    }
    this.world.colliders.resolve(trial, r, this.pos.y);
    // граница мира
    const lim = WORLD.size / 2 - 16;
    const rr = Math.hypot(trial.x, trial.z);
    if (rr > lim) { trial.x *= lim / rr; trial.z *= lim / rr; }
    this.pos.x = trial.x;
    this.pos.z = trial.z;

    // вертикаль
    const g = this.world.groundAt(this.pos.x, this.pos.z, this.pos.y);
    this.onPlatform = g.platform;
    const deepHere = g.y < -P.swimDepth && !g.platform;
    if (deepHere && this.pos.y <= waveHeight(this.pos.x, this.pos.z, elapsed, waveAmp) + 0.1) {
      this.swimming = true;
    } else if (!deepHere && g.y > -P.swimDepth + 0.2) this.swimming = false;

    if (this.swimming) {
      const surf = waveHeight(this.pos.x, this.pos.z, elapsed, waveAmp) - 1.0;
      this.pos.y += (surf - this.pos.y) * Math.min(1, dt * 6);
      this.vel.y = 0;
      this.onGround = false;
      if (canMove && inp.held.has('jump')) { /* всплытие уже на поверхности */ }
      // выбраться на берег, если мелко
      if (g.y > -P.swimDepth + 0.1) this.swimming = false;
    } else {
      if (this.onGround && canMove && inp.held.has('jump') && !this.crouching) {
        this.vel.y = P.jump;
        this.onGround = false;
      }
      if (!this.onGround) {
        if (this.vel.y > 0 === false && this.fallStart === 0) this.fallStart = this.pos.y;
        this.vel.y -= P.gravity * dt;
      }
      this.pos.y += this.vel.y * dt;
      const wasAir = !this.onGround;
      if (this.pos.y <= g.y) {
        if (wasAir && this.vel.y < -6) {
          this.landed = 0.25;
          this.audio.land();
          const drop = this.fallStart ? this.fallStart - g.y : 0;
          if (drop > 4.5) this.fallDamage = (drop - 4.5) * 9;
        }
        this.pos.y = g.y;
        this.vel.y = 0;
        this.onGround = true;
        this.fallStart = 0;
      } else if (this.pos.y > g.y + 0.06) {
        if (this.onGround && this.vel.y <= 0) this.fallStart = this.pos.y;
        this.onGround = false;
      }
      if (this.onGround && this.vel.y === 0) this.pos.y = g.y;
    }

    // шаги и покачивание
    const hs = Math.hypot(this.vel.x, this.vel.z);
    this.speed = hs;
    const walking = this.onGround && hs > 0.6;
    if (walking) {
      this.stepDist += hs * dt;
      const stride = this.sprinting ? 2.2 : 1.7;
      if (this.stepDist > stride) {
        this.stepDist = 0;
        this.audio.step(this.surface(), this.sprinting);
      }
      this.bobPhase += hs * dt * 1.9;
    }
    const targetBob = walking && settings.value.cameraBob ? Math.min(1, hs / P.sprint) : 0;
    this.bobAmount += (targetBob - this.bobAmount) * Math.min(1, dt * 8);

    const eyeT = this.crouching ? P.crouchEye : this.swimming ? 0.55 : P.eyeHeight;
    this.eye += (eyeT - this.eye) * Math.min(1, dt * 10);
  }

  applyCamera(camera: THREE.PerspectiveCamera): void {
    const bobY = Math.sin(this.bobPhase * 2) * 0.045 * this.bobAmount;
    const bobX = Math.cos(this.bobPhase) * 0.03 * this.bobAmount;
    const landDip = this.landed > 0 ? -0.12 * (this.landed / 0.25) : 0;
    camera.position.set(this.pos.x, this.pos.y + this.eye + bobY + landDip, this.pos.z);
    // не даём камере уйти под землю
    const gy = heightAt(this.pos.x, this.pos.z);
    if (camera.position.y < gy + 0.25 && !this.swimming) camera.position.y = gy + 0.25;
    camera.rotation.set(this.input.pitch, this.input.yaw, bobX * 0.4);
    camera.translateX(bobX);
  }
}
