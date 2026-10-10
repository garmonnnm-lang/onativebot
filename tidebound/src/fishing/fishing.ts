import * as THREE from 'three';
import { BAIT_BITE, CATCH_TABLE, biteTimeFactor, type CatchEntry } from '../../shared/fishing';
import { getItem } from '../../shared/items';
import { rollLoot } from '../../shared/loot';
import { heightAt, waveHeight, makeRng } from '../../shared/terrain';
import type { Game } from '../core/game';

export type FishState = 'idle' | 'cast' | 'wait' | 'bite' | 'hook' | 'reel' | 'catch' | 'fail' | 'cancel';

export const BAITS = ['none', 'bait_worm', 'bait_crumb', 'bait_lure'] as const;

/**
 * Рыбалка: idle → cast → wait → bite → hook → reel → catch | fail | cancel.
 * Улов бросается по таблице shared/fishing.ts (глубина, время суток, погода, наживка, удочка).
 * В кооперативе бросок будет выполняться на сервере; здесь он локальный.
 */
export class Fishing {
  state: FishState = 'idle';
  bait: string = 'bait_worm';
  private timer = 0;
  private biteWindow = 0;
  private target = new THREE.Vector3();
  private floatPos = new THREE.Vector3();
  private flight = 0;
  private dip = 0;
  private entry: CatchEntry | null = null;
  private kg = 0;
  progress = 0;
  tension = 0;
  private pulling = 0;
  private nextPull = 0;
  private resultTimer = 0;
  private reelSoundT = 0;
  private rng = makeRng((Math.random() * 1e9) | 0);
  readonly bobber: THREE.Group;
  private line: THREE.Line;
  private linePos: Float32Array;
  private spotName = '';
  private depthK = 0;

  constructor(private game: Game) {
    this.bobber = new THREE.Group();
    const top = new THREE.Mesh(new THREE.SphereGeometry(0.11, 12, 8, 0, Math.PI * 2, 0, Math.PI / 2), new THREE.MeshStandardMaterial({ color: 0xe84c3d, roughness: 0.5 }));
    const bot = new THREE.Mesh(new THREE.SphereGeometry(0.11, 12, 8, 0, Math.PI * 2, Math.PI / 2, Math.PI / 2), new THREE.MeshStandardMaterial({ color: 0xf5f5f0, roughness: 0.5 }));
    const stick = new THREE.Mesh(new THREE.CylinderGeometry(0.012, 0.012, 0.16, 6), new THREE.MeshStandardMaterial({ color: 0xffd23f }));
    stick.position.y = 0.17;
    this.bobber.add(top, bot, stick);
    this.bobber.visible = false;
    const N = 16;
    this.linePos = new Float32Array(N * 3);
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(this.linePos, 3));
    this.line = new THREE.Line(geo, new THREE.LineBasicMaterial({ color: 0xf2f2f2, transparent: true, opacity: 0.85 }));
    this.line.frustumCulled = false;
    this.line.visible = false;
    game.renderer.scene.add(this.bobber, this.line);
  }

  get active(): boolean { return this.state !== 'idle'; }

  /** Список наживок, доступных игроку (включая «без наживки»). */
  availableBaits(): string[] {
    return BAITS.filter((b) => b === 'none' || this.game.inventory.has(b, 1));
  }

  cycleBait(): void {
    const list = this.availableBaits();
    const i = list.indexOf(this.bait);
    this.bait = list[(i + 1) % list.length];
    const name = this.bait === 'none' ? 'без наживки' : getItem(this.bait).name;
    this.game.ui.toast(`Наживка: ${name}`, 'info');
    this.game.audio.ui();
  }

  ensureBait(): void {
    if (this.bait !== 'none' && !this.game.inventory.has(this.bait, 1)) {
      const list = this.availableBaits();
      this.bait = list.find((b) => b !== 'none') ?? 'none';
    }
  }

  /** ЛКМ нажата. */
  primary(): void {
    const g = this.game;
    switch (this.state) {
      case 'idle': this.tryCast(); break;
      case 'bite': this.hookFish(); break;
      case 'cast': case 'wait':
        break;
      default: break;
    }
    void g;
  }

  secondary(): void {
    if (this.active && this.state !== 'catch' && this.state !== 'fail') this.cancel('Леска смотана.');
  }

  cancel(msg?: string): void {
    if (this.state === 'idle') return;
    this.state = 'idle';
    this.hide();
    this.game.ui.fishHint(null);
    this.game.ui.reel(null);
    this.game.held.waiting = false;
    this.game.held.reelAmount = 0;
    if (msg) this.game.ui.toast(msg, 'info');
  }

  private hide(): void {
    this.bobber.visible = false;
    this.line.visible = false;
  }

  private tryCast(): void {
    const g = this.game;
    this.ensureBait();
    const cam = g.renderer.camera;
    const dir = new THREE.Vector3();
    cam.getWorldDirection(dir);
    if (dir.y > -0.04) { g.ui.toast('Смотрите на воду, чтобы забросить удочку.', 'info'); return; }
    const t = (cam.position.y - 0.0) / -dir.y;
    const x = cam.position.x + dir.x * t, z = cam.position.z + dir.z * t;
    const dist = Math.hypot(x - cam.position.x, z - cam.position.z);
    const depth = -heightAt(x, z);
    if (dist < 3.5) { g.ui.toast('Слишком близко — забросьте дальше.', 'info'); return; }
    if (dist > 30) { g.ui.toast('Слишком далеко — прицельтесь ближе.', 'info'); return; }
    if (depth < 0.5) { g.ui.toast('Здесь слишком мелко для рыбалки.', 'info'); return; }

    // наживка расходуется при забросе
    if (this.bait !== 'none') g.inventory.remove(this.bait, 1);
    this.target.set(x, 0, z);
    this.depthK = Math.min(1, depth / 9);
    this.spotName = g.world.zoneAt(g.player.pos.x, g.player.pos.z);
    this.state = 'cast';
    this.flight = 0;
    this.bobber.visible = true;
    this.line.visible = true;
    this.floatPos.copy(g.held.tipWorld(new THREE.Vector3()));
    this.dip = 0;
    g.held.cast();
    g.audio.cast();
    g.ui.fishHint('Забросили… ждите поклёвку');
  }

  private rollEntry(): CatchEntry {
    const g = this.game;
    const clock = g.daynight.get().clock;
    const night = clock >= 21 || clock < 5;
    const stormy = g.weather.kind === 'storm' || g.weather.kind === 'rain';
    const tier = g.equippedRodTier();
    let total = 0;
    const ws = CATCH_TABLE.map((e) => {
      let w = e.weight;
      w *= e.bait?.[this.bait] ?? 1;
      if (night && e.night) w *= e.night;
      if (stormy && e.storm) w *= e.storm;
      w *= Math.exp(-Math.abs(e.depth - this.depthK) * 2.2);
      if (e.kind === 'fish' && e.weight < 5) w *= 1 + (tier - 1) * 0.35; // редкая рыба лучше с хорошей удочкой
      total += w;
      return w;
    });
    let r = this.rng() * total;
    for (let i = 0; i < ws.length; i++) {
      r -= ws[i];
      if (r <= 0) return CATCH_TABLE[i];
    }
    return CATCH_TABLE[0];
  }

  private scheduleBite(): void {
    const g = this.game;
    const t = g.daynight.get();
    let f = (BAIT_BITE[this.bait] ?? 1) * biteTimeFactor(t.clock);
    if (g.weather.kind === 'rain') f *= 1.2;
    if (g.weather.kind === 'storm') f *= 0.75;
    f *= 1 + (g.equippedRodTier() - 1) * 0.12;
    this.timer = (9 + this.rng() * 12) / f;
  }

  private hookFish(): void {
    if (!this.entry) return;
    this.state = 'reel';
    this.progress = 22;
    this.tension = 15;
    this.nextPull = 1 + this.rng() * 1.2;
    this.pulling = 0;
    this.game.ui.fishHint(null);
    this.game.held.waiting = false;
    this.game.audio.splash();
  }

  update(dt: number, elapsed: number): void {
    const g = this.game;
    if (this.state === 'idle') return;
    const amp = g.weather.cur.waves;

    // обрыв: игрок ушёл слишком далеко или убрал удочку
    const far = Math.hypot(g.player.pos.x - this.target.x, g.player.pos.z - this.target.z);
    if (this.state !== 'catch' && this.state !== 'fail' && (far > 38 || g.held.current !== 'rod')) {
      this.cancel('Леска смотана.');
      return;
    }

    // положение поплавка
    const surf = waveHeight(this.target.x, this.target.z, elapsed, amp);
    const tip = g.held.tipWorld(new THREE.Vector3());
    if (this.state === 'cast') {
      this.flight += dt / 0.8;
      const k = Math.min(1, this.flight);
      this.floatPos.lerpVectors(tip, new THREE.Vector3(this.target.x, surf, this.target.z), k);
      this.floatPos.y += Math.sin(k * Math.PI) * 4.5;
      if (k >= 1) {
        this.state = 'wait';
        g.audio.splash();
        this.scheduleBite();
        g.held.waiting = true;
        g.ui.fishHint('Ждите поклёвку…');
      }
    } else {
      let y = surf + 0.03;
      let ox = 0, oz = 0;
      if (this.state === 'wait') {
        // редкие «пробы» рыбы
        if (this.rng() < dt * 0.35) this.dip = 0.12;
        this.dip = Math.max(0, this.dip - dt * 0.4);
        y -= this.dip;
      }
      if (this.state === 'bite') {
        y -= 0.22 + Math.sin(elapsed * 30) * 0.05;
      }
      if (this.state === 'reel') {
        // рыба тянет поплавок к игроку по мере вываживания
        const k = this.progress / 100;
        const sx = g.player.pos.x, sz = g.player.pos.z;
        ox = (sx - this.target.x) * k * 0.85 + Math.sin(elapsed * 9) * 0.15 * (this.pulling > 0 ? 1 : 0.3);
        oz = (sz - this.target.z) * k * 0.85;
        y = waveHeight(this.target.x + ox, this.target.z + oz, elapsed, amp) + 0.02;
      }
      this.floatPos.set(this.target.x + ox, y, this.target.z + oz);
    }
    this.bobber.position.copy(this.floatPos);
    this.bobber.rotation.z = Math.sin(elapsed * 2) * 0.1;
    this.updateLine(tip);

    switch (this.state) {
      case 'wait':
        this.timer -= dt;
        if (this.timer <= 0) {
          this.entry = this.rollEntry();
          this.kg = this.entry.kg[0] + (this.entry.kg[1] - this.entry.kg[0]) * this.rng() ** 1.5;
          this.state = 'bite';
          this.biteWindow = g.weather.kind === 'storm' ? 0.85 : 1.25;
          g.audio.bite();
          g.ui.fishHint('Поклёвка! Подсекайте — ЛКМ', true);
        }
        break;
      case 'bite':
        this.biteWindow -= dt;
        if (this.biteWindow <= 0) this.fail('Рыба сорвалась — вы не успели подсечь.');
        break;
      case 'reel': this.updateReel(dt); break;
      case 'catch': case 'fail':
        this.resultTimer -= dt;
        if (this.resultTimer <= 0) { this.state = 'idle'; this.hide(); }
        break;
      default: break;
    }
    g.held.reelAmount = this.state === 'reel' ? 0.4 + this.tension / 160 : 0;
  }

  private updateReel(dt: number): void {
    const g = this.game;
    const e = this.entry as CatchEntry;
    const holding = g.input.primaryHeld;
    const tier = g.equippedRodTier();
    const fight = e.fight * (tier >= 3 ? 0.85 : 1) * (g.weather.kind === 'storm' ? 1.2 : 1);

    this.nextPull -= dt;
    if (this.nextPull <= 0 && this.pulling <= 0) {
      this.pulling = 0.8 + this.rng() * 0.9;
      this.nextPull = 1.6 + this.rng() * 2.4 / fight;
    }
    if (this.pulling > 0) this.pulling -= dt;
    const pull = this.pulling > 0;

    const towards = tier >= 3 ? 0.78 : 1;
    if (holding) {
      this.progress += (pull ? 3 : 15) * dt / fight;
      this.tension += (pull ? 78 : 30) * dt * towards * fight * 0.8;
      this.reelSoundT -= dt;
      if (this.reelSoundT <= 0) { this.reelSoundT = 0.11; g.audio.reel(); }
    } else {
      this.progress -= (pull ? 8 : 3.5) * dt * fight;
      this.tension -= 42 * dt;
    }
    this.tension = Math.max(0, Math.min(110, this.tension));
    g.ui.reel({ progress: this.progress, tension: this.tension, pull });

    if (this.tension >= 100) { g.audio.snap(); this.fail('Леска порвалась! Не тяните, когда рыба рвётся.'); return; }
    if (this.progress <= 0) { this.fail('Рыба ушла.'); return; }
    if (this.progress >= 100) this.land();
  }

  private fail(msg: string): void {
    this.state = 'fail';
    this.resultTimer = 0.6;
    this.game.ui.reel(null);
    this.game.ui.fishHint(null);
    this.game.held.waiting = false;
    this.game.ui.toast(msg, 'warn');
    this.game.audio.warn();
    this.entry = null;
  }

  private land(): void {
    const g = this.game;
    const e = this.entry as CatchEntry;
    g.ui.reel(null);
    g.ui.fishHint(null);
    g.held.waiting = false;
    let given: { id: string; count: number }[] = [];
    if (e.kind === 'container') given = rollLoot(e.lootTable ?? 'stash_tools', (Math.random() * 1e9) | 0);
    else given = [{ id: e.id, count: 1 }];
    const ok = g.inventory.hasRoomFor(given);
    if (ok) for (const x of given) g.inventory.add(x.id, x.count);
    this.state = 'catch';
    this.resultTimer = 1.0;
    const def = getItem(e.id);
    g.audio.catchFish();
    g.ui.catchCard({
      icon: def.icon,
      title: e.kind === 'container' ? 'Плавучий ящик' : def.name,
      sub: e.kind === 'container' ? given.map((x) => `${getItem(x.id).name} ×${x.count}`).join(', ') : `${this.kg.toFixed(1)} кг · ${this.spotName}`,
      rarity: def.rarity,
      lost: !ok,
    });
    if (ok) {
      g.addXp(e.xp, 'рыбалка');
      g.stats.caught++;
    } else g.ui.toast('Инвентарь полон — улов выпущен обратно.', 'warn');
    this.entry = null;
  }

  private updateLine(tip: THREE.Vector3): void {
    const N = this.linePos.length / 3;
    const a = tip, b = this.floatPos;
    const slack = this.state === 'reel' ? 0.15 + (1 - this.tension / 100) * 0.4 : 0.9;
    for (let i = 0; i < N; i++) {
      const t = i / (N - 1);
      this.linePos[i * 3] = a.x + (b.x - a.x) * t;
      this.linePos[i * 3 + 1] = a.y + (b.y - a.y) * t - Math.sin(t * Math.PI) * slack * (this.state === 'cast' ? 0.3 : 1);
      this.linePos[i * 3 + 2] = a.z + (b.z - a.z) * t;
    }
    (this.line.geometry.getAttribute('position') as THREE.BufferAttribute).needsUpdate = true;
  }
}
