import * as THREE from 'three';
import { Inventory, HOTBAR_SIZE } from '../../shared/inventory';
import { getRecipe, type Station } from '../../shared/recipes';
import { getItem } from '../../shared/items';
import { LANDMARKS, heightAt } from '../../shared/terrain';
import { AssetManager } from '../renderer/assets';
import { Atmosphere } from '../renderer/atmosphere';
import { GameRenderer } from '../renderer/renderer';
import { Sky } from '../renderer/sky';
import { Water } from '../renderer/water';
import { World } from '../world/world';
import { Player } from '../player/player';
import { Input } from '../input/input';
import { AudioSystem } from '../audio/audio';
import { DayNight } from '../daynight/daynight';
import { Weather, type WeatherKind } from '../weather/weather';
import { Rain } from '../weather/rain';
import { Vitals } from '../survival/vitals';
import { HeldItem } from '../animation/held';
import { Fishing } from '../fishing/fishing';
import { UI } from '../ui/ui';
import { SaveManager, type SaveData } from '../persistence/saves';
import { DroppedItems } from '../items/dropped';
import { Multiplayer } from './multiplayer';
import { TouchControls } from '../input/touch';
import type { WorldSnapshot } from '../../shared/net';
import { CONFIG } from './config';
import { QUALITY_PRESETS, settings } from './settings';
import type { Interactable } from '../world/types';

export type GameState = 'loading' | 'menu' | 'playing' | 'paused' | 'inventory' | 'dead';

const START_KIT: [string, number][] = [
  ['rod_basic', 1], ['axe', 1], ['hammer', 1], ['bread', 3], ['water', 2], ['bait_worm', 12], ['bait_crumb', 6],
];

/** Главный класс: связывает мир, игрока, интерфейс, рыбалку, выживание и сохранения. */
export class Game {
  state: GameState = 'loading';
  readonly canvas = document.getElementById('game-canvas') as HTMLCanvasElement;
  readonly renderer = new GameRenderer(this.canvas);
  readonly assets = new AssetManager();
  readonly input = new Input(this.canvas);
  readonly audio = new AudioSystem();
  readonly daynight = new DayNight();
  readonly weather = new Weather();
  readonly vitals = new Vitals();
  readonly inventory = new Inventory();
  readonly saves = new SaveManager();
  readonly stats = { caught: 0 };
  readonly mp = new Multiplayer(this);
  ui!: UI;
  world!: World;
  player!: Player;
  held!: HeldItem;
  fishing!: Fishing;
  dropped!: DroppedItems;
  hotbarIndex = 0;
  private sky = new Sky();
  private water!: Water;
  private atmosphere!: Atmosphere;
  private rain!: Rain;
  private elapsed = 0;
  private fps = 60;
  private target: Interactable | null = null;
  private swingCooldown = 0;
  private pendingHit = -1;
  private pendingAction: (() => void) | null = null;
  private autosaveT = 0;
  private lastLockRequest = 0;
  private busy = false;
  private debugOn = false;
  private lastTime = 0;
  private menuT = 0;
  private reportedError = false;

  async start(): Promise<void> {
    this.ui = new UI(this);
    this.ui.show('loading');
    this.assets.onProgress = () => { /* прогресс показывает построение мира */ };

    window.addEventListener('error', (e) => this.fatal(e.error ?? e.message));
    window.addEventListener('unhandledrejection', (e) => this.fatal(e.reason));
    this.renderer.onContextLost = () => { this.ui.toast('Графический контекст потерян. Пытаемся восстановить…', 'warn'); };
    this.renderer.onContextRestored = () => { this.ui.toast('Графический контекст восстановлен.', 'good'); };

    try {
      this.ui.setLoading('Подготовка…', 0.02);
      const q = settings.value.quality;
      this.water = new Water(QUALITY_PRESETS[q].waterSeg);
      this.renderer.scene.add(this.sky.mesh, this.water.mesh);
      this.atmosphere = new Atmosphere(this.renderer.scene, this.sky, this.water);
      this.atmosphere.setShadowMapSize(settings.value.shadows ? QUALITY_PRESETS[q].shadowMap : 0);
      this.rain = new Rain(QUALITY_PRESETS[q].rain);
      this.renderer.scene.add(this.rain.mesh);

      this.world = await World.create(this.assets, q, (t, k) => this.ui.setLoading(t, k));
      this.renderer.scene.add(this.world.group);
      this.renderer.scene.add(this.mp.remote.group);

      this.player = new Player(this.world, this.input, this.audio);
      this.held = new HeldItem(this.renderer.camera);
      this.fishing = new Fishing(this);
      this.dropped = new DroppedItems(this);
      this.ui.bindInventory();
      this.wireInput();
      if (this.input.touch) new TouchControls(this.input, { heldKind: () => this.held.current });
      this.weather.onThunder = (d, p) => this.audio.thunder(d, p);
      this.weather.onChange = (k) => this.onWeatherChange(k);
      settings.subscribe(() => this.applySettings());
      this.applySettings();
      this.resetNewGame();
      if (this.assets.failed.length) console.warn('[assets] не загружены:', this.assets.failed);
      this.ui.setLoading('Готово', 1);
      this.toMenu();
    } catch (e) {
      console.error(e);
      this.ui.loadingError(`Не удалось запустить игру: ${(e as Error).message}`);
      return;
    }

    const params = new URLSearchParams(location.search);
    const room = params.get('room');
    if (room) this.ui.openJoinFromUrl(room.toUpperCase());

    requestAnimationFrame((t) => this.frame(t));
  }

  private fatal(err: unknown): void {
    console.error(err);
    if (this.reportedError || !this.ui) return;
    this.reportedError = true;
    this.ui.toast(`Ошибка: ${(err as Error)?.message ?? String(err)}. Подробности в консоли.`, 'warn');
    setTimeout(() => { this.reportedError = false; }, 5000);
  }

  // ───────────────────────────── Состояния ─────────────────────────────

  private toMenu(): void {
    this.state = 'menu';
    this.input.enabled = false;
    this.input.exitLock();
    this.ui.hud(false);
    this.ui.clickToPlay(false);
    this.ui.show('main');
    this.held.setKind('none');
    void this.ui.refreshContinue();
  }

  private startPlaying(): void {
    this.state = 'playing';
    this.ui.show(null);
    this.ui.hud(true);
    this.ui.applyHudSettings();
    this.input.enabled = true;
    this.requestLock();
    this.selectSlot(this.hotbarIndex);
    this.ui.renderHotbar();
    this.autosaveT = 0;
  }

  private requestLock(): void {
    this.lastLockRequest = performance.now();
    this.input.requestLock();
  }

  async newSingleGame(): Promise<void> {
    this.audio.unlock();
    if (await this.hasSave() && !window.confirm('Начать новую игру? Существующее сохранение будет перезаписано при следующем сохранении.')) return;
    this.resetNewGame();
    this.startPlaying();
    this.ui.showStartHints();
  }

  async continueGame(): Promise<void> {
    this.audio.unlock();
    const data = await this.saves.load();
    if (!data) {
      this.ui.toast('Сохранение не найдено или повреждено.', 'warn');
      void this.ui.refreshContinue();
      return;
    }
    this.applySave(data);
    this.startPlaying();
    this.ui.toast('Сохранение загружено.', 'good');
  }

  hasSave(): Promise<boolean> {
    return this.saves.load().then((d) => d !== null).catch(() => false);
  }

  private resetNewGame(): void {
    this.vitals.reset();
    this.vitals.level = 1; this.vitals.xp = 0;
    this.inventory.slots.fill(null);
    for (const [id, n] of START_KIT) this.inventory.add(id, n);
    // быстрые слоты по умолчанию: 1 удочка, 2 топор, 3 молот, 4 еда, 5 вода
    const order = ['rod_basic', 'axe', 'hammer', 'bread', 'water'];
    order.forEach((id, i) => {
      const at = this.inventory.slots.findIndex((s) => s?.id === id);
      if (at >= 0 && at !== i) this.inventory.move(at, i);
    });
    this.daynight.setTime(this.daynight.cycleSeconds * 0.0 + CONFIG.day.dayMinutes * 60 * 0.3, 1);
    this.weather.kind = 'clear';
    this.weather.cur.cloud = 0.25; this.weather.cur.rain = 0; this.weather.cur.wind = 0.2; this.weather.cur.waves = 0.35; this.weather.cur.dark = 0;
    this.world.resetState();
    this.dropped.clear();
    this.fishing.cancel();
    this.hotbarIndex = 0;
    this.stats.caught = 0;
    this.respawnPlayer();
  }

  private respawnPlayer(): void {
    const s = LANDMARKS.spawn;
    this.player.teleport(s.x, s.z, s.yaw + Math.PI);
    this.input.pitch = -0.08;
  }

  /** Начало кооперативной игры: свежий персонаж + авторитетный снимок мира комнаты. */
  startCoop(snap: WorldSnapshot): void {
    this.audio.unlock();
    this.resetNewGame();
    this.world.applySnapshot(snap);
    this.daynight.setTime(snap.time.seconds, snap.time.day);
    this.weather.autoChange = false;
    this.setWeatherNet(snap.weather);
    this.mp.active = true;
    this.startPlaying();
    this.ui.toast(`Комната ${this.mp.room?.code ?? ''}: кооператив. Прогресс инвентаря хранится только в этой сессии.`, 'info');
  }

  /** Вызывается, когда кооперативная сессия закончилась (выход, обрыв, закрытие). */
  onCoopEnded(): void {
    this.weather.autoChange = true;
    this.ui.netBanner(null);
  }

  /** Серверная погода: переключаем без случайной смены. */
  setWeatherNet(kind: WeatherKind): void {
    this.weather.autoChange = false;
    this.weather.set(kind);
  }

  leaveToMenuNoSave(): void {
    this.fishing.cancel();
    this.world.resetState();
    this.resetNewGame();
    this.toMenu();
  }

  resume(): void {
    if (this.state !== 'paused' && this.state !== 'inventory') return;
    this.state = 'playing';
    this.ui.show(null);
    this.input.enabled = true;
    this.requestLock();
  }

  private pause(): void {
    if (this.state !== 'playing') return;
    this.state = 'paused';
    this.fishing.cancel();
    this.input.exitLock();
    this.ui.clickToPlay(false);
    this.ui.show('pause');
  }

  /** Открыть меню крафта (руками или у верстака). Состояние игры то же, что у инвентаря. */
  openCraft(station: Station = 'hand'): void {
    if (this.state === 'playing') {
      this.state = 'inventory';
      this.input.exitLock();
      this.ui.clickToPlay(false);
    } else if (this.state !== 'inventory') return;
    this.craftStation = station;
    this.ui.openCraft(station);
    this.audio.ui();
  }

  openInventoryScreen(): void {
    if (this.state !== 'inventory') return;
    this.ui.show('inventory');
  }

  private craftStation: Station = 'hand';

  canCraftAt(station: Station): boolean {
    return station === 'hand' || station === this.craftStation;
  }

  /** Выполнить рецепт. Возвращает причину отказа или null при успехе. */
  craft(recipeId: string): string | null {
    const r = getRecipe(recipeId);
    if (!r) return 'Неизвестный рецепт.';
    if (!this.canCraftAt(r.station)) return 'Нужен верстак.';
    if (r.minLevel && this.vitals.level < r.minLevel) return `Нужен уровень ${r.minLevel}.`;
    if (!this.inventory.hasAll(r.inputs)) return 'Не хватает материалов.';
    if (!this.inventory.exchange(r.inputs, [r.output])) return 'Нет места в инвентаре.';
    this.addXp(r.xp, 'крафт');
    this.ui.feed(`Создано: ${getItem(r.output.id).name} ×${r.output.count}`);
    this.audio.ui();
    const cur = this.inventory.slots[this.hotbarIndex];
    this.held.setKind(cur ? getItem(cur.id).held ?? 'none' : 'none');
    return null;
  }

  private toggleInventory(): void {
    if (this.state === 'playing') {
      this.state = 'inventory';
      this.input.exitLock();
      this.ui.clickToPlay(false);
      this.ui.show('inventory');
      this.audio.ui();
    } else if (this.state === 'inventory') {
      this.resume();
    }
  }

  async leaveToMenu(): Promise<void> {
    if (this.mp.inRoom) {
      this.mp.leave();
      this.leaveToMenuNoSave();
      return;
    }
    await this.saveNow(false);
    this.fishing.cancel();
    this.toMenu();
  }

  respawn(): void {
    if (this.state !== 'dead') return;
    this.vitals.reset();
    this.inventory.slots.fill(null);
    for (const [id, n] of START_KIT) this.inventory.add(id, n);
    ['rod_basic', 'axe', 'hammer', 'bread', 'water'].forEach((id, i) => {
      const at = this.inventory.slots.findIndex((s) => s?.id === id);
      if (at >= 0 && at !== i) this.inventory.move(at, i);
    });
    this.respawnPlayer();
    this.state = 'playing';
    this.ui.show(null);
    this.input.enabled = true;
    this.requestLock();
    this.ui.toast('Вы возродились на причале. Ваши вещи остались в рюкзаке на месте гибели.', 'info');
  }

  private die(cause: string): void {
    if (this.state === 'dead') return;
    this.fishing.cancel();
    // рюкзак с содержимым
    const stacks = this.inventory.slots.filter((s): s is { id: string; count: number } => !!s).map((s) => ({ id: s.id, count: s.count }));
    if (stacks.length) this.dropped.spawn(this.player.pos.x, this.player.pos.z, stacks, true, 1800);
    this.state = 'dead';
    this.input.enabled = false;
    this.input.exitLock();
    const el = document.getElementById('death-cause');
    if (el) el.textContent = cause;
    this.ui.show('death');
    this.audio.hurt();
  }

  // ───────────────────────────── Ввод ─────────────────────────────

  private wireInput(): void {
    const inp = this.input;
    inp.on('lockChange', (locked) => {
      if (this.state !== 'playing') return;
      if (locked) { this.ui.clickToPlay(false); return; }
      // курсор не удалось захватить сразу после запроса — просим кликнуть; иначе пользователь нажал Esc
      if (performance.now() - this.lastLockRequest < 600) this.ui.clickToPlay(true);
      else this.pause();
    });
    inp.on('action', (a) => {
      this.audio.unlock();
      if (a === 'pause') {
        if (this.ui.handleEscape()) return;
        if (this.state === 'playing') this.pause();
        else if (this.state === 'paused') this.resume();
        else if (this.state === 'inventory') this.resume();
        return;
      }
      if (a === 'inventory') { this.toggleInventory(); return; }
      if (a === 'craft') { if (this.state === 'inventory' && this.ui.current === 'craft') this.resume(); else this.openCraft('hand'); return; }
      if (this.state !== 'playing' || this.busy) return;
      if (a === 'interact') this.interact();
      else if (a === 'reload') { if (this.held.current === 'rod') this.fishing.cycleBait(); }
    });
    inp.on('slot', (i) => { if (this.state === 'playing') this.selectSlot(i); });
    inp.on('wheel', (d) => { if (this.state === 'playing') this.selectSlot((this.hotbarIndex + d + HOTBAR_SIZE) % HOTBAR_SIZE); });
    inp.on('primaryDown', () => { this.audio.unlock(); if (this.state === 'playing' && !this.busy) this.primary(); });
    inp.on('secondaryDown', () => { if (this.state === 'playing' && this.held.current === 'rod') this.fishing.secondary(); });
    inp.on('toggleDebug', () => { this.debugOn = !this.debugOn; if (!this.debugOn) this.ui.debug(null); });
  }

  selectSlot(i: number): void {
    if (i === this.hotbarIndex && this.held.current !== 'none') { this.ui.renderHotbar(); return; }
    if (this.fishing && this.fishing.active) this.fishing.cancel();
    this.hotbarIndex = i;
    const s = this.inventory.slots[i];
    this.held.setKind(s ? getItem(s.id).held ?? 'none' : 'none');
    this.ui.renderHotbar();
    this.audio.ui();
  }

  equippedRodTier(): number {
    const s = this.inventory.slots[this.hotbarIndex];
    return s ? getItem(s.id).tool?.rodTier ?? 1 : 1;
  }

  private primary(): void {
    const kind = this.held.current;
    if (kind === 'rod') this.fishing.primary();
    else if (kind === 'axe' || kind === 'hammer') this.swingTool();
    else if (kind === 'food' || kind === 'drink') {
      if (this.swingCooldown <= 0) { this.useItem(this.hotbarIndex); this.swingCooldown = 0.6; }
    }
  }

  private swingTool(): void {
    if (this.swingCooldown > 0 || this.pendingHit >= 0) return;
    if (this.vitals.stamina < 6) { this.ui.toast('Не хватает сил — отдохните.', 'info'); this.swingCooldown = 0.5; return; }
    this.held.swing();
    this.mp.sendSwing();
    this.pendingHit = 0.24;
    this.swingCooldown = 0.62;
    this.vitals.spendStamina(6);
  }

  private resolveHit(): void {
    const kind = this.held.current;
    const cam = this.renderer.camera;
    const dir = new THREE.Vector3();
    cam.getWorldDirection(dir);
    const origin = cam.position.clone();
    const node = this.world.findResource(origin, dir, CONFIG.player.reach + 0.4);
    if (!node) { this.audio.hit(); return; }
    if (node.kind === 'tree' && kind !== 'axe') { this.ui.toast('Деревья нужно рубить топором.', 'info'); return; }
    if (node.kind === 'rock' && kind !== 'hammer') { this.ui.toast('Камень можно добыть молотом.', 'info'); return; }
    if (node.kind === 'tree') this.audio.chop(); else this.audio.hit();
    if (this.mp.active) { this.mp.sendHit(node); return; }
    if (this.world.damageResource(node, 1)) {
      const n = node.gives.min + Math.floor(Math.random() * (node.gives.max - node.gives.min + 1));
      this.giveOrDrop(node.gives.id, n);
      this.ui.feed(`+${n} ${getItem(node.gives.id).name}`);
      this.addXp(node.kind === 'tree' ? 5 : 6, 'сбор ресурсов');
    }
  }

  /** Выдаёт предмет; если не помещается — выбрасывает рядом. */
  giveOrDrop(id: string, n: number): void {
    if (this.inventory.add(id, n)) { this.audio.pickup(); return; }
    this.dropped.spawn(this.player.pos.x + (Math.random() - 0.5) * 1.5, this.player.pos.z + (Math.random() - 0.5) * 1.5, [{ id, count: n }]);
    this.ui.toast('Инвентарь полон — предмет упал на землю.', 'warn');
  }

  private interact(): void {
    if (!this.target) return;
    this.target.onUse(this);
  }

  itemName(id: string): string { return getItem(id).name; }

  useItem(index: number): void {
    const s = this.inventory.slots[index];
    if (!s) return;
    const def = getItem(s.id);
    if (!def.food && !def.drink) { this.ui.toast('Этот предмет нельзя использовать напрямую.', 'info'); return; }
    this.inventory.remove(s.id, 1);
    const v = this.vitals;
    if (def.food) {
      v.hunger = Math.min(100, v.hunger + def.food.hunger);
      if (def.food.health) { if (def.food.health > 0) v.heal(def.food.health); else { v.damage(-def.food.health); this.ui.damageFlash(); } }
      this.audio.eat();
    }
    if (def.drink) {
      v.thirst = Math.min(100, v.thirst + def.drink.thirst);
      if (def.drink.health) { v.damage(-def.drink.health); this.ui.damageFlash(); }
      this.audio.drink();
    }
    this.ui.feed(`Использовано: ${def.name}`);
    // если стопка закончилась — оставляем слот пустым и обновляем руку
    const cur = this.inventory.slots[this.hotbarIndex];
    this.held.setKind(cur ? getItem(cur.id).held ?? 'none' : 'none');
  }

  dropItem(index: number, all: boolean): void {
    const s = this.inventory.slots[index];
    if (!s) return;
    const n = all ? s.count : 1;
    const id = s.id;
    this.inventory.removeAt(index, n);
    const yaw = this.input.yaw;
    const x = this.player.pos.x - Math.sin(yaw) * 1.4, z = this.player.pos.z - Math.cos(yaw) * 1.4;
    this.dropped.spawn(x, z, [{ id, count: n }]);
    this.ui.feed(`Выброшено: ${getItem(id).name} ×${n}`);
    const cur = this.inventory.slots[this.hotbarIndex];
    this.held.setKind(cur ? getItem(cur.id).held ?? 'none' : 'none');
  }

  addXp(n: number, reason: string): void {
    const gained = this.vitals.addXp(n);
    this.ui.feed(`+${n} XP · ${reason}`);
    if (gained > 0) {
      this.audio.levelUp();
      this.ui.toast(`Новый уровень: ${this.vitals.level}!`, 'good');
    }
  }

  /** Сон в кровати: пропуск ночи (одиночный режим). */
  sleep(): void {
    if (this.busy) return;
    if (this.mp.active) { this.ui.toast('В кооперативе время идёт по серверным часам — сон недоступен.', 'info'); return; }
    const t = this.daynight.get();
    if (t.phase === 'day') { this.ui.toast('Спать можно только вечером или ночью.', 'info'); return; }
    this.busy = true;
    this.ui.fade(true);
    setTimeout(() => {
      this.daynight.skipToMorning();
      this.vitals.stamina = 100;
      this.vitals.heal(25);
      this.vitals.hunger = Math.max(15, this.vitals.hunger - 12);
      this.vitals.thirst = Math.max(15, this.vitals.thirst - 12);
      this.ui.fade(false);
      this.ui.toast('Вы выспались. Наступило утро.', 'good');
      this.busy = false;
    }, 1500);
  }

  /** Приготовление еды на костре: сырую рыбу и мясо — в жареные. */
  cookAll(): void {
    const map: Record<string, string> = {
      fish_raw: 'fish_cooked', fish_common: 'fish_cooked', fish_flat: 'fish_cooked', fish_tang: 'fish_cooked', fish_tuna: 'fish_cooked', meat_raw: 'meat_cooked',
    };
    let total = 0;
    for (const [raw, cooked] of Object.entries(map)) {
      const n = this.inventory.count(raw);
      if (n > 0 && this.inventory.exchange([{ id: raw, count: n }], [{ id: cooked, count: n }])) total += n;
    }
    if (total === 0) { this.ui.toast('Нет сырой рыбы или мяса для готовки.', 'info'); return; }
    this.audio.eat();
    this.ui.toast(`Приготовлено блюд: ${total}`, 'good');
    this.addXp(total * 2, 'готовка');
  }

  private onWeatherChange(k: WeatherKind): void {
    if (this.state === 'playing') {
      if (k === 'storm') { this.ui.toast('Надвигается шторм! Волны и ветер усиливаются.', 'warn'); this.audio.warn(); }
      else if (k === 'rain') this.ui.toast('Начинается дождь.', 'info');
    }
  }

  setWeather(k: WeatherKind): void { this.weather.autoChange = false; this.weather.set(k); }

  applySettings(): void {
    if (!this.world) return;
    this.renderer.applySettings();
    const q = QUALITY_PRESETS[settings.value.quality];
    this.atmosphere.setShadowMapSize(settings.value.shadows ? q.shadowMap : 0);
    this.audio.applyVolumes();
    this.ui.applyHudSettings();
  }

  // ───────────────────────────── Сохранение ─────────────────────────────

  private makeSave(): SaveData {
    return {
      version: CONFIG.save.version,
      savedAt: Date.now(),
      player: { x: this.player.pos.x, y: this.player.pos.y, z: this.player.pos.z, yaw: this.input.yaw, pitch: this.input.pitch },
      vitals: this.vitals.serialize(),
      inventory: this.inventory.serialize(),
      hotbarIndex: this.hotbarIndex,
      time: { seconds: this.daynight.seconds, day: this.daynight.dayNumber },
      weather: this.weather.kind,
      world: { ...this.world.serialize(), dropped: this.dropped.serialize(), caught: this.stats.caught },
    };
  }

  private applySave(d: SaveData): void {
    this.vitals.restore(d.vitals as never);
    this.inventory.restore(d.inventory as never);
    this.daynight.setTime(d.time.seconds, d.time.day);
    const w = d.weather as WeatherKind;
    if (['clear', 'cloudy', 'fog', 'rain', 'storm'].includes(w)) this.weather.set(w);
    this.world.resetState();
    const world = d.world as { dropped?: never; caught?: number } & Record<string, unknown>;
    this.world.restore(world as never);
    this.dropped.restore(world?.dropped);
    this.stats.caught = typeof world?.caught === 'number' ? world.caught : 0;
    this.hotbarIndex = Math.max(0, Math.min(HOTBAR_SIZE - 1, Math.floor(d.hotbarIndex) || 0));
    const p = d.player;
    this.player.teleport(p.x, p.z, p.yaw);
    this.player.pos.y = Math.max(this.player.pos.y, this.world.groundAt(p.x, p.z, 99).y);
    this.input.pitch = Number.isFinite(p.pitch) ? p.pitch : 0;
  }

  async saveNow(manual: boolean): Promise<void> {
    if (this.state === 'loading' || this.state === 'menu') return;
    if (this.state === 'dead' || this.mp.active) return;
    try {
      const where = await this.saves.save(this.makeSave());
      if (manual) this.ui.toast(where === 'indexeddb' ? 'Игра сохранена.' : 'Игра сохранена (резервное хранилище).', 'good');
    } catch (e) {
      console.error(e);
      this.ui.toast('Не удалось сохранить игру: хранилище недоступно.', 'warn');
    }
  }

  // ───────────────────────────── Главный цикл ─────────────────────────────

  private frame(now: number): void {
    requestAnimationFrame((t) => this.frame(t));
    const dt = Math.min(0.05, (now - this.lastTime) / 1000 || 0.016);
    this.lastTime = now;
    this.fps += (1 / Math.max(dt, 1e-4) - this.fps) * 0.05;
    if (this.state === 'playing') this.renderer.autoScale(dt, this.fps);
    try {
      this.step(dt);
    } catch (e) {
      this.fatal(e);
    }
    this.renderer.render();
  }

  private step(dt: number): void {
    const running = this.state === 'playing' || this.state === 'inventory' || this.state === 'menu' || this.state === 'dead';
    this.elapsed += dt;
    const cam = this.renderer.camera;
    const focus = new THREE.Vector3();

    if (running) {
      this.daynight.update(dt);
      this.weather.update(dt);
    }
    const time = this.daynight.get();
    this.mp.update(dt);

    if (this.state === 'menu' || this.state === 'loading') {
      this.menuT += dt;
      const L = LANDMARKS;
      const a = Math.sin(this.menuT * 0.05) * 0.35;
      const dx = L.dockDir.x, dz = L.dockDir.z;
      const base = { x: L.dockStart.x + dx * 26, z: L.dockStart.z + dz * 26 };
      cam.position.set(base.x + Math.sin(a) * 2, 4.6 + Math.sin(this.menuT * 0.3) * 0.2, base.z);
      const look = new THREE.Vector3(L.square.x - 10 + Math.sin(a) * 40, 9, L.square.z - 30);
      cam.lookAt(look);
      focus.copy(cam.position);
      this.input.yaw = cam.rotation.y;
      this.held?.setKind('none');
    } else if (this.state === 'playing' || this.state === 'inventory' || this.state === 'dead') {
      this.stepPlay(dt, time);
      this.player.applyCamera(cam);
      focus.copy(this.player.pos);
    } else {
      focus.copy(this.player.pos);
    }

    const night = 1 - time.daylight;
    const waves = this.weather.cur.waves;
    this.atmosphere.update(time, this.weather, focus, this.elapsed);
    this.world.update(running ? dt : 0, this.elapsed, night, waves, focus);
    this.rain.update(this.elapsed, cam.position, this.weather.cur.rain, this.weather.cur.wind, time.daylight);
    this.sky.mesh.position.copy(cam.position);
    this.water.mesh.position.x = Math.round(cam.position.x / 8) * 8;
    this.water.mesh.position.z = Math.round(cam.position.z / 8) * 8;

    if (this.state === 'playing' || this.state === 'inventory' || this.state === 'paused') {
      this.ui.update(dt, this.fps);
      const shore = Math.max(0, 1 - Math.max(0, heightAt(this.player.pos.x, this.player.pos.z) + 2) / 40);
      this.audio.updateEnvironment(dt, { waves, wind: this.weather.cur.wind, rain: this.weather.cur.rain, shore, daylight: time.daylight });
    } else if (this.state === 'menu') {
      this.audio.updateEnvironment(dt, { waves, wind: this.weather.cur.wind, rain: this.weather.cur.rain, shore: 0.7, daylight: time.daylight });
    }

    if (this.debugOn) {
      const p = this.player.pos;
      this.ui.debug(`FPS ${this.fps.toFixed(0)}  state=${this.state}\npos ${p.x.toFixed(1)} ${p.y.toFixed(2)} ${p.z.toFixed(1)}  ground=${this.player.onGround} swim=${this.player.swimming}\nweather=${this.weather.kind} time=${time.clock.toFixed(1)} day=${time.day}\ndraw calls=${this.renderer.renderer.info.render.calls} tris=${this.renderer.renderer.info.render.triangles}\nfishing=${this.fishing.state}`);
    }
  }

  private stepPlay(dt: number, time: ReturnType<DayNight['get']>): void {
    const canMove = this.state === 'playing' && !this.busy;
    const v = this.vitals;
    this.player.update(dt, canMove, { value: v.stamina, spend: (n) => v.spendStamina(n) }, this.elapsed, this.weather.cur.waves);
    if (this.player.fallDamage > 0) {
      v.damage(this.player.fallDamage);
      this.ui.damageFlash();
      this.audio.hurt();
    }
    if (this.state === 'dead') return;

    const cause = v.update(dt, { sprinting: this.player.sprinting && this.player.moving, cold: 0 });
    if (cause) { this.die(cause); return; }
    if (v.hunger < 15 && Math.floor(this.elapsed) % 25 === 0 && Math.abs(this.elapsed % 1) < dt) this.ui.toast('Вы очень голодны — найдите еду.', 'warn');
    if (v.thirst < 15 && Math.floor(this.elapsed) % 25 === 12 && Math.abs(this.elapsed % 1) < dt) this.ui.toast('Вас мучает жажда — найдите воду.', 'warn');

    // инструменты
    this.swingCooldown = Math.max(0, this.swingCooldown - dt);
    if (this.pendingHit >= 0) {
      this.pendingHit -= dt;
      if (this.pendingHit < 0) { this.pendingHit = -1; this.resolveHit(); }
    }
    if (this.state === 'playing' && this.input.primaryHeld && (this.held.current === 'axe' || this.held.current === 'hammer')) this.swingTool();

    this.fishing.update(dt, this.elapsed);
    this.held.update(dt, this.player.speed, this.player.bobAmount, this.player.bobPhase, this.player.sprinting);
    this.dropped.update(dt, this.elapsed);
    this.updateTarget();

    // автосохранение
    const every = settings.value.autosaveMinutes;
    if (every > 0 && this.state === 'playing' && !this.mp.active) {
      this.autosaveT += dt;
      if (this.autosaveT >= every * 60) { this.autosaveT = 0; void this.saveNow(false).then(() => this.ui.feed('Автосохранение')); }
    }
    void time;
  }

  /** Подбирает ближайший интерактивный объект в поле зрения и в допустимой дистанции. */
  private updateTarget(): void {
    if (this.state !== 'playing' || this.busy) { this.target = null; this.ui.setPrompt(null); return; }
    if (this.fishing.active) { this.target = null; this.ui.setPrompt(null); return; }
    const cam = this.renderer.camera;
    const eye = cam.position;
    const dir = new THREE.Vector3();
    cam.getWorldDirection(dir);
    let best: Interactable | null = null;
    let bestScore = 0;
    let label: string | null = null;
    const to = new THREE.Vector3();
    for (const it of this.world.interactables) {
      const d = it.position.distanceTo(eye);
      if (d > it.radius) continue;
      const l = it.label();
      if (l === null) continue;
      to.copy(it.position).sub(eye).normalize();
      const dot = to.dot(dir);
      if (dot < 0.72) continue;
      const score = dot * 2 - d * 0.18;
      if (score > bestScore || best === null) {
        // проверка «через стену»: отрезок взгляда не должен пересекать чужие коллайдеры
        if (this.world.colliders.segmentBlocked(eye.x, eye.z, it.position.x, it.position.z, 2.2)) continue;
        best = it; bestScore = score; label = l;
      }
    }
    this.target = best;
    this.ui.setPrompt(best ? label : null);
  }
}
