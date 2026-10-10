import type { Input } from './input';
import { canFullscreen, isFullscreen, toggleFullscreen } from './device';

interface Hooks {
  /** Тип предмета в руках (rod / axe / hammer / food / ...). */
  heldKind: () => string;
}

const STICK_R = 54;      // радиус джойстика, px
const DEAD = 0.14;       // мёртвая зона

/**
 * Экранное управление для телефонов и планшетов. Живёт внутри #hud:
 * правее/выше него лежат экраны меню (z-index 20), поэтому при открытом меню касания сюда не доходят.
 *  — левая часть экрана: плавающий джойстик (появляется под пальцем);
 *  — правая часть: обзор перетаскиванием;
 *  — кнопки: действие (ЛКМ), прыжок, взаимодействие, бег, приседание, удочка (смотать/наживка), меню.
 */
export class TouchControls {
  private root: HTMLElement;
  private pad: HTMLElement;
  private stickBase: HTMLElement;
  private stickKnob: HTMLElement;
  private lookId: number | null = null;
  private stickId: number | null = null;
  private lookLast = { x: 0, y: 0 };
  private stickOrigin = { x: 0, y: 0 };
  private sprintOn = false;
  private sprintBtn!: HTMLElement;
  private interactBtn!: HTMLElement;
  private rodBtns!: HTMLElement;
  private lastKind = '';

  constructor(private input: Input, private hooks: Hooks) {
    const hud = document.getElementById('hud')!;
    this.root = document.createElement('div');
    this.root.id = 'touch-controls';
    this.root.innerHTML = `
      <div class="t-pad" id="t-pad"></div>
      <div class="t-stick hidden" id="t-stick-base"><div class="t-knob" id="t-stick-knob"></div></div>
      <div class="t-btns">
        <button class="t-btn t-primary" data-k="primary" aria-label="Действие">✋</button>
        <button class="t-btn t-jump" data-k="jump" aria-label="Прыжок">⤒</button>
        <button class="t-btn t-interact" data-k="interact" aria-label="Взаимодействовать">E</button>
        <button class="t-btn t-sprint t-small" data-k="sprint" aria-label="Бег">»</button>
        <button class="t-btn t-crouch t-small" data-k="crouch" aria-label="Присесть">▾</button>
        <div class="t-rod hidden" id="t-rod">
          <button class="t-btn t-small" data-k="reel" aria-label="Смотать леску">⟲</button>
          <button class="t-btn t-small" data-k="bait" aria-label="Сменить наживку">🪱</button>
        </div>
      </div>
      <div class="t-menu">
        <button class="t-mini" data-k="craft" aria-label="Крафт">🔨</button>
        <button class="t-mini" data-k="inventory" aria-label="Инвентарь">🎒</button>
        <button class="t-mini" data-k="fs" aria-label="Полный экран" id="t-fs">⛶</button>
        <button class="t-mini" data-k="pause" aria-label="Пауза">⏸</button>
      </div>`;
    hud.prepend(this.root);
    const q = <T extends HTMLElement>(s: string): T => this.root.querySelector<T>(s)!;
    this.pad = q('#t-pad');
    this.stickBase = q('#t-stick-base');
    this.stickKnob = q('#t-stick-knob');
    this.sprintBtn = q('.t-sprint');
    this.interactBtn = q('.t-interact');
    this.rodBtns = q('#t-rod');
    if (!canFullscreen()) q('#t-fs').classList.add('hidden');

    this.bindPad();
    this.bindButtons();
    input.onReleaseAll = () => this.reset();

    // подсветка кнопки «E», когда есть что использовать
    const prompt = document.getElementById('prompt');
    if (prompt) {
      const sync = (): void => { this.interactBtn.classList.toggle('ready', !prompt.classList.contains('hidden')); };
      new MutationObserver(sync).observe(prompt, { attributes: true, attributeFilter: ['class'] });
      sync();
    }
    // кнопки удочки показываем, только когда она в руках
    setInterval(() => this.syncHeld(), 200);
    document.addEventListener('fullscreenchange', () => { document.getElementById('t-fs')?.classList.toggle('on', isFullscreen()); });
  }

  private syncHeld(): void {
    const k = this.hooks.heldKind();
    if (k === this.lastKind) return;
    this.lastKind = k;
    this.rodBtns.classList.toggle('hidden', k !== 'rod');
  }

  private reset(): void {
    this.sprintOn = false;
    this.sprintBtn?.classList.remove('on');
    this.root?.querySelector('.t-crouch')?.classList.remove('on');
    this.root?.querySelectorAll('.t-btn.down').forEach((b) => b.classList.remove('down'));
    this.endStick();
    this.lookId = null;
  }

  // ───────── Джойстик и обзор ─────────

  private bindPad(): void {
    const pad = this.pad;
    pad.addEventListener('pointerdown', (e) => {
      e.preventDefault();
      pad.setPointerCapture(e.pointerId);
      const leftSide = e.clientX < window.innerWidth * 0.42;
      if (leftSide && this.stickId === null) {
        this.stickId = e.pointerId;
        this.stickOrigin = { x: e.clientX, y: e.clientY };
        this.stickBase.style.left = `${e.clientX}px`;
        this.stickBase.style.top = `${e.clientY}px`;
        this.stickBase.classList.remove('hidden');
        this.moveKnob(0, 0);
      } else if (!leftSide && this.lookId === null) {
        this.lookId = e.pointerId;
        this.lookLast = { x: e.clientX, y: e.clientY };
      }
    });
    pad.addEventListener('pointermove', (e) => {
      if (e.pointerId === this.stickId) {
        let dx = e.clientX - this.stickOrigin.x;
        let dy = e.clientY - this.stickOrigin.y;
        const len = Math.hypot(dx, dy);
        if (len > STICK_R) {
          // «подтягиваем» центр за пальцем, чтобы не приходилось возвращаться
          const k = (len - STICK_R) / len;
          this.stickOrigin.x += dx * k; this.stickOrigin.y += dy * k;
          this.stickBase.style.left = `${this.stickOrigin.x}px`;
          this.stickBase.style.top = `${this.stickOrigin.y}px`;
          dx = e.clientX - this.stickOrigin.x; dy = e.clientY - this.stickOrigin.y;
        }
        this.moveKnob(dx, dy);
        let nx = dx / STICK_R, nz = -dy / STICK_R;
        const m = Math.hypot(nx, nz);
        if (m < DEAD) { nx = 0; nz = 0; } else { const s = (m - DEAD) / (1 - DEAD) / m; nx *= s; nz *= s; }
        this.input.setStick(nx, nz);
        if (this.sprintOn) this.input.setHeld('sprint', nz > 0.1);
      } else if (e.pointerId === this.lookId) {
        // объединённые события дают точное движение
        const evs = e.getCoalescedEvents?.() ?? [e];
        for (const c of evs.length ? evs : [e]) {
          this.input.touchLook(c.clientX - this.lookLast.x, c.clientY - this.lookLast.y);
          this.lookLast = { x: c.clientX, y: c.clientY };
        }
      }
    });
    const end = (e: PointerEvent): void => {
      if (e.pointerId === this.stickId) this.endStick();
      if (e.pointerId === this.lookId) this.lookId = null;
    };
    pad.addEventListener('pointerup', end);
    pad.addEventListener('pointercancel', end);
    pad.addEventListener('lostpointercapture', end);
  }

  private moveKnob(dx: number, dy: number): void {
    this.stickKnob.style.transform = `translate(${dx}px, ${dy}px)`;
  }

  private endStick(): void {
    this.stickId = null;
    this.stickBase?.classList.add('hidden');
    this.input.setStick(0, 0);
    if (this.sprintOn) { this.sprintOn = false; this.sprintBtn.classList.remove('on'); this.input.setHeld('sprint', false); }
  }

  // ───────── Кнопки ─────────

  private bindButtons(): void {
    this.root.querySelectorAll<HTMLElement>('[data-k]').forEach((btn) => {
      const k = btn.dataset.k!;
      const press = (down: boolean): void => {
        btn.classList.toggle('down', down);
        switch (k) {
          case 'primary': this.input.touchPrimary(down); break;
          case 'jump': this.input.setHeld('jump', down); if (down) this.input.touchAction('jump'); break;
          case 'interact': if (down) this.input.touchAction('interact'); break;
          case 'reel': if (down) this.input.touchSecondary(); break;
          case 'bait': if (down) this.input.touchAction('reload'); break;
          case 'sprint':
            if (down) { this.sprintOn = !this.sprintOn; btn.classList.toggle('on', this.sprintOn); this.input.setHeld('sprint', this.sprintOn); }
            break;
          case 'crouch':
            if (down) { const on = !this.input.held.has('crouch'); btn.classList.toggle('on', on); this.input.setHeld('crouch', on); }
            break;
          case 'inventory': case 'craft': case 'pause': if (down) this.input.touchAction(k); break;
          case 'fs': if (down) void toggleFullscreen(); break;
        }
      };
      btn.addEventListener('pointerdown', (e) => { e.preventDefault(); e.stopPropagation(); btn.setPointerCapture(e.pointerId); press(true); if (navigator.vibrate && k !== 'primary') navigator.vibrate(8); });
      const up = (e: PointerEvent): void => { e.preventDefault(); if (btn.classList.contains('down') || k === 'primary') press(false); };
      btn.addEventListener('pointerup', up);
      btn.addEventListener('pointercancel', up);
    });
  }
}
