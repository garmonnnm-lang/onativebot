import { settings } from '../core/settings';
import { isTouchDevice } from './device';

export type Action =
  | 'forward' | 'back' | 'left' | 'right' | 'jump' | 'sprint' | 'crouch'
  | 'interact' | 'inventory' | 'craft' | 'reload' | 'pause';

const KEYMAP: Record<string, Action> = {
  KeyW: 'forward', ArrowUp: 'forward', KeyS: 'back', ArrowDown: 'back', KeyA: 'left', ArrowLeft: 'left',
  KeyD: 'right', ArrowRight: 'right', Space: 'jump', ShiftLeft: 'sprint', ShiftRight: 'sprint',
  ControlLeft: 'crouch', KeyC: 'crouch', KeyE: 'interact', Tab: 'inventory', KeyG: 'craft', KeyR: 'reload', Escape: 'pause',
};

export interface InputEvents {
  action: (a: Action) => void;
  slot: (index: number) => void;
  wheel: (dir: number) => void;
  primaryDown: () => void;
  primaryUp: () => void;
  secondaryDown: () => void;
  lockChange: (locked: boolean) => void;
  toggleDebug: () => void;
}

/**
 * Ввод: клавиатура, мышь, Pointer Lock. Когда `enabled` = false (открыт интерфейс),
 * игровые действия игнорируются, а состояние клавиш сбрасывается.
 */
export class Input {
  readonly held = new Set<Action>();
  yaw = 0;
  pitch = 0;
  enabled = false;
  locked = false;
  primaryHeld = false;
  /** Накопленное движение мыши (для эффектов). */
  private handlers: Partial<InputEvents> = {};
  /** Сенсорный режим: нет Pointer Lock, обзор и движение приходят от экранных элементов. */
  readonly touch = isTouchDevice;
  private stick = { x: 0, z: 0 };
  /** Вызывается при сбросе всех нажатий (потеря фокуса, выход из игры) — экранные кнопки сбрасывают своё состояние. */
  onReleaseAll: (() => void) | null = null;

  constructor(private canvas: HTMLCanvasElement) {
    window.addEventListener('keydown', (e) => this.onKey(e, true));
    window.addEventListener('keyup', (e) => this.onKey(e, false));
    window.addEventListener('blur', () => this.releaseAll());
    document.addEventListener('mousemove', (e) => this.onMouse(e));
    canvas.addEventListener('mousedown', (e) => this.onMouseButton(e, true));
    window.addEventListener('mouseup', (e) => this.onMouseButton(e, false));
    canvas.addEventListener('contextmenu', (e) => e.preventDefault());
    canvas.addEventListener('wheel', (e) => {
      if (!this.enabled || !settings.value.wheelSwitch) return;
      e.preventDefault();
      this.handlers.wheel?.(Math.sign(e.deltaY));
    }, { passive: false });
    document.addEventListener('pointerlockchange', () => {
      this.locked = document.pointerLockElement === this.canvas;
      if (!this.locked) this.releaseAll();
      this.handlers.lockChange?.(this.locked);
    });
    document.addEventListener('pointerlockerror', () => {
      console.warn('[input] Pointer Lock отклонён браузером');
      this.handlers.lockChange?.(false);
    });
  }

  on<K extends keyof InputEvents>(k: K, fn: InputEvents[K]): void {
    this.handlers[k] = fn;
  }

  requestLock(): void {
    if (this.touch) { this.locked = true; this.handlers.lockChange?.(true); return; }
    try {
      const p = this.canvas.requestPointerLock() as unknown as Promise<void> | undefined;
      if (p && typeof p.catch === 'function') p.catch(() => this.handlers.lockChange?.(false));
    } catch {
      this.handlers.lockChange?.(false);
    }
  }

  exitLock(): void {
    if (this.touch) { this.locked = false; this.releaseAll(); return; }
    if (document.pointerLockElement) document.exitPointerLock();
  }

  private releaseAll(): void {
    this.held.clear();
    this.stick.x = 0; this.stick.z = 0;
    this.onReleaseAll?.();
    if (this.primaryHeld) { this.primaryHeld = false; this.handlers.primaryUp?.(); }
  }

  private onKey(e: KeyboardEvent, down: boolean): void {
    if (e.code === 'F3' && down) { e.preventDefault(); this.handlers.toggleDebug?.(); return; }
    const target = e.target as HTMLElement | null;
    if (target && (target.tagName === 'INPUT' || target.tagName === 'SELECT' || target.tagName === 'TEXTAREA')) return;
    const action = KEYMAP[e.code];
    if (e.code === 'Tab') e.preventDefault();
    if (down && !e.repeat) {
      // Esc и Tab должны работать и при открытом интерфейсе
      if (action === 'pause' || action === 'inventory' || action === 'craft') { this.handlers.action?.(action); return; }
    }
    if (!this.enabled) return;
    if (action) {
      if (down) {
        if (!this.held.has(action)) {
          this.held.add(action);
          if (!e.repeat && (action === 'interact' || action === 'jump' || action === 'reload')) this.handlers.action?.(action);
        }
      } else this.held.delete(action);
      if (['Space', 'ArrowUp', 'ArrowDown'].includes(e.code)) e.preventDefault();
    } else if (down && !e.repeat && /^Digit[1-5]$/.test(e.code)) {
      this.handlers.slot?.(Number(e.code.slice(5)) - 1);
    }
  }

  /** Сенсорный обзор: dx/dy в пикселях экрана. */
  touchLook(dx: number, dy: number): void {
    if (!this.enabled) return;
    const sens = 0.0030 * (0.4 + settings.value.mouseSensitivity * 0.32) / 2.0;
    this.yaw -= dx * sens;
    this.pitch -= dy * sens;
    const lim = Math.PI / 2 - 0.02;
    this.pitch = Math.max(-lim, Math.min(lim, this.pitch));
  }

  /** Виртуальный джойстик: x вправо, z вперёд, длина 0..1. */
  setStick(x: number, z: number): void {
    this.stick.x = this.enabled ? x : 0;
    this.stick.z = this.enabled ? z : 0;
  }

  /** Экранная кнопка-«удержание» (бег, приседание, прыжок). */
  setHeld(a: Action, down: boolean): void {
    if (!this.enabled) { this.held.delete(a); return; }
    if (down) this.held.add(a); else this.held.delete(a);
  }

  /** Экранная кнопка основного действия (ЛКМ). */
  touchPrimary(down: boolean): void {
    if (!this.enabled) { if (!down && this.primaryHeld) { this.primaryHeld = false; this.handlers.primaryUp?.(); } return; }
    if (down === this.primaryHeld) return;
    this.primaryHeld = down;
    if (down) this.handlers.primaryDown?.(); else this.handlers.primaryUp?.();
  }

  touchSecondary(): void { if (this.enabled) this.handlers.secondaryDown?.(); }
  touchSlot(i: number): void { if (this.enabled) this.handlers.slot?.(i); }
  /** Разовое действие: interact / jump / reload / inventory / craft / pause. */
  touchAction(a: Action): void {
    if (a === 'pause' || a === 'inventory' || a === 'craft') { this.handlers.action?.(a); return; }
    if (this.enabled) this.handlers.action?.(a);
  }

  private onMouse(e: MouseEvent): void {
    if (this.touch || !this.enabled || !this.locked) return;
    const sens = 0.0006 * (0.4 + settings.value.mouseSensitivity * 0.32);
    this.yaw -= e.movementX * sens;
    this.pitch -= e.movementY * sens;
    const lim = Math.PI / 2 - 0.02;
    this.pitch = Math.max(-lim, Math.min(lim, this.pitch));
  }

  private onMouseButton(e: MouseEvent, down: boolean): void {
    if (this.touch || !this.enabled) return;
    if (e.button === 0) {
      if (down && !this.locked) return; // клик, возвращающий захват курсора, не должен быть действием
      this.primaryHeld = down;
      if (down) this.handlers.primaryDown?.(); else this.handlers.primaryUp?.();
    } else if (e.button === 2 && down) {
      this.handlers.secondaryDown?.();
    }
  }

  axis(): { x: number; z: number } {
    let x = 0, z = 0;
    if (this.held.has('forward')) z += 1;
    if (this.held.has('back')) z -= 1;
    if (this.held.has('right')) x += 1;
    if (this.held.has('left')) x -= 1;
    if (this.stick.x || this.stick.z) { x += this.stick.x; z += this.stick.z; }
    const l = Math.hypot(x, z);
    return l > 1 ? { x: x / l, z: z / l } : { x, z };
  }
}
