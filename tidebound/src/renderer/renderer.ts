import * as THREE from 'three';
import { QUALITY_PRESETS, settings } from '../core/settings';

/** Обёртка над WebGLRenderer: размер, качество, потеря контекста. */
export class GameRenderer {
  readonly renderer: THREE.WebGLRenderer;
  readonly scene = new THREE.Scene();
  readonly camera: THREE.PerspectiveCamera;
  onContextLost: (() => void) | null = null;
  onContextRestored: (() => void) | null = null;

  constructor(readonly canvas: HTMLCanvasElement) {
    this.renderer = new THREE.WebGLRenderer({ canvas, antialias: true, powerPreference: 'high-performance' });
    this.renderer.outputColorSpace = THREE.SRGBColorSpace;
    this.renderer.toneMapping = THREE.ACESFilmicToneMapping;
    this.renderer.toneMappingExposure = 0.95;
    this.renderer.shadowMap.type = THREE.PCFShadowMap;

    this.camera = new THREE.PerspectiveCamera(settings.value.fov, 1, 0.08, 1200);
    this.camera.rotation.order = 'YXZ';
    this.scene.add(this.camera);

    canvas.addEventListener('webglcontextlost', (e) => { e.preventDefault(); this.onContextLost?.(); });
    canvas.addEventListener('webglcontextrestored', () => this.onContextRestored?.());

    this.applySettings();
    window.addEventListener('resize', () => this.resize());
  }

  /** Динамический масштаб разрешения (0.5–1): подстраивается под FPS. */
  dynScale = 1;
  private autoTimer = 0;

  private basePixelRatio(): number {
    const q = QUALITY_PRESETS[settings.value.quality];
    return Math.min(window.devicePixelRatio, 1.5) * q.pixelRatio;
  }

  /** Вызывать каждый кадр с усреднённым FPS: при просадке снижает разрешение, при запасе — возвращает. */
  autoScale(dt: number, fps: number): void {
    this.autoTimer += dt;
    if (this.autoTimer < 1.5) return;
    this.autoTimer = 0;
    let next = this.dynScale;
    if (fps < 45) next = Math.max(0.5, this.dynScale * 0.88);
    else if (fps > 58 && this.dynScale < 1) next = Math.min(1, this.dynScale * 1.06);
    if (Math.abs(next - this.dynScale) > 0.01) {
      this.dynScale = next;
      this.renderer.setPixelRatio(this.basePixelRatio() * this.dynScale);
      this.resize();
    }
  }

  applySettings(): void {
    const q = QUALITY_PRESETS[settings.value.quality];
    this.renderer.setPixelRatio(this.basePixelRatio() * this.dynScale);
    this.renderer.shadowMap.enabled = settings.value.shadows && q.shadowMap > 0;
    this.camera.fov = settings.value.fov;
    this.camera.updateProjectionMatrix();
    this.resize();
  }

  resize(): void {
    const w = window.innerWidth;
    const h = window.innerHeight;
    this.renderer.setSize(w, h, false);
    this.camera.aspect = w / Math.max(1, h);
    this.camera.updateProjectionMatrix();
  }

  render(): void {
    this.renderer.render(this.scene, this.camera);
  }
}
