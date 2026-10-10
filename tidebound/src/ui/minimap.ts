import { LANDMARKS, WORLD, heightAt } from '../../shared/terrain';

/** Мини-карта: береговая линия запекается один раз в offscreen-canvas; север сверху, стрелка показывает взгляд. */
export class Minimap {
  private base: HTMLCanvasElement;
  private ctx: CanvasRenderingContext2D;
  private px = 512;
  /** Радиус отображаемой области, м. */
  private view = 140;

  constructor(private canvas: HTMLCanvasElement) {
    this.ctx = canvas.getContext('2d') as CanvasRenderingContext2D;
    this.base = document.createElement('canvas');
    this.base.width = this.base.height = this.px;
    this.bake();
  }

  private bake(): void {
    const c = this.base.getContext('2d') as CanvasRenderingContext2D;
    const img = c.createImageData(this.px, this.px);
    for (let j = 0; j < this.px; j++) {
      for (let i = 0; i < this.px; i++) {
        const x = (i / this.px - 0.5) * WORLD.size;
        const z = (j / this.px - 0.5) * WORLD.size;
        const h = heightAt(x, z);
        let r: number, g: number, b: number;
        if (h < -4) { r = 14; g = 70; b = 128; }
        else if (h < -0.2) { const k = (h + 4) / 3.8; r = 20 + k * 20; g = 100 + k * 60; b = 150 + k * 30; }
        else if (h < 1.5) { r = 224; g = 208; b = 150; }
        else if (h < 17) { const k = Math.min(1, h / 17); r = 100 - k * 30; g = 160 - k * 30; b = 80 - k * 10; }
        else { r = 140; g = 144; b = 150; }
        const o = (j * this.px + i) * 4;
        img.data[o] = r; img.data[o + 1] = g; img.data[o + 2] = b; img.data[o + 3] = 255;
      }
    }
    c.putImageData(img, 0, 0);
  }

  draw(px: number, pz: number, yaw: number): void {
    const ctx = this.ctx;
    const W = this.canvas.width;
    const scale = W / (this.view * 2); // px на метр
    ctx.save();
    ctx.clearRect(0, 0, W, W);
    ctx.beginPath();
    ctx.arc(W / 2, W / 2, W / 2 - 1, 0, Math.PI * 2);
    ctx.clip();
    ctx.fillStyle = '#0e4678';
    ctx.fillRect(0, 0, W, W);
    // карта: world→canvas
    const k = scale * (WORLD.size / this.px);
    ctx.drawImage(this.base, W / 2 - (px + WORLD.size / 2) * scale, W / 2 - (pz + WORLD.size / 2) * scale, this.px * k, this.px * k);

    const mark = (x: number, z: number, color: string, label: string) => {
      const sx = W / 2 + (x - px) * scale, sy = W / 2 + (z - pz) * scale;
      const d = Math.hypot(sx - W / 2, sy - W / 2);
      let ex = sx, ey = sy;
      if (d > W / 2 - 10) { ex = W / 2 + ((sx - W / 2) / d) * (W / 2 - 10); ey = W / 2 + ((sy - W / 2) / d) * (W / 2 - 10); }
      ctx.fillStyle = color;
      ctx.beginPath(); ctx.arc(ex, ey, 4.5, 0, 7); ctx.fill();
      ctx.strokeStyle = '#0b1e3a'; ctx.lineWidth = 1.5; ctx.stroke();
      if (d <= W / 2 - 10) { ctx.fillStyle = '#fff'; ctx.font = '10px sans-serif'; ctx.textAlign = 'center'; ctx.fillText(label, ex, ey - 7); }
    };
    const L = LANDMARKS;
    mark(L.lighthouse.x, L.lighthouse.z, '#ff6b5a', 'Маяк');
    mark(L.dockStart.x + L.dockDir.x * 30, L.dockStart.z + L.dockDir.z * 30, '#ffd23f', 'Причал');
    mark(L.workshop.x, L.workshop.z, '#ff9a3c', 'Мастерская');
    mark(L.garage.x, L.garage.z, '#4fc3f7', 'Гараж');
    mark(L.square.x, L.square.z, '#ffffff', 'Площадь');

    // игрок: стрелка по направлению взгляда (yaw=0 → на -Z, то есть вверх по карте)
    ctx.translate(W / 2, W / 2);
    ctx.rotate(-yaw);
    ctx.fillStyle = '#fff';
    ctx.strokeStyle = '#0b3a8a';
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.moveTo(0, -9); ctx.lineTo(6.5, 7); ctx.lineTo(0, 3.5); ctx.lineTo(-6.5, 7); ctx.closePath();
    ctx.fill(); ctx.stroke();
    ctx.restore();
  }
}
