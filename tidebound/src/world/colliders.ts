import type { BoxCollider, CircleCollider, Collider, Platform } from './types';

export function makeCircle(x: number, z: number, r: number, top = 99): CircleCollider {
  return { kind: 'circle', x, z, r, enabled: true, top };
}

/** `yaw` — то же значение, что mesh.rotation.y. */
export function makeBox(cx: number, cz: number, hx: number, hz: number, yaw = 0, top = 99): BoxCollider {
  return { kind: 'box', cx, cz, hx, hz, cos: Math.cos(yaw), sin: -Math.sin(yaw), enabled: true, top, bound: Math.hypot(hx, hz) };
}

export function makePlatform(cx: number, cz: number, hx: number, hz: number, yaw: number, top: number): Platform {
  return { cx, cz, hx, hz, cos: Math.cos(yaw), sin: -Math.sin(yaw), top };
}

export function insidePlatform(p: Platform, x: number, z: number): boolean {
  const dx = x - p.cx;
  const dz = z - p.cz;
  const lx = dx * p.cos + dz * p.sin;
  const lz = -dx * p.sin + dz * p.cos;
  return Math.abs(lx) <= p.hx && Math.abs(lz) <= p.hz;
}

/**
 * Пространственная сетка коллайдеров: запросы к ближайшим ячейкам вместо перебора сотен деревьев.
 */
export class ColliderGrid {
  readonly all: Collider[] = [];
  private cells = new Map<number, Collider[]>();
  private readonly cell = 16;
  private stamp = 0;
  private marks = new WeakMap<Collider, number>();

  private key(ix: number, iz: number): number {
    return (ix + 1024) * 4096 + (iz + 1024);
  }

  add(c: Collider): void {
    this.all.push(c);
    const x = c.kind === 'circle' ? c.x : c.cx;
    const z = c.kind === 'circle' ? c.z : c.cz;
    const r = c.kind === 'circle' ? c.r : c.bound;
    const x0 = Math.floor((x - r) / this.cell), x1 = Math.floor((x + r) / this.cell);
    const z0 = Math.floor((z - r) / this.cell), z1 = Math.floor((z + r) / this.cell);
    for (let ix = x0; ix <= x1; ix++) {
      for (let iz = z0; iz <= z1; iz++) {
        const k = this.key(ix, iz);
        let list = this.cells.get(k);
        if (!list) { list = []; this.cells.set(k, list); }
        list.push(c);
      }
    }
  }

  query(x: number, z: number, r: number, out: Collider[] = []): Collider[] {
    out.length = 0;
    this.stamp++;
    const x0 = Math.floor((x - r) / this.cell), x1 = Math.floor((x + r) / this.cell);
    const z0 = Math.floor((z - r) / this.cell), z1 = Math.floor((z + r) / this.cell);
    for (let ix = x0; ix <= x1; ix++) {
      for (let iz = z0; iz <= z1; iz++) {
        const list = this.cells.get(this.key(ix, iz));
        if (!list) continue;
        for (const c of list) {
          if (this.marks.get(c) === this.stamp) continue;
          this.marks.set(c, this.stamp);
          out.push(c);
        }
      }
    }
    return out;
  }

  private tmp: Collider[] = [];

  /** Выталкивает окружность из коллайдеров; низкие объекты можно перепрыгнуть (feetY > top). */
  resolve(pos: { x: number; z: number }, radius: number, feetY: number): boolean {
    return resolveColliders(this.query(pos.x, pos.z, radius + 4, this.tmp), pos, radius, feetY);
  }

  /** Пересекает ли отрезок (в плане) включённый коллайдер (для проверки «через стену»). */
  segmentBlocked(ax: number, az: number, bx: number, bz: number, ignoreRadius = 0): boolean {
    const mx = (ax + bx) / 2, mz = (az + bz) / 2;
    const r = Math.hypot(bx - ax, bz - az) / 2 + 1;
    for (const c of this.query(mx, mz, r, this.tmp)) {
      if (!c.enabled) continue;
      if (ignoreRadius > 0) {
        const cx = c.kind === 'circle' ? c.x : c.cx, cz = c.kind === 'circle' ? c.z : c.cz;
        if (Math.hypot(cx - bx, cz - bz) < ignoreRadius) continue;
      }
      if (c.kind === 'circle') {
        if (c.top < 1.2) continue;
        if (distPointSegment(c.x, c.z, ax, az, bx, bz) < c.r) return true;
      } else {
        // в локальные координаты бокса
        const l0x = (ax - c.cx) * c.cos + (az - c.cz) * c.sin, l0z = -(ax - c.cx) * c.sin + (az - c.cz) * c.cos;
        const l1x = (bx - c.cx) * c.cos + (bz - c.cz) * c.sin, l1z = -(bx - c.cx) * c.sin + (bz - c.cz) * c.cos;
        if (segmentHitsAabb(l0x, l0z, l1x, l1z, c.hx, c.hz)) return true;
      }
    }
    return false;
  }
}

function distPointSegment(px: number, pz: number, ax: number, az: number, bx: number, bz: number): number {
  const dx = bx - ax, dz = bz - az;
  const l2 = dx * dx + dz * dz;
  const t = l2 < 1e-9 ? 0 : Math.max(0, Math.min(1, ((px - ax) * dx + (pz - az) * dz) / l2));
  return Math.hypot(px - (ax + dx * t), pz - (az + dz * t));
}

function segmentHitsAabb(x0: number, z0: number, x1: number, z1: number, hx: number, hz: number): boolean {
  let t0 = 0, t1 = 1;
  const dx = x1 - x0, dz = z1 - z0;
  for (const [p, q] of [[-dx, x0 + hx], [dx, hx - x0], [-dz, z0 + hz], [dz, hz - z0]] as const) {
    if (Math.abs(p) < 1e-9) { if (q < 0) return false; }
    else {
      const t = q / p;
      if (p < 0) { if (t > t1) return false; if (t > t0) t0 = t; }
      else { if (t < t0) return false; if (t < t1) t1 = t; }
    }
  }
  return true;
}

/** Выталкивает окружность игрока из коллайдеров. Низкие объекты можно перепрыгнуть (feetY > top). */
export function resolveColliders(colliders: readonly Collider[], pos: { x: number; z: number }, radius: number, feetY: number): boolean {
  let hit = false;
  for (let iter = 0; iter < 2; iter++) {
    for (const c of colliders) {
      if (!c.enabled || feetY > c.top) continue;
      if (c.kind === 'circle') {
        const dx = pos.x - c.x;
        const dz = pos.z - c.z;
        const min = radius + c.r;
        if (Math.abs(dx) > min || Math.abs(dz) > min) continue;
        const d2 = dx * dx + dz * dz;
        if (d2 >= min * min) continue;
        const d = Math.sqrt(d2) || 0.0001;
        const push = min - d;
        pos.x += (dx / d) * push;
        pos.z += (dz / d) * push;
        hit = true;
      } else {
        const dx = pos.x - c.cx;
        const dz = pos.z - c.cz;
        if (Math.abs(dx) > c.bound + radius || Math.abs(dz) > c.bound + radius) continue;
        const lx = dx * c.cos + dz * c.sin;
        const lz = -dx * c.sin + dz * c.cos;
        const qx = Math.max(-c.hx, Math.min(c.hx, lx));
        const qz = Math.max(-c.hz, Math.min(c.hz, lz));
        const ox = lx - qx;
        const oz = lz - qz;
        const d2 = ox * ox + oz * oz;
        if (d2 >= radius * radius) continue;
        let nx: number;
        let nz: number;
        let push: number;
        if (d2 > 1e-8) {
          const d = Math.sqrt(d2);
          nx = ox / d;
          nz = oz / d;
          push = radius - d;
        } else {
          const px = c.hx - Math.abs(lx);
          const pz = c.hz - Math.abs(lz);
          if (px < pz) { nx = Math.sign(lx) || 1; nz = 0; push = px + radius; }
          else { nx = 0; nz = Math.sign(lz) || 1; push = pz + radius; }
        }
        // обратно в мир: инверсия поворота (cos, -sin ↔ cos, sin)
        const wx = nx * c.cos - nz * c.sin;
        const wz = nx * c.sin + nz * c.cos;
        pos.x += wx * push;
        pos.z += wz * push;
        hit = true;
      }
    }
  }
  return hit;
}
