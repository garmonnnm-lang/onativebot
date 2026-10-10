import { getItem } from './items';

export interface ItemStack { id: string; count: number }
export const INVENTORY_SIZE = 30;
export const HOTBAR_SIZE = 5;
export interface InventoryData { slots: (ItemStack | null)[] }

/**
 * Инвентарь. Операции атомарны: либо выполняются полностью, либо состояние не меняется.
 * Не зависит от DOM/Three.js — тот же класс будет использоваться сервером.
 */
export class Inventory {
  slots: (ItemStack | null)[];
  private listeners = new Set<() => void>();

  constructor(size = INVENTORY_SIZE) {
    this.slots = new Array(size).fill(null);
  }

  onChange(fn: () => void): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }
  private emit(): void { for (const fn of this.listeners) fn(); }

  count(id: string): number {
    let n = 0;
    for (const s of this.slots) if (s && s.id === id) n += s.count;
    return n;
  }
  has(id: string, n = 1): boolean { return this.count(id) >= n; }

  capacityFor(id: string): number {
    const max = getItem(id).stack;
    let room = 0;
    for (const s of this.slots) {
      if (!s) room += max;
      else if (s.id === id) room += max - s.count;
    }
    return room;
  }
  canAdd(id: string, n: number): boolean { return this.capacityFor(id) >= n; }

  /** Поместятся ли все предметы списка одновременно. */
  hasRoomFor(list: ItemStack[]): boolean {
    const copy = new Inventory(this.slots.length);
    copy.slots = this.slots.map((x) => (x ? { id: x.id, count: x.count } : null));
    return list.every((l) => copy.add(l.id, l.count));
  }

  /** Добавляет всё или ничего. */
  add(id: string, n = 1): boolean {
    if (n <= 0) return true;
    if (!this.canAdd(id, n)) return false;
    const max = getItem(id).stack;
    let left = n;
    for (const s of this.slots) {
      if (left === 0) break;
      if (s && s.id === id && s.count < max) {
        const put = Math.min(max - s.count, left);
        s.count += put;
        left -= put;
      }
    }
    for (let i = 0; i < this.slots.length && left > 0; i++) {
      if (!this.slots[i]) {
        const put = Math.min(max, left);
        this.slots[i] = { id, count: put };
        left -= put;
      }
    }
    this.emit();
    return true;
  }

  remove(id: string, n = 1): boolean {
    if (!this.has(id, n)) return false;
    let left = n;
    for (let i = this.slots.length - 1; i >= 0 && left > 0; i--) {
      const s = this.slots[i];
      if (s && s.id === id) {
        const take = Math.min(s.count, left);
        s.count -= take;
        left -= take;
        if (s.count === 0) this.slots[i] = null;
      }
    }
    this.emit();
    return true;
  }

  hasAll(req: ItemStack[]): boolean { return req.every((r) => this.has(r.id, r.count)); }

  /** Атомарный обмен: списать cost, выдать gain. Если не влезает — откат. */
  exchange(cost: ItemStack[], gain: ItemStack[]): boolean {
    if (!this.hasAll(cost)) return false;
    const snap = this.serialize();
    for (const c of cost) this.remove(c.id, c.count);
    for (const g of gain) {
      if (!this.add(g.id, g.count)) {
        this.restore(snap);
        return false;
      }
    }
    return true;
  }

  removeAt(index: number, n?: number): ItemStack | null {
    const s = this.slots[index];
    if (!s) return null;
    const take = Math.min(n ?? s.count, s.count);
    s.count -= take;
    const out = { id: s.id, count: take };
    if (s.count === 0) this.slots[index] = null;
    this.emit();
    return out;
  }

  move(from: number, to: number): void {
    if (from === to) return;
    const a = this.slots[from];
    const b = this.slots[to];
    if (!a) return;
    if (b && b.id === a.id) {
      const max = getItem(a.id).stack;
      const put = Math.min(max - b.count, a.count);
      b.count += put;
      a.count -= put;
      if (a.count === 0) this.slots[from] = null;
    } else {
      this.slots[from] = b;
      this.slots[to] = a;
    }
    this.emit();
  }

  split(index: number): boolean {
    const s = this.slots[index];
    if (!s || s.count < 2) return false;
    const free = this.slots.findIndex((x) => x === null);
    if (free < 0) return false;
    const half = Math.floor(s.count / 2);
    s.count -= half;
    this.slots[free] = { id: s.id, count: half };
    this.emit();
    return true;
  }

  /** Объединяет стопки одного типа, сохраняя порядок первого появления; быстрые слоты не трогает. */
  consolidate(): void {
    const keep = this.slots.slice(0, HOTBAR_SIZE);
    const totals = new Map<string, number>();
    for (let i = HOTBAR_SIZE; i < this.slots.length; i++) {
      const s = this.slots[i];
      if (s) totals.set(s.id, (totals.get(s.id) ?? 0) + s.count);
    }
    for (let i = HOTBAR_SIZE; i < this.slots.length; i++) this.slots[i] = null;
    let i = HOTBAR_SIZE;
    for (const [id, total] of totals) {
      let left = total;
      const max = getItem(id).stack;
      while (left > 0) {
        const put = Math.min(max, left);
        this.slots[i++] = { id, count: put };
        left -= put;
      }
    }
    for (let k = 0; k < HOTBAR_SIZE; k++) this.slots[k] = keep[k] ?? null;
    this.emit();
  }

  serialize(): InventoryData {
    return { slots: this.slots.map((s) => (s ? { id: s.id, count: s.count } : null)) };
  }

  restore(data: InventoryData): void {
    this.slots = new Array(this.slots.length).fill(null);
    data.slots.forEach((s, i) => {
      if (i < this.slots.length && s && s.count > 0) {
        try {
          const def = getItem(s.id);
          this.slots[i] = { id: s.id, count: Math.min(Math.floor(s.count), def.stack) };
        } catch { /* неизвестный предмет из старого сохранения */ }
      }
    });
    this.emit();
  }
}
