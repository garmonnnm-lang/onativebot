type Handler<T> = (payload: T) => void;

/** Минимальный типизированный эмиттер событий. */
export class Emitter<Events extends { [K in keyof Events]: unknown }> {
  private map = new Map<keyof Events, Set<Handler<never>>>();

  on<K extends keyof Events>(type: K, fn: Handler<Events[K]>): () => void {
    let set = this.map.get(type);
    if (!set) {
      set = new Set();
      this.map.set(type, set);
    }
    set.add(fn as Handler<never>);
    return () => this.off(type, fn);
  }

  off<K extends keyof Events>(type: K, fn: Handler<Events[K]>): void {
    this.map.get(type)?.delete(fn as Handler<never>);
  }

  emit<K extends keyof Events>(type: K, payload: Events[K]): void {
    const set = this.map.get(type);
    if (!set) return;
    for (const fn of [...set]) (fn as Handler<Events[K]>)(payload);
  }
}
