import { Inventory, HOTBAR_SIZE } from '../../shared/inventory';
import { CATEGORY_NAMES, RARITY_COLORS, RARITY_NAMES, getItem } from '../../shared/items';

export interface InventoryActions {
  use(index: number): void;
  drop(index: number, all: boolean): void;
  onHotbarChange(): void;
}

/** Окно инвентаря: сетка 30 слотов, перенос/слияние кликом, подробности и действия. */
export class InventoryUi {
  private grid = document.getElementById('inv-grid') as HTMLElement;
  private detail = document.getElementById('inv-detail') as HTMLElement;
  private selected = -1;

  constructor(private inv: Inventory, private actions: InventoryActions) {
    inv.onChange(() => { this.render(); this.actions.onHotbarChange(); });
    this.render();
  }

  render(): void {
    const g = this.grid;
    g.innerHTML = '';
    this.inv.slots.forEach((s, i) => {
      const el = document.createElement('button');
      el.type = 'button';
      el.className = 'slot' + (i < HOTBAR_SIZE ? ' hot' : '') + (i === this.selected ? ' sel' : '');
      if (s) {
        const def = getItem(s.id);
        el.style.setProperty('--rar', RARITY_COLORS[def.rarity]);
        el.title = def.name;
        el.innerHTML = `<span class="slot-ic">${def.icon}</span>${s.count > 1 ? `<span class="slot-n">${s.count}</span>` : ''}`;
      }
      if (i < HOTBAR_SIZE) el.insertAdjacentHTML('beforeend', `<span class="slot-k">${i + 1}</span>`);
      el.addEventListener('click', () => this.click(i));
      el.addEventListener('dblclick', () => { if (this.inv.slots[i]) this.actions.use(i); });
      g.appendChild(el);
    });
    this.renderDetail();
  }

  private click(i: number): void {
    if (this.selected >= 0 && this.selected !== i && this.inv.slots[this.selected]) {
      const from = this.selected;
      this.selected = -1;
      this.inv.move(from, i);
      this.selected = i;
      this.render();
      return;
    }
    this.selected = this.selected === i ? -1 : i;
    this.render();
  }

  reset(): void { this.selected = -1; this.render(); }

  private renderDetail(): void {
    const d = this.detail;
    const s = this.selected >= 0 ? this.inv.slots[this.selected] : null;
    if (!s) {
      d.innerHTML = `<div class="muted">Выберите предмет.</div>
        <div class="inv-actions"><button class="btn small ghost" id="inv-sort">Объединить стопки</button></div>`;
      d.querySelector('#inv-sort')?.addEventListener('click', () => { this.inv.consolidate(); });
      return;
    }
    const def = getItem(s.id);
    const usable = !!(def.food || def.drink);
    d.innerHTML = `
      <div class="inv-title"><span class="inv-big">${def.icon}</span><div><div class="inv-name">${def.name}</div>
      <div class="small" style="color:${RARITY_COLORS[def.rarity]}">${RARITY_NAMES[def.rarity]} · ${CATEGORY_NAMES[def.category]}</div></div></div>
      <p class="muted">${def.description}</p>
      <div class="small muted">Количество: ${s.count} · Вес: ${(def.weight * s.count).toFixed(1)} кг</div>
      ${def.food ? `<div class="small">Сытость +${def.food.hunger}${def.food.health ? `, здоровье ${def.food.health > 0 ? '+' : ''}${def.food.health}` : ''}</div>` : ''}
      ${def.drink ? `<div class="small">Жажда +${def.drink.thirst}${def.drink.health ? `, здоровье ${def.drink.health}` : ''}</div>` : ''}
      <div class="inv-actions">
        ${usable ? '<button class="btn small primary" id="inv-use">Использовать</button>' : ''}
        ${s.count > 1 ? '<button class="btn small" id="inv-split">Разделить</button>' : ''}
        <button class="btn small ghost" id="inv-drop">Выбросить 1</button>
        ${s.count > 1 ? '<button class="btn small ghost" id="inv-dropall">Выбросить всё</button>' : ''}
      </div>`;
    const sel = this.selected;
    d.querySelector('#inv-use')?.addEventListener('click', () => this.actions.use(sel));
    d.querySelector('#inv-split')?.addEventListener('click', () => { this.inv.split(sel); });
    d.querySelector('#inv-drop')?.addEventListener('click', () => this.actions.drop(sel, false));
    d.querySelector('#inv-dropall')?.addEventListener('click', () => this.actions.drop(sel, true));
  }
}
