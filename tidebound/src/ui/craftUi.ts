import { RECIPES, STATION_NAMES, recipesFor, type Recipe, type Station } from '../../shared/recipes';
import { getItem } from '../../shared/items';
import type { Game } from '../core/game';

/** Окно крафта: список рецептов слева, состав и кнопка «Создать» справа. */
export class CraftUi {
  private list = document.getElementById('craft-list') as HTMLElement;
  private detail = document.getElementById('craft-detail') as HTMLElement;
  private title = document.getElementById('craft-title') as HTMLElement;
  private station: Station = 'hand';
  private selected = '';

  constructor(private game: Game) {
    game.inventory.onChange(() => { if (game.ui.current === 'craft') this.render(); });
  }

  setStation(s: Station): void {
    this.station = s;
    if (!recipesFor(s).some((r) => r.id === this.selected)) this.selected = '';
  }

  private status(r: Recipe): string | null {
    const g = this.game;
    if (r.minLevel && g.vitals.level < r.minLevel) return `Нужен уровень ${r.minLevel}`;
    if (!g.inventory.hasAll(r.inputs)) return 'Не хватает материалов';
    return null;
  }

  render(): void {
    this.title.textContent = `Крафт — ${STATION_NAMES[this.station]}`;
    const order = ['Материалы', 'Инструменты', 'Рыбалка'];
    const recipes = [...recipesFor(this.station)].sort((a, b) => order.indexOf(a.group) - order.indexOf(b.group));
    if (!this.selected && recipes.length) this.selected = recipes[0].id;
    this.list.innerHTML = '';
    let group = '';
    for (const r of recipes) {
      if (r.group !== group) {
        group = r.group;
        const h = document.createElement('div');
        h.className = 'craft-group';
        h.textContent = group;
        this.list.appendChild(h);
      }
      const def = getItem(r.output.id);
      const ok = this.status(r) === null;
      const el = document.createElement('button');
      el.type = 'button';
      el.className = 'craft-row' + (r.id === this.selected ? ' sel' : '') + (ok ? '' : ' off');
      el.innerHTML = `<span class="ic">${def.icon}</span><span class="nm">${r.name}${r.output.count > 1 ? ` ×${r.output.count}` : ''}</span><span class="tag">${r.station === 'workbench' ? 'верстак' : 'руками'}</span>`;
      el.addEventListener('click', () => { this.selected = r.id; this.render(); });
      el.addEventListener('dblclick', () => { this.selected = r.id; this.make(); });
      this.list.appendChild(el);
    }
    this.renderDetail();
  }

  private renderDetail(): void {
    const r = RECIPES.find((x) => x.id === this.selected);
    const d = this.detail;
    if (!r) { d.innerHTML = '<p class="muted">Выберите рецепт.</p>'; return; }
    const out = getItem(r.output.id);
    const inv = this.game.inventory;
    const reason = this.status(r);
    const ing = r.inputs.map((i) => {
      const have = inv.count(i.id);
      return `<div class="craft-ing${have < i.count ? ' miss' : ''}"><span>${getItem(i.id).icon} ${getItem(i.id).name}</span><span>${have} / ${i.count}</span></div>`;
    }).join('');
    d.innerHTML = `
      <div class="inv-title"><span class="inv-big">${out.icon}</span><div><div class="inv-name">${out.name} ×${r.output.count}</div><div class="muted small">+${r.xp} XP</div></div></div>
      <p class="muted">${out.description}</p>
      <div>${ing}</div>
      ${reason ? `<p class="small" style="color:#ff8f8f">${reason}</p>` : ''}
      <div class="inv-actions"><button class="btn small" id="craft-make"${reason ? ' disabled' : ''}>Создать</button></div>`;
    d.querySelector('#craft-make')?.addEventListener('click', () => this.make());
  }

  private make(): void {
    const err = this.game.craft(this.selected);
    if (err) this.game.ui.toast(err, 'info');
    this.render();
  }
}
