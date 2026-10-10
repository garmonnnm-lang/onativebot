import { HOTBAR_SIZE } from '../../shared/inventory';
import { RARITY_COLORS, getItem, type Rarity } from '../../shared/items';
import { DEFAULT_SETTINGS, settings, type Settings } from '../core/settings';
import { WEATHER_ICONS, WEATHER_NAMES } from '../weather/weather';
import { formatClock } from '../daynight/daynight';
import { xpForLevel } from '../survival/vitals';
import { checkServer, fetchRooms, validRoomCode } from '../network/client';
import type { Station } from '../../shared/recipes';
import { InventoryUi } from './inventoryUi';
import { CraftUi } from './craftUi';
import { cleanName, type Privacy, type RoomInfo } from '../../shared/net';
import { Minimap } from './minimap';
import { mountIcons } from './icons';
import type { Game } from '../core/game';
import { isTouchDevice } from '../input/device';

export type ToastKind = 'info' | 'good' | 'warn';
export type ScreenId =
  | 'loading' | 'main' | 'lobbies' | 'create' | 'join' | 'settings' | 'controls' | 'pause' | 'death' | 'inventory' | 'craft' | 'players';

const $ = <T extends HTMLElement = HTMLElement>(id: string): T => {
  const el = document.getElementById(id);
  if (!el) throw new Error(`Нет элемента #${id}`);
  return el as T;
};

const TOUCH_CONTROLS: [string, string][] = [
  ['Левый палец', 'Джойстик: ходьба (появляется под пальцем)'], ['Правый палец', 'Обзор — перетаскивание по экрану'],
  ['✋', 'Действие предмета: заброс, подсечка, вываживание (удерживать), удар'], ['⤒', 'Прыжок'], ['E', 'Взаимодействие (подсвечивается, когда доступно)'],
  ['»', 'Бег (вкл/выкл, сбрасывается при отпускании джойстика)'], ['▾', 'Приседание (вкл/выкл)'], ['⟲  🪱', 'Смотать леску / сменить наживку (с удочкой)'],
  ['Панель внизу', 'Нажмите на слот, чтобы взять предмет'], ['🔨 🎒 ⏸', 'Крафт, инвентарь, пауза'], ['⛶', 'Полный экран'],
];
const CONTROLS_KB: [string, string][] = [
  ['W A S D', 'Движение'], ['Мышь', 'Обзор'], ['Пробел', 'Прыжок'], ['Shift', 'Бег'], ['Ctrl / C', 'Приседание'],
  ['E', 'Взаимодействие'], ['Tab', 'Инвентарь'], ['1 – 5', 'Быстрые слоты'], ['Колесо мыши', 'Смена слота'],
  ['ЛКМ', 'Действие предмета (заброс, подсечка, вываживание — удерживать, удар)'], ['ПКМ', 'Смотать леску'],
  ['R', 'Сменить наживку (с удочкой)'], ['Esc', 'Меню паузы / назад'], ['F3', 'Отладочная панель'],
];
const CONTROLS = isTouchDevice ? TOUCH_CONTROLS : CONTROLS_KB;

/** Весь HTML-интерфейс: экраны, меню, настройки, HUD, уведомления. */
export class UI {
  current: ScreenId | null = 'loading';
  private back: ScreenId = 'main';
  private hudEl = $('hud');
  private toastLayer: HTMLElement;
  private minimap: Minimap;
  private inventoryUi!: InventoryUi;
  private craftUi!: CraftUi;
  private draft: Settings = { ...settings.value };
  private tab = 'game';
  private frame = 0;
  private lastHud = new Map<string, string>();
  private fishHintTimer = 0;
  private catchTimer = 0;
  private feedItems: { el: HTMLElement; t: number }[] = [];
  private lastZone = '';
  private hurt = 0;

  constructor(private game: Game) {
    this.toastLayer = document.createElement('div');
    this.toastLayer.className = 'toasts';
    document.body.appendChild(this.toastLayer);
    mountIcons();
    this.minimap = new Minimap($('minimap') as HTMLCanvasElement);
    this.wire();
  }

  /** Инвентарь создаётся после того, как у игры появился объект Inventory. */
  bindInventory(): void {
    this.inventoryUi = new InventoryUi(this.game.inventory, {
      use: (i) => this.game.useItem(i),
      drop: (i, all) => this.game.dropItem(i, all),
      onHotbarChange: () => this.renderHotbar(),
    });
    this.craftUi = new CraftUi(this.game);
    this.renderHotbar();
  }

  openCraft(station: Station): void {
    this.craftUi.setStation(station);
    this.show('craft');
  }

  // ───────────────────────────── Экраны ─────────────────────────────

  show(id: ScreenId | null): void {
    document.querySelectorAll<HTMLElement>('.screen').forEach((s) => {
      s.classList.toggle('active', id !== null && s.id === `screen-${id}`);
    });
    this.current = id;
    if (id === 'inventory') this.inventoryUi.reset();
    if (id === 'craft') this.craftUi.render();
    if (id === 'players') this.renderPlayers();
  }

  hud(visible: boolean): void {
    this.hudEl.classList.toggle('hidden', !visible);
  }

  setLoading(text: string, k: number): void {
    ($('loading-bar')).style.width = `${Math.round(k * 100)}%`;
    $('loading-text').textContent = text;
  }

  loadingError(msg: string): void {
    const b = $('loading-error');
    b.textContent = msg;
    b.classList.remove('hidden');
  }

  private wire(): void {
    const g = this.game;
    const click = (id: string, fn: () => void) => $(id).addEventListener('click', () => { g.audio.unlock(); g.audio.ui(); fn(); });

    // главное меню
    click('btn-continue', () => void g.continueGame());
    click('btn-play', () => this.openLobbies());
    click('btn-create', () => this.openCreate());
    click('btn-join', () => this.openJoin());
    click('btn-single', () => void g.newSingleGame());
    click('btn-settings', () => this.openSettings('main'));
    click('btn-controls', () => this.openControls('main'));
    click('btn-exit', () => {
      window.close();
      this.toast('Браузер не позволяет закрыть вкладку программно — закройте её вручную.', 'info');
    });

    // лобби
    click('lobby-back', () => this.show('main'));
    click('lobby-refresh', () => void this.refreshLobbies());
    $('lobby-search').addEventListener('input', () => { this.lobbyPage = 0; this.renderLobbyRows(); });

    // создание комнаты
    click('create-back', () => this.show('main'));
    click('room-create', () => void this.createRoom());
    click('room-start', () => g.mp.startRoom());
    click('room-leave', () => { g.mp.leave(); this.show('main'); });
    click('copy-code', () => void this.copyText(($('room-code') as HTMLInputElement).value, 'Код скопирован.'));
    click('copy-link', () => void this.copyText(($('room-link-url') as HTMLInputElement).value, 'Ссылка скопирована.'));
    $('room-max').addEventListener('change', () => { $('room-players-max').textContent = ($('room-max') as HTMLSelectElement).value; this.renderRoomPlayers(); });

    // присоединение
    click('join-back', () => this.show('main'));
    click('join-go', () => void this.tryJoin());
    $('join-code').addEventListener('input', (e) => {
      const el = e.target as HTMLInputElement;
      el.value = el.value.toUpperCase().replace(/[^A-Z0-9]/g, '');
    });
    $('join-code').addEventListener('keydown', (e) => { if ((e as KeyboardEvent).key === 'Enter') void this.tryJoin(); });

    // настройки / управление
    click('settings-back', () => this.show(this.back));
    click('controls-back', () => this.show(this.back));
    click('settings-apply', () => { settings.set(this.draft); g.applySettings(); this.toast('Настройки применены.', 'good'); });
    click('settings-reset', () => { settings.reset(); this.draft = { ...settings.value }; g.applySettings(); this.renderSettings(); this.toast('Настройки сброшены.', 'info'); });
    $('settings-tabs').addEventListener('click', (e) => {
      const t = (e.target as HTMLElement).closest<HTMLElement>('.tab');
      if (!t) return;
      this.tab = t.dataset.tab ?? 'game';
      this.renderSettings();
    });

    // пауза
    click('pause-resume', () => g.resume());
    click('pause-save', () => void g.saveNow(true));
    click('pause-settings', () => this.openSettings('pause'));
    click('pause-controls', () => this.openControls('pause'));
    click('pause-players-btn', () => { this.back = 'pause'; this.show('players'); });
    click('pause-leave', () => void g.leaveToMenu());
    click('inv-close', () => g.resume());
    click('craft-close', () => g.resume());
    click('inv-to-craft', () => g.openCraft('hand'));
    click('craft-to-inv', () => g.openInventoryScreen());
    click('players-close', () => this.show(g.state === 'paused' ? 'pause' : null));
    click('death-respawn', () => g.respawn());

    $('click-to-play').addEventListener('click', () => { g.audio.unlock(); g.input.requestLock(); });
    this.renderControls();
  }

  /** Esc: возврат по иерархии экранов. true — нажатие обработано интерфейсом. */
  handleEscape(): boolean {
    switch (this.current) {
      case 'lobbies': case 'create': case 'join': this.show('main'); return true;
      case 'settings': case 'controls': this.show(this.back); return true;
      case 'craft': this.game.resume(); return true;
      case 'players': this.show(this.game.state === 'paused' ? 'pause' : null); return true;
      default: return false;
    }
  }

  async refreshContinue(): Promise<void> {
    const has = await this.game.hasSave();
    $('btn-continue').classList.toggle('hidden', !has);
  }

  // ───────────────────────────── Лобби / комнаты ─────────────────────────────

  private lobbyRooms: RoomInfo[] = [];
  private lobbyPage = 0;
  private busyNet = false;

  private playerName(inputId: string): string {
    const v = ($(inputId) as HTMLInputElement).value.trim();
    const name = cleanName(v, '');
    if (name && name !== settings.value.playerName) settings.set({ playerName: name });
    return name || settings.value.playerName || `Игрок${Math.floor(100 + Math.random() * 900)}`;
  }

  private fillNames(): void {
    if (!settings.value.playerName) settings.set({ playerName: `Игрок${Math.floor(100 + Math.random() * 900)}` });
    ($('room-player-name') as HTMLInputElement).value = settings.value.playerName;
    ($('join-name') as HTMLInputElement).value = settings.value.playerName;
  }

  private async openLobbies(): Promise<void> {
    this.show('lobbies');
    this.lobbyPage = 0;
    await this.refreshLobbies();
  }

  private async refreshLobbies(): Promise<void> {
    const st = $('lobby-state');
    st.className = 'state-box';
    st.innerHTML = '<span class="spinner"></span> Загрузка списка лобби…';
    this.lobbyRooms = [];
    this.renderLobbyRows();
    const s = await checkServer();
    if (this.current !== 'lobbies') return;
    if (!s.ok) {
      st.className = 'state-box error';
      st.innerHTML = `<div>${s.error ?? 'Сервер недоступен.'}</div><div class="state-actions"><button class="btn small" id="lobby-retry">Повторить</button><button class="btn small primary" id="lobby-single">Одиночная игра</button></div>`;
      $('lobby-retry').addEventListener('click', () => void this.refreshLobbies());
      $('lobby-single').addEventListener('click', () => void this.game.newSingleGame());
      $('lobby-total').textContent = 'Всего лобби: 0';
      $('lobby-pager').innerHTML = '';
      return;
    }
    try {
      this.lobbyRooms = await fetchRooms();
    } catch {
      st.className = 'state-box error';
      st.textContent = 'Не удалось получить список лобби. Нажмите «Обновить».';
      return;
    }
    if (this.current !== 'lobbies') return;
    this.renderLobbyRows();
  }

  private renderLobbyRows(): void {
    const q = ($('lobby-search') as HTMLInputElement).value.trim().toLowerCase();
    const rooms = this.lobbyRooms.filter((r) => !q || r.name.toLowerCase().includes(q) || r.code.toLowerCase().includes(q));
    const per = 8;
    const pages = Math.max(1, Math.ceil(rooms.length / per));
    this.lobbyPage = Math.min(this.lobbyPage, pages - 1);
    const slice = rooms.slice(this.lobbyPage * per, this.lobbyPage * per + per);
    const body = $('lobby-rows');
    body.innerHTML = '';
    for (const r of slice) {
      const tr = document.createElement('tr');
      const full = r.players >= r.max;
      const cells = [
        r.name, r.code, `${r.players}/${r.max}`, 'Кооператив',
      ];
      for (const c of cells) { const td = document.createElement('td'); td.textContent = c; tr.appendChild(td); }
      const st = document.createElement('td');
      st.innerHTML = `<span class="pill${r.status === 'playing' ? ' live' : ''}">${full ? 'Заполнена' : r.status === 'playing' ? 'Идёт игра' : 'Ожидание'}</span>`;
      tr.appendChild(st);
      const act = document.createElement('td');
      const b = document.createElement('button');
      b.className = 'btn small primary join-btn';
      b.textContent = 'Войти';
      b.disabled = full;
      b.addEventListener('click', () => void this.joinByCode(r.code, 'lobbies'));
      act.appendChild(b);
      tr.appendChild(act);
      body.appendChild(tr);
    }
    const st = $('lobby-state');
    if (this.lobbyRooms.length === 0 && !st.classList.contains('error') && !st.querySelector('.spinner')) {
      st.className = 'state-box';
      st.textContent = 'Открытых лобби пока нет. Создайте комнату — она появится в списке.';
    } else if (this.lobbyRooms.length && !rooms.length) {
      st.className = 'state-box';
      st.textContent = 'Ничего не найдено.';
    } else if (this.lobbyRooms.length) {
      st.className = 'state-box hidden';
      st.textContent = '';
    }
    $('lobby-total').textContent = `Всего лобби: ${this.lobbyRooms.length}`;
    const pager = $('lobby-pager');
    pager.innerHTML = '';
    if (pages > 1) {
      for (let i = 0; i < pages; i++) {
        const b = document.createElement('button');
        b.className = 'btn small' + (i === this.lobbyPage ? ' primary' : ' ghost');
        b.textContent = String(i + 1);
        b.addEventListener('click', () => { this.lobbyPage = i; this.renderLobbyRows(); });
        pager.appendChild(b);
      }
    }
  }

  private async joinByCode(code: string, from: ScreenId): Promise<void> {
    if (this.busyNet) return;
    this.busyNet = true;
    const err = await this.game.mp.joinRoom(this.playerName(from === 'join' ? 'join-name' : 'room-player-name'), code);
    this.busyNet = false;
    if (!err) return;
    if (from === 'join') this.joinError(err); else this.toast(err, 'warn');
  }

  // ───── создание комнаты / лобби комнаты ─────

  private async openCreate(): Promise<void> {
    this.fillNames();
    this.show('create');
    this.roomLobbyMode('form');
    const err = $('create-error');
    err.classList.add('hidden');
    const s = await checkServer();
    if (this.current !== 'create') return;
    if (!s.ok) {
      err.textContent = s.error ?? 'Сервер недоступен.';
      err.classList.remove('hidden');
    }
  }

  /** form — настройка новой комнаты; host/guest — комната создана, ждём начала. */
  private roomLobbyMode(mode: 'form' | 'host' | 'guest'): void {
    const formOnly = mode === 'form';
    for (const id of ['room-name', 'room-mode', 'room-max', 'room-privacy', 'room-link', 'room-player-name']) {
      ($(id) as HTMLInputElement).disabled = !formOnly;
    }
    $('room-create').classList.toggle('hidden', !formOnly);
    $('room-leave').classList.toggle('hidden', formOnly);
    $('room-start').classList.toggle('hidden', mode !== 'host');
    ($('room-start') as HTMLButtonElement).disabled = mode !== 'host';
    const mp = this.game.mp;
    const code = formOnly ? '—' : mp.room?.code ?? '—';
    ($('room-code') as HTMLInputElement).value = code;
    ($('room-link-url') as HTMLInputElement).value = formOnly || !mp.room ? '—' : mp.link;
    ($('copy-code') as HTMLButtonElement).disabled = formOnly;
    ($('copy-link') as HTMLButtonElement).disabled = formOnly;
    $('create-back').classList.toggle('hidden', !formOnly);
    this.renderRoomPlayers();
  }

  private async createRoom(): Promise<void> {
    if (this.busyNet) return;
    const err = $('create-error');
    err.classList.add('hidden');
    this.busyNet = true;
    ($('room-create') as HTMLButtonElement).disabled = true;
    const msg = await this.game.mp.createRoom(
      this.playerName('room-player-name'),
      ($('room-name') as HTMLInputElement).value.trim() || 'Комната',
      Number(($('room-max') as HTMLSelectElement).value) || 4,
      ($('room-privacy') as HTMLSelectElement).value as Privacy,
    );
    this.busyNet = false;
    ($('room-create') as HTMLButtonElement).disabled = false;
    if (msg) { err.textContent = msg; err.classList.remove('hidden'); }
  }

  /** Игрок вошёл в комнату, но игра ещё не началась. */
  showRoomLobby(): void {
    this.show('create');
    this.refreshRoomLobby();
  }

  refreshRoomLobby(): void {
    const mp = this.game.mp;
    if (this.current !== 'create' || !mp.inRoom) return;
    this.roomLobbyMode(mp.isHost ? 'host' : 'guest');
    const e = $('create-error');
    e.classList.toggle('hidden', mp.isHost);
    if (!mp.isHost) e.textContent = 'Вы вошли в комнату. Ждём, когда хозяин начнёт игру.';
    if (mp.room) {
      ($('room-name') as HTMLInputElement).value = mp.room.name;
      ($('room-max') as HTMLSelectElement).value = String(mp.room.max);
      ($('room-privacy') as HTMLSelectElement).value = mp.room.privacy;
    }
  }

  private renderRoomPlayers(): void {
    const mp = this.game.mp;
    const max = mp.room?.max ?? (Number(($('room-max') as HTMLSelectElement).value) || 4);
    $('room-players-max').textContent = String(max);
    const list = mp.inRoom ? mp.players : [];
    $('room-players-count').textContent = String(mp.inRoom ? list.length : 1);
    const items: string[] = [];
    if (!mp.inRoom) {
      items.push(`<li><span class="crown">♛</span><span class="avatar me">Я</span> Вы <span class="muted">(хозяин)</span></li>`);
      for (let i = 1; i < Math.min(max, 4); i++) items.push('<li class="empty"><span class="avatar"></span> Пусто</li>');
    } else {
      for (const p of list) {
        const mine = p.id === mp.myId;
        const col = '#' + p.color.toString(16).padStart(6, '0');
        items.push(`<li>${p.host ? '<span class="crown">♛</span>' : ''}<span class="avatar${mine ? ' me' : ''}" style="background:${col}">${escapeHtml(p.name.charAt(0).toUpperCase())}</span> ${escapeHtml(p.name)}${mine ? ' <span class="muted">(вы)</span>' : ''}${p.host ? ' <span class="muted">(хозяин)</span>' : ''}${p.online ? '' : ' <span class="muted">(нет связи)</span>'}</li>`);
      }
      for (let i = list.length; i < Math.min(max, 6); i++) items.push('<li class="empty"><span class="avatar"></span> Пусто</li>');
    }
    $('room-players').innerHTML = items.join('');
  }

  private openJoin(code = ''): void {
    this.fillNames();
    this.show('join');
    ($('join-code') as HTMLInputElement).value = code;
    $('join-error').classList.add('hidden');
    ($('join-code') as HTMLInputElement).focus();
  }

  /** Вызывается при загрузке страницы со ссылкой вида ?room=K7F3. */
  openJoinFromUrl(code: string): void {
    this.openJoin(validRoomCode(code) ? code : '');
    if (!validRoomCode(code)) this.joinError('Ссылка содержит некорректный код комнаты.');
    else this.joinError('Введите имя и нажмите «Присоединиться».', true);
  }

  private joinError(msg: string, info = false): void {
    const e = $('join-error');
    e.textContent = msg;
    e.classList.toggle('info', info);
    e.classList.remove('hidden');
  }

  private async tryJoin(): Promise<void> {
    const code = ($('join-code') as HTMLInputElement).value.trim().toUpperCase();
    if (!validRoomCode(code)) { this.joinError('Введите код из 4–8 латинских букв и цифр.'); return; }
    this.joinError('Подключаемся…', true);
    await this.joinByCode(code, 'join');
  }

  netBanner(text: string | null): void {
    const b = $('net-banner');
    b.textContent = text ?? '';
    b.classList.toggle('hidden', !text);
  }

  refreshPlayers(): void {
    if (this.current === 'players') this.renderPlayers();
    this.renderRoomPlayers();
  }

  // ───────────────────────────── Настройки ─────────────────────────────

  private openSettings(from: ScreenId): void {
    this.back = from;
    this.draft = { ...settings.value };
    this.tab = 'game';
    this.show('settings');
    this.renderSettings();
  }

  private openControls(from: ScreenId): void {
    this.back = from;
    this.show('controls');
  }

  private renderControls(): void {
    $('controls-table').innerHTML = CONTROLS.map(([k, v]) => `<tr><td><span class="kbd">${k}</span></td><td>${v}</td></tr>`).join('');
  }

  private renderSettings(): void {
    document.querySelectorAll<HTMLElement>('#settings-tabs .tab').forEach((t) => t.classList.toggle('active', t.dataset.tab === this.tab));
    const root = $('settings-content');
    root.innerHTML = '';
    const d = this.draft;
    const section = (title: string) => {
      const h = document.createElement('h3');
      h.textContent = title;
      root.appendChild(h);
    };
    const row = (label: string, ctl: HTMLElement) => {
      const r = document.createElement('div');
      r.className = 'srow';
      const l = document.createElement('span');
      l.textContent = label;
      r.append(l, ctl);
      root.appendChild(r);
    };
    const select = <K extends keyof Settings>(key: K, opts: [string, string][]) => {
      const s = document.createElement('select');
      s.className = 'input';
      for (const [v, n] of opts) {
        const o = document.createElement('option');
        o.value = v; o.textContent = n;
        if (String(d[key]) === v) o.selected = true;
        s.appendChild(o);
      }
      s.addEventListener('change', () => {
        const cur = d[key];
        (d as unknown as Record<string, unknown>)[key as string] = typeof cur === 'number' ? Number(s.value) : s.value;
      });
      return s;
    };
    const slider = (key: keyof Settings, min: number, max: number, step: number, fmt: (v: number) => string) => {
      const wrap = document.createElement('div');
      wrap.className = 'slider-wrap';
      const i = document.createElement('input');
      i.type = 'range';
      i.min = String(min); i.max = String(max); i.step = String(step);
      i.value = String(d[key]);
      const val = document.createElement('span');
      val.className = 'slider-val';
      val.textContent = fmt(Number(i.value));
      i.addEventListener('input', () => { (d as unknown as Record<string, number>)[key as string] = Number(i.value); val.textContent = fmt(Number(i.value)); });
      wrap.append(i, val);
      return wrap;
    };
    const check = (key: keyof Settings, label: string) => {
      const l = document.createElement('label');
      l.className = 'check srow-check';
      const c = document.createElement('input');
      c.type = 'checkbox';
      c.checked = Boolean(d[key]);
      c.addEventListener('change', () => { (d as unknown as Record<string, boolean>)[key as string] = c.checked; });
      l.append(c, document.createTextNode(' ' + label));
      root.appendChild(l);
    };
    const pct = (v: number) => `${Math.round(v * 100)}%`;
    const qualityOpts: [string, string][] = [['low', 'Низкое'], ['medium', 'Среднее'], ['high', 'Высокое']];

    if (this.tab === 'game') {
      section('Общие');
      row('Язык', select('language', [['ru', 'Русский']]));
      row(isTouchDevice ? 'Чувствительность обзора' : 'Чувствительность мыши', slider('mouseSensitivity', 1, 10, 0.5, (v) => v.toFixed(1)));
      check('cameraBob', 'Покачивание камеры');
      check('wheelSwitch', 'Смена оружия колесом мыши');
      check('showHints', 'Показывать подсказки');
      section('Игровой процесс');
      row('Сложность', select('difficulty', [['easy', 'Лёгкая'], ['normal', 'Обычная'], ['hard', 'Сложная']]));
      row('Автосохранение (одиночная игра)', select('autosaveMinutes', [['0', 'Выкл.'], ['2', 'Каждые 2 минуты'], ['5', 'Каждые 5 минут'], ['10', 'Каждые 10 минут']]));
      row('Качество графики', select('quality', qualityOpts));
    } else if (this.tab === 'graphics') {
      section('Графика');
      row('Качество', select('quality', qualityOpts));
      row('Поле зрения', slider('fov', 60, 100, 1, (v) => `${v}°`));
      check('shadows', 'Динамические тени');
      check('showFps', 'Показывать FPS');
      root.insertAdjacentHTML('beforeend', '<p class="muted small">Качество влияет на разрешение, тени, густоту растительности и дождь. Лучше применять смену качества в меню (часть параметров мира перестраивается при новой загрузке).</p>');
    } else if (this.tab === 'sound') {
      section('Громкость');
      row('Общая', slider('masterVolume', 0, 1, 0.05, pct));
      row('Эффекты', slider('effectsVolume', 0, 1, 0.05, pct));
      row('Окружение', slider('ambientVolume', 0, 1, 0.05, pct));
      check('muted', 'Отключить звук');
    } else if (this.tab === 'controls') {
      section('Управление');
      root.insertAdjacentHTML('beforeend', `<table class="keys-table">${CONTROLS.map(([k, v]) => `<tr><td><span class="kbd">${k}</span></td><td>${v}</td></tr>`).join('')}</table><p class="muted small">Переназначение клавиш пока не реализовано.</p>`);
    } else {
      section('Интерфейс');
      row('Масштаб интерфейса', slider('hudScale', 0.8, 1.4, 0.05, pct));
      check('showMinimap', 'Показывать мини-карту');
      check('showHints', 'Показывать подсказки управления');
    }
    void DEFAULT_SETTINGS;
  }

  /** Применяет масштаб интерфейса и видимость мини-карты. */
  applyHudSettings(): void {
    const s = settings.value;
    document.documentElement.style.setProperty('--hud-scale', String(s.hudScale));
    $('minimap-wrap').classList.toggle('hidden', !s.showMinimap);
    $('hud-fps').classList.toggle('hidden', !s.showFps);
  }

  // ───────────────────────────── Уведомления ─────────────────────────────

  toast(msg: string, kind: ToastKind = 'info'): void {
    const el = document.createElement('div');
    el.className = `toast ${kind}`;
    el.textContent = msg;
    this.toastLayer.appendChild(el);
    while (this.toastLayer.children.length > 4) this.toastLayer.firstElementChild?.remove();
    setTimeout(() => el.classList.add('out'), 3800);
    setTimeout(() => el.remove(), 4400);
  }

  feed(msg: string): void {
    const feed = $('feed');
    const el = document.createElement('div');
    el.className = 'feed-item';
    el.textContent = msg;
    feed.appendChild(el);
    this.feedItems.push({ el, t: 5 });
    while (this.feedItems.length > 6) this.feedItems.shift()?.el.remove();
  }

  setPrompt(text: string | null): void {
    const p = $('prompt');
    if (text === null) { p.classList.add('hidden'); return; }
    if (this.lastHud.get('prompt') !== text) { $('prompt-text').textContent = text; this.lastHud.set('prompt', text); }
    p.classList.remove('hidden');
  }

  fishHint(text: string | null, urgent = false): void {
    const h = $('fish-hint');
    if (text === null) { h.classList.add('hidden'); return; }
    h.textContent = text;
    h.classList.toggle('urgent', urgent);
    h.classList.remove('hidden');
  }

  reel(info: { progress: number; tension: number; pull: boolean } | null): void {
    const box = $('reel-box');
    if (!info) { box.classList.add('hidden'); return; }
    box.classList.remove('hidden');
    $('reel-progress').style.width = `${Math.max(0, Math.min(100, info.progress))}%`;
    const t = $('reel-tension');
    t.style.width = `${Math.max(0, Math.min(100, info.tension))}%`;
    t.classList.toggle('hot', info.tension > 70);
    const p = $('reel-pull');
    p.textContent = info.pull ? 'Рыба тянет! Отпустите ЛКМ' : 'Удерживайте ЛКМ — выбирайте леску';
    p.classList.toggle('urgent', info.pull);
  }

  catchCard(c: { icon: string; title: string; sub: string; rarity: Rarity; lost: boolean }): void {
    const el = $('catch-card');
    el.style.setProperty('--rar', RARITY_COLORS[c.rarity]);
    el.innerHTML = `<div class="catch-ic">${c.icon}</div><div><div class="catch-title">${c.title}</div><div class="muted small">${c.sub}</div>${c.lost ? '<div class="small warn-t">Инвентарь полон</div>' : ''}</div>`;
    el.classList.remove('hidden');
    this.catchTimer = 3.6;
  }

  damageFlash(): void { this.hurt = 1; }

  fade(on: boolean): void { $('fade').classList.toggle('on', on); }

  // ───────────────────────────── HUD ─────────────────────────────

  private set(id: string, text: string): void {
    if (this.lastHud.get(id) === text) return;
    this.lastHud.set(id, text);
    $(id).textContent = text;
  }

  renderHotbar(): void {
    if (!this.inventoryUi) return;
    const inv = this.game.inventory;
    const hb = $('hotbar');
    hb.innerHTML = '';
    for (let i = 0; i < HOTBAR_SIZE; i++) {
      const s = inv.slots[i];
      const el = document.createElement('div');
      el.className = 'hslot' + (i === this.game.hotbarIndex ? ' active' : '');
      if (s) {
        const def = getItem(s.id);
        el.style.setProperty('--rar', RARITY_COLORS[def.rarity]);
        el.innerHTML = `<span class="slot-ic">${def.icon}</span>${s.count > 1 ? `<span class="slot-n">${s.count}</span>` : ''}`;
        el.title = def.name;
      }
      el.insertAdjacentHTML('beforeend', `<span class="slot-k">${i + 1}</span>`);
      if (isTouchDevice) el.addEventListener('pointerdown', (e) => { e.preventDefault(); e.stopPropagation(); this.game.input.touchSlot(i); });
      hb.appendChild(el);
    }
  }

  update(dt: number, fps: number): void {
    const g = this.game;
    this.frame++;
    const t = g.daynight.get();
    const icon = t.phase === 'day' ? '☀' : t.phase === 'dusk' ? '🌅' : '🌙';
    this.set('hud-time', `${icon} День ${t.day} · ${formatClock(t.clock)}`);
    this.set('hud-weather', `${WEATHER_ICONS[g.weather.kind]} ${WEATHER_NAMES[g.weather.kind]}`);
    const mp = g.mp;
    if (mp.active && mp.room) {
      const online = mp.players.filter((p) => p.online).length;
      this.set('hud-room', `${mp.room.name} · ${mp.room.code}`);
      this.set('hud-players', `👥 ${online}/${mp.room.max}`);
      this.set('pause-players', `${online}/${mp.room.max}`);
    } else {
      this.set('hud-room', 'Одиночная игра');
      this.set('hud-players', '👥 1/1');
      this.set('pause-players', '1/1');
    }
    if (settings.value.showFps) this.set('hud-fps', `${Math.round(fps)} FPS`);

    const v = g.vitals;
    const w = (id: string, val: number) => {
      const key = `w-${id}`;
      const s = `${Math.round(val)}`;
      if (this.lastHud.get(key) === s) return;
      this.lastHud.set(key, s);
      $(id).style.width = `${Math.max(0, Math.min(100, val))}%`;
    };
    w('bar-hp', v.health); w('bar-food', v.hunger); w('bar-water', v.thirst); w('bar-stam', v.stamina);
    const need = xpForLevel(v.level);
    w('bar-xp', (v.xp / need) * 100);
    this.set('lvl-text', `Ур. ${v.level}`);
    this.set('xp-text', `${v.xp} / ${need}`);
    $('bar-food').parentElement?.classList.toggle('low', v.hunger < 20);
    $('bar-water').parentElement?.classList.toggle('low', v.thirst < 20);
    $('bar-hp').parentElement?.classList.toggle('low', v.health < 30);

    const zone = g.world.zoneAt(g.player.pos.x, g.player.pos.z);
    if (zone !== this.lastZone) { this.lastZone = zone; this.feed(`📍 ${zone}`); }

    if (this.frame % 2 === 0 && settings.value.showMinimap) this.minimap.draw(g.player.pos.x, g.player.pos.z, g.input.yaw);

    this.hurt = Math.max(0, this.hurt - dt * 1.6);
    const low = Math.max(0, (30 - v.health) / 30) * 0.5;
    $('damage-vignette').style.opacity = String(Math.min(1, this.hurt * 0.8 + low));

    if (this.catchTimer > 0) {
      this.catchTimer -= dt;
      if (this.catchTimer <= 0) $('catch-card').classList.add('hidden');
    }
    for (let i = this.feedItems.length - 1; i >= 0; i--) {
      const it = this.feedItems[i];
      it.t -= dt;
      if (it.t < 1) it.el.style.opacity = String(Math.max(0, it.t));
      if (it.t <= 0) { it.el.remove(); this.feedItems.splice(i, 1); }
    }
  }

  /** Подсказка управления при старте (если включены подсказки). */
  showStartHints(): void {
    if (!settings.value.showHints) return;
    this.feed('WASD — ходьба, Shift — бег, Пробел — прыжок');
    this.feed(isTouchDevice ? 'Слева — джойстик, справа — обзор. ✋ — действие, E — взаимодействие' : '1–5 — предметы, ЛКМ — действие, Tab — инвентарь, G — крафт');
    this.feed('Возьмите удочку (1), встаньте на причал и забросьте леску в воду');
  }

  private renderPlayers(): void {
    const mp = this.game.mp;
    if (!mp.active) {
      $('players-list').innerHTML = `<li><span class="crown">♛</span><span class="avatar me">Я</span><div><div>Вы</div><div class="muted small">Одиночная игра</div></div><span class="dot"></span></li>`;
      $('players-note').textContent = 'Чтобы играть вместе, создайте комнату в главном меню.';
      return;
    }
    $('players-list').innerHTML = mp.players.map((p) => {
      const col = '#' + p.color.toString(16).padStart(6, '0');
      const mine = p.id === mp.myId;
      return `<li>${p.host ? '<span class="crown">♛</span>' : ''}<span class="avatar${mine ? ' me' : ''}" style="background:${col}">${escapeHtml(p.name.charAt(0).toUpperCase())}</span><div><div>${escapeHtml(p.name)}${mine ? ' (вы)' : ''}</div><div class="muted small">${p.host ? 'Хозяин · ' : ''}${p.online ? 'в игре' : 'нет связи'}</div></div><span class="dot${p.online ? '' : ' off'}"></span></li>`;
    }).join('');
    $('players-note').textContent = `Комната ${mp.room?.code ?? ''} · пинг ${mp.net.ping || '—'} мс · ссылка: ${mp.link}`;
  }

  private async copyText(text: string, ok: string): Promise<void> {
    try { await navigator.clipboard.writeText(text); this.toast(ok, 'good'); } catch {
      const el = document.createElement('textarea');
      el.value = text; document.body.appendChild(el); el.select();
      try { document.execCommand('copy'); this.toast(ok, 'good'); } catch { this.toast('Не удалось скопировать. Скопируйте вручную.', 'warn'); }
      el.remove();
    }
  }

  debug(text: string | null): void {
    const d = $('debug');
    if (text === null) { d.classList.add('hidden'); return; }
    d.textContent = text;
    d.classList.remove('hidden');
  }

  clickToPlay(on: boolean): void { $('click-to-play').classList.toggle('hidden', !on); }
}

function escapeHtml(t: string): string {
  return t.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c] as string));
}
