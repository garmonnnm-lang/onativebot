/** Встроенные SVG-иконки интерфейса (без внешних файлов). */
const P = (d: string) => `<svg viewBox="0 0 24 24" width="1em" height="1em" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${d}</svg>`;

export const ICONS: Record<string, string> = {
  play: P('<path d="M7 4.5v15l12-7.5z" fill="currentColor"/>'),
  people: P('<circle cx="9" cy="8" r="3.2"/><path d="M3 20c.4-3.6 3-5.6 6-5.6s5.6 2 6 5.6"/><circle cx="17" cy="9" r="2.5"/><path d="M16.5 14.6c2.6.2 4.2 1.9 4.5 4.4"/>'),
  person: P('<circle cx="12" cy="8" r="3.8"/><path d="M4.5 20.5c.6-4 3.6-6.2 7.5-6.2s6.9 2.2 7.5 6.2"/>'),
  link: P('<path d="M10 14a4.5 4.5 0 0 0 6.4 0l3-3a4.5 4.5 0 0 0-6.4-6.4l-1 1"/><path d="M14 10a4.5 4.5 0 0 0-6.4 0l-3 3a4.5 4.5 0 0 0 6.4 6.4l1-1"/>'),
  gear: P('<circle cx="12" cy="12" r="3.2"/><path d="M19.4 14.5a1.6 1.6 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.6 1.6 0 0 0-1.8-.3 1.6 1.6 0 0 0-1 1.5V21a2 2 0 1 1-4 0v-.1a1.6 1.6 0 0 0-1-1.5 1.6 1.6 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.6 1.6 0 0 0 .3-1.8 1.6 1.6 0 0 0-1.5-1H3a2 2 0 1 1 0-4h.1a1.6 1.6 0 0 0 1.5-1 1.6 1.6 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.6 1.6 0 0 0 1.8.3h0a1.6 1.6 0 0 0 1-1.5V3a2 2 0 1 1 4 0v.1a1.6 1.6 0 0 0 1 1.5h0a1.6 1.6 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.6 1.6 0 0 0-.3 1.8v0a1.6 1.6 0 0 0 1.5 1H21a2 2 0 1 1 0 4h-.1a1.6 1.6 0 0 0-1.5 1z"/>'),
  pad: P('<path d="M6 8h12a3.5 3.5 0 0 1 3.4 4.3l-1 4.4a2.2 2.2 0 0 1-3.7 1l-2-2.2H9.3l-2 2.2a2.2 2.2 0 0 1-3.7-1l-1-4.4A3.5 3.5 0 0 1 6 8z"/><path d="M8 11v3M6.5 12.5h3"/><circle cx="16" cy="11.5" r=".6" fill="currentColor"/><circle cx="18" cy="13.5" r=".6" fill="currentColor"/>'),
  exit: P('<path d="M10 4H6a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h4"/><path d="M15 8l4 4-4 4M9 12h10"/>'),
  refresh: P('<path d="M20 11a8 8 0 1 0-2.3 5.7"/><path d="M20 4v7h-7"/>'),
  monitor: P('<rect x="3" y="4" width="18" height="12" rx="2"/><path d="M8 20h8M12 16v4"/>'),
  sound: P('<path d="M4 9.5v5h3.5L12 18.5v-13L7.5 9.5z" fill="currentColor"/><path d="M15.5 9a4 4 0 0 1 0 6M18 6.5a7.5 7.5 0 0 1 0 11"/>'),
  layout: P('<rect x="3.5" y="4" width="17" height="16" rx="2"/><path d="M3.5 9h17M9 9v11"/>'),
  check: P('<path d="M5 12.5l4.5 4.5L19 7.5"/>'),
};

/** Подставляет иконки в элементы с data-icon. */
export function mountIcons(root: ParentNode = document): void {
  root.querySelectorAll<HTMLElement>('[data-icon]').forEach((el) => {
    const svg = ICONS[el.dataset.icon ?? ''];
    if (!svg || el.dataset.iconDone) return;
    el.dataset.iconDone = '1';
    if (el.classList.contains('ic')) el.innerHTML = svg;
    else el.insertAdjacentHTML('afterbegin', `<span class="ic">${svg}</span>`);
  });
}
