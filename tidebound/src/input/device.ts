/**
 * Определение типа устройства и «мобильная оболочка»:
 * класс is-touch на <body>, полноэкранный режим и блокировка ориентации по первому касанию,
 * запрет масштабирования жестами (iOS).
 * Принудительно включить/выключить сенсорный режим: ?touch=1 / ?touch=0.
 */
function detect(): boolean {
  try {
    const q = new URLSearchParams(location.search).get('touch');
    if (q === '1') return true;
    if (q === '0') return false;
  } catch { /* нет location */ }
  const coarse = typeof matchMedia === 'function' && matchMedia('(pointer: coarse)').matches;
  const noHover = typeof matchMedia === 'function' && matchMedia('(hover: none)').matches;
  return coarse && noHover;
}

export const isTouchDevice: boolean = detect();

export function canFullscreen(): boolean {
  const el = document.documentElement as HTMLElement & { webkitRequestFullscreen?: () => void };
  return !!(el.requestFullscreen || el.webkitRequestFullscreen);
}

export function isFullscreen(): boolean {
  const d = document as Document & { webkitFullscreenElement?: Element };
  return !!(document.fullscreenElement || d.webkitFullscreenElement);
}

export async function toggleFullscreen(): Promise<void> {
  const el = document.documentElement as HTMLElement & { webkitRequestFullscreen?: () => void };
  const d = document as Document & { webkitExitFullscreen?: () => void };
  try {
    if (isFullscreen()) {
      if (document.exitFullscreen) await document.exitFullscreen(); else d.webkitExitFullscreen?.();
      (screen.orientation as ScreenOrientation & { unlock?: () => void })?.unlock?.();
    } else {
      if (el.requestFullscreen) await el.requestFullscreen({ navigationUI: 'hide' } as FullscreenOptions);
      else el.webkitRequestFullscreen?.();
      try { await (screen.orientation as ScreenOrientation & { lock?: (o: string) => Promise<void> }).lock?.('landscape'); } catch { /* не поддерживается */ }
    }
  } catch { /* браузер отклонил — не критично */ }
}

export function initMobileShell(): void {
  if (!isTouchDevice) return;
  document.body.classList.add('is-touch');
  // запрет pinch-zoom / двойного тапа / контекстного меню по долгому нажатию
  for (const ev of ['gesturestart', 'gesturechange', 'gestureend']) document.addEventListener(ev, (e) => e.preventDefault());
  document.addEventListener('contextmenu', (e) => e.preventDefault());
  let last = 0;
  document.addEventListener('touchend', (e) => {
    const now = Date.now();
    if (now - last < 350 && !(e.target as HTMLElement | null)?.closest?.('input, select, textarea')) e.preventDefault();
    last = now;
  }, { passive: false });
  // полноэкранный режим по первому касанию (нужен жест пользователя); если выйдут — больше не навязываем
  if (canFullscreen() && !isFullscreen()) {
    const once = (): void => { document.removeEventListener('pointerup', once); void toggleFullscreen(); };
    document.addEventListener('pointerup', once);
  }
}
