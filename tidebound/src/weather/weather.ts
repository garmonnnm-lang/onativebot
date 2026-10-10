export type WeatherKind = 'clear' | 'cloudy' | 'fog' | 'rain' | 'storm';

export interface WeatherParams {
  cloud: number; rain: number; wind: number; waves: number; fog: number; dark: number; lightning: boolean;
}

const TARGETS: Record<WeatherKind, WeatherParams> = {
  clear: { cloud: 0.25, rain: 0, wind: 0.2, waves: 0.35, fog: 0.0016, dark: 0, lightning: false },
  cloudy: { cloud: 0.7, rain: 0, wind: 0.4, waves: 0.55, fog: 0.0024, dark: 0.2, lightning: false },
  fog: { cloud: 0.55, rain: 0, wind: 0.1, waves: 0.25, fog: 0.011, dark: 0.15, lightning: false },
  rain: { cloud: 0.9, rain: 0.65, wind: 0.55, waves: 0.8, fog: 0.0042, dark: 0.45, lightning: false },
  storm: { cloud: 1, rain: 1, wind: 1, waves: 1.6, fog: 0.0068, dark: 0.75, lightning: true },
};

export const WEATHER_NAMES: Record<WeatherKind, string> = {
  clear: 'Ясно', cloudy: 'Облачно', fog: 'Туман', rain: 'Дождь', storm: 'Шторм',
};
export const WEATHER_ICONS: Record<WeatherKind, string> = {
  clear: '☀️', cloudy: '☁️', fog: '🌫️', rain: '🌧️', storm: '⛈️',
};

/** Погода с плавными переходами. В кооперативе целевое состояние придёт с сервера. */
export class Weather {
  kind: WeatherKind = 'clear';
  cur: WeatherParams = { ...TARGETS.clear };
  autoChange = true;
  flash = 0;
  onThunder: ((delay: number, power: number) => void) | null = null;
  onChange: ((k: WeatherKind) => void) | null = null;
  private timer = 240 + Math.random() * 180;
  private nextLightning = 6;

  set(kind: WeatherKind): void {
    if (this.kind === kind) return;
    this.kind = kind;
    this.onChange?.(kind);
  }

  update(dt: number): void {
    if (this.autoChange) {
      this.timer -= dt;
      if (this.timer <= 0) {
        this.timer = 200 + Math.random() * 240;
        this.set(this.pickNext());
      }
    }
    const t = TARGETS[this.kind];
    const k = 1 - Math.exp(-dt * 0.35);
    const c = this.cur;
    c.cloud += (t.cloud - c.cloud) * k;
    c.rain += (t.rain - c.rain) * k;
    c.wind += (t.wind - c.wind) * k;
    c.waves += (t.waves - c.waves) * k;
    c.fog += (t.fog - c.fog) * k;
    c.dark += (t.dark - c.dark) * k;
    c.lightning = t.lightning && c.rain > 0.7;

    this.flash = Math.max(0, this.flash - dt * 2.6);
    if (c.lightning) {
      this.nextLightning -= dt;
      if (this.nextLightning <= 0) {
        this.nextLightning = 4 + Math.random() * 11;
        this.flash = 1;
        const dist = 0.4 + Math.random() * 3.2;
        this.onThunder?.(dist, 1 - dist / 4);
      }
    }
  }

  private pickNext(): WeatherKind {
    const r = Math.random();
    switch (this.kind) {
      case 'clear': return r < 0.5 ? 'cloudy' : r < 0.65 ? 'fog' : 'clear';
      case 'cloudy': return r < 0.35 ? 'clear' : r < 0.75 ? 'rain' : r < 0.85 ? 'fog' : 'cloudy';
      case 'fog': return r < 0.6 ? 'clear' : 'cloudy';
      case 'rain': return r < 0.2 ? 'storm' : r < 0.6 ? 'cloudy' : 'clear';
      case 'storm': return r < 0.7 ? 'rain' : 'cloudy';
    }
  }
}
