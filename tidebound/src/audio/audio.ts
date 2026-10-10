import { settings } from '../core/settings';

type Surface = 'sand' | 'grass' | 'wood' | 'rock';

/**
 * Процедурный звук на Web Audio (готовых аудиофайлов в проекте нет, поэтому ошибок загрузки не бывает).
 * AudioContext создаётся только после жеста пользователя.
 */
export class AudioSystem {
  private ctx: AudioContext | null = null;
  private master!: GainNode;
  private sfx!: GainNode;
  private amb!: GainNode;
  private noiseBuf!: AudioBuffer;
  private waves!: { gain: GainNode; filter: BiquadFilterNode };
  private wind!: { gain: GainNode; filter: BiquadFilterNode };
  private rain!: { gain: GainNode };
  private birds = 0;

  /** Вызывать из обработчика клика/клавиши. */
  unlock(): void {
    try {
      if (!this.ctx) this.init();
      if (this.ctx && this.ctx.state === 'suspended') void this.ctx.resume();
    } catch (e) {
      console.warn('[audio] недоступно', e);
    }
  }

  private init(): void {
    const Ctor = window.AudioContext ?? (window as unknown as { webkitAudioContext?: typeof AudioContext }).webkitAudioContext;
    if (!Ctor) return;
    const ctx = new Ctor();
    this.ctx = ctx;
    this.master = ctx.createGain();
    this.sfx = ctx.createGain();
    this.amb = ctx.createGain();
    this.sfx.connect(this.master);
    this.amb.connect(this.master);
    this.master.connect(ctx.destination);

    const len = ctx.sampleRate * 2;
    this.noiseBuf = ctx.createBuffer(1, len, ctx.sampleRate);
    const d = this.noiseBuf.getChannelData(0);
    let b0 = 0;
    for (let i = 0; i < len; i++) {
      const w = Math.random() * 2 - 1;
      b0 = 0.98 * b0 + 0.02 * w; // розоватый шум
      d[i] = (w * 0.5 + b0 * 6) * 0.5;
    }
    this.waves = this.loopNoise(500, 0.0, 'lowpass');
    this.wind = this.loopNoise(700, 0.0, 'bandpass');
    this.rain = { gain: this.loopNoise(4500, 0.0, 'highpass').gain };
    this.applyVolumes();
  }

  private loopNoise(freq: number, gain: number, type: BiquadFilterType): { gain: GainNode; filter: BiquadFilterNode } {
    const ctx = this.ctx as AudioContext;
    const src = ctx.createBufferSource();
    src.buffer = this.noiseBuf;
    src.loop = true;
    src.playbackRate.value = 0.8 + Math.random() * 0.4;
    const filter = ctx.createBiquadFilter();
    filter.type = type;
    filter.frequency.value = freq;
    const g = ctx.createGain();
    g.gain.value = gain;
    src.connect(filter).connect(g).connect(this.amb);
    src.start();
    return { gain: g, filter };
  }

  applyVolumes(): void {
    if (!this.ctx) return;
    const s = settings.value;
    const t = this.ctx.currentTime;
    this.master.gain.setTargetAtTime(s.muted ? 0 : s.masterVolume, t, 0.05);
    this.sfx.gain.setTargetAtTime(s.effectsVolume, t, 0.05);
    this.amb.gain.setTargetAtTime(s.ambientVolume, t, 0.05);
  }

  /** Окружение: громкость волн, ветра, дождя; ночью тише птицы. */
  updateEnvironment(dt: number, o: { waves: number; wind: number; rain: number; shore: number; daylight: number }): void {
    if (!this.ctx) return;
    const t = this.ctx.currentTime;
    this.waves.gain.gain.setTargetAtTime(0.05 + 0.3 * o.shore * (0.4 + o.waves * 0.4), t, 0.5);
    this.waves.filter.frequency.setTargetAtTime(380 + o.waves * 500, t, 0.5);
    this.wind.gain.gain.setTargetAtTime(0.02 + o.wind * 0.22, t, 0.6);
    this.wind.filter.frequency.setTargetAtTime(500 + o.wind * 900, t, 0.6);
    this.rain.gain.gain.setTargetAtTime(o.rain * 0.16, t, 0.5);
    this.birds -= dt;
    if (this.birds <= 0) {
      this.birds = 4 + Math.random() * 8;
      if (o.daylight > 0.5 && o.rain < 0.2) this.bird();
    }
  }

  private tone(freq: number, dur: number, type: OscillatorType, vol: number, slideTo?: number, delay = 0): void {
    if (!this.ctx) return;
    const ctx = this.ctx;
    const t0 = ctx.currentTime + delay;
    const o = ctx.createOscillator();
    o.type = type;
    o.frequency.setValueAtTime(freq, t0);
    if (slideTo) o.frequency.exponentialRampToValueAtTime(slideTo, t0 + dur);
    const g = ctx.createGain();
    g.gain.setValueAtTime(0.0001, t0);
    g.gain.exponentialRampToValueAtTime(vol, t0 + 0.01);
    g.gain.exponentialRampToValueAtTime(0.0001, t0 + dur);
    o.connect(g).connect(this.sfx);
    o.start(t0);
    o.stop(t0 + dur + 0.05);
  }

  private burst(freq: number, q: number, dur: number, vol: number, type: BiquadFilterType = 'bandpass', delay = 0): void {
    if (!this.ctx) return;
    const ctx = this.ctx;
    const t0 = ctx.currentTime + delay;
    const src = ctx.createBufferSource();
    src.buffer = this.noiseBuf;
    src.playbackRate.value = 0.7 + Math.random() * 0.6;
    const f = ctx.createBiquadFilter();
    f.type = type;
    f.frequency.value = freq;
    f.Q.value = q;
    const g = ctx.createGain();
    g.gain.setValueAtTime(0.0001, t0);
    g.gain.exponentialRampToValueAtTime(vol, t0 + 0.01);
    g.gain.exponentialRampToValueAtTime(0.0001, t0 + dur);
    src.connect(f).connect(g).connect(this.sfx);
    src.start(t0, Math.random());
    src.stop(t0 + dur + 0.05);
  }

  step(surface: Surface, sprint: boolean): void {
    const v = sprint ? 0.5 : 0.32;
    switch (surface) {
      case 'wood': this.burst(260, 2, 0.1, v * 1.1, 'bandpass'); this.tone(110, 0.08, 'triangle', v * 0.5); break;
      case 'sand': this.burst(1500, 0.7, 0.16, v * 0.7, 'lowpass'); break;
      case 'rock': this.burst(900, 3, 0.07, v * 0.9); break;
      default: this.burst(2200, 0.6, 0.12, v * 0.55, 'bandpass');
    }
  }
  land(): void { this.burst(200, 1, 0.14, 0.5, 'lowpass'); }
  door(open: boolean): void { this.tone(open ? 140 : 110, 0.22, 'sawtooth', 0.09, open ? 90 : 70); this.burst(300, 2, 0.12, 0.25); }
  ui(): void { this.tone(660, 0.06, 'sine', 0.08); }
  pickup(): void { this.tone(520, 0.08, 'sine', 0.15); this.tone(780, 0.12, 'sine', 0.15, undefined, 0.07); }
  cast(): void { this.burst(1800, 0.8, 0.35, 0.35, 'highpass'); }
  splash(): void { this.burst(1200, 0.5, 0.4, 0.5, 'lowpass'); this.burst(3000, 0.5, 0.2, 0.15, 'highpass'); }
  bite(): void { this.tone(880, 0.12, 'square', 0.18); this.tone(1175, 0.18, 'square', 0.18, undefined, 0.13); }
  reel(): void { this.burst(2500, 4, 0.05, 0.18); }
  snap(): void { this.tone(1500, 0.12, 'sawtooth', 0.2, 300); }
  catchFish(): void { [523, 659, 784, 1047].forEach((f, i) => this.tone(f, 0.18, 'triangle', 0.18, undefined, i * 0.08)); }
  chop(): void { this.burst(500, 2, 0.1, 0.8); this.tone(90, 0.12, 'square', 0.18, 60); }
  hit(): void { this.burst(1200, 2, 0.06, 0.7); this.tone(160, 0.1, 'triangle', 0.2, 100); }
  eat(): void { this.burst(1500, 3, 0.08, 0.35); this.burst(1200, 3, 0.08, 0.3, 'bandpass', 0.14); }
  drink(): void { this.burst(900, 2, 0.15, 0.3, 'lowpass'); this.burst(900, 2, 0.15, 0.3, 'lowpass', 0.2); }
  warn(): void { this.tone(440, 0.2, 'sawtooth', 0.12); this.tone(330, 0.3, 'sawtooth', 0.12, undefined, 0.22); }
  levelUp(): void { [392, 523, 659, 784].forEach((f, i) => this.tone(f, 0.25, 'triangle', 0.2, undefined, i * 0.1)); }
  hurt(): void { this.tone(140, 0.2, 'sawtooth', 0.25, 70); }
  thunder(delay: number, power: number): void {
    this.burst(120, 0.7, 2.6, 0.9 * Math.max(0.2, power), 'lowpass', delay);
    this.burst(70, 0.7, 3.2, 0.7 * Math.max(0.2, power), 'lowpass', delay + 0.2);
  }
  private bird(): void {
    const f = 2200 + Math.random() * 1500;
    for (let i = 0; i < 3; i++) this.tone(f + i * 120, 0.09, 'sine', 0.05, f * 1.25, i * 0.12);
  }
}
