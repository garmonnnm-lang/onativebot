import type * as THREE from 'three';
import type { Game } from '../core/game';

export interface CircleCollider { kind: 'circle'; x: number; z: number; r: number; enabled: boolean; top: number }

export interface BoxCollider {
  kind: 'box'; cx: number; cz: number; hx: number; hz: number; cos: number; sin: number;
  enabled: boolean; top: number; bound: number;
}

export type Collider = CircleCollider | BoxCollider;

/** Горизонтальная проходимая плита с известной высотой (пирс, пол). */
export interface Platform { cx: number; cz: number; hx: number; hz: number; cos: number; sin: number; top: number }

export interface Interactable {
  id: string;
  position: THREE.Vector3;
  radius: number;
  /** null — сейчас недоступно. */
  label: () => string | null;
  onUse: (game: Game) => void;
}

export interface LightSource {
  position: THREE.Vector3;
  color: number;
  intensity: number;
  distance: number;
  nightOnly: boolean;
  flicker?: boolean;
}
