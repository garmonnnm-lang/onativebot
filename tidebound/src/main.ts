import './styles-loader';
import * as THREE from 'three';
import { Game } from './core/game';
import { initMobileShell } from './input/device';
import { LANDMARKS } from '../shared/terrain';

initMobileShell();
const game = new Game();
// Для отладки и автотестов: window.__tidebound.game
(window as unknown as { __tidebound: { game: Game; L: typeof LANDMARKS; THREE: typeof THREE } }).__tidebound = { game, L: LANDMARKS, THREE };
void game.start();
