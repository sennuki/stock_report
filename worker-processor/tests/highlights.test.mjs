/**
 * 配当利回り正規化のリグレッションテスト。
 * yfinance は dividendYield をバージョンによってパーセント単位 (0.36 = 0.36%) で
 * 返すことがある。[symbol].astro は小数を前提に ×100 して表示するため、
 * 格納前に必ず小数化する必要がある。
 *
 * 実行: node --test worker-processor/tests/highlights.test.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { normalizeDividendYield } from '../scripts/highlights-utils.mjs';

// --- 正常系 ---

test('小数形式 (0.005 = 0.5%) はそのまま', () => {
  assert.strictEqual(normalizeDividendYield(0.005), 0.005);
});

test('小数形式 (0.0444 = 4.44%) はそのまま', () => {
  assert.strictEqual(normalizeDividendYield(0.0444), 0.0444);
});

test('0.1 ちょうどはそのまま (境界値)', () => {
  assert.strictEqual(normalizeDividendYield(0.1), 0.1);
});

test('パーセント単位 AAPL (0.36 = 0.36%) → 0.0036 に正規化', () => {
  assert.strictEqual(normalizeDividendYield(0.36), 0.0036);
});

test('パーセント単位 MSFT (0.86 = 0.86%) → 0.0086 に正規化', () => {
  assert.strictEqual(normalizeDividendYield(0.86), 0.0086);
});

test('パーセント単位 大きい値 (5.0 = 5%) → 0.05 に正規化', () => {
  assert.strictEqual(normalizeDividendYield(5.0), 0.05);
});

// --- null / 無効値 ---

test('null は null を返す', () => {
  assert.strictEqual(normalizeDividendYield(null), null);
});

test('undefined は null を返す', () => {
  assert.strictEqual(normalizeDividendYield(undefined), null);
});

test('0 は 0 を返す (無配)', () => {
  assert.strictEqual(normalizeDividendYield(0), 0);
});
