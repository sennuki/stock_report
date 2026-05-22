/**
 * yfinance の dividendYield を小数に正規化する。
 * yfinance はバージョンによって percent 単位 (0.36 = 0.36%) で返すことがある。
 * 他の percent 指標 (ROE, margins) は小数 (0.34 = 34%) なので単位を揃える。
 * 0.1 超 = 明らかに % 単位なので ÷100 する。
 */
export function normalizeDividendYield(y) {
  if (typeof y !== 'number') return null;
  return y > 0.1 ? y / 100 : y;
}
