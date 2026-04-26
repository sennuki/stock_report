import yf from 'yahoo-finance2';

// yahoo-finance2 v3 では default エクスポートがクラスである場合がある
const yahooFinance = new (yf as any)();

export interface RiskMetrics {
  Symbol: string;
  Daily_Change: number;
  Earnings_Date: string | null;
  [key: string]: string | number | null;
}

export const PERIOD_CONFIGS = [
  { key: '1M', label: '1ヶ月', days: 21 },
  { key: '3M', label: '3ヶ月', days: 63 },
  { key: '6M', label: '6ヶ月', days: 126 },
  { key: 'YTD', label: '年初来', days: 'YTD' },
  { key: '1Y', label: '1年', days: 252 },
  { key: '3Y', label: '3年', days: 756 },
  { key: '5Y', label: '5年', days: 1260 },
  { key: '10Y', label: '10年', days: 2520 },
];

/**
 * 1銘柄のリスク(HV)とリターンを計算
 */
export async function calculateRiskMetrics(symbol: string): Promise<RiskMetrics | null> {
  try {
    // 過去10年分のデータを取得 (chart を使用)
    const result = await yahooFinance.chart(symbol, {
      period1: '2014-01-01',
      interval: '1d'
    });
    
    const history = result.quotes;
    
    if (!history || history.length < 5) return null;

    const results: RiskMetrics = {
      Symbol: symbol,
      Daily_Change: 0,
      Earnings_Date: null,
    };

    // 前日比
    if (history.length >= 2) {
      const last = history[history.length - 1];
      const prev = history[history.length - 2];
      if (last.close !== null && prev.close !== null) {
        results.Daily_Change = (last.close - prev.close) / prev.close;
      }
    }

    // 対数収益率の計算 (nullを除外)
    const validQuotes = history.filter((q: any) => q.close !== null) as { date: Date, close: number }[];
    const logReturns: number[] = [];
    for (let i = 1; i < validQuotes.length; i++) {
      logReturns.push(Math.log(validQuotes[i].close / validQuotes[i - 1].close));
    }

    const lastDate = new Date(validQuotes[validQuotes.length - 1].date);

    for (const p of PERIOD_CONFIGS) {
      let subHistory: any[] = [];
      let isValidPeriod = true;

      if (p.days === 'YTD') {
        const startOfYear = new Date(lastDate.getFullYear(), 0, 1);
        subHistory = validQuotes.filter(h => new Date(h.date) >= startOfYear);
        if (subHistory.length < 5) subHistory = validQuotes.slice(-21);
      } else {
        const days = p.days as number;
        subHistory = validQuotes.slice(-days);
        if (subHistory.length < days * 0.8) isValidPeriod = false;
      }

      if (subHistory.length < 5 || !isValidPeriod) {
        results[`HV_${p.key}`] = null;
        results[`Ret_${p.key}`] = null;
      } else {
        // HV (年率換算ボラティリティ)
        const startIndex = validQuotes.indexOf(subHistory[0]);
        const subReturns = logReturns.slice(Math.max(0, startIndex - 1), validQuotes.indexOf(subHistory[subHistory.length - 1]));
        
        if (subReturns.length > 0) {
          const mean = subReturns.reduce((a, b) => a + b, 0) / subReturns.length;
          const variance = subReturns.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / (subReturns.length - 1);
          results[`HV_${p.key}`] = Math.sqrt(variance) * Math.sqrt(252);
        } else {
          results[`HV_${p.key}`] = 0;
        }

        // リターン (年率換算)
        const firstPrice = subHistory[0].close;
        const lastPrice = subHistory[subHistory.length - 1].close;
        const totalRet = (lastPrice / firstPrice) - 1;
        const daysDiff = (lastDate.getTime() - new Date(subHistory[0].date).getTime()) / (1000 * 60 * 60 * 24);
        
        if (daysDiff > 5) {
          const annRet = Math.pow(1 + totalRet, 365.0 / daysDiff) - 1;
          results[`Ret_${p.key}`] = isFinite(annRet) ? annRet : 0;
        } else {
          results[`Ret_${p.key}`] = totalRet;
        }
      }
    }

    return results;
  } catch (e) {
    console.error(`Error calculating metrics for ${symbol}:`, e);
    return null;
  }
}

export { yahooFinance };
