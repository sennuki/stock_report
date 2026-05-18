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
    // 過去11年分のデータを取得
    const result = await yahooFinance.chart(symbol, {
      period1: new Date(Date.now() - 4000 * 24 * 60 * 60 * 1000), 
      interval: '1d'
    });
    
    const history = result.quotes;
    
    if (!history || history.length < 5) return null;

    const results: RiskMetrics = {
      Symbol: symbol,
      Daily_Change: 0,
      Earnings_Date: null,
    };

    const validQuotes = history.filter((q: any) => q.close !== null) as { date: Date, close: number }[];
    if (validQuotes.length < 5) return null;

    const logReturns: number[] = [];
    for (let i = 1; i < validQuotes.length; i++) {
      logReturns.push(Math.log(validQuotes[i].close / validQuotes[i - 1].close));
    }

    const lastDate = new Date(validQuotes[validQuotes.length - 1].date);

    for (const p of PERIOD_CONFIGS) {
      let subHistory: any[] = [];

      if (p.days === 'YTD') {
        const startOfYear = new Date(lastDate.getFullYear(), 0, 1);
        subHistory = validQuotes.filter(h => new Date(h.date) >= startOfYear);
      } else {
        const days = p.days as number;
        subHistory = validQuotes.slice(-days);
      }

      // 判定条件の緩和: 半分以上のデータがあれば計算を試みる
      if (subHistory.length < 5) {
        results[`HV_${p.key}`] = null;
        results[`Ret_${p.key}`] = null;
      } else {
        // HV
        const startIndex = validQuotes.indexOf(subHistory[0]);
        const subReturns = logReturns.slice(Math.max(0, startIndex - 1), validQuotes.indexOf(subHistory[subHistory.length - 1]));
        
        if (subReturns.length > 0) {
          const mean = subReturns.reduce((a, b) => a + b, 0) / subReturns.length;
          const variance = subReturns.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / (subReturns.length - 1);
          results[`HV_${p.key}`] = Math.sqrt(variance) * Math.sqrt(252);
        } else {
          results[`HV_${p.key}`] = 0;
        }

        // リターン
        const firstPrice = subHistory[0].close;
        const lastPrice = subHistory[subHistory.length - 1].close;
        const totalRet = (lastPrice / firstPrice) - 1;
        
        // 期間日数を計算
        const daysDiff = (lastDate.getTime() - new Date(subHistory[0].date).getTime()) / (1000 * 60 * 60 * 24);
        
        // 1年（360日）以上の期間のみ年率換算
        if (daysDiff >= 360) {
          const annRet = Math.pow(1 + totalRet, 365.0 / daysDiff) - 1;
          results[`Ret_${p.key}`] = isFinite(annRet) ? annRet : totalRet;
        } else {
          // 1年未満（1M, 3M, 6M, YTD）は単純な累積リターンを表示
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
