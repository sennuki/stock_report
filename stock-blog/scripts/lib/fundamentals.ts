import { yahooFinance } from './risk-analysis.ts';

export async function getFinancialData(symbol: string) {
  try {
    const summary = await yahooFinance.quoteSummary(symbol, {
      modules: [
        'incomeStatementHistory', 'balanceSheetHistory', 'cashflowStatementHistory',
        'incomeStatementHistoryQuarterly', 'balanceSheetHistoryQuarterly', 'cashflowStatementHistoryQuarterly',
        'earnings', 'financialData', 'defaultKeyStatistics'
      ]
    });

    return {
      is: formatFinancials(summary.incomeStatementHistory?.incomeStatementHistory, 'totalRevenue'),
      bs: formatFinancials(summary.balanceSheetHistory?.balanceSheetStatements, 'totalAssets'),
      cf: formatFinancials(summary.cashflowStatementHistory?.cashflowStatements, 'operatingCashflow'),
      earnings: summary.earnings,
      financialData: summary.financialData,
      stats: summary.defaultKeyStatistics
    };
  } catch (e) {
    console.error(`Error fetching financials for ${symbol}:`, e);
    return { is: {}, bs: {}, cf: {}, earnings: {}, financialData: {}, stats: {} };
  }
}

function formatFinancials(statements: any[], _keyField: string) {
  if (!statements) return { annual: {} };
  const annual: any = {};
  statements.forEach((s: any) => {
    const date = s.endDate.toISOString().split('T')[0];
    Object.keys(s).forEach(key => {
      if (typeof s[key] === 'number') {
        if (!annual[key]) annual[key] = {};
        annual[key][date] = s[key];
      }
    });
  });
  return { annual };
}

/**
 * 簡易DCF計算 (Python版のロジックを移植)
 */
export function calculateDCF(_symbol: string, financialData: any) {
  try {
    const price = financialData.financialData?.currentPrice;
    const fcf = financialData.financialData?.freeCashflow;
    const rawGrowth = financialData.financialData?.revenueGrowth || 0.05;
    const growth = Math.min(Math.max(rawGrowth, 0.05), 0.20);
    const wacc = 0.08; // 簡易的な割引率
    const terminalGrowth = 0.04; // 簡易的なRisk Free Rate
    
    if (!price || !fcf) return null;

    // 10年間の予測
    let totalValue = 0;
    let currentFCF = fcf;
    for (let i = 1; i <= 10; i++) {
      let rate = growth;
      if (i > 5) {
        // 6年目以降はTerminal Growthに向けて線形に漸減させる
        rate = growth - (i - 5) * (growth - terminalGrowth) / 5;
      }
      currentFCF *= (1 + rate);
      totalValue += currentFCF / Math.pow(1 + wacc, i);
    }

    // 終端価値
    const terminalValue = (currentFCF * (1 + terminalGrowth)) / (wacc - terminalGrowth);
    totalValue += terminalValue / Math.pow(1 + wacc, 10);

    const shares = financialData.stats?.sharesOutstanding;
    if (!shares) return null;

    const fairPrice = totalValue / shares;
    return {
      fair_price: fairPrice,
      current_price: price,
      discount: (fairPrice / price) - 1
    };
  } catch (e) {
    return null;
  }
}
