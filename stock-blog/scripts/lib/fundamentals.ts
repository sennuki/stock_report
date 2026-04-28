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

function formatFinancials(statements: any[], keyField: string) {
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
export function calculateDCF(symbol: string, financialData: any) {
  try {
    const price = financialData.financialData?.currentPrice;
    const fcf = financialData.financialData?.freeCashflow;
    const growth = financialData.financialData?.revenueGrowth || 0.05;
    const wacc = 0.08; // 簡易的な割引率
    
    if (!price || !fcf) return null;

    // 10年間の予測
    let totalValue = 0;
    let currentFCF = fcf;
    for (let i = 1; i <= 10; i++) {
      currentFCF *= (1 + growth);
      totalValue += currentFCF / Math.pow(1 + wacc, i);
    }

    // 終端価値
    const terminalValue = (currentFCF * (1 + 0.02)) / (wacc - 0.02);
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
