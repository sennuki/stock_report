import { yahooFinance } from './risk-analysis.ts';

/**
 * 財務データを取得・整形 (fundamentalsTimeSeries を使用)
 */
export async function getFinancialData(symbol: string) {
  try {
    // 2024年以降、quoteSummary の財務モジュールが機能しなくなったため fundamentalsTimeSeries を使用
    const result = await yahooFinance.fundamentalsTimeSeries(symbol, {
      period1: '2020-01-01', // 過去5年分
      type: 'annual',
      module: [
        'all' // 'all' ですべての指標を取得
      ]
    });

    // 指標名と日付のマッピング
    // fundamentalsTimeSeries の戻り値は [{ dataId: '...', timestamp: Date, reportedValue: { raw: 123 } }, ...] 形式
    const dataMap: Record<string, Record<string, number>> = {};

    for (const item of result) {
      const id = item.dataId;
      const date = item.timestamp.toISOString().split('T')[0];
      const val = item.reportedValue.raw;

      if (!dataMap[id]) dataMap[id] = {};
      dataMap[id][date] = val;
    }

    // 従来のエイリアス名に合わせて構造を構築
    // IS (Income Statement)
    const isMap = {
      'Total Revenue': dataMap['TotalRevenue'] || {},
      'Gross Profit': dataMap['GrossProfit'] || {},
      'Operating Income': dataMap['OperatingIncome'] || {},
      'Net Income': dataMap['NetIncome'] || {},
      'Basic EPS': dataMap['BasicEPS'] || {}
    };

    // BS (Balance Sheet)
    const bsMap = {
      'Total Assets': dataMap['TotalAssets'] || {},
      'Total Equity Gross Minority Interest': dataMap['TotalEquity'] || {},
      'Stockholders Equity': dataMap['StockholdersEquity'] || {},
      'Total Liabilities Net Minority Interest': dataMap['TotalLiabilities'] || {},
      'Current Assets': dataMap['CurrentAssets'] || {},
      'Total Non Current Assets': dataMap['TotalNonCurrentAssets'] || {},
      'Current Liabilities': dataMap['CurrentLiabilities'] || {},
      'Total Non Current Liabilities Net Minority Interest': dataMap['TotalNonCurrentLiabilities'] || {},
      'Long Term Debt And Capital Lease Obligation': dataMap['LongTermDebt'] || {}
    };

    // CF (Cash Flow)
    const cfMap = {
      'Operating Cash Flow': dataMap['OperatingCashFlow'] || {},
      'Investing Cash Flow': dataMap['InvestingCashFlow'] || {},
      'Financing Cash Flow': dataMap['FinancingCashFlow'] || {},
      'Free Cash Flow': dataMap['FreeCashFlow'] || {}
    };

    return {
      bs: { annual: bsMap, quarterly: {} }, // Quarterly は必要に応じて追加
      is: { annual: isMap, quarterly: {} },
      cf: { annual: cfMap, quarterly: {} }
    };
  } catch (e) {
    console.error(`Error fetching fundamentals for ${symbol}:`, e);
    return {
      bs: { annual: {}, quarterly: {} },
      is: { annual: {}, quarterly: {} },
      cf: { annual: {}, quarterly: {} }
    };
  }
}
