/**
 * 財務データ計算ユーティリティ
 */

export interface DCFResult {
  fair_price: number;
  current_price: number;
  enterprise_value: number;
  equity_value: number;
  shares: number;
  cash_value: number;
  total_debt: number;
  growth_1_5y: number;
  wacc: number;
}

/**
 * 簡易版DCF計算ロジック
 * (Pythonのutils.pyのロジックを移植)
 */
export function calculateDCF(rawData: any): DCFResult | null {
  try {
    const info = rawData.info;

    // 1. 基本データの取得
    const currentPrice = info.currentPrice || info.regularMarketPrice;
    const shares = info.sharesOutstanding;
    if (!currentPrice || !shares) return null;

    // 2. WACC（加重平均資本コスト）の推定
    // 本来は詳細な計算が必要だが、簡易的に市場平均やBetaから推定
    const beta = info.beta || 1.0;
    const riskFreeRate = 0.04; // 10Y Treasury 基準
    const marketReturn = 0.10; // S&P500 長期平均
    const costOfEquity = riskFreeRate + beta * (marketReturn - riskFreeRate);
    
    // 簡易的なWACC (負債比率を考慮せずEquity Costをベースにするか、一律8-10%程度を置く)
    const wacc = Math.max(costOfEquity, 0.08); 

    // 3. 成長率の推定 (直近の成長率から)
    const revGrowth = info.revenueGrowth || 0.05;
    const earningsGrowth = info.earningsGrowth || 0.05;
    const growth_1_5y = (revGrowth * 0.6 + earningsGrowth * 0.4);
    
    // 4. キャッシュフロー予測 (TTM FCF)
    const freeCashFlow = info.freeCashflow || 0;
    if (freeCashFlow <= 0) return null;

    let totalNPV = 0;
    let lastFCF = freeCashFlow;

    // 1-5年目の予測
    for (let i = 1; i <= 5; i++) {
      lastFCF *= (1 + growth_1_5y);
      totalNPV += lastFCF / Math.pow(1 + wacc, i);
    }

    // 6-10年目 (減衰させる)
    const growth_6_10y = growth_1_5y * 0.5;
    for (let i = 6; i <= 10; i++) {
      lastFCF *= (1 + growth_6_10y);
      totalNPV += lastFCF / Math.pow(1 + wacc, i);
    }

    // 継続価値 (Terminal Value)
    const terminalGrowth = 0.02; // 永続成長率 2%
    const terminalValue = (lastFCF * (1 + terminalGrowth)) / (wacc - terminalGrowth);
    const npvTerminalValue = terminalValue / Math.pow(1 + wacc, 10);

    const enterpriseValue = totalNPV + npvTerminalValue;

    // 5. 株式価値の算出 (EV + Cash - Debt)
    const cash = info.totalCash || 0;
    const debt = info.totalDebt || 0;
    const equityValue = enterpriseValue + cash - debt;
    const fairPrice = equityValue / shares;

    return {
      fair_price: fairPrice,
      current_price: currentPrice,
      enterprise_value: enterpriseValue,
      equity_value: equityValue,
      shares: shares,
      cash_value: cash,
      total_debt: debt,
      growth_1_5y: growth_1_5y,
      wacc: wacc
    };
  } catch (e) {
    console.error("DCF calculation error:", e);
    return null;
  }
}

/**
 * Chart.js 用のデータ整形 (財務推移)
 */
export function formatFinancialChart(financialData: any, field: string) {
  if (!financialData || typeof financialData !== 'object') return null;

  // yfinanceの辞書形式 (日付キー) を配列に変換してソート
  const dates = Object.keys(financialData).sort();
  const values = dates.map(d => financialData[d][field] || 0);

  return {
    labels: dates.map(d => (typeof d === 'string' ? d.split(' ')[0] : String(d).split(' ')[0])), // YYYY-MM-DD
    datasets: [{
      label: field,
      data: values,
      backgroundColor: 'rgba(54, 162, 235, 0.5)',
      borderColor: 'rgb(54, 162, 235)',
      borderWidth: 1
    }]
  };
}
