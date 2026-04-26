/**
 * R2の生データを旧Pythonスクリプトと同等のレポートJSON形式に変換するユーティリティ
 */

export interface ReportData {
  symbol: string;
  symbol_yf: string;
  security: string;
  security_ja?: string;
  business_summary_ja?: string;
  dcf_valuation: any;
  sector: string;
  sub_industry: string;
  exchange: string;
  full_symbol: string;
  sector_etf: string;
  is_financial: boolean;
  is_available_monex: boolean;
  is_available_rakuten: boolean;
  is_available_sbi: boolean;
  is_available_mufg: boolean;
  is_available_matsui: boolean;
  is_available_dmm: boolean;
  is_available_paypay: boolean;
  is_available_moomoo: boolean;
  is_available_iwaicosmo: boolean;
  charts: {
    bs?: any;
    is?: any;
    cf?: any;
    tp?: any;
    dps?: any;
    dps_history?: any;
    segment?: any;
    geo?: any;
  };
  earnings_surprise?: any;
  next_earnings?: any;
  consensus?: any;
  highlights?: any;
  analyst_ratings?: any;
  rating_changes?: any[];
  movement_reason?: string;
}

/**
 * 生データをレポート形式に変換
 */
export function transformRawToReport(rawData: any): ReportData {
  const info = rawData.info || {};
  const metadata = rawData.metadata || {};
  const symbol = rawData.symbol;
  
  // セクターETFのマッピング
  const sectorMap: Record<string, string> = {
    "Communication Services": "XLC",
    "Consumer Discretionary": "XLY",
    "Consumer Staples": "XLP",
    "Energy": "XLE",
    "Financials": "XLF",
    "Health Care": "XLV",
    "Industrials": "XLI",
    "Information Technology": "XLK",
    "Materials": "XLB",
    "Real Estate": "XLRE",
    "Utilities": "XLU"
  };
  const sector = metadata['GICS Sector'] || info.sector || "";
  const sectorEtf = sectorMap[sector] || "SPY";
  const isFinancial = ["Financials", "Real Estate"].includes(sector);

  // TradingView用シンボル
  const exchange = metadata['Exchange'] || info.exchange || "NASDAQ";
  const fullSymbol = `${exchange}:${symbol.replace("-", ".")}`;

  // DCF評価の正規化 (Python側の出力をAstroの期待に合わせる)
  let dcfValuation = rawData.dcf_valuation;
  if (dcfValuation && dcfValuation.wacc_details) {
    dcfValuation = {
      ...dcfValuation,
      wacc: dcfValuation.wacc_details.wacc,
      growth_1_5y: dcfValuation.cagr_details ? (dcfValuation.cagr_details.revenue * 0.4 + dcfValuation.cagr_details.fcf * 0.6) : 0.05
    };
  }

  // チャートデータの整形
  const charts: any = {};
  
  const getField = (stmt: any, fieldNames: string[]) => {
    if (!stmt) return null;
    for (const name of fieldNames) {
      if (stmt[name]) return stmt[name];
    }
    return null;
  };

  // 1. IS Chart
  if (rawData.income_stmt) {
    const rev = getField(rawData.income_stmt, ["Total Revenue", "TotalRevenue", "Revenue"]);
    const ni = getField(rawData.income_stmt, ["Net Income", "NetIncome", "Net Income Common Stockholders"]);
    const gp = getField(rawData.income_stmt, ["Gross Profit", "GrossProfit"]);
    const op = getField(rawData.income_stmt, ["Operating Income", "OperatingIncome"]);
    
    if (rev && ni) {
      charts.is = formatFinancialChartMulti({ 
        "Total Revenue": rev, 
        "Net Income": ni,
        "Gross Profit": gp,
        "Operating Income": op
      }, ["Total Revenue", "Gross Profit", "Operating Income", "Net Income"]);
    }
  }
  
  // 2. BS Chart
  if (rawData.balancesheet) {
    const assets = getField(rawData.balancesheet, ["Total Assets", "TotalAssets"]);
    const liab = getField(rawData.balancesheet, ["Total Liabilities Net Minority Interest", "TotalLiabilitiesNetMinorityInterest", "Total Liabilities"]);
    const equity = getField(rawData.balancesheet, ["Total Equity Gross Minority Interest", "TotalEquityGrossMinorityInterest", "Stockholders Equity"]);
    
    if (assets && liab) {
      charts.bs = formatFinancialChartMulti({ 
        "Total Assets": assets, 
        "Total Liabilities": liab,
        "Total Equity": equity
      }, ["Total Assets", "Total Liabilities", "Total Equity"]);
    }
  }

  // 3. CF Chart
  if (rawData.cashflow) {
    const fcf = getField(rawData.cashflow, ["Free Cash Flow", "FreeCashFlow"]);
    const ocf = getField(rawData.cashflow, ["Operating Cash Flow", "OperatingCashFlow"]);
    const icf = getField(rawData.cashflow, ["Investing Cash Flow", "InvestingCashFlow"]);
    const fcf_fin = getField(rawData.cashflow, ["Financing Cash Flow", "FinancingCashFlow"]);
    
    if (ocf) {
      charts.cf = formatFinancialChartMulti({ 
        "Operating Cash Flow": ocf,
        "Investing Cash Flow": icf,
        "Financing Cash Flow": fcf_fin,
        "Free Cash Flow": fcf
      }, ["Operating Cash Flow", "Investing Cash Flow", "Financing Cash Flow", "Free Cash Flow"]);
    }
  }

  // 決算データの抽出
  let earningsSurprise: any = null;
  if (rawData.earnings_dates && Array.isArray(rawData.earnings_dates) && rawData.earnings_dates.length > 0) {
    const reported = rawData.earnings_dates
      .filter((d: any) => d['Reported EPS'] !== "None" && d['Reported EPS'] !== null && d['Reported EPS'] !== "nan" && d['Reported EPS'] !== undefined)
      .sort((a: any, b: any) => new Date(b.index || b.Date).getTime() - new Date(a.index || a.Date).getTime());
    
    if (reported.length > 0) {
      const latest = reported[0];
      const rawDate = latest.index || latest.Date || "";
      earningsSurprise = {
        date: typeof rawDate === 'string' ? rawDate.split(' ')[0] : String(rawDate).split(' ')[0],
        actual: parseFloat(latest['Reported EPS']),
        estimate: (latest['EPS Estimate'] !== "None" && latest['EPS Estimate'] !== "nan") ? parseFloat(latest['EPS Estimate']) : null,
        surprise_pct: (latest['Surprise(%)'] !== "None" && latest['Surprise(%)'] !== "nan") ? parseFloat(latest['Surprise(%)']) : null
      };
    }
  }

  // 次回決算
  let nextEarnings: any = null;
  const now = new Date();
  if (rawData.calendar && rawData.calendar['Earnings Date']) {
    const dates = rawData.calendar['Earnings Date'];
    const nextDate = Array.isArray(dates) ? dates[0] : dates;
    if (nextDate && new Date(nextDate).toString() !== 'Invalid Date' && new Date(nextDate) >= now) {
      nextEarnings = {
        date: typeof nextDate === 'string' ? nextDate.split(' ')[0] : new Date(nextDate).toISOString().split('T')[0],
        estimate: rawData.calendar['EPS Average'] || info.earningsAverage || null
      };
    }
  } else if (info.nextEarningsDate) {
    const nextDate = new Date(info.nextEarningsDate * 1000);
    if (nextDate.toString() !== 'Invalid Date' && nextDate >= now) {
      nextEarnings = {
        date: nextDate.toISOString().split('T')[0],
        estimate: info.earningsAverage || null
      };
    }
  }

  // コンセンサスデータの整形
  const consensus: any = { earnings: {}, revenue: {} };
  const periods = ["0q", "+1q", "0y", "+1y"];
  
  consensus.earnings["0q"] = {
    avg: info.earningsAverage,
    low: info.earningsLow,
    high: info.earningsHigh,
    growth: info.earningsGrowth,
    numberOfAnalysts: info.numberOfAnalystOpinions
  };
  consensus.revenue["0q"] = {
    avg: info.revenueAverage,
    low: info.revenueLow,
    high: info.revenueHigh,
    growth: info.revenueGrowth,
    numberOfAnalysts: info.numberOfAnalystOpinions
  };

  // Highlights の計算 (Python側のロジック移植)
  const getGrowth = (val: any) => {
    if (val === null || val === undefined) return null;
    return (val > 1.0 || val < -1.0) ? val / 100.0 : val;
  };

  const getYield = (val: any) => {
    if (val === null || val === undefined) return null;
    return val > 0.1 ? val / 100.0 : val;
  };

  return {
    symbol: symbol,
    symbol_yf: symbol,
    security: metadata['Security'] || info.longName || "",
    security_ja: metadata['Security_JA'] || null,
    business_summary_ja: rawData.business_summary_ja || info.longBusinessSummary,
    dcf_valuation: dcfValuation,
    sector: sector,
    sub_industry: metadata['GICS Sub-Industry'] || info.sector || "",
    exchange: exchange,
    full_symbol: fullSymbol,
    sector_etf: sectorEtf,
    is_financial: isFinancial,
    is_available_monex: metadata['is_available_monex'] || false,
    is_available_rakuten: metadata['is_available_rakuten'] || false,
    is_available_sbi: metadata['is_available_sbi'] || false,
    is_available_mufg: metadata['is_available_mufg'] || false,
    is_available_matsui: metadata['is_available_matsui'] || false,
    is_available_dmm: metadata['is_available_dmm'] || false,
    is_available_paypay: metadata['is_available_paypay'] || false,
    is_available_moomoo: metadata['is_available_moomoo'] || false,
    is_available_iwaicosmo: metadata['is_available_iwaicosmo'] || false,
    charts: charts,
    earnings_surprise: earningsSurprise,
    next_earnings: nextEarnings,
    consensus: consensus,
    analyst_ratings: rawData.analyst_ratings || {
      recommendationKey: info.recommendationKey,
      targetHighPrice: info.targetHighPrice,
      targetLowPrice: info.targetLowPrice,
      targetMeanPrice: info.targetMeanPrice,
      targetMedianPrice: info.targetMedianPrice,
      currentPrice: info.currentPrice || info.regularMarketPrice,
      numberOfAnalystOpinions: info.numberOfAnalystOpinions
    },
    rating_changes: rawData.rating_changes || [],
    movement_reason: rawData.movement_reason,
    highlights: {
      revenue_growth: getGrowth(info.revenueGrowth),
      earnings_growth: getGrowth(info.earningsGrowth),
      profit_margins: info.profitMargins,
      operating_margins: info.operatingMargins,
      roe: info.returnOnEquity,
      roa: info.returnOnAssets,
      eps_ttm: info.trailingEps,
      eps_forward: info.forwardEps,
      pe_ttm: info.trailingPE,
      pe_forward: info.forwardPE,
      dividend_yield: getYield(info.dividendYield),
      payout_ratio: info.payoutRatio,
      debt_to_equity: info.debtToEquity,
      current_ratio: info.currentRatio
    }
  };
}

/**
 * 複数の項目を1つのチャートデータにまとめる
 */
function formatFinancialChartMulti(stmtFields: Record<string, any>, fields: string[]) {
  // 有効なデータがあるフィールドのみ抽出
  const activeFields = fields.filter(f => stmtFields[f] && Object.keys(stmtFields[f]).length > 0);
  if (activeFields.length === 0) return null;
  
  // 日付ラベルの取得とソート (すべてのフィールドからユニークな日付を集める)
  const allDates = new Set<string>();
  activeFields.forEach(f => {
    Object.keys(stmtFields[f]).forEach(d => allDates.add(d));
  });
  const sortedDates = Array.from(allDates).sort();
  
  // 直近8件に制限
  const displayDates = sortedDates.slice(-8);

  const colors = [
    { bg: 'rgba(54, 162, 235, 0.5)', border: 'rgb(54, 162, 235)' },   // Blue
    { bg: 'rgba(255, 99, 132, 0.5)', border: 'rgb(255, 99, 132)' },   // Red
    { bg: 'rgba(75, 192, 192, 0.5)', border: 'rgb(75, 192, 192)' },   // Teal
    { bg: 'rgba(255, 159, 64, 0.5)', border: 'rgb(255, 159, 64)' },   // Orange
    { bg: 'rgba(153, 102, 255, 0.5)', border: 'rgb(153, 102, 255)' }  // Purple
  ];
  
  return {
    labels: displayDates.map(d => (typeof d === 'string' ? d.split(' ')[0] : String(d).split(' ')[0])),
    datasets: activeFields.map((field, i) => ({
      label: field,
      data: displayDates.map(d => stmtFields[field][d] || 0),
      backgroundColor: colors[i % colors.length].bg,
      borderColor: colors[i % colors.length].border,
      borderWidth: 1
    }))
  };
}
