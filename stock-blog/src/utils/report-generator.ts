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
  const sector = metadata['GICS Sector'] || "";
  const sectorEtf = sectorMap[sector] || "SPY";
  const isFinancial = ["Financials", "Real Estate"].includes(sector);

  // TradingView用シンボル
  const exchange = metadata['Exchange'] || "NASDAQ";
  const fullSymbol = `${exchange}:${symbol.replace("-", ".")}`;

  // DCF評価の正規化 (Python側の出力をAstroの期待に合わせる)
  let dcfValuation = rawData.dcf_valuation;
  if (dcfValuation && dcfValuation.wacc_details) {
    // 既存の Python dict を正規化
    dcfValuation = {
      ...dcfValuation,
      wacc: dcfValuation.wacc_details.wacc,
      growth_1_5y: dcfValuation.cagr_details ? (dcfValuation.cagr_details.revenue * 0.4 + dcfValuation.cagr_details.fcf * 0.6) : 0.05
    };
  }

  // チャートデータの整形
  const charts: any = {};
  
  // 財務項目のキーは yfinance バージョンにより異なるため複数パターン試行
  const getField = (stmt: any, fieldNames: string[]) => {
    for (const name of fieldNames) {
      if (stmt[name]) return stmt[name];
    }
    return null;
  };

  if (rawData.income_stmt) {
    const rev = getField(rawData.income_stmt, ["Total Revenue", "TotalRevenue", "Revenue"]);
    const ni = getField(rawData.income_stmt, ["Net Income", "NetIncome", "Net Income Common Stockholders"]);
    if (rev && ni) {
      charts.is = formatFinancialChart({ "Total Revenue": rev, "Net Income": ni }, ["Total Revenue", "Net Income"], "損益計算書推移");
    }
  }
  
  if (rawData.balancesheet) {
    const assets = getField(rawData.balancesheet, ["Total Assets", "TotalAssets"]);
    const liab = getField(rawData.balancesheet, ["Total Liabilities Net Minority Interest", "TotalLiabilitiesNetMinorityInterest", "Total Liabilities"]);
    if (assets && liab) {
      charts.bs = formatFinancialChart({ "Total Assets": assets, "Total Liabilities": liab }, ["Total Assets", "Total Liabilities"], "貸借対照表推移");
    }
  }

  if (rawData.cashflow) {
    const fcf = getField(rawData.cashflow, ["Free Cash Flow", "FreeCashFlow"]);
    const ocf = getField(rawData.cashflow, ["Operating Cash Flow", "OperatingCashFlow"]);
    if (fcf && ocf) {
      charts.cf = formatFinancialChart({ "Free Cash Flow": fcf, "Operating Cash Flow": ocf }, ["Free Cash Flow", "Operating Cash Flow"], "キャッシュフロー推移");
    }
  }

  // 決算データの抽出
  let earningsSurprise: any = null;
  if (rawData.earnings_dates && Array.isArray(rawData.earnings_dates) && rawData.earnings_dates.length > 0) {
    const reported = rawData.earnings_dates
      .filter((d: any) => d['Reported EPS'] !== "None" && d['Reported EPS'] !== null && d['Reported EPS'] !== "nan")
      .sort((a: any, b: any) => new Date(b.index).getTime() - new Date(a.index).getTime());
    
    if (reported.length > 0) {
      const latest = reported[0];
      earningsSurprise = {
        date: latest.index.split(' ')[0],
        actual: parseFloat(latest['Reported EPS']),
        estimate: (latest['EPS Estimate'] !== "None" && latest['EPS Estimate'] !== "nan") ? parseFloat(latest['EPS Estimate']) : null,
        surprise_pct: (latest['Surprise(%)'] !== "None" && latest['Surprise(%)'] !== "nan") ? parseFloat(latest['Surprise(%)']) : null
      };
    }
  }

  // 次回決算 (calendar または info から)
  let nextEarnings: any = null;
  const now = new Date();
  if (rawData.calendar && rawData.calendar['Earnings Date']) {
    const dates = rawData.calendar['Earnings Date'];
    const nextDate = Array.isArray(dates) ? dates[0] : dates;
    if (new Date(nextDate) >= now) {
      nextEarnings = {
        date: nextDate.split(' ')[0],
        estimate: rawData.calendar['EPS Average'] || info.earningsAverage || null
      };
    }
  } else if (info.nextEarningsDate) {
    const nextDate = new Date(info.nextEarningsDate * 1000);
    if (nextDate >= now) {
      nextEarnings = {
        date: nextDate.toISOString().split('T')[0],
        estimate: info.earningsAverage || null
      };
    }
  }

  return {
    symbol: symbol,
    symbol_yf: symbol,
    security: metadata['Security'] || "",
    security_ja: metadata['Security_JA'] || null,
    business_summary_ja: rawData.business_summary_ja || info.longBusinessSummary,
    dcf_valuation: dcfValuation,
    sector: sector,
    sub_industry: metadata['GICS Sub-Industry'] || "",
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
    highlights: {
      revenue_growth: info.revenueGrowth,
      earnings_growth: info.earningsGrowth,
      operating_margins: info.operatingMargins,
      roe: info.returnOnEquity,
      pe_forward: info.forwardPE,
      pe_ttm: info.trailingPE,
      eps_ttm: info.trailingEps,
      eps_forward: info.forwardEps,
      dividend_yield: info.dividendYield,
      payout_ratio: info.payoutRatio,
      debt_to_equity: info.debtToEquity,
      current_ratio: info.currentRatio
    }
  };
}

/**
 * 財務データをChart.js形式に変換
 */
function formatFinancialChart(stmtFields: Record<string, any>, fields: string[], label: string) {
  if (!stmtFields || Object.keys(stmtFields).length === 0) return null;
  
  // 日付ラベルの取得とソート
  const firstFieldName = Object.keys(stmtFields)[0];
  const dates = Object.keys(stmtFields[firstFieldName]).sort();
  
  return {
    labels: dates.map(d => d.split(' ')[0]),
    datasets: fields.map((field, i) => ({
      label: field,
      data: dates.map(d => stmtFields[field] ? stmtFields[field][d] : 0),
      backgroundColor: i === 0 ? 'rgba(54, 162, 235, 0.5)' : 'rgba(255, 99, 132, 0.5)',
      borderColor: i === 0 ? 'rgb(54, 162, 235)' : 'rgb(255, 99, 132)',
      borderWidth: 1
    }))
  };
}
