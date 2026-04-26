/**
 * R2から取得した生の yfinance/defeatbeta データを
 * フロントエンド向けの加工済みレポート JSON に変換するユーティリティ
 */

export interface ReportData {
  symbol: string;
  symbol_yf: string;
  security: string;
  security_ja?: string | null;
  business_summary_ja?: string | null;
  dcf_valuation: any;
  sector: string;
  sub_industry: string;
  exchange: string;
  full_symbol: string;
  sector_etf: string;
  is_financial: boolean;
  charts: {
    bs?: any;
    is?: any;
    cf?: any;
    risk_return?: any;
    performance?: any;
  };
  highlights: any;
  earnings_surprise?: any;
  next_earnings?: any;
  movement_reason?: string;
}

/**
 * 生データをレポート形式に変換
 */
export function transformRawToReport(rawData: any, metadata: any = {}): ReportData {
  const info = rawData.info || {};
  const symbol = rawData.symbol;
  
  // 1. セクターETFのマッピング
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

  // 2. リスク・リターン指標の計算 (移植したロジック)
  const riskReturn = calculateRiskReturnFromHistory(rawData.history, symbol);

  // 3. 財務チャートの整形 (fundamentals.ts のロジックを統合)
  const charts: any = {
    risk_return: riskReturn,
    is: formatFinancialChart(rawData.income_stmt, ["Total Revenue", "Gross Profit", "Operating Income", "Net Income"]),
    bs: formatFinancialChart(rawData.balancesheet, ["Total Assets", "Total Liabilities Net Minority Interest", "Stockholders Equity"]),
    cf: formatFinancialChart(rawData.cashflow, ["Operating Cash Flow", "Investing Cash Flow", "Financing Cash Flow", "Free Cash Flow"]),
  };

  return {
    symbol: symbol,
    symbol_yf: symbol,
    security: metadata['Security'] || info.longName || "",
    security_ja: metadata['Security_JA'] || null,
    business_summary_ja: rawData.business_summary_ja || info.longBusinessSummary,
    dcf_valuation: rawData.dcf_valuation,
    sector: sector,
    sub_industry: metadata['GICS Sub-Industry'] || info.sector || "",
    exchange: info.exchange || "NASDAQ",
    full_symbol: `${info.exchange || 'NASDAQ'}:${symbol.replace("-", ".")}`,
    sector_etf: sectorEtf,
    is_financial: isFinancial,
    charts: charts,
    highlights: {
      revenue_growth: info.revenueGrowth,
      earnings_growth: info.earningsGrowth,
      profit_margins: info.profitMargins,
      operating_margins: info.operatingMargins,
      pe_ttm: info.trailingPE,
      pe_forward: info.forwardPE,
      dividend_yield: info.dividendYield,
    },
    movement_reason: rawData.movement_reason
  };
}

/**
 * 株価履歴からリスク(HV)とリターンを計算
 */
function calculateRiskReturnFromHistory(history: any[], symbol: string) {
  if (!history || history.length < 5) return null;

  // 1年分のデータで簡易計算 (必要に応じて全期間に拡張)
  const lastQuotes = history.slice(-252);
  const returns = [];
  for (let i = 1; i < lastQuotes.length; i++) {
    returns.push(Math.log(lastQuotes[i].Close / lastQuotes[i - 1].Close));
  }

  const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
  const variance = returns.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / (returns.length - 1);
  const hv = Math.sqrt(variance) * Math.sqrt(252);

  const totalReturn = (lastQuotes[lastQuotes.length - 1].Close / lastQuotes[0].Close) - 1;

  return {
    data: [{
      x: [hv],
      y: [totalReturn],
      text: [symbol],
      mode: 'markers+text',
      name: symbol,
      marker: { size: 16, color: 'red' }
    }],
    layout: {
      xaxis: { title: 'リスク', tickformat: '.0%' },
      yaxis: { title: 'リターン', tickformat: '.0%' }
    }
  };
}

/**
 * 財務データを Chart.js/Plotly 向けに整形
 */
function formatFinancialChart(stmt: any[], fields: string[]) {
  if (!stmt || !Array.isArray(stmt) || stmt.length === 0) return null;

  // 日付順にソート
  const sortedStmt = [...stmt].sort((a, b) => new Date(a.index || a.Date).getTime() - new Date(b.index || b.Date).getTime());
  const dates = sortedStmt.map(s => (s.index || s.Date || "").toString().split(' ')[0]);

  const colors = [
    { bg: 'rgba(54, 162, 235, 0.5)', border: 'rgb(54, 162, 235)' },
    { bg: 'rgba(255, 99, 132, 0.5)', border: 'rgb(255, 99, 132)' },
    { bg: 'rgba(75, 192, 192, 0.5)', border: 'rgb(75, 192, 192)' },
    { bg: 'rgba(255, 159, 64, 0.5)', border: 'rgb(255, 159, 64)' }
  ];

  return {
    labels: dates,
    datasets: fields.map((field, i) => ({
      label: field,
      data: sortedStmt.map(s => s[field] || 0),
      backgroundColor: colors[i % colors.length].bg,
      borderColor: colors[i % colors.length].border,
      borderWidth: 1
    }))
  };
}
