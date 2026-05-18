import * as fs from 'fs';
import * as path from 'path';
import { execSync } from 'child_process';
import { fetchSp500Companies, getBrokerageAvailability } from './lib/market-data.ts';
import { calculateRiskMetrics, yahooFinance } from './lib/risk-analysis.ts';
import { generatePerformanceChartData } from './lib/performance-comparison.ts';

const REPORTS_DIR = path.join(process.cwd(), 'public/reports');
const STOCKS_JSON_PATH = path.join(process.cwd(), 'src/data/stocks.json');
const PYTHON_SCRIPT = path.join(process.cwd(), '../code_master_ref/export_stock_data.py');

// キャッシュ: SPYやセクターETFの重複取得を避ける
const metricCache = new Map<string, any>();
const performanceCache = new Map<string, any>();

async function retry<T>(fn: () => Promise<T>, retries = 2, delay = 2000): Promise<T> {
  try {
    return await fn();
  } catch (e: any) {
    if (retries <= 0) throw e;
    console.warn(`    - Retrying fetch due to error: ${e.message}. Retries left: ${retries}`);
    await new Promise(resolve => setTimeout(resolve, delay));
    return retry(fn, retries - 1, delay * 2);
  }
}

async function main() {
  console.log('--- Hybrid Data Pipeline (TypeScript + DefeatBeta Python) Started ---');

  if (!fs.existsSync(REPORTS_DIR)) fs.mkdirSync(REPORTS_DIR, { recursive: true });

  interface StockMetadata {
    Symbol: string;
    Symbol_YF: string;
    Security: string;
    Security_JA?: string;
    'GICS Sector': string;
    'GICS Sub-Industry': string;
    Exchange: string;
    Index?: string;
    Daily_Change?: number;
    Has_Movement_Reason?: boolean;
  }

  let stocks: StockMetadata[] = [];

  if (process.env.TEST_MODE === 'true') {
    stocks = [
      { Symbol: 'MSFT', Symbol_YF: 'MSFT', Security: 'Microsoft Corp', 'GICS Sector': 'Information Technology', 'GICS Sub-Industry': 'Systems Software', Exchange: 'NASDAQ', Index: 'S&P 500' },
      { Symbol: 'AAPL', Symbol_YF: 'AAPL', Security: 'Apple Inc', 'GICS Sector': 'Information Technology', 'GICS Sub-Industry': 'Technology Hardware Storage & Peripherals', Exchange: 'NASDAQ', Index: 'S&P 500' },
      { Symbol: 'RPM', Symbol_YF: 'RPM', Security: 'RPM International Inc', 'GICS Sector': 'Materials', 'GICS Sub-Industry': 'Specialty Chemicals', Exchange: 'NYSE', Index: 'S&P 400' },
      { Symbol: 'ENSG', Symbol_YF: 'ENSG', Security: 'The Ensign Group Inc', 'GICS Sector': 'Health Care', 'GICS Sub-Industry': 'Health Care Facilities', Exchange: 'NASDAQ', Index: 'S&P 600' }
    ];
  } else if (fs.existsSync(STOCKS_JSON_PATH)) {
    // Read stocks from the JSON file created by Python main.py
    const data = fs.readFileSync(STOCKS_JSON_PATH, 'utf-8');
    stocks = JSON.parse(data);
  } else {
    // Fallback: try to fetch from Wikipedia
    stocks = await fetchSp500Companies();
  }

  const brokerages = await getBrokerageAvailability();

  console.log(`Processing ${stocks.length} stocks...`);

  // 並列度を制限するヘルパー関数
  const MAX_CONCURRENT = 5;
  async function processStocksInBatches(stockList: typeof stocks) {
    const results = [];
    for (let i = 0; i < stockList.length; i += MAX_CONCURRENT) {
      const batch = stockList.slice(i, i + MAX_CONCURRENT);
      const batchResults = await Promise.all(batch.map(stock => processStock(stock)));
      results.push(...batchResults);
      console.log(`Progress: ${Math.min(i + MAX_CONCURRENT, stockList.length)}/${stockList.length} stocks processed`);
    }
    return results;
  }

  // 1銘柄の処理をasync関数に抽出
  async function processStock(stock: typeof stocks[0]): Promise<any> {
    console.log(`[${stock.Symbol}] Fetching integrated data...`);
    
    try {
      // 1. DefeatBeta (Python) から詳細データを取得
      let dbData: any = {};
      try {
        const pythonPath = path.join(process.cwd(), '../code_master_ref/.venv/bin/python3');
        const output = execSync(`${pythonPath} ${PYTHON_SCRIPT} ${stock.Symbol}`, { encoding: 'utf-8', cwd: path.dirname(PYTHON_SCRIPT) });

        // JSONの開始位置を探す (ゴミが混ざっていた場合への対策)
        const jsonStart = output.indexOf('{');
        if (jsonStart !== -1) {
          dbData = JSON.parse(output.substring(jsonStart));
        }
      } catch (e: any) {
        console.error(`  - DefeatBeta error for ${stock.Symbol}:`, e);
      }

      // セクターに応じた比較対象ETFの選択
      const sectorEtfMap: Record<string, string> = {
        'Information Technology': 'XLK',
        'Consumer Discretionary': 'XLY',
        'Financials': 'XLF',
        'Health Care': 'XLV',
        'Communication Services': 'XLC',
        'Industrials': 'XLI',
        'Consumer Staples': 'XLP',
        'Energy': 'XLE',
        'Utilities': 'XLU',
        'Real Estate': 'XLRE',
        'Materials': 'XLB',
        'Homebuilding': 'XHB'
      };

      const broadSectorEtfMap: Record<string, string> = {
        'Information Technology': 'VGT',
        'Consumer Discretionary': 'VCR',
        'Financials': 'VFH',
        'Health Care': 'VHT',
        'Communication Services': 'VOX',
        'Industrials': 'VIS',
        'Consumer Staples': 'VDC',
        'Energy': 'VDE',
        'Utilities': 'VPU',
        'Real Estate': 'VNQ',
        'Materials': 'VAW',
        'Homebuilding': 'ITB'
      };

      const marketIndexMap: Record<string, string> = {
        'S&P 500': 'SPY',
        'S&P 400': 'MDY',
        'S&P 600': 'IJR'
      };

      const etfFullNameMap: Record<string, string> = {
        'XLK': 'Technology Select Sector SPDR Fund',
        'XLY': 'Consumer Discretionary Select Sector SPDR Fund',
        'XLF': 'Financial Select Sector SPDR Fund',
        'XLV': 'Health Care Select Sector SPDR Fund',
        'XLC': 'Communication Services Select Sector SPDR Fund',
        'XLI': 'Industrial Select Sector SPDR Fund',
        'XLP': 'Consumer Staples Select Sector SPDR Fund',
        'XLE': 'Energy Select Sector SPDR Fund',
        'XLU': 'Utilities Select Sector SPDR Fund',
        'XLRE': 'Real Estate Select Sector SPDR Fund',
        'XLB': 'Materials Select Sector SPDR Fund',
        'XHB': 'SPDR S&P Homebuilders ETF',
        'VGT': 'Vanguard Information Technology ETF',
        'VCR': 'Vanguard Consumer Discretionary ETF',
        'VFH': 'Vanguard Financials ETF',
        'VHT': 'Vanguard Health Care ETF',
        'VOX': 'Vanguard Communication Services ETF',
        'VIS': 'Vanguard Industrials ETF',
        'VDC': 'Vanguard Consumer Staples ETF',
        'VDE': 'Vanguard Energy ETF',
        'VPU': 'Vanguard Utilities ETF',
        'VNQ': 'Vanguard Real Estate ETF',
        'VAW': 'Vanguard Materials ETF',
        'ITB': 'iShares U.S. Home Construction ETF',
        'SPY': 'SPDR S&P 500 ETF Trust',
        'MDY': 'SPDR S&P MidCap 400 ETF Trust',
        'IJR': 'iShares Core S&P Small-Cap ETF'
      };

      const targetEtf = sectorEtfMap[stock['GICS Sub-Industry']] || sectorEtfMap[stock['GICS Sector']] || 'SPY';
      const broadEtf = broadSectorEtfMap[stock['GICS Sub-Industry']] || broadSectorEtfMap[stock['GICS Sector']] || 'SPY';
      const marketEtf = (stock.Index && marketIndexMap[stock.Index]) || 'SPY';

      // ベンチマーク情報の保存用
      const benchmarkInfo = {
        sector: { symbol: targetEtf, name: etfFullNameMap[targetEtf] || targetEtf },
        broad: { symbol: broadEtf, name: etfFullNameMap[broadEtf] || broadEtf },
        index: { symbol: marketEtf, name: etfFullNameMap[marketEtf] || marketEtf },
        market: { symbol: 'SPY', name: etfFullNameMap['SPY'] }
      };

      // 2. YFinance (TypeScript) から詳細データを取得 (個別失敗を許容)
      // Node.jsの接続が不安定なため、Python側で取得したデータを優先的に使用し、失敗した場合はPythonデータをフォールバックとする
      
      const pyYfData = dbData.yf_data || {};
      const pyInfo = pyYfData.info || {};
      
      console.log(`    - Fetching quote...`);
      let quote = await retry(() => yahooFinance.quote(stock.Symbol_YF)).catch((e: any) => {
        console.warn(`    - Quote fetch failed for ${stock.Symbol}, using Python fallback:`, e.message);
        return {
          regularMarketPrice: pyInfo.currentPrice || pyInfo.regularMarketPrice || 0,
          regularMarketChangePercent: pyInfo.regularMarketChangePercent || 0,
          exchange: pyInfo.exchange || 'NMS',
          forwardPE: pyInfo.forwardPE,
          trailingPE: pyInfo.trailingPE,
          dividendYield: pyInfo.dividendYield,
          averageAnalystRating: pyInfo.recommendationKey ? `0 - ${pyInfo.recommendationKey}` : undefined
        };
      });

      console.log(`    - Fetching summary...`);
      let summary = await retry(() => yahooFinance.quoteSummary(stock.Symbol_YF, {
        modules: [
          'financialData', 
          'defaultKeyStatistics', 
          'recommendationTrend', 
          'upgradeDowngradeHistory',
          'earnings',
          'calendar'
        ]
      })).catch((e: any) => {
        console.warn(`    - Summary fetch failed for ${stock.Symbol}, using Python fallback:`, e.message);
        return {
          financialData: {
            currentPrice: pyInfo.currentPrice,
            targetMedianPrice: pyInfo.targetMedianPrice,
            targetMeanPrice: pyInfo.targetMeanPrice,
            targetHighPrice: pyInfo.targetHighPrice,
            targetLowPrice: pyInfo.targetLowPrice,
            numberOfAnalystOpinions: pyInfo.numberOfAnalystOpinions,
            totalCash: pyInfo.totalCash,
            totalDebt: pyInfo.totalDebt,
            revenuePerShare: pyInfo.revenuePerShare,
            returnOnEquity: pyInfo.returnOnEquity,
            grossProfits: pyInfo.grossProfits,
            freeCashflow: pyInfo.freeCashflow,
            operatingCashflow: pyInfo.operatingCashflow,
            revenueGrowth: pyInfo.revenueGrowth,
            ebitda: pyInfo.ebitda,
            operatingMargins: pyInfo.operatingMargins,
            profitMargins: pyInfo.profitMargins,
            debtToEquity: pyInfo.debtToEquity,
            earningsGrowth: pyInfo.earningsGrowth,
          },
          defaultKeyStatistics: {
            enterpriseValue: pyInfo.enterpriseValue,
            forwardPE: pyInfo.forwardPE,
            profitMargins: pyInfo.profitMargins,
            enterpriseToEbitda: pyInfo.enterpriseToEbitda,
            enterpriseToRevenue: pyInfo.enterpriseToRevenue,
            bookValue: pyInfo.bookValue,
            priceToBook: pyInfo.priceToBook,
            forwardEps: pyInfo.forwardEps,
            trailingEps: pyInfo.trailingEps,
          }
        };
      });

      console.log(`    - Fetching risk metrics...`);
      const benchmarks = Array.from(new Set([targetEtf, broadEtf, marketEtf, 'SPY']));
      const benchmarkMetricsPromises = benchmarks.map(ticker => {
        if (metricCache.has(ticker)) return Promise.resolve(metricCache.get(ticker));
        return retry(() => calculateRiskMetrics(ticker)).then(res => {
          const finalRes = res || pyYfData.risk_metrics?.[ticker] || null;
          if (finalRes) metricCache.set(ticker, finalRes);
          return finalRes;
        });
      });

      const riskMetricsResults = await Promise.all([
        retry(() => calculateRiskMetrics(stock.Symbol_YF)).then(res => res || pyYfData.risk_metrics?.[stock.Symbol_YF] || null),
        ...benchmarkMetricsPromises
      ]);

      const stockMetrics = riskMetricsResults[0];
      const metricsList = [stockMetrics];
      const labels = [stock.Symbol];

      const addBenchmark = (ticker: string, label: string) => {
        const bIdx = benchmarks.indexOf(ticker);
        if (bIdx !== -1 && riskMetricsResults[bIdx + 1]) {
          metricsList.push(riskMetricsResults[bIdx + 1]);
          labels.push(label);
        }
      };

      addBenchmark(targetEtf, `Sector (${targetEtf})`);
      if (broadEtf !== targetEtf) addBenchmark(broadEtf, `Broad (${broadEtf})`);
      if (marketEtf !== 'SPY' && marketEtf !== targetEtf && marketEtf !== broadEtf) addBenchmark(marketEtf, `Index (${marketEtf})`);
      if (!labels.some(l => l.includes('SPY'))) addBenchmark('SPY', 'Market (SPY)');

      // リスク・リターンの統合 (全期間タブ対応)
      const riskReturnData = formatRiskReturnGroups(metricsList, labels);

      console.log(`    - Fetching performance data...`);
      let perfData = await retry(() => generatePerformanceChartData(stock.Symbol_YF, targetEtf)).catch((e: any) => {
        console.warn(`    - Performance data fetch failed for ${stock.Symbol}, using Python fallback:`, e.message);
        return null; 
      });

      console.log(`    - Fetching chart data...`);
      let chartResult = await retry(() => yahooFinance.chart(stock.Symbol_YF, { 
        period1: '2010-01-01', 
        interval: '1d' 
      })).catch((e: any) => {
        console.warn(`    - Chart fetch failed for ${stock.Symbol}, using Python fallback:`, e.message);
        return { events: { dividends: [] } };
      });

      const dividends = (chartResult as any)?.events?.dividends || [];
      const financialData = (summary as any).financialData || {};
      const recommendationTrend = (summary as any).recommendationTrend?.trend?.[0] || {};
      const upgradeDowngradeHistory = (summary as any).upgradeDowngradeHistory?.history || [];
      const quotes = (chartResult as any)?.quotes || [];
      const earnings = (summary as any).earnings || {};
      const calendar = (summary as any).calendar || {};

      // 決算情報の抽出 (本番版に合わせる)
      const earningsHistory = earnings.earningsChart?.quarterly || [];
      const latestEarnings = earningsHistory.length > 0 ? earningsHistory[earningsHistory.length - 1] : null;
      const earningsSurprise = latestEarnings ? {
        date: latestEarnings.date,
        actual: latestEarnings.actual,
        estimate: latestEarnings.estimate,
        surprise_pct: latestEarnings.estimate !== 0 ? ((latestEarnings.actual - latestEarnings.estimate) / Math.abs(latestEarnings.estimate)) * 100 : 0
      } : null;

      const nextEarnings = calendar.earnings?.earningsDate?.[0] ? {
        date: new Date(calendar.earnings.earningsDate[0]).toISOString().split('T')[0],
        estimate: calendar.earnings.earningsAverage || null
      } : null;

      const consensus = {
        earnings: {
          "0q": {
            avg: financialData.earningsAverage || null,
            low: financialData.earningsLow || null,
            high: financialData.earningsHigh || null,
            growth: financialData.earningsGrowth || null,
            numberOfAnalysts: financialData.numberOfAnalystOpinions || 0
          }
        },
        revenue: {
          "0q": {
            avg: financialData.revenueAverage || null,
            low: financialData.revenueLow || null,
            high: financialData.revenueHigh || null,
            growth: financialData.revenueGrowth || null,
            numberOfAnalysts: financialData.numberOfAnalystOpinions || 0
          }
        }
      };

      // ヘルパー: 指定された日付またはその直近の株価を取得
      const getPriceAtDate = (targetDate: Date) => {
        const targetTime = targetDate.getTime();
        let closestQuote = null;
        let minDiff = Infinity;
        
        for (const q of quotes) {
          if (!q.date || q.adjclose === null) continue;
          const qTime = new Date(q.date).getTime();
          const diff = Math.abs(targetTime - qTime);
          if (diff < minDiff && qTime <= targetTime) {
            minDiff = diff;
            closestQuote = q;
          }
          if (qTime > targetTime) break;
        }
        return closestQuote ? closestQuote.adjclose : null;
      };

      // 3. データの統合 (Native Chart.js 形式に移行)
      
      const is = generateFinancialChartNative(dbData.financials?.income_statement, 
        ["Total Revenue", "Gross Profit", "Operating Income", "Net Income"]);
      
      const bs = generateFinancialChartNative(dbData.financials?.balance_sheet, 
        ["Total Assets", "Total Liabilities Net Minority Interest", "Stockholders' Equity"]);
      
      const cf = generateFinancialChartNative(dbData.financials?.cash_flow, 
        ["Operating Cash Flow", "Investing Cash Flow", "Financing Cash Flow", "Free Cash Flow"]);

      const tp = generateTpChartNative(dbData.financials?.cash_flow, dbData.financials?.income_statement);

      // 取引所マッピング (TradingView 互換)
      const exchangeMap: Record<string, string> = {
        'NMS': 'NASDAQ',
        'NYQ': 'NYSE',
        'NCM': 'NASDAQ',
        'NGM': 'NASDAQ',
        'PCX': 'NYSE',
        'ASE': 'AMEX'
      };
      const exchange = exchangeMap[(quote as any).exchange] || (quote as any).exchange;
      const isFinancial = ["Financials", "Real Estate"].includes(stock['GICS Sector']);

      // 4. パフォーマンス・リスクリターンデータの変換 (Plotly -> Chart.js)
      const convertToChartJs = (plotlyObj: any) => {
        if (!plotlyObj || !plotlyObj.data) return null;
        const labels = plotlyObj.data[0]?.x || [];
        return {
          labels,
          datasets: plotlyObj.data.map((d: any) => ({
            label: d.name,
            data: d.y,
            borderColor: d.line?.color || d.marker?.color || '#000',
            backgroundColor: d.marker?.color || d.line?.color || '#000',
            type: d.mode?.includes('markers') ? 'scatter' : 'line',
            fill: false,
            pointRadius: d.mode?.includes('markers') ? 5 : 0,
            hidden: d.visible === false
          }))
        };
      };

      const reportData = {
        symbol: stock.Symbol,
        symbol_yf: stock.Symbol_YF,
        security: stock.Security,
        security_ja: stock.Security_JA || null,
        business_summary_ja: dbData.dcf?.business_summary_ja || null,
        sector: stock['GICS Sector'],
        sub_industry: stock['GICS Sub-Industry'],
        exchange: exchange,
        full_symbol: `${exchange}:${stock.Symbol.replace('-', '.')}`,
        is_financial: isFinancial,
        benchmark_info: benchmarkInfo,

        // 証券会社フラグ
        is_available_monex: brokerages.monex.has(stock.Symbol),
        is_available_rakuten: brokerages.rakuten.has(stock.Symbol),
        is_available_sbi: brokerages.sbi.has(stock.Symbol),
        is_available_mufg: brokerages.mufg.has(stock.Symbol),
        is_available_matsui: brokerages.matsui.has(stock.Symbol),
        is_available_dmm: brokerages.dmm.has(stock.Symbol),
        is_available_paypay: brokerages.paypay.has(stock.Symbol),
        is_available_moomoo: brokerages.moomoo.has(stock.Symbol),
        is_available_iwaicosmo: brokerages.iwaicosmo.has(stock.Symbol),

        earnings_surprise: earningsSurprise,
        next_earnings: nextEarnings,
        consensus: consensus,

        // DefeatBeta由来の詳細なDCF
        dcf_valuation: dbData.dcf,

        charts: {
          risk_return: convertToChartJs(riskReturnData),
          performance: convertToChartJs(perfData),
          is: is,
          bs: bs,
          cf: cf,
          tp: tp,
          dps: generateDividendChart(dividends, chartResult, getPriceAtDate),
          segment: generateSegmentChartNative(dbData.segments, 'セグメント別収益'),
          geo: generateSegmentChartNative(dbData.geography, '地域別収益')
        },

        highlights: {
          revenue_growth: financialData.revenueGrowth,
          roe: financialData.returnOnEquity,
          operating_margins: financialData.operatingMargins,
          pe_forward: (quote as any).forwardPE,
          pe_ttm: (quote as any).trailingPE,
          dividend_yield: (quote as any).dividendYield,
          debt_to_equity: financialData.debtToEquity,
          earnings_growth: financialData.earningsGrowth,
          profit_margins: financialData.profitMargins
        },

        analyst_ratings: {
          recommendationKey: (quote as any).averageAnalystRating?.split(' - ')[1] || "hold",
          strongBuy: recommendationTrend.strongBuy || 0,
          buy: recommendationTrend.buy || 0,
          hold: recommendationTrend.hold || 0,
          sell: recommendationTrend.sell || 0,
          strongSell: recommendationTrend.strongSell || 0,
          targetMeanPrice: financialData.targetMeanPrice,
          targetHighPrice: financialData.targetHighPrice,
          targetLowPrice: financialData.targetLowPrice,
          targetMedianPrice: financialData.targetMedianPrice,
          numberOfAnalystOpinions: financialData.numberOfAnalystOpinions,
          currentPrice: (quote as any).regularMarketPrice
        },

        rating_changes: upgradeDowngradeHistory.slice(0, 10).map((h: any) => {
          const ratingDate = h.epochGradeDate instanceof Date ? h.epochGradeDate : new Date(h.epochGradeDate);
          const priceAtRating = getPriceAtDate(ratingDate);
          return {
            GradeDate: ratingDate.toISOString().split('T')[0],
            Firm: h.firm,
            ToGrade: h.toGrade,
            FromGrade: h.fromGrade,
            Action: h.action,
            currentPriceTarget: h.currentPriceTarget,
            priorPriceTarget: h.priorPriceTarget,
            PriceAtRating: priceAtRating
          };
        }),

        peers: {
          sub_industry: stocks
            .filter(s => s['GICS Sub-Industry'] === stock['GICS Sub-Industry'] && s.Symbol !== stock.Symbol)
            .slice(0, 10)
            .map(p => ({ Symbol: p.Symbol, Symbol_YF: p.Symbol_YF })),
          sector: stocks
            .filter(s => s['GICS Sector'] === stock['GICS Sector'] && s['GICS Sub-Industry'] !== stock['GICS Sub-Industry'])
            .slice(0, 10)
            .map(p => ({ Symbol: p.Symbol, Symbol_YF: p.Symbol_YF }))
        }
      };

      fs.writeFileSync(path.join(REPORTS_DIR, `${stock.Symbol_YF}.json`), JSON.stringify(reportData, null, 2));
      console.log(`  - [${stock.Symbol}] Successfully saved report.`);
      return { ...stock, Daily_Change: (quote as any).regularMarketChangePercent / 100 };

    } catch (e: any) {
      console.error(`    - Unexpected error processing ${stock.Symbol}:`, e);
      return null;
    }
  }

  const updatedStocks = (await processStocksInBatches(stocks)).filter(s => s !== null);

  fs.writeFileSync(STOCKS_JSON_PATH, JSON.stringify(updatedStocks, null, 2));
  console.log('--- Integration Finished! ---');
}

// === Native Chart Helpers (aligned with production) ===

function getValFromSplit(splitData: any, field: string, date: string) {
  if (!splitData || !splitData.columns || !splitData.data) return null;
  const fieldRow = splitData.data.find((r: any) => r[0] === field);
  if (!fieldRow) return null;
  const dateIdx = splitData.columns.indexOf(date);
  return dateIdx !== -1 ? fieldRow[dateIdx] : null;
}

function generateFinancialChartNative(splitData: any, fields: string[]) {
  if (!splitData || !splitData.columns || !splitData.data) return null;

  // TTMを除外して日付を抽出
  const dates = splitData.columns
    .filter((c: any, i: number) => i > 0 && c !== 'TTM')
    .sort();

  if (dates.length === 0) return null;

  const colors = [
    { bg: "rgba(54, 162, 235, 0.5)", border: "rgb(54, 162, 235)" },
    { bg: "rgba(255, 99, 132, 0.5)", border: "rgb(255, 99, 132)" },
    { bg: "rgba(75, 192, 192, 0.5)", border: "rgb(75, 192, 192)" },
    { bg: "rgba(255, 159, 64, 0.5)", border: "rgb(255, 159, 64)" },
  ];

  const labelMap: Record<string, string> = {
    "Total Revenue": "売上高",
    "Gross Profit": "売上総利益",
    "Operating Income": "営業利益",
    "Net Income": "純利益",
    "Net Income Common Stockholders": "純利益",
    "Total Assets": "総資産",
    "Total Current Assets": "流動資産",
    "Total non-current assets": "固定資産",
    "Total Liabilities Net Minority Interest": "総負債",
    "Stockholders Equity": "純資産",
    "Stockholders' Equity": "純資産",
    "Total Non Current Liabilities": "固定負債",
    "Total Current Liabilities": "流動負債",
    "Operating Cash Flow": "営業CF",
    "Investing Cash Flow": "投資CF",
    "Financing Cash Flow": "財務CF",
    "Free Cash Flow": "フリーCF"
  };

  return {
    labels: dates.map((d: any) => d.split("T")[0]),
    datasets: fields.map((field, i) => ({
      label: labelMap[field] || field,
      data: dates.map((d: any) => getValFromSplit(splitData, field, d)),
      backgroundColor: colors[i % colors.length].bg,
      borderColor: colors[i % colors.length].border,
      borderWidth: 1,
    })),
  };
}

function generateTpChartNative(cfSplit: any, isSplit: any) {
  if (!cfSplit || !isSplit) return null;
  const dates = isSplit.columns
    .filter((c: any, i: number) => i > 0 && c !== 'TTM')
    .sort();
  if (dates.length === 0) return null;

  const niData = dates.map((d: any) => getValFromSplit(isSplit, "Net Income", d));
  const divData = dates.map((d: any) =>
    Math.abs(getValFromSplit(cfSplit, "Cash Dividends Paid", d) || getValFromSplit(cfSplit, "Common Stock Dividend Paid", d) || 0),
  );
  const repoData = dates.map((d: any) =>
    Math.abs(getValFromSplit(cfSplit, "Repurchase of Capital Stock", d) || 0),
  );

  const divRatio = niData.map((ni, i) => (ni > 0 ? divData[i] / ni : 0));
  const totalRatio = niData.map((ni, i) =>
    ni > 0 ? (divData[i] + repoData[i]) / ni : 0,
  );

  return {
    labels: dates.map((d: any) => d.split("T")[0]),
    datasets: [
      {
        type: "bar",
        label: "純利益",
        data: niData,
        backgroundColor: "rgba(44, 160, 44, 0.6)",
        yAxisID: "y",
      },
      {
        type: "bar",
        label: "配当金",
        data: divData,
        backgroundColor: "rgba(174, 199, 232, 0.6)",
        yAxisID: "y",
      },
      {
        type: "bar",
        label: "自社株買い",
        data: repoData,
        backgroundColor: "rgba(31, 119, 180, 0.6)",
        yAxisID: "y",
      },
      {
        type: "line",
        label: "配当性向",
        data: divRatio,
        borderColor: "#ffbb78",
        yAxisID: "y1",
      },
      {
        type: "line",
        label: "総還元性向",
        data: totalRatio,
        borderColor: "#ff7f0e",
        yAxisID: "y1",
      },
    ],
  };
}

function generateSegmentChartNative(splitData: any, title: string) {
  if (!splitData || !splitData.columns || !splitData.data) return null;
  const labels = splitData.columns.slice(1).sort();
  const segments = splitData.data;

  const colors = [
    "rgba(31, 119, 180, 0.7)",
    "rgba(255, 127, 14, 0.7)",
    "rgba(44, 160, 44, 0.7)",
    "rgba(214, 39, 40, 0.7)",
    "rgba(148, 103, 189, 0.7)",
    "rgba(140, 86, 75, 0.7)",
    "rgba(227, 119, 194, 0.7)",
    "rgba(127, 127, 127, 0.7)",
    "rgba(188, 189, 34, 0.7)",
    "rgba(23, 190, 207, 0.7)",
  ];

  return {
    labels: labels.map((d: any) => d.split("T")[0]),
    datasets: segments.map((seg: any, i: number) => ({
      label: seg[0],
      data: labels.map((l: any) => seg[splitData.columns.indexOf(l)]),
      backgroundColor: colors[i % colors.length],
    })),
  };
}

// 補助関数: 指定されたラベル、データ、背景色、境界色を持つデータセットを作成
function createChartJsDataset(label: string, data: any[], colorIdx: number, type: 'bar' | 'line' = 'bar', yAxisID: string = 'y') {
  const colors = [
    { bg: "rgba(54, 162, 235, 0.5)", border: "rgb(54, 162, 235)" },
    { bg: "rgba(255, 99, 132, 0.5)", border: "rgb(255, 99, 132)" },
    { bg: "rgba(75, 192, 192, 0.5)", border: "rgb(75, 192, 192)" },
    { bg: "rgba(255, 159, 64, 0.5)", border: "rgb(255, 159, 64)" },
  ];
  const color = colors[colorIdx % colors.length];
  return {
    label,
    data,
    type,
    backgroundColor: color.bg,
    borderColor: color.border,
    borderWidth: 1,
    yAxisID
  };
}

// 補助関数: DefeatBeta (Split形式) から ChartJs 用のシリーズ形式に変換 (旧互換用、徐々に移行)
function convertDBFinancials(splitData: any, allowedKeys?: string[], offsetGroupMap?: Record<string, string>, suffix: string = '', visible: boolean = true) {

  if (!splitData || !splitData.columns || !splitData.data) return { data: [] };
  
  const columns = splitData.columns; // ["Breakdown", "TTM", "2025-06-30", ...]
  
  // TTM (Trailing Twelve Months) は四半期データに混ざることがあるため除外
  const dataColIndices = columns.map((_c: any, i: number) => i)
    .filter((i: number) => i > 0 && columns[i] !== 'TTM')
    .reverse(); // 過去から現在へ

  const dates = dataColIndices.map((i: number) => columns[i]);
  
  // マッピング: 英語名 -> 日本語名 (ChartJs.astro のロジック用)
  const translationMap: Record<string, string> = {
    "Total Assets": "総資産",
    "Current Assets": "流動資産",
    "Total Current Assets": "流動資産",
    "Total Current Liabilities": "流動負債",
    "Current Liabilities": "流動負債",
    "Total Non Current Assets": "固定資産",
    "Total non-current assets": "固定資産",
    "Total Liabilities Net Minority Interest": "負債合計",
    "Total Liabilities": "負債合計",
    "Total Non Current Liabilities Net Minority Interest": "固定負債",
    "Total Non Current Liabilities": "固定負債",
    "Stockholders Equity": "純資産",
    "Stockholders' Equity": "純資産",
    "Total Equity Gross Minority Interest": "純資産",
    "Total Equity": "純資産",
    "Total Revenue": "売上高",
    "Gross Profit": "売上総利益",
    "Operating Income": "営業利益",
    "Net Income": "純利益",
    "Net Income Common Stockholders": "純利益",
    "Net Income Continuous Operations": "純利益",
    "Net Income from Continuing Operations": "純利益",
    "Operating Cash Flow": "営業CF",
    "Investing Cash Flow": "投資CF",
    "Financing Cash Flow": "財務CF",
    "Free Cash Flow": "フリーCF",
    "Cash Dividends Paid": "配当金支払",
    "Common Stock Dividend Paid": "配当金支払",
    "Repurchase of Capital Stock": "自社株買い"
  };

  // 個別カラーマッピング
  const colorMap: Record<string, string> = {
    "固定資産": "#1f77b4",
    "流動資産": "#aec7e8",
    "純資産": "#2ca02c",
    "固定負債": "#ff7f0e",
    "流動負債": "#ffbb78",
    "売上高": "#aec7e8",
    "売上総利益": "#1f77b4",
    "営業利益": "#ffbb78",
    "純利益": "#2ca02c",
    "営業CF": "#aec7e8",
    "投資CF": "#1f77b4",
    "財務CF": "#ffbb78",
    "フリーCF": "#9467bd",
    "配当金支払": "#aec7e8",
    "自社株買い": "#1f77b4"
  };

  const filteredRows = splitData.data.filter((row: any) => !allowedKeys || allowedKeys.includes(row[0]));
  
  // 全ての項目でデータが空(null or 0)のインデックスを特定して除外
  const validRelativeIndices: number[] = [];
  for (let i = 0; i < dates.length; i++) {
    const colIdx = dataColIndices[i];
    const hasData = filteredRows.some((row: any) => {
      const v = row[colIdx];
      return v !== null && v !== 0 && v !== "*";
    });
    if (hasData) validRelativeIndices.push(i);
  }

  const finalDates = validRelativeIndices.map(i => dates[i]);

  const traces = filteredRows.map((row: any) => {
    const name = row[0];
    const values = validRelativeIndices.map(i => {
      const colIdx = dataColIndices[i];
      const v = row[colIdx];
      return (v === "*" || v === null) ? null : parseFloat(v);
    });
    
    const translatedName = translationMap[name] || name;
    const finalName = suffix ? `${translatedName} (${suffix})` : translatedName;
    return {
      name: finalName,
      originalName: name, // 元の名前を保持
      x: finalDates,
      y: values,
      type: 'bar',
      visible: visible,
      offsetgroup: offsetGroupMap ? offsetGroupMap[name] : undefined,
      marker: { color: colorMap[translatedName] }
    };
  });

  return { data: traces };
}

function convertDBSegments(splitData: any, _title: string) {
  if (!splitData || !splitData.columns || !splitData.data) return null;
  
  const columns = splitData.columns; // ["symbol", "report_date", "Mac", "Services", ...]
  const segmentCols = columns.filter((c: string) => c !== 'symbol' && c !== 'report_date');
  const colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
                  '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'];

  // 1. 四半期トレースの準備 (データのない日付、またはデータが不十分な期間を除外)
  const validIndicesQ: number[] = [];
  splitData.data.forEach((row: any, i: number) => {
    // 2017年以前の不十分なデータを除外 (ユーザー要望)
    const year = parseInt(row[1].substring(0, 4));
    if (year < 2017) return;

    const hasSignificantData = segmentCols.some((col: string) => {
      // 'Corporate and Other' しかデータがない場合は不十分とみなす (MSFTの古いデータ対策)
      if (col === 'Corporate and Other') return false;
      const idx = columns.indexOf(col);
      const v = row[idx];
      return v !== null && v !== 0 && v !== "*";
    });
    if (hasSignificantData) validIndicesQ.push(i);
  });

  const qTraces = segmentCols.map((col: string, i: number) => {
    const colIdx = columns.indexOf(col);
    const dates = validIndicesQ.map(idx => splitData.data[idx][1]);
    const values = validIndicesQ.map(idx => splitData.data[idx][colIdx]);
    
    return {
      name: `${col} (四半期)`,
      x: dates,
      y: values,
      type: 'bar',
      marker: { color: colors[i % colors.length] },
      visible: false, // 四半期はデフォルト非表示
      _stack: 'segment'
    };
  });

  // 2. 通年データの集計 (年度ごと)
  const annualMap: Record<string, Record<string, number>> = {};
  splitData.data.forEach((row: any) => {
    const year = row[1].substring(0, 4);
    if (!annualMap[year]) {
      annualMap[year] = {};
      segmentCols.forEach((col: string) => annualMap[year][col] = 0);
    }
    segmentCols.forEach((col: string) => {
      const colIdx = columns.indexOf(col);
      annualMap[year][col] += (row[colIdx] || 0);
    });
  });

  // データが完全に空の年度、または不十分な年度(2017年以前など)を除外
  const years = Object.keys(annualMap).sort().filter(y => {
    const yearInt = parseInt(y);
    if (yearInt < 2017) return false;

    return segmentCols.some((col: string) => {
      if (col === 'Corporate and Other') return false;
      return annualMap[y][col] !== 0;
    });
  });

  const aTraces = segmentCols.map((col: string, i: number) => {
    return {
      name: `${col} (通年)`,
      x: years,
      y: years.map(y => annualMap[y][col]),
      type: 'bar',
      marker: { color: colors[i % colors.length] },
      visible: true, // 通年はデフォルト表示
      _stack: 'segment'
    };
  });

  return {
    data: [...qTraces, ...aTraces]
  };
}

/**
 * 株主還元 (Total Payout) チャートデータの生成
 */
function generatePayoutChart(isData: any, cfData: any, suffix: string = '', visible: boolean = true) {
  if (!isData || !cfData || !isData.data || !cfData.data) return { data: [] };

  const niName = suffix ? `純利益 (${suffix})` : "純利益";
  const divName = suffix ? `配当金 (${suffix})` : "配当金";
  const buyName = suffix ? `自社株買い (${suffix})` : "自社株買い";
  const ratioName = suffix ? `総還元性向 (${suffix})` : "総還元性向";

  const netIncomeTrace = isData.data.find((t: any) => t.name === (suffix ? `純利益 (${suffix})` : "純利益"));
  
  // キャッシュフローから配当と自社株買いを探す
  const dividendsTrace = cfData.data.find((t: any) => t.name === (suffix ? `配当金支払 (${suffix})` : "配当金支払") || t.originalName === "Cash Dividends Paid");
  const buybacksTrace = cfData.data.find((t: any) => t.name === (suffix ? `自社株買い (${suffix})` : "自社株買い") || t.originalName === "Repurchase of Capital Stock");

  if (!netIncomeTrace || (!dividendsTrace && !buybacksTrace)) return { data: [] };

  // データが存在する日付のみをフィルタリング
  const dates = (isData.data[0]?.x || []).filter((d: string) => {
    const ni = netIncomeTrace.x.indexOf(d) !== -1 && netIncomeTrace.y[netIncomeTrace.x.indexOf(d)] !== null;
    const div = dividendsTrace && dividendsTrace.x.indexOf(d) !== -1 && dividendsTrace.y[dividendsTrace.x.indexOf(d)] !== null;
    const buy = buybacksTrace && buybacksTrace.x.indexOf(d) !== -1 && buybacksTrace.y[buybacksTrace.x.indexOf(d)] !== null;
    return ni && (div || buy);
  });

  if (dates.length === 0) return { data: [] };

  const traces: any[] = [];
  
  // Helper to get value by date from a trace
  const getValueByDate = (trace: any, date: string) => {
    const idx = trace.x.indexOf(date);
    return idx !== -1 ? trace.y[idx] : null;
  };

  // 1. 純利益 (左側: group-0)
  const niValues = dates.map((d: string) => getValueByDate(netIncomeTrace, d));
  traces.push({
    name: niName,
    x: dates,
    y: niValues,
    type: 'bar',
    offsetgroup: '0',
    visible: visible,
    marker: { color: "#2ca02c" }
  });

  // 2. 配当金 (右側下: group-1)
  if (dividendsTrace) {
    traces.push({
      name: divName,
      x: dates,
      y: dates.map((d: string) => {
        const v = getValueByDate(dividendsTrace, d);
        return v ? Math.abs(v) : 0;
      }),
      type: 'bar',
      offsetgroup: '1',
      visible: visible,
      marker: { color: "#aec7e8" }
    });
  }

  // 3. 自社株買い (右側上: group-1)
  if (buybacksTrace) {
    traces.push({
      name: buyName,
      x: dates,
      y: dates.map((d: string) => {
        const v = getValueByDate(buybacksTrace, d);
        return v ? Math.abs(v) : 0;
      }),
      type: 'bar',
      offsetgroup: '1',
      visible: visible,
      marker: { color: "#1f77b4" }
    });
  }

  // 4. 配当性向 (第2軸: 折れ線)
  if (dividendsTrace) {
    const divRatioName = suffix ? `配当性向 (${suffix})` : "配当性向";
    const dividendRatio = dates.map((d: string) => {
      const ni = getValueByDate(netIncomeTrace, d);
      if (!ni || ni <= 0) return null;
      const div = Math.abs(getValueByDate(dividendsTrace, d) || 0);
      return div / ni;
    });

    traces.push({
      name: divRatioName,
      x: dates,
      y: dividendRatio,
      type: 'scatter',
      mode: 'lines+markers',
      yaxis: 'y2',
      visible: visible,
      marker: { color: "#ffbb78" }
    });
  }

  // 5. 総還元性向 (第2軸: 折れ線)
  const payoutRatio = dates.map((d: string) => {
    const ni = getValueByDate(netIncomeTrace, d);
    if (!ni || ni <= 0) return null;
    const div = dividendsTrace ? Math.abs(getValueByDate(dividendsTrace, d) || 0) : 0;
    const buy = buybacksTrace ? Math.abs(getValueByDate(buybacksTrace, d) || 0) : 0;
    return (div + buy) / ni;
  });

  traces.push({
    name: ratioName,
    x: dates,
    y: payoutRatio,
    type: 'scatter',
    mode: 'lines+markers',
    yaxis: 'y2',
    visible: visible,
    marker: { color: "#ff7f0e" }
  });

  return { 
    data: traces,
    layout: {
      barmode: 'group',
      yaxis2: { title: '総還元性向', overlaying: 'y', side: 'right', tickformat: '.0%' }
    }
  };
}

/**
 * 配当履歴チャートデータの生成
 */
function generateDividendChart(dividends: any[], chartResult: any, getPriceAtDate: (d: Date) => number | null) {
  if (!dividends || !Array.isArray(dividends) || dividends.length === 0) return null;

  const now = new Date();
  const currentYear = now.getFullYear();
  const quotes = chartResult?.quotes || [];

  // 年ごとの配当回数を集計 (利回り計算用)
  const frequencyMap: Record<number, number> = {};
  dividends.forEach(d => {
    const y = new Date(d.date).getFullYear();
    frequencyMap[y] = (frequencyMap[y] || 0) + 1;
  });

  // 1. 権利落日別の生データ (過去10年程度)
  const sortedDivs = [...dividends].sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime()).slice(-40);
  
  const rawDivX = sortedDivs.map(d => d.date.toISOString().split('T')[0]);
  const rawDivY = sortedDivs.map(d => d.amount);
  const rawYieldY = sortedDivs.map(d => {
    const price = getPriceAtDate(new Date(d.date));
    const year = new Date(d.date).getFullYear();
    const freq = frequencyMap[year] || 4;
    return price ? (d.amount * freq) / price : null;
  });

  const rawTraces = [
    {
      name: "配当金 (権利落日別)",
      x: rawDivX,
      y: rawDivY,
      type: 'bar',
      visible: true,
      marker: { color: "#1f77b4" }
    },
    {
      name: "配当利回り (権利落日別)",
      x: rawDivX,
      y: rawYieldY,
      type: 'scatter',
      mode: 'lines+markers',
      yaxis: 'y2',
      visible: true,
      marker: { color: "#ff6b01" }
    }
  ];

  // 2. 年間推移の集計と推定
  const yearsSet = new Set(dividends.map(d => new Date(d.date).getFullYear()));
  const years = Array.from(yearsSet).sort((a, b) => a - b);
  const recentYears = years.filter(y => y > currentYear - 11 && y <= currentYear);

  // 推定ロジック: 前年の配当回数を基準にする
  const lastFullYear = currentYear - 1;
  const frequency = frequencyMap[lastFullYear] || 4; 
  const lastYearDivs = dividends.filter(d => new Date(d.date).getFullYear() === lastFullYear);
  const lastAmount = lastYearDivs.length > 0 ? lastYearDivs[lastYearDivs.length - 1].amount : 0;

  const getPriceAtYearStart = (year: number) => {
    const firstQuote = quotes.find((q: any) => q.date && new Date(q.date).getFullYear() === year && q.adjclose !== null);
    return firstQuote ? firstQuote.adjclose : null;
  };

  const actualY: (number | null)[] = [];
  const estimatedY: (number | null)[] = [];
  const yieldsY: (number | null)[] = [];

  recentYears.forEach(year => {
    const yearDivs = dividends.filter(d => new Date(d.date).getFullYear() === year);
    const actualSum = yearDivs.reduce((sum, d) => sum + d.amount, 0);
    let totalForYear = actualSum;
    
    if (year < currentYear) {
      actualY.push(actualSum);
      estimatedY.push(0);
    } else {
      actualY.push(actualSum);
      const remainingCount = Math.max(0, frequency - yearDivs.length);
      const estimate = remainingCount * (actualSum > 0 ? (yearDivs[yearDivs.length - 1].amount) : lastAmount);
      estimatedY.push(estimate);
      totalForYear += estimate;
    }

    const startPrice = getPriceAtYearStart(year);
    yieldsY.push(startPrice ? totalForYear / startPrice : null);
  });

  const annualTraces = [
    {
      name: "実績配当 (年間推移)",
      x: recentYears.map(String),
      y: actualY,
      type: 'bar',
      visible: false,
      offsetgroup: 'annual',
      marker: { color: "#1f77b4" }
    },
    {
      name: "推定配当 (年間推移)",
      x: recentYears.map(String),
      y: estimatedY,
      type: 'bar',
      visible: false,
      offsetgroup: 'annual',
      marker: { color: "#aec7e8" }
    },
    {
      name: "配当利回り (年間推移)",
      x: recentYears.map(String),
      y: yieldsY,
      type: 'scatter',
      mode: 'lines+markers',
      yaxis: 'y2',
      visible: false,
      marker: { color: "#ff6b01" }
    }
  ];

  return { 
    data: [
      ...rawTraces.map(t => ({ ...t, name: t.name, label: t.name })),
      ...annualTraces.map(t => ({ ...t, name: t.name, label: t.name }))
    ],
    layout: {
      barmode: 'stack',
      yaxis2: { title: '配当利回り', overlaying: 'y', side: 'right', tickformat: '.1%' }
    }
  };
}

function formatRiskReturnGroups(metricsList: any[], symbols: string[]) {
  const datasets: any[] = [];
  const periods = [
    { key: '1M', label: '1ヶ月' },
    { key: '3M', label: '3ヶ月' },
    { key: '6M', label: '6ヶ月' },
    { key: 'YTD', label: '年初来' },
    { key: '1Y', label: '1年' },
    { key: '3Y', label: '3年' },
    { key: '5Y', label: '5年' },
    { key: '10Y', label: '10年' }
  ];

  const colors = ['#ff6b01', '#006cac', '#22c55e'];

  periods.forEach(p => {
    metricsList.forEach((m, i) => {
      if (!m || m[`HV_${p.key}`] === null || m[`Ret_${p.key}`] === null) return;

      datasets.push({
        name: `${symbols[i]} (${p.label})`,
        x: [m[`HV_${p.key}`]],
        y: [m[`Ret_${p.key}`]],
        text: [symbols[i]],
        type: 'scatter',
        mode: 'markers+text',
        marker: { 
          size: i === 0 ? 12 : 8, 
          color: colors[i],
          symbol: i === 0 ? 'circle' : 'diamond'
        },
        textposition: 'top center',
        visible: p.label === '1年', // ここが true なら初期表示される
        _periodVisible: p.label === '1年' // ChartJs.astro の内部フラグ用
      });
    });
  });

  return {
    data: datasets,
    layout: {
      xaxis: { title: 'リスク (ボラティリティ)', tickformat: '.0%' },
      yaxis: { title: 'リターン (年率換算)', tickformat: '.0%' },
      hovermode: 'closest'
    }
  };
}

function addMarginRatiosToIS(is: any) {
  if (!is || !is.data || is.data.length === 0) return is;

  // 最初のトレース名からサフィックスを抽出 (例: "売上高 (通年)" -> " (通年)")
  const firstLabel = is.data[0].name;
  const suffixMatch = firstLabel.match(/\s\([^)]+\)$/);
  const suffix = suffixMatch ? suffixMatch[0] : '';

  const rev = is.data.find((t: any) => t.name === `売上高${suffix}`);
  const gross = is.data.find((t: any) => t.name === `売上総利益${suffix}`);
  const op = is.data.find((t: any) => t.name === `営業利益${suffix}`);
  const net = is.data.find((t: any) => t.name === `純利益${suffix}`);

  if (!rev) return is;

  const margins = [
    { source: gross, name: `売上総利益率${suffix}`, color: "#1f77b4" },
    { source: op, name: `営業利益率${suffix}`, color: "#ffbb78" },
    { source: net, name: `純利益率${suffix}`, color: "#2ca02c" }
  ];

  margins.forEach(m => {
    if (!m.source) return;
    is.data.push({
      name: m.name,
      x: rev.x,
      y: m.source.y.map((v: number | null, i: number) => (v !== null && rev.y[i]) ? v / rev.y[i] : null),
      type: 'scatter',
      mode: 'lines+markers',
      yaxis: 'y2', // 第2 Y 軸の指定（ChartJs.astro 側で yAxisID にマップ）
      marker: { color: m.color },
      line: { color: m.color, width: 2 }
    });
  });

  return is;
}

main().catch(console.error);
