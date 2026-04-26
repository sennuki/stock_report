import * as fs from 'fs';
import * as path from 'path';
import { fetchSp500Companies, getMonexAvailableSymbols, getRakutenAvailableSymbols } from './lib/market-data.ts';
import type { StockInfo } from './lib/market-data.ts';
import { calculateRiskMetrics, yahooFinance } from './lib/risk-analysis.ts';
import { translateSummary } from './lib/ai-processor.ts';
import { getFinancialData } from './lib/fundamentals.ts';
import { generatePerformanceChartData } from './lib/performance-comparison.ts';

const REPORTS_DIR = path.join(process.cwd(), 'public/reports');
const STOCKS_JSON_PATH = path.join(process.cwd(), 'src/data/stocks.json');

const SECTOR_ETF_MAP: Record<string, string> = {
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

async function main() {
  console.log('--- Stock Data Pipeline (TypeScript) Started ---');

  if (!fs.existsSync(REPORTS_DIR)) fs.mkdirSync(REPORTS_DIR, { recursive: true });

  // 1. 銘柄リスト取得
  let stocks = await fetchSp500Companies();
  if (process.env.TEST_MODE === 'true') {
    stocks = stocks.filter(s => s.Symbol === 'MSFT');
  }
  
  if (stocks.length === 0) {
    if (process.env.TEST_MODE === 'true') {
        stocks = [{ Symbol: 'MSFT', Symbol_YF: 'MSFT', Security: 'Microsoft Corp', 'GICS Sector': 'Information Technology', 'GICS Sub-Industry': 'Systems Software' }];
    } else {
        return;
    }
  }

  // 2. 証券会社取扱情報取得
  const monexMap = await getMonexAvailableSymbols();
  const rakutenSet = await getRakutenAvailableSymbols();

  console.log(`Processing ${stocks.length} stocks...`);

  const updatedStocks: StockInfo[] = [];

  for (const stock of stocks) {
    console.log(`Processing ${stock.Symbol}...`);
    
    try {
      const sectorEtf = SECTOR_ETF_MAP[stock['GICS Sector']] || 'SPY';

      // データの取得
      const [quote, summary, metrics, financials, perfData] = await Promise.all([
        yahooFinance.quote(stock.Symbol_YF),
        yahooFinance.quoteSummary(stock.Symbol_YF, {
          modules: ['summaryProfile', 'financialData', 'defaultKeyStatistics']
        }),
        calculateRiskMetrics(stock.Symbol_YF),
        getFinancialData(stock.Symbol_YF),
        generatePerformanceChartData(stock.Symbol_YF, sectorEtf)
      ]);

      stock.Security_JA = monexMap[stock.Symbol] || null;
      stock.Daily_Change = quote.regularMarketChangePercent ? quote.regularMarketChangePercent / 100 : 0;

      const reportPath = path.join(REPORTS_DIR, `${stock.Symbol_YF}.json`);
      let businessSummaryJa = null;
      if (fs.existsSync(reportPath)) {
        try {
          const existingData = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
          businessSummaryJa = existingData.business_summary_ja;
        } catch (e) {}
      }

      if (!businessSummaryJa && summary.summaryProfile?.longBusinessSummary && process.env.GEMINI_API_KEY) {
        businessSummaryJa = await translateSummary(summary.summaryProfile.longBusinessSummary);
      }

      const reportData: any = {
        symbol: stock.Symbol,
        symbol_yf: stock.Symbol_YF,
        security: stock.Security,
        security_ja: stock.Security_JA,
        business_summary_ja: businessSummaryJa,
        sector: stock['GICS Sector'],
        sub_industry: stock['GICS Sub-Industry'],
        exchange: quote.exchange,
        is_available_monex: !!monexMap[stock.Symbol],
        is_available_rakuten: rakutenSet.has(stock.Symbol),
        charts: {
          risk_return: metrics ? generateRiskReturnChartData(metrics, stock.Symbol_YF) : null,
          is: generateFinancialChart(financials.is, ['Total Revenue', 'Gross Profit', 'Operating Income', 'Net Income']),
          bs: generateFinancialChart(financials.bs, ['Total Assets', 'Total Liabilities Net Minority Interest', 'Stockholders Equity']),
          cf: generateFinancialChart(financials.cf, ['Operating Cash Flow', 'Investing Cash Flow', 'Financing Cash Flow', 'Free Cash Flow']),
          performance: perfData
        },
        highlights: {
            revenue_growth: summary.financialData?.revenueGrowth,
            earnings_growth: summary.financialData?.earningsGrowth,
            pe_ttm: summary.defaultKeyStatistics?.trailingPE,
            pe_forward: summary.defaultKeyStatistics?.forwardPE,
            dividend_yield: summary.defaultKeyStatistics?.dividendYield
        }
      };

      fs.writeFileSync(reportPath, JSON.stringify(reportData, null, 2));
      updatedStocks.push({ ...stock, Daily_Change: stock.Daily_Change });

    } catch (e) {
      console.error(`Failed to process ${stock.Symbol}:`, e);
      updatedStocks.push(stock);
    }

    await new Promise(resolve => setTimeout(resolve, 500));
  }

  fs.writeFileSync(STOCKS_JSON_PATH, JSON.stringify(updatedStocks, null, 2));
  console.log(`--- Finished! Processed ${updatedStocks.length} stocks ---`);
}

function generateFinancialChart(data: any, fields: string[]) {
  const annual = data.annual;
  if (!annual || Object.keys(annual).length === 0) return null;
  const dates = Object.keys(annual[fields[0]] || {}).sort().slice(-8);
  if (dates.length === 0) return null;
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
      data: dates.map(d => annual[field] ? annual[field][d] || 0 : 0),
      backgroundColor: colors[i % colors.length].bg,
      borderColor: colors[i % colors.length].border,
      borderWidth: 1
    }))
  };
}

function generateRiskReturnChartData(metrics: any, targetSymbol: string) {
  return {
    data: [{
      x: [metrics.HV_1Y], y: [metrics.Ret_1Y], text: [targetSymbol],
      mode: 'markers+text', name: targetSymbol, marker: { size: 16, color: 'red' }
    }],
    layout: { xaxis: { title: 'リスク', tickformat: '.0%' }, yaxis: { title: 'リターン', tickformat: '.0%' } }
  };
}

main().catch(console.error);
