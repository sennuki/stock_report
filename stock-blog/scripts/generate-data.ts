import * as fs from 'fs';
import * as path from 'path';
import { execSync } from 'child_process';
import { fetchSp500Companies, getBrokerageAvailability } from './lib/market-data.ts';
import { calculateRiskMetrics, yahooFinance } from './lib/risk-analysis.ts';
import { generatePerformanceChartData } from './lib/performance-comparison.ts';

const REPORTS_DIR = path.join(process.cwd(), 'public/reports');
const STOCKS_JSON_PATH = path.join(process.cwd(), 'src/data/stocks.json');
const PYTHON_SCRIPT = path.join(process.cwd(), '../code_master_ref/export_stock_data.py');

async function main() {
  console.log('--- Hybrid Data Pipeline (TypeScript + DefeatBeta Python) Started ---');

  if (!fs.existsSync(REPORTS_DIR)) fs.mkdirSync(REPORTS_DIR, { recursive: true });

  let allStocks = await fetchSp500Companies();
  const brokerages = await getBrokerageAvailability();
  
  let stocks = [];
  if (process.env.TEST_MODE === 'true') {
    stocks = [
      { Symbol: 'MSFT', Symbol_YF: 'MSFT', Security: 'Microsoft Corp', 'GICS Sector': 'Information Technology', 'GICS Sub-Industry': 'Systems Software', Exchange: 'NASDAQ' },
      { Symbol: 'AAPL', Symbol_YF: 'AAPL', Security: 'Apple Inc', 'GICS Sector': 'Information Technology', 'GICS Sub-Industry': 'Technology Hardware Storage & Peripherals', Exchange: 'NASDAQ' },
      { Symbol: 'PHM', Symbol_YF: 'PHM', Security: 'PulteGroup Inc', 'GICS Sector': 'Consumer Discretionary', 'GICS Sub-Industry': 'Homebuilding', Exchange: 'NYSE' }
    ];
    if (allStocks.length === 0) allStocks = stocks;
  } else {
    stocks = allStocks;
  }
  
  console.log(`Processing ${stocks.length} stocks...`);

  const updatedStocks = [];

  for (const stock of stocks) {
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
      } catch (e) {
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
        'Homebuilding': 'XHB' // Sub-Industry 用
      };

      const targetEtf = sectorEtfMap[stock['GICS Sub-Industry']] || sectorEtfMap[stock['GICS Sector']] || 'SPY';

      // 2. YFinance (TypeScript) から速報データを取得
      const [quote, riskMetricsList, perfData] = await Promise.all([
        yahooFinance.quote(stock.Symbol_YF),
        Promise.all([
          calculateRiskMetrics(stock.Symbol_YF),
          calculateRiskMetrics(targetEtf),
          calculateRiskMetrics('SPY')
        ]),
        generatePerformanceChartData(stock.Symbol_YF, targetEtf)
      ]);

      // リスク・リターンの統合 (全期間タブ対応)
      const riskReturnData = formatRiskReturnGroups(riskMetricsList, [stock.Symbol, `Sector ${targetEtf}`, 'S&P 500']);

      // 3. データの統合 (Astroレイアウトの期待値に合わせる)
      const reportData = {
        symbol: stock.Symbol,
        symbol_yf: stock.Symbol_YF,
        security: stock.Security,
        security_ja: stock.Security_JA || null,
        business_summary_ja: dbData.dcf?.business_summary_ja || null,
        sector: stock['GICS Sector'],
        sub_industry: stock['GICS Sub-Industry'],
        exchange: quote.exchange,
        full_symbol: `${quote.exchange}:${stock.Symbol.replace('-', '.')}`,
        
        // 証券会社フラグ
        is_available_monex: brokerages.monex.has(stock.Symbol),
        is_available_rakuten: brokerages.rakuten.has(stock.Symbol),
        is_available_sbi: brokerages.sbi.has(stock.Symbol),
        is_available_moomoo: brokerages.moomoo.has(stock.Symbol),

        // DefeatBeta由来の詳細なDCF
        dcf_valuation: dbData.dcf,
        
        charts: {
          risk_return: riskReturnData,
          performance: perfData,
          // 財務チャートデータ (PythonのDataFrame形式から変換が必要だが一旦そのまま)
          is: convertDBFinancials(dbData.financials?.income_statement),
          bs: convertDBFinancials(dbData.financials?.balance_sheet),
          cf: convertDBFinancials(dbData.financials?.cash_flow),
          segment: convertDBSegments(dbData.segments, 'セグメント別収益'),
          geo: convertDBSegments(dbData.geography, '地域別収益')
        },

        highlights: {
          revenue_growth: quote.revenueGrowth,
          roe: quote.returnOnEquity,
          operating_margins: quote.operatingMargins,
          pe_forward: quote.forwardPE,
          pe_ttm: quote.trailingPE,
          dividend_yield: quote.dividendYield,
          debt_to_equity: quote.debtToEquity
        },

        analyst_ratings: {
          recommendationKey: quote.averageAnalystRating?.split(' - ')[1] || "hold",
          targetMeanPrice: quote.targetMeanPrice,
          currentPrice: quote.regularMarketPrice
        },

        peers: {
          sub_industry: allStocks
            .filter(s => s['GICS Sub-Industry'] === stock['GICS Sub-Industry'] && s.Symbol !== stock.Symbol)
            .slice(0, 10)
            .map(p => ({ Symbol: p.Symbol, Symbol_YF: p.Symbol_YF })),
          sector: allStocks
            .filter(s => s['GICS Sector'] === stock['GICS Sector'] && s['GICS Sub-Industry'] !== stock['GICS Sub-Industry'])
            .slice(0, 10)
            .map(p => ({ Symbol: p.Symbol, Symbol_YF: p.Symbol_YF }))
        }
      };

      fs.writeFileSync(path.join(REPORTS_DIR, `${stock.Symbol_YF}.json`), JSON.stringify(reportData, null, 2));
      updatedStocks.push({ ...stock, Daily_Change: quote.regularMarketChangePercent / 100 });

    } catch (e) {
      console.error(`Failed to process ${stock.Symbol}:`, e);
    }
    
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  fs.writeFileSync(STOCKS_JSON_PATH, JSON.stringify(updatedStocks, null, 2));
  console.log('--- Integration Finished! ---');
}

// 補助関数: DefeatBeta (Split形式) から ChartJs 用に簡易変換
function convertDBFinancials(splitData: any) {
  if (!splitData) return null;
  // ここで本来は時系列データの整形を行う
  return { data: splitData }; 
}

function convertDBSegments(splitData: any, title: string) {
  if (!splitData) return null;
  return { 
    title,
    data: {
      labels: splitData.columns.filter((c: string) => c !== 'symbol' && c !== 'report_date'),
      datasets: [{
        data: splitData.data[0]?.slice(2) || []
      }]
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

// 古い formatRiskReturn は削除または置換
function formatRiskReturn(metrics: any) {
  return formatRiskReturnGroups([metrics], [metrics.Symbol]);
}

main().catch(console.error);
