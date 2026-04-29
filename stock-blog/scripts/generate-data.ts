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

      // 2. YFinance (TypeScript) から詳細データを取得
      const [quote, summary, riskMetricsList, perfData, chartResult] = await Promise.all([
        yahooFinance.quote(stock.Symbol_YF),
        yahooFinance.quoteSummary(stock.Symbol_YF, {
          modules: ['financialData', 'defaultKeyStatistics', 'recommendationTrend']
        }).catch(() => ({})),
        Promise.all([
          calculateRiskMetrics(stock.Symbol_YF),
          calculateRiskMetrics(targetEtf),
          calculateRiskMetrics('SPY')
        ]),
        generatePerformanceChartData(stock.Symbol_YF, targetEtf),
        yahooFinance.chart(stock.Symbol_YF, { 
          period1: '2010-01-01', 
          interval: '1d' 
        }).catch(() => ({ events: { dividends: [] } }))
      ]);

      const dividends = chartResult.events?.dividends || [];
      const financialData = summary.financialData || {};
      const stats = summary.defaultKeyStatistics || {};

      // リスク・リターンの統合 (全期間タブ対応)
      const riskReturnData = formatRiskReturnGroups(riskMetricsList, [stock.Symbol, `Sector ${targetEtf}`, 'S&P 500']);

      // 3. データの統合 (Astroレイアウトの期待値に合わせる)
      const is = convertDBFinancials(dbData.financials?.income_statement);
      const bs = convertDBFinancials(dbData.financials?.balance_sheet);
      const cf = convertDBFinancials(dbData.financials?.cash_flow);
      
      // 取引所マッピング (TradingView 互換)
      const exchangeMap: Record<string, string> = {
        'NMS': 'NASDAQ',
        'NYQ': 'NYSE',
        'NCM': 'NASDAQ',
        'NGM': 'NASDAQ',
        'PCX': 'NYSE',
        'ASE': 'AMEX'
      };
      const exchange = exchangeMap[quote.exchange] || quote.exchange;
      const isFinancial = ["Financials", "Real Estate"].includes(stock['GICS Sector']);

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
          is: is,
          bs: bs,
          cf: cf,
          tp: generatePayoutChart(is, cf),
          dps: generateDividendChart(dividends),
          segment: convertDBSegments(dbData.segments, 'セグメント別収益'),
          geo: convertDBSegments(dbData.geography, '地域別収益')
        },

        highlights: {
          revenue_growth: financialData.revenueGrowth,
          roe: financialData.returnOnEquity,
          operating_margins: financialData.operatingMargins,
          pe_forward: quote.forwardPE,
          pe_ttm: quote.trailingPE,
          dividend_yield: quote.dividendYield,
          debt_to_equity: financialData.debtToEquity,
          earnings_growth: financialData.earningsGrowth,
          profit_margins: financialData.profitMargins
        },

        analyst_ratings: {
          recommendationKey: quote.averageAnalystRating?.split(' - ')[1] || "hold",
          targetMeanPrice: financialData.targetMeanPrice,
          targetHighPrice: financialData.targetHighPrice,
          targetLowPrice: financialData.targetLowPrice,
          targetMedianPrice: financialData.targetMedianPrice,
          numberOfAnalystOpinions: financialData.numberOfAnalystOpinions,
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
      console.log(`  - [${stock.Symbol}] Successfully saved report and updating stocks list.`);
      updatedStocks.push({ ...stock, Daily_Change: quote.regularMarketChangePercent / 100 });

    } catch (e) {
      console.error(`Failed to process ${stock.Symbol}:`, e);
    }
    
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  fs.writeFileSync(STOCKS_JSON_PATH, JSON.stringify(updatedStocks, null, 2));
  console.log('--- Integration Finished! ---');
}

// 補助関数: DefeatBeta (Split形式) から ChartJs (Plotly風) 用に変換
function convertDBFinancials(splitData: any) {
  if (!splitData || !splitData.columns || !splitData.data) return null;
  
  const columns = splitData.columns; // ["Breakdown", "2025-06-30", ...]
  const dates = columns.slice(1).reverse(); // 過去から現在の順に並べる
  
  // マッピング: 英語名 -> 日本語名 (ChartJs.astro のロジック用)
  const translationMap: Record<string, string> = {
    "Total Assets": "総資産",
    "Total Current Assets": "流動資産",
    "Total Non Current Assets": "固定資産",
    "Total Liabilities Net Minority Interest": "負債合計",
    "Current Liabilities": "流動負債",
    "Total Non Current Liabilities Net Minority Interest": "固定負債",
    "Stockholders Equity": "純資産",
    "Total Equity Gross Minority Interest": "純資産",
    "Total Revenue": "売上高",
    "Gross Profit": "売上総利益",
    "Operating Income": "営業利益",
    "Net Income": "純利益",
    "Net Income Common Stockholders": "純利益",
    "Operating Cash Flow": "営業CF",
    "Investing Cash Flow": "投資CF",
    "Financing Cash Flow": "財務CF",
    "Free Cash Flow": "フリーCF",
    "Cash Dividends Paid": "配当金支払",
    "Common Stock Dividend Paid": "配当金支払",
    "Repurchase of Capital Stock": "自社株買い"
  };

  const traces = splitData.data.map((row: any) => {
    const name = row[0];
    const values = row.slice(1).map((v: any) => (v === "*" || v === null) ? null : parseFloat(v)).reverse();
    
    return {
      name: translationMap[name] || name,
      originalName: name, // 元の名前を保持
      x: dates,
      y: values,
      type: 'bar',
      visible: true
    };
  });

  return { data: traces };
}

function convertDBSegments(splitData: any, title: string) {
  if (!splitData) return null;
  
  // ChartJs.astro が期待する Plotly 形式に変換
  // data: [{ name, x: [labels], y: [values], type: 'bar' }]
  if (splitData.labels && splitData.datasets) {
    return {
      data: splitData.datasets.map((ds: any, i: number) => ({
        name: i === 0 ? title : `${title} ${i}`,
        x: splitData.labels,
        y: ds.data,
        type: 'bar'
      }))
    };
  }

  // フォールバック: Split 形式の場合
  if (splitData.columns && splitData.data) {
    const labels = splitData.columns.filter((c: string) => c !== 'symbol' && c !== 'report_date');
    const data = splitData.data[splitData.data.length - 1]?.slice(2) || [];
    return {
      data: [{
        name: title,
        x: labels,
        y: data,
        type: 'bar'
      }]
    };
  }

  return null;
}

/**
 * 株主還元 (Total Payout) チャートデータの生成
 */
function generatePayoutChart(isData: any, cfData: any) {
  if (!isData || !cfData) return null;

  const dates = isData.data[0]?.x || [];
  const netIncomeTrace = isData.data.find((t: any) => t.name === "純利益");
  
  // キャッシュフローから配当と自社株買いを探す
  const dividendsTrace = cfData.data.find((t: any) => t.name === "配当金支払" || t.originalName === "Cash Dividends Paid");
  const buybacksTrace = cfData.data.find((t: any) => t.name === "自社株買い" || t.originalName === "Repurchase of Capital Stock");

  if (!netIncomeTrace || (!dividendsTrace && !buybacksTrace)) return null;

  const traces: any[] = [];
  
  if (dividendsTrace) {
    traces.push({
      name: "配当金",
      x: dates,
      y: dividendsTrace.y.map((v: number | null) => v ? Math.abs(v) : 0),
      type: 'bar'
    });
  }

  if (buybacksTrace) {
    traces.push({
      name: "自社株買い",
      x: dates,
      y: buybacksTrace.y.map((v: number | null) => v ? Math.abs(v) : 0),
      type: 'bar'
    });
  }

  traces.push({
    name: "純利益",
    x: dates,
    y: netIncomeTrace.y,
    type: 'line'
  });

  // 総還元性向の計算
  const payoutRatio = dates.map((_: any, i: number) => {
    const ni = netIncomeTrace.y[i];
    if (!ni || ni <= 0) return null;
    const div = dividendsTrace ? Math.abs(dividendsTrace.y[i] || 0) : 0;
    const buy = buybacksTrace ? Math.abs(buybacksTrace.y[i] || 0) : 0;
    return (div + buy) / ni;
  });

  traces.push({
    name: "総還元性向",
    x: dates,
    y: payoutRatio,
    type: 'line',
    yaxis: 'y2'
  });

  return { data: traces };
}

/**
 * 配当履歴チャートデータの生成
 */
function generateDividendChart(dividends: any[]) {
  if (!dividends || !Array.isArray(dividends) || dividends.length === 0) return null;

  // 権利落日別の生データ (過去10年程度)
  const sortedDivs = [...dividends].sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime()).slice(-40);
  const rawTraces = [{
    name: "配当金 (権利落日別)",
    x: sortedDivs.map(d => d.date.toISOString().split('T')[0]),
    y: sortedDivs.map(d => d.amount),
    type: 'bar',
    visible: true
  }];

  // 年間推移の集計
  const annualDivs: Record<number, number> = {};
  dividends.forEach(d => {
    const year = new Date(d.date).getFullYear();
    annualDivs[year] = (annualDivs[year] || 0) + d.amount;
  });

  const years = Object.keys(annualDivs).map(Number).sort((a, b) => a - b);
  // 現在の年は未完了の可能性があるため、直近10年分
  const recentYears = years.slice(-11); 
  
  const annualTraces = [{
    name: "配当金 (年間推移)",
    x: recentYears.map(String),
    y: recentYears.map(y => annualDivs[y]),
    type: 'bar',
    visible: false
  }];

  return { 
    data: [
      ...rawTraces.map(t => ({ ...t, name: t.name, label: t.name })),
      ...annualTraces.map(t => ({ ...t, name: t.name, label: t.name }))
    ]
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
