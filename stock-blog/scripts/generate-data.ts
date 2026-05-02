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

      const targetEtf = sectorEtfMap[stock['GICS Sub-Industry']] || sectorEtfMap[stock['GICS Sector']] || 'SPY';

      // 2. YFinance (TypeScript) から詳細データを取得 (個別失敗を許容)
      const [quote, summary, riskMetricsList, perfData, chartResult] = await Promise.all([
        yahooFinance.quote(stock.Symbol_YF).catch((e: any) => {
          console.error(`    - Quote fetch failed for ${stock.Symbol}:`, e.message);
          return { regularMarketPrice: 0, regularMarketChangePercent: 0, exchange: 'NMS' };
        }),
        yahooFinance.quoteSummary(stock.Symbol_YF, {
          modules: ['financialData', 'defaultKeyStatistics', 'recommendationTrend', 'upgradeDowngradeHistory']
        }).catch((e: any) => {
          console.error(`    - Summary fetch failed for ${stock.Symbol}:`, e.message);
          return {};
        }),
        Promise.all([
          calculateRiskMetrics(stock.Symbol_YF).catch(() => null),
          calculateRiskMetrics(targetEtf).catch(() => null),
          calculateRiskMetrics('SPY').catch(() => null)
        ]).catch(() => [null, null, null]),
        generatePerformanceChartData(stock.Symbol_YF, targetEtf).catch((e: any) => {
          console.error(`    - Performance data fetch failed for ${stock.Symbol}:`, e.message);
          return null;
        }),
        yahooFinance.chart(stock.Symbol_YF, { 
          period1: '2010-01-01', 
          interval: '1d' 
        }).catch((e: any) => {
          console.error(`    - Chart fetch failed for ${stock.Symbol}:`, e.message);
          return { events: { dividends: [] } };
        })
      ]);

      const dividends = (chartResult as any)?.events?.dividends || [];
      const financialData = (summary as any).financialData || {};
      const recommendationTrend = (summary as any).recommendationTrend?.trend?.[0] || {};
      const upgradeDowngradeHistory = (summary as any).upgradeDowngradeHistory?.history || [];
      const quotes = (chartResult as any)?.quotes || [];

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

      // リスク・リターンの統合 (全期間タブ対応)
      const riskReturnData = formatRiskReturnGroups(riskMetricsList, [stock.Symbol, `Sector ${targetEtf}`, 'S&P 500']);

      // 3. データの統合 (Astroレイアウトの期待値に合わせる)
      // IS, BS, CFを年次・四半期それぞれ生成して結合
      const is_a = convertDBFinancials(dbData.financials?.income_statement,
        ["Total Revenue", "Gross Profit", "Operating Income", "Net Income", "Net Income Common Stockholders"], undefined, '通年', true);
      const is_q = convertDBFinancials(dbData.financials?.income_statement_quarterly,
        ["Total Revenue", "Gross Profit", "Operating Income", "Net Income", "Net Income Common Stockholders"], undefined, '四半期', false);
      const is = { data: [...addMarginRatiosToIS(is_a).data, ...addMarginRatiosToIS(is_q).data] };

      const bsCols = [
        "Total non-current assets", "Total Current Assets",
        "Stockholders' Equity", "Total Non Current Liabilities", 
        "Total Current Liabilities"
      ];
      const bsMap = {
        "Total non-current assets": "0",
        "Total Current Assets": "0",
        "Stockholders' Equity": "1",
        "Total Non Current Liabilities": "1",
        "Total Current Liabilities": "1"
      };
      const bs_a = convertDBFinancials(dbData.financials?.balance_sheet, bsCols, bsMap, '通年', true);
      const bs_q = convertDBFinancials(dbData.financials?.balance_sheet_quarterly, bsCols, bsMap, '四半期', false);
      const bs = { data: [...bs_a.data, ...bs_q.data] };

      const cfCols = ["Net Income", "Net Income Common Stockholders", "Net Income from Continuing Operations", "Operating Cash Flow", "Investing Cash Flow", "Financing Cash Flow", "Free Cash Flow", "Cash Dividends Paid", "Common Stock Dividend Paid", "Repurchase of Capital Stock"];
      const cf_a_full = convertDBFinancials(dbData.financials?.cash_flow, cfCols, undefined, '通年', true);
      const cf_q_full = convertDBFinancials(dbData.financials?.cash_flow_quarterly, cfCols, undefined, '四半期', false);

      const cfFilter = (t: any) => t.name.includes("純利益") || t.name.includes("営業CF") || t.name.includes("投資CF") || t.name.includes("財務CF") || t.name.includes("フリーCF");
      const cfSort = (a: any, b: any) => {
        const order = ["純利益", "営業CF", "投資CF", "財務CF", "フリーCF"];
        const nameA = a.name.split(' (')[0];
        const nameB = b.name.split(' (')[0];
        return order.indexOf(nameA) - order.indexOf(nameB);
      };
      const cf = { data: [
        ...cf_a_full.data.filter(cfFilter).sort(cfSort),
        ...cf_q_full.data.filter(cfFilter).sort(cfSort)
      ]};

      const tp_a = generatePayoutChart(is_a, cf_a_full, '通年', true);
      const tp_q = generatePayoutChart(is_q, cf_q_full, '四半期', false);
      const tp = { 
        data: [...tp_a.data, ...tp_q.data],
        layout: tp_a.layout
      };

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
        is_available_mufg: brokerages.mufg.has(stock.Symbol),
        is_available_matsui: brokerages.matsui.has(stock.Symbol),
        is_available_dmm: brokerages.dmm.has(stock.Symbol),
        is_available_paypay: brokerages.paypay.has(stock.Symbol),
        is_available_moomoo: brokerages.moomoo.has(stock.Symbol),
        is_available_iwaicosmo: brokerages.iwaicosmo.has(stock.Symbol),

        // DefeatBeta由来の詳細なDCF
        dcf_valuation: dbData.dcf,

        charts: {
          risk_return: riskReturnData,
          performance: perfData,
          is: is,
          bs: bs,
          cf: cf,
          tp: tp,
          dps: generateDividendChart(dividends, chartResult, getPriceAtDate),
          segment: convertDBSegments(dbData.segments, 'セグメント別収益'),
          geo: convertDBSegments(dbData.geography, '地域別収益')
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
      console.log(`  - [${stock.Symbol}] Successfully saved report.`);
      updatedStocks.push({ ...stock, Daily_Change: (quote as any).regularMarketChangePercent / 100 });

    } catch (e: any) {
      console.error(`    - Unexpected error processing ${stock.Symbol}:`, e);
    }
    
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  fs.writeFileSync(STOCKS_JSON_PATH, JSON.stringify(updatedStocks, null, 2));
  console.log('--- Integration Finished! ---');
}

// 補助関数: DefeatBeta (Split形式) から ChartJs (Plotly風) 用に変換
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
    "売上高": "#1f77b4",
    "売上総利益": "#ff7f0e",
    "営業利益": "#2ca02c",
    "純利益": "#d62728",
    "営業CF": "#1f77b4",
    "投資CF": "#ff7f0e",
    "財務CF": "#2ca02c",
    "フリーCF": "#d62728",
    "配当金支払": "#aec7e8",
    "自社株買い": "#ffbb78"
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

  const niName = suffix ? `純利益 (${suffix})` : "純利益 ";
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
    marker: { color: "#ef4444" }
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
      marker: { color: "#22c55e" }
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
      marker: { color: "#3b82f6" }
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
    marker: { color: "#ff6b01" }
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
    { source: gross, name: `売上総利益率${suffix}`, color: "#60a5fa" }, // Light Blue
    { source: op, name: `営業利益率${suffix}`, color: "#34d399" },    // Emerald/Green
    { source: net, name: `純利益率${suffix}`, color: "#f87171" }      // Light Red/Rose
  ];

  margins.forEach(m => {
    if (!m.source) return;
    is.data.push({
      name: m.name,
      x: rev.x,
      y: m.source.y.map((v: number | null, i: number) => (v !== null && rev.y[i]) ? v / rev.y[i] : null),
      type: 'scatter',
      mode: 'lines+markers',
      yaxis: 'y2', // Plotly format for secondary Y axis
      marker: { color: m.color },
      line: { color: m.color, width: 2 }
    });
  });

  return is;
}

main().catch(console.error);
