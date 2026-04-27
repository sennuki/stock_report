export interface Env {
  STOCK_DATA: R2Bucket;
  GEMINI_API_KEY?: string;
}

export default {
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    ctx.waitUntil(processAllStocks(env));
  },
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    const url = new URL(request.url);
    if (url.pathname === '/batch') {
      // 非同期で実行開始して即レスポンスを返す（Workersのタイムアウト回避のため）
      ctx.waitUntil(processAllStocks(env));
      return new Response("Batch processing started in background.", { status: 202 });
    }
    return new Response("Use /batch to trigger processing.", { status: 400 });
  }
};

export async function processAllStocks(env: Env) {
  console.log("--- Batch Processing Started ---");
  
  // 1. raw/ 配下の全オブジェクトリストを取得
  const objects = await env.STOCK_DATA.list({ prefix: 'raw/' });
  
  let baseStocksList: any[] = [];
  try {
    const listObj = await env.STOCK_DATA.get('raw/stocks_list.json');
    if (listObj) {
      baseStocksList = JSON.parse(await listObj.text());
    }
  } catch (e) {
    console.log("No raw/stocks_list.json found.");
  }

  const rawDataMap: Record<string, any> = {};
  const riskReturnMetrics: any[] = [];
  const topMovers: string[] = [];

  // R2からデータを並列（バッチ）で読み込む
  const objectKeys = objects.objects.map(o => o.key).filter(k => k.endsWith('.json') && k !== 'raw/stocks_list.json');
  console.log(`Found ${objectKeys.length} raw data files.`);

  const BATCH_SIZE = 50;
  for (let i = 0; i < objectKeys.length; i += BATCH_SIZE) {
    const batch = objectKeys.slice(i, i + BATCH_SIZE);
    await Promise.all(batch.map(async (key) => {
      try {
        const obj = await env.STOCK_DATA.get(key);
        if (!obj) return;
        const text = await obj.text();
        const data = JSON.parse(text.replace(/\bNaN\b/g, "null"));
        const symbol = data.symbol;
        rawDataMap[symbol] = data;

        // リスクとリターンを計算 (1年)
        const rr = calculateRiskReturn(data.history, symbol);
        if (rr) {
          riskReturnMetrics.push(rr);
          
          // 前日比の計算
          if (data.history && data.history.length >= 2) {
            const last = data.history[data.history.length - 1].Close;
            const prev = data.history[data.history.length - 2].Close;
            const change = (last - prev) / prev;
            if (Math.abs(change) >= 0.03) { // 3%以上
              topMovers.push(symbol);
            }
          }
        }
      } catch (e) {
        console.error(`Failed to read/parse ${key}:`, e);
      }
    }));
  }

  // 2. Gemini AI で理由生成（トップムーバーのみ）
  const movementReasons: Record<string, string> = {};
  if (env.GEMINI_API_KEY && topMovers.length > 0) {
    console.log(`Generating AI reasons for ${topMovers.length} top movers...`);
    // Gemini 2.5 Flash Lite
    const model = 'models/gemini-2.5-flash-lite';
    for (const symbol of topMovers.slice(0, 20)) { // 上限20件程度に制限
      try {
        const data = rawDataMap[symbol];
        const lastClose = data.history[data.history.length - 1].Close;
        const prevClose = data.history[data.history.length - 2].Close;
        const diffPct = ((lastClose - prevClose) / prevClose * 100).toFixed(2);
        
        const prompt = `${symbol}の株価が直近で${diffPct}%変動しました。企業概要（${data.info?.longBusinessSummary?.substring(0, 500) || '情報なし'}）や一般的な市場動向を基に、この株価変動の主な理由や要因を日本語で推測・要約してください。100文字以内で簡潔に。`;
        
        const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/${model}:generateContent?key=${env.GEMINI_API_KEY}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            contents: [{ parts: [{ text: prompt }] }],
            generationConfig: { maxOutputTokens: 200 }
          })
        });
        
        if (res.ok) {
          const json: any = await res.json();
          movementReasons[symbol] = json.candidates?.[0]?.content?.parts?.[0]?.text || "AI生成に失敗しました。";
        }
      } catch (e) {
        console.error(`Gemini API failed for ${symbol}:`, e);
      }
    }
  }

  // 3. 各銘柄のレポート生成
  const updatedStocksList = [];
  
  // ETF等のデータもあるので、全てループ
  for (const symbol of Object.keys(rawDataMap)) {
    const rawData = rawDataMap[symbol];
    const isETF = ["SPY", "XLC", "XLY", "XLP", "XLE", "XLF", "XLV", "XLI", "XLK", "XLB", "XLRE", "XLU"].includes(symbol);
    
    // ベースリストからメタデータを検索
    const metadata = baseStocksList.find(s => s.Symbol_YF === symbol || s.Symbol === symbol) || {};

    const sectorEtf = getSectorETF(metadata['GICS Sector'] || rawData.info?.sector);
    const etfRawData = rawDataMap[sectorEtf];
    
    const highlights = extractHighlights(rawData);
    
    // リスクリターンチャート (全500銘柄の背景 + ターゲット赤色)
    const riskReturnChart = generateRiskReturnChart(riskReturnMetrics, symbol);

    // 各種チャート
    const isChart = generateFinancialChart(rawData.income_stmt || {}, ["Total Revenue", "Gross Profit", "Operating Income", "Net Income"], "bar", "group");
    const bsChart = generateFinancialChart(rawData.balancesheet || {}, ["Total Assets", "Total Liabilities Net Minority Interest", "Stockholders Equity"], "bar", "stack");
    const cfChart = generateFinancialChart(rawData.cashflow || {}, ["Operating Cash Flow", "Investing Cash Flow", "Financing Cash Flow", "Free Cash Flow"], "bar", "group");
    
    const perfChart = generatePerformanceChart(rawData.history, etfRawData?.history, symbol, sectorEtf);

    const reportData = {
      symbol: metadata.Symbol || symbol,
      symbol_yf: symbol,
      security: metadata.Security || rawData.info?.longName || rawData.info?.shortName || symbol,
      security_ja: metadata.Security_JA || null,
      business_summary_ja: rawData.info?.longBusinessSummary || null,
      sector: metadata['GICS Sector'] || rawData.info?.sector,
      sub_industry: metadata['GICS Sub-Industry'] || rawData.info?.industry,
      exchange: rawData.info?.exchange || "NASDAQ",
      full_symbol: `${rawData.info?.exchange || 'NASDAQ'}:${symbol.replace("-", ".")}`,
      sector_etf: sectorEtf,
      is_financial: ["Financials", "Real Estate"].includes(metadata['GICS Sector'] || rawData.info?.sector),
      movement_reason: movementReasons[symbol] || null,
      highlights: highlights,
      dcf_valuation: rawData.dcf_valuation || null,
      charts: {
        risk_return: riskReturnChart,
        is: isChart,
        bs: bsChart,
        cf: cfChart,
        performance: perfChart
      },
      last_updated: new Date().toISOString()
    };

    // R2に保存 (ETF自体は画面に出さないならスキップでもよいが保存しておく)
    await env.STOCK_DATA.put(`reports/${symbol}.json`, JSON.stringify(reportData), {
      httpMetadata: { contentType: 'application/json' }
    });

    if (!isETF || metadata.Symbol) {
      updatedStocksList.push({
        ...metadata,
        Symbol_YF: symbol,
        Daily_Change: calculateDailyChange(rawData.history),
        Has_Movement_Reason: !!movementReasons[symbol]
      });
    }
  }

  // 4. 更新された stocks.json を保存
  if (updatedStocksList.length > 0) {
    await env.STOCK_DATA.put('reports/stocks.json', JSON.stringify(updatedStocksList, null, 2), {
      httpMetadata: { contentType: 'application/json' }
    });
    console.log(`Saved reports/stocks.json with ${updatedStocksList.length} items.`);
  }
  
  console.log("--- Batch Processing Completed ---");
}

function calculateDailyChange(history: any[]) {
  if (!history || history.length < 2) return 0;
  const last = history[history.length - 1].Close;
  const prev = history[history.length - 2].Close;
  return (last - prev) / prev;
}

function calculateRiskReturn(history: any[], symbol: string) {
  if (!history || history.length < 5) return null;
  // 直近1年 (約252日)
  const lastQuotes = history.slice(-252);
  const returns = [];
  for (let i = 1; i < lastQuotes.length; i++) {
    returns.push(Math.log(lastQuotes[i].Close / lastQuotes[i - 1].Close));
  }
  if (returns.length === 0) return null;

  const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
  const variance = returns.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / (returns.length - 1);
  const hv = Math.sqrt(variance) * Math.sqrt(252);

  const totalReturn = (lastQuotes[lastQuotes.length - 1].Close / lastQuotes[0].Close) - 1;

  return { symbol, hv, ret: totalReturn };
}

function getSectorETF(sector?: string) {
  const map: Record<string, string> = {
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
  return sector && map[sector] ? map[sector] : "SPY";
}

function extractHighlights(rawData: any) {
  const info = rawData.info || {};
  return {
    revenue_growth: info.revenueGrowth,
    earnings_growth: info.earningsGrowth,
    profit_margins: info.profitMargins,
    operating_margins: info.operatingMargins,
    roe: info.returnOnEquity,
    roa: info.returnOnAssets,
    pe_ttm: info.trailingPE,
    pe_forward: info.forwardPE,
    dividend_yield: info.dividendYield,
  };
}

function generateRiskReturnChart(allMetrics: any[], targetSymbol: string) {
  const others = allMetrics.filter(m => m.symbol !== targetSymbol);
  const target = allMetrics.find(m => m.symbol === targetSymbol);

  const datasets: any[] = [
    {
      label: 'Other S&P 500',
      data: others.map(m => ({ x: m.hv, y: m.ret, symbol: m.symbol })),
      backgroundColor: 'rgba(200, 200, 200, 0.5)',
      pointRadius: 4
    }
  ];

  if (target) {
    datasets.push({
      label: target.symbol,
      data: [{ x: target.hv, y: target.ret, symbol: target.symbol }],
      backgroundColor: 'rgba(255, 0, 0, 0.9)',
      pointRadius: 8
    });
  }

  return { datasets };
}

function generateFinancialChart(data: any, fields: string[], type: string, barmode: string) {
  const dates = Object.keys(data).sort();
  if (dates.length === 0) return null;

  const colors = [
    { bg: 'rgba(54, 162, 235, 0.5)', border: 'rgb(54, 162, 235)' },
    { bg: 'rgba(255, 99, 132, 0.5)', border: 'rgb(255, 99, 132)' },
    { bg: 'rgba(75, 192, 192, 0.5)', border: 'rgb(75, 192, 192)' },
    { bg: 'rgba(255, 159, 64, 0.5)', border: 'rgb(255, 159, 64)' }
  ];

  return {
    labels: dates.map(d => d.split(' ')[0]),
    datasets: fields.map((field, i) => {
      return {
        label: field,
        data: dates.map(d => data[d]?.[field] || 0),
        backgroundColor: colors[i % colors.length].bg,
        borderColor: colors[i % colors.length].border,
        borderWidth: 1
      };
    })
  };
}

function generatePerformanceChart(history: any[], etfHistory: any[], symbol: string, etfSymbol: string) {
  if (!history || history.length === 0) return null;
  
  const targetData = history.slice(-252);
  if (targetData.length === 0) return null;
  
  const startClose = targetData[0].Close;
  const dates = targetData.map(h => h.Date.split(' ')[0]);
  const targetReturns = targetData.map(h => (h.Close / startClose) - 1);

  const datasets: any[] = [
    {
      label: symbol,
      data: targetReturns,
      borderColor: "#1f77b4",
      borderWidth: 2,
      fill: false,
      pointRadius: 0
    }
  ];

  if (etfHistory && etfHistory.length > 0) {
    const etfMap = new Map(etfHistory.map(h => [h.Date.split(' ')[0], h.Close]));
    let etfStartClose = etfMap.get(dates[0]);
    if (!etfStartClose) {
       for(const d of dates) {
          if (etfMap.has(d)) { etfStartClose = etfMap.get(d); break; }
       }
    }

    if (etfStartClose) {
      const etfReturns = dates.map(d => {
        const close = etfMap.get(d) || etfStartClose;
        return ((close as number) / (etfStartClose as number)) - 1;
      });
      datasets.push({
        label: `Sector ETF (${etfSymbol})`,
        data: etfReturns,
        borderColor: "#ff7f0e",
        borderWidth: 2,
        borderDash: [5, 5],
        fill: false,
        pointRadius: 0
      });
    }
  }

  return {
    labels: dates,
    datasets
  };
}