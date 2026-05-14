export interface Env {
  STOCK_DATA: R2Bucket;
  GEMINI_API_KEY?: string;
}

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

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

export default {
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    ctx.waitUntil(processAllStocks(env));
  },
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    const url = new URL(request.url);

    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    if (url.pathname === '/batch') {
      ctx.waitUntil(processAllStocks(env));
      return new Response("Batch processing started in background.", { status: 202 });
    }

    if (url.pathname === '/api/list' && request.method === 'GET') {
      const prefix = url.searchParams.get('prefix') ?? '';
      const limit = Math.min(Number(url.searchParams.get('limit') ?? '20'), 1000);
      try {
        let total = 0;
        const sample: Array<{ key: string; size: number; uploaded: string }> = [];
        let cursor: string | undefined = undefined;
        do {
          const res: any = await env.STOCK_DATA.list({ prefix, cursor, limit: 1000 });
          total += res.objects.length;
          for (const o of res.objects) {
            if (sample.length < limit) {
              sample.push({ key: o.key, size: o.size, uploaded: String(o.uploaded) });
            }
          }
          cursor = res.truncated ? res.cursor : undefined;
        } while (cursor);
        return new Response(JSON.stringify({ prefix, total, sample }, null, 2), {
          status: 200,
          headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
        });
      } catch (e: any) {
        return new Response(JSON.stringify({ error: String(e?.message || e) }), {
          status: 500,
          headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
        });
      }
    }

    const reportMatch = url.pathname.match(/^\/api\/report\/([^/]+)$/);
    if (reportMatch && request.method === 'GET') {
      const symbol = decodeURIComponent(reportMatch[1]);
      try {
        const obj = await env.STOCK_DATA.get(`reports/${symbol}.json`);
        if (!obj) {
          return new Response(JSON.stringify({ error: 'not_found', symbol }), {
            status: 404,
            headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
          });
        }
        const body = await obj.text();
        return new Response(body, {
          status: 200,
          headers: {
            'Content-Type': 'application/json',
            'Cache-Control': 'public, max-age=300',
            ...CORS_HEADERS,
          },
        });
      } catch (e: any) {
        return new Response(JSON.stringify({ error: 'internal', message: String(e?.message || e) }), {
          status: 500,
          headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
        });
      }
    }

    // Debug endpoint: inspect raw/{symbol}.json structure.
    // Use ?fields=earnings_estimate,revenue_estimate,upgrades_downgrades to limit response size.
    const rawMatch = url.pathname.match(/^\/api\/raw\/([^/]+)$/);
    if (rawMatch && request.method === 'GET') {
      const symbol = decodeURIComponent(rawMatch[1]);
      const fields = (url.searchParams.get('fields') || '').split(',').map(s => s.trim()).filter(Boolean);
      try {
        const obj = await env.STOCK_DATA.get(`raw/${symbol}.json`);
        if (!obj) {
          return new Response(JSON.stringify({ error: 'not_found', key: `raw/${symbol}.json` }), {
            status: 404,
            headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
          });
        }
        const body = await obj.text();
        const data = JSON.parse(body.replace(/\bNaN\b/g, 'null').replace(/\b-?Infinity\b/g, 'null'));
        const uploaded = String((obj as any).uploaded || '');
        const result: any = { symbol, uploaded, available_keys: Object.keys(data) };
        if (fields.length === 0) {
          // Default: show small summary of analyst-related fields only.
          result.earnings_estimate = data.earnings_estimate;
          result.revenue_estimate = data.revenue_estimate;
          result.analyst_ratings = data.analyst_ratings;
          result.upgrades_downgrades_count = Array.isArray(data.upgrades_downgrades) ? data.upgrades_downgrades.length : null;
          result.upgrades_downgrades_first = Array.isArray(data.upgrades_downgrades) ? data.upgrades_downgrades[0] : null;
        } else {
          for (const f of fields) result[f] = (data as any)[f];
        }
        return new Response(JSON.stringify(result, null, 2), {
          status: 200,
          headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
        });
      } catch (e: any) {
        return new Response(JSON.stringify({ error: 'internal', message: String(e?.message || e) }), {
          status: 500,
          headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
        });
      }
    }

    return new Response("Use /batch to trigger processing, or /api/report/{symbol} to fetch a report.", { status: 400 });
  }
};

export async function processAllStocks(env: Env) {
  console.log("--- Batch Processing Started ---");

  const allObjects: { key: string }[] = [];
  let cursor: string | undefined = undefined;
  let pages = 0;
  do {
    const res: any = await env.STOCK_DATA.list({ prefix: 'raw/', cursor, limit: 1000 });
    allObjects.push(...res.objects);
    cursor = res.truncated ? res.cursor : undefined;
    pages++;
  } while (cursor);
  console.log(`Listed ${allObjects.length} raw objects across ${pages} page(s).`);

  let baseStocksList: any[] = [];
  try {
    const listObj = await env.STOCK_DATA.get('raw/stocks_list.json');
    if (listObj) {
      const raw = await listObj.text();
      baseStocksList = JSON.parse(raw.replace(/\bNaN\b/g, "null").replace(/\b-?Infinity\b/g, "null"));
    }
  } catch (e) {
    console.log("No raw/stocks_list.json found.");
  }

  const rawDataMap: Record<string, any> = {};
  const riskReturnMetrics: any[] = [];
  const topMovers: string[] = [];

  const objectKeys = allObjects.map(o => o.key).filter(k => k.endsWith('.json') && k !== 'raw/stocks_list.json');
  console.log(`Found ${objectKeys.length} raw data files.`);

  let translations: Record<string, any> = {};
  try {
    const transObj = await env.STOCK_DATA.get('translations/business_summaries.json');
    if (transObj) {
      translations = JSON.parse(await transObj.text());
    }
  } catch (e) {
    console.log("No translations/business_summaries.json found.");
  }

  const BATCH_SIZE = 50;
  for (let i = 0; i < objectKeys.length; i += BATCH_SIZE) {
    const batch = objectKeys.slice(i, i + BATCH_SIZE);
    await Promise.all(batch.map(async (key) => {
      try {
        const obj = await env.STOCK_DATA.get(key);
        if (!obj) return;
        const text = await obj.text();
        const data = JSON.parse(text.replace(/\bNaN\b/g, "null"));
        const symbol = data.symbol || key.replace('raw/', '').replace('.json', '');
        rawDataMap[symbol] = data;

        const rr = calculateRiskReturn(data.history, symbol);
        if (rr) {
          riskReturnMetrics.push(rr);
          if (data.history && data.history.length >= 2) {
            const last = data.history[data.history.length - 1].Close;
            const prev = data.history[data.history.length - 2].Close;
            const change = (last - prev) / prev;
            if (Math.abs(change) >= 0.03) {
              topMovers.push(symbol);
            }
          }
        }
      } catch (e) {
        console.error(`Failed to read/parse ${key}:`, e);
      }
    }));
  }

  const movementReasons: Record<string, string> = {};

  const subIndustryMap: Record<string, any[]> = {};
  const sectorMap: Record<string, any[]> = {};
  
  for (const symbol of Object.keys(rawDataMap)) {
    const rawData = rawDataMap[symbol];
    const metadata = baseStocksList.find(s => s.Symbol_YF === symbol || s.Symbol === symbol) || {};
    const sector = metadata['GICS Sector'] || rawData.info?.sector || "Unknown";
    const subInd = metadata['GICS Sub-Industry'] || rawData.info?.industry || "Unknown";
    
    const dailyChange = calculateDailyChange(rawData.history);
    const peerInfo = { Symbol: metadata.Symbol || symbol, Symbol_YF: symbol, Daily_Change: dailyChange };
    
    if (!sectorMap[sector]) sectorMap[sector] = [];
    sectorMap[sector].push(peerInfo);
    
    if (!subIndustryMap[subInd]) subIndustryMap[subInd] = [];
    subIndustryMap[subInd].push(peerInfo);
  }

  const updatedStocksList = [];
  
  const testSymbols = ["MSFT", "AAPL", "NVDA"];
  for (const symbol of Object.keys(rawDataMap)) {
    if (!testSymbols.includes(symbol)) continue;
    const rawData = rawDataMap[symbol];
    const isETF = ["SPY", "XLC", "XLY", "XLP", "XLE", "XLF", "XLV", "XLI", "XLK", "XLB", "XLRE", "XLU"].includes(symbol);
    const metadata = baseStocksList.find(s => s.Symbol_YF === symbol || s.Symbol === symbol) || {};

    const sectorEtf = getSectorETF(
      metadata['GICS Sector'] || rawData.info?.sector,
      metadata['GICS Sub-Industry'] || rawData.info?.industry
    );
    const etfRawData = rawDataMap[sectorEtf];
    
    const highlights = extractHighlights(rawData);
    const earningsSurprise = extractEarningsSurprise(rawData);
    const nextEarnings = extractNextEarnings(rawData);
    const consensus = extractConsensus(rawData);
    const ratingChanges = extractRatingChanges(rawData);
    const analystRatings = extractAnalystRatings(rawData);

    const riskReturnChart = generateRiskReturnChart(riskReturnMetrics, symbol);
    const isChart = generateFinancialChart(rawData.income_stmt || [], ["Total Revenue", "Gross Profit", "Operating Income", "Net Income"], "bar", "group");
    const bsChart = generateFinancialChart(rawData.balancesheet || [], ["Total Assets", "Total Liabilities Net Minority Interest", "Stockholders Equity"], "bar", "stack");
    const cfChart = generateFinancialChart(rawData.cashflow || [], ["Operating Cash Flow", "Investing Cash Flow", "Financing Cash Flow", "Free Cash Flow"], "bar", "group");
    const perfChart = generatePerformanceChart(rawData.history, etfRawData?.history, symbol, sectorEtf);
    
    const tpChart = generateTpChart(rawData.cashflow || [], rawData.income_stmt || []);
    const dpsChart = generateDpsEpsChart(rawData.dividends);
    const segmentChart = generateSegmentChart(rawData.revenue_by_segment);
    const geoChart = generateSegmentChart(rawData.revenue_by_geography);

    const sector = metadata['GICS Sector'] || rawData.info?.sector || "Unknown";
    const subInd = metadata['GICS Sub-Industry'] || rawData.info?.industry || "Unknown";

    let summary_ja = translations[symbol]?.business_summary_ja || translations[symbol] || null;
    if (summary_ja && typeof summary_ja === 'string') {
      summary_ja = formatSummary(summary_ja);
    } else {
      summary_ja = null;
    }

    const reportData = {
      symbol: metadata.Symbol || symbol,
      symbol_yf: symbol,
      security: metadata.Security || rawData.info?.longName || rawData.info?.shortName || symbol,
      security_ja: metadata.Security_JA || null,
      business_summary_ja: summary_ja,
      sector: sector,
      sub_industry: subInd,
      exchange: toTradingViewExchange(rawData.info?.exchange),
      full_symbol: `${toTradingViewExchange(rawData.info?.exchange)}:${symbol.replace("-", ".")}`,
      sector_etf: sectorEtf,
      is_financial: ["Financials", "Real Estate"].includes(sector),
      is_available_monex: true,
      is_available_rakuten: true,
      is_available_sbi: true,
      is_available_mufg: true,
      is_available_matsui: true,
      is_available_dmm: true,
      is_available_paypay: true,
      is_available_moomoo: true,
      is_available_iwaicosmo: true,
      movement_reason: movementReasons[symbol] || null,
      highlights,
      earnings_surprise: earningsSurprise,
      next_earnings: nextEarnings,
      consensus,
      consensus_raw: {
        eps_trend: rawData.info?.epsTrend || null,
        eps_revisions: rawData.info?.epsRevisions || null
      },
      rating_changes: ratingChanges,
      analyst_ratings: analystRatings,
      peers: {
        sub_industry: (subIndustryMap[subInd] || []).filter(s => s.Symbol_YF !== symbol),
        sector: (sectorMap[sector] || []).filter(s => s.Symbol_YF !== symbol && !subIndustryMap[subInd]?.find(si => si.Symbol_YF === s.Symbol_YF))
      },
      benchmark_info: getBenchmarkInfo(metadata),
      dcf_valuation: rawData.dcf_valuation || null,
      charts: {
        risk_return: riskReturnChart,
        is: isChart,
        bs: bsChart,
        cf: cfChart,
        performance: perfChart,
        tp: tpChart,
        dps: dpsChart,
        segment: segmentChart,
        geo: geoChart
      },
      last_updated: new Date().toISOString()
    };

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

function getSectorETF(sector?: string, subIndustry?: string) {
  return sectorEtfMap[subIndustry || ''] || sectorEtfMap[sector || ''] || 'SPY';
}

function getBenchmarkInfo(metadata: any) {
  const sector = metadata['GICS Sector'];
  const subInd = metadata['GICS Sub-Industry'];
  const index = metadata['Index'] || 'S&P 500';

  const targetEtf = sectorEtfMap[subInd] || sectorEtfMap[sector] || 'SPY';
  const broadEtf = broadSectorEtfMap[subInd] || broadSectorEtfMap[sector] || 'SPY';
  const marketEtf = marketIndexMap[index] || 'SPY';

  return {
    sector: { symbol: targetEtf, name: etfFullNameMap[targetEtf] || targetEtf },
    broad: { symbol: broadEtf, name: etfFullNameMap[broadEtf] || broadEtf },
    index: { symbol: marketEtf, name: etfFullNameMap[marketEtf] || marketEtf },
    market: { symbol: 'SPY', name: etfFullNameMap['SPY'] }
  };
}

function calculateRiskReturn(history: any[], symbol: string) {
  if (!history || history.length < 5) return null;
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

function toTradingViewExchange(yfExchange?: string): string {
  const map: Record<string, string> = {
    NMS: "NASDAQ", NGM: "NASDAQ", NCM: "NASDAQ", NAS: "NASDAQ",
    NYQ: "NYSE", NYS: "NYSE",
    ASE: "AMEX", AMX: "AMEX",
    PCX: "NYSEARCA", BATS: "BATS", BTS: "BATS",
  };
  if (!yfExchange) return "NASDAQ";
  return map[yfExchange] || yfExchange;
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
    eps_ttm: info.trailingEps,
    eps_forward: info.forwardEps,
    pe_ttm: info.trailingPE,
    pe_forward: info.forwardPE,
    dividend_yield: info.dividendYield,
    payout_ratio: info.payoutRatio || (info.dividendRate && info.trailingEps ? info.dividendRate / info.trailingEps : null),
    debt_to_equity: info.debtToEquity,
    current_ratio: info.currentRatio
  };
}

function extractEarningsSurprise(rawData: any) {
  const ed = rawData.earnings_dates;
  if (!ed || !Array.isArray(ed) || ed.length === 0) return null;
  const sorted = [...ed].sort((a, b) => new Date(b.index || b.Date).getTime() - new Date(a.index || a.Date).getTime());
  
  for (const item of sorted) {
    if (item['Reported EPS'] !== null && item['Reported EPS'] !== undefined) {
      return {
        date: String(item.index || item.Date).split(' ')[0],
        actual: item['Reported EPS'],
        estimate: item['EPS Estimate'],
        surprise_pct: item['Surprise(%)']
      };
    }
  }
  return null;
}

function extractNextEarnings(rawData: any) {
  const ed = rawData.earnings_dates;
  if (!ed || !Array.isArray(ed) || ed.length === 0) return null;
  const now = new Date();
  const sorted = [...ed].sort((a, b) => new Date(a.index || a.Date).getTime() - new Date(b.index || b.Date).getTime());
  
  for (const item of sorted) {
    const d = new Date(item.index || item.Date);
    if (d >= now && (item['Reported EPS'] === null || item['Reported EPS'] === undefined)) {
      return {
        date: String(item.index || item.Date).split(' ')[0],
        estimate: item['EPS Estimate'] || rawData.info?.earningsAverage || null
      };
    }
  }
  
  if (rawData.calendar?.['Earnings Date']) {
    const cDates = Array.isArray(rawData.calendar['Earnings Date']) ? rawData.calendar['Earnings Date'] : [rawData.calendar['Earnings Date']];
    if (cDates.length > 0 && new Date(cDates[0]) >= now) {
      return {
        date: cDates[0].split(' ')[0],
        estimate: rawData.calendar['Earnings Average'] || rawData.calendar['EPS Estimate'] || rawData.info?.earningsAverage || null
      };
    }
  }
  
  return null;
}

function extractConsensus(rawData: any) {
  const info = rawData.info || {};
  return {
    earnings: {
      "0q": {
        avg: info.earningsAverage || null,
        low: info.earningsLow || null,
        high: info.earningsHigh || null,
        growth: info.earningsGrowth || null,
        numberOfAnalysts: info.numberOfAnalystOpinions || 0
      }
    },
    revenue: {
      "0q": {
        avg: info.revenueAverage || null,
        low: info.revenueLow || null,
        high: info.revenueHigh || null,
        growth: info.revenueGrowth || null,
        numberOfAnalysts: info.numberOfAnalystOpinions || 0
      }
    }
  };
}

function extractRatingChanges(rawData: any) {
  const ud = rawData.upgrades_downgrades;
  if (!ud || !Array.isArray(ud)) return [];
  const sorted = [...ud].sort((a, b) => new Date(b.index || b.Date).getTime() - new Date(a.index || a.Date).getTime());
  return sorted.slice(0, 10).map(x => {
    return {
      GradeDate: String(x.index || x.Date).split(' ')[0],
      Firm: x.Firm || x.firm,
      ToGrade: x.ToGrade || x.toGrade || x["To Grade"],
      FromGrade: x.FromGrade || x.fromGrade || x["From Grade"],
      Action: x.Action || x.action
    };
  });
}

function extractAnalystRatings(rawData: any) {
  const info = rawData.info || {};
  const ratings = rawData.analyst_ratings || {};
  return {
    targetHighPrice: info.targetHighPrice,
    targetLowPrice: info.targetLowPrice,
    targetMeanPrice: info.targetMeanPrice,
    targetMedianPrice: info.targetMedianPrice,
    currentPrice: info.currentPrice,
    numberOfAnalystOpinions: info.numberOfAnalystOpinions,
    recommendationKey: (info.recommendationKey || "hold").toLowerCase(),
    ...ratings
  };
}

function generateRiskReturnChart(allMetrics: any[], targetSymbol: string) {
  const others = allMetrics.filter(m => m.symbol !== targetSymbol);
  const target = allMetrics.find(m => m.symbol === targetSymbol);

  const datasets: any[] = [
    {
      label: 'その他のS&P 500銘柄',
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

function getValFromArray(data: any[], itemLabel: string, date: string): number {
  const row = data.find(r => r.index === itemLabel);
  return row ? (Number(row[date]) || 0) : 0;
}

function generateFinancialChart(data: any[], fields: string[], type: string, barmode: string) {
  if (data.length === 0) return null;
  const dates = Object.keys(data[0]).filter(k => k !== 'index').sort();
  if (dates.length === 0) return null;

  const colors = [
    { bg: 'rgba(54, 162, 235, 0.5)', border: 'rgb(54, 162, 235)' },
    { bg: 'rgba(255, 99, 132, 0.5)', border: 'rgb(255, 99, 132)' },
    { bg: 'rgba(75, 192, 192, 0.5)', border: 'rgb(75, 192, 192)' },
    { bg: 'rgba(255, 159, 64, 0.5)', border: 'rgb(255, 159, 64)' }
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
    labels: dates.map(d => d.split(' ')[0]),
    datasets: fields.map((field, i) => {
      return {
        label: labelMap[field] || field,
        data: dates.map(d => getValFromArray(data, field, d)),
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
  const dates = targetData.map(h => (h.Date || h.index).split(' ')[0]);
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
    const etfMap = new Map(etfHistory.map(h => [(h.Date || h.index).split(' ')[0], h.Close]));
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
  return { labels: dates, datasets };
}

function generateTpChart(cf: any[], is: any[]) {
  if (is.length === 0 && cf.length === 0) return null;
  const dates = Object.keys(is[0] || cf[0] || {}).filter(k => k !== 'index').sort();
  if (dates.length === 0) return null;

  const niKeys = ['Net Income', 'Net Income From Continuing Operations', 'Net Income Common Stockholders'];
  const divKeys = ['Cash Dividends Paid', 'Common Stock Dividend Paid'];
  const repoKeys = ['Repurchase Of Capital Stock', 'Repurchase Of Common Stock', 'Common Stock Repurchased'];

  const getAnyVal = (arr: any[], keys: string[], date: string) => {
    for (const k of keys) {
      const v = getValFromArray(arr, k, date);
      if (v !== 0) return v;
    }
    return 0;
  };

  const niData = dates.map(d => getAnyVal(is, niKeys, d) || getAnyVal(cf, niKeys, d));
  const divData = dates.map(d => Math.abs(getAnyVal(cf, divKeys, d)));
  const repoData = dates.map(d => Math.abs(getAnyVal(cf, repoKeys, d)));

  const divRatio = niData.map((ni, i) => ni > 0 ? divData[i] / ni : 0);
  const totalRatio = niData.map((ni, i) => ni > 0 ? (divData[i] + repoData[i]) / ni : 0);

  return {
    labels: dates.map(d => d.split(' ')[0]),
    datasets: [
      { type: 'bar', label: '純利益', data: niData, backgroundColor: 'rgba(44, 160, 44, 0.6)', yAxisID: 'y' },
      { type: 'bar', label: '配当金', data: divData, backgroundColor: 'rgba(174, 199, 232, 0.6)', yAxisID: 'y' },
      { type: 'bar', label: '自社株買い', data: repoData, backgroundColor: 'rgba(31, 119, 180, 0.6)', yAxisID: 'y' },
      { type: 'line', label: '配当性向', data: divRatio, borderColor: '#ffbb78', yAxisID: 'y1' },
      { type: 'line', label: '総還元性向', data: totalRatio, borderColor: '#ff7f0e', yAxisID: 'y1' }
    ]
  };
}

function generateDpsEpsChart(dividends: any[]) {
  if (!dividends || !Array.isArray(dividends) || dividends.length === 0) return null;
  const annual: Record<string, number> = {};
  
  dividends.forEach(d => {
    const dateStr = d.Date || d.index;
    if (!dateStr) return;
    const year = String(dateStr).substring(0, 4);
    const val = Number(d.Dividends || d.Value || 0);
    annual[year] = (annual[year] || 0) + val;
  });
  
  const labels = Object.keys(annual).sort();
  if (labels.length === 0) return null;

  return {
    labels,
    datasets: [{
      type: 'bar',
      label: '年間配当金',
      data: labels.map(y => annual[y]),
      backgroundColor: 'rgba(31, 119, 180, 0.8)'
    }]
  };
}

function generateSegmentChart(segmentData: any[]) {
  if (!segmentData || !Array.isArray(segmentData) || segmentData.length === 0) return null;
  const segmentsArr = Object.keys(segmentData[0]).filter(k => !['Date', 'report_date', 'symbol', 'index'].includes(k));
  if (segmentsArr.length === 0) return null;
  const labels = segmentData.map(row => String(row.Date || row.report_date || row.index).split(' ')[0]);
  const colors = [
    'rgba(31, 119, 180, 0.7)', 'rgba(255, 127, 14, 0.7)', 'rgba(44, 160, 44, 0.7)',
    'rgba(214, 39, 40, 0.7)', 'rgba(148, 103, 189, 0.7)', 'rgba(140, 86, 75, 0.7)',
    'rgba(227, 119, 194, 0.7)', 'rgba(127, 127, 127, 0.7)', 'rgba(188, 189, 34, 0.7)',
    'rgba(23, 190, 207, 0.7)'
  ];
  const datasets = segmentsArr.map((seg, i) => {
    return {
      label: seg,
      data: segmentData.map(row => Number(row[seg]) || 0),
      backgroundColor: colors[i % colors.length]
    };
  });
  return { labels, datasets };
}

function formatSummary(text: string): string {
  if (!text) return text;
  const normalized = text.replace(/\n\n/g, "\n").replace(/\n/g, "");
  const sentences = normalized.split(/(?<=。)/);
  const formattedSentences = [];
  let chunkSize = 0;
  for (let i = 0; i < sentences.length; i++) {
    const s = sentences[i].trim();
    if (!s) continue;
    formattedSentences.push(s);
    chunkSize += s.length;
    if (i < sentences.length - 1) {
      const nextSentence = sentences[i + 1].trim();
      let shouldBreak = false;
      if (chunkSize > 150) shouldBreak = true;
      const startKeywords = ["また、", "さらに", "加えて", "同社は", "同社の", "主要な", "事業は"];
      if (startKeywords.some(word => nextSentence.startsWith(word))) {
        if (chunkSize > 40) shouldBreak = true;
      }
      if (shouldBreak) {
        formattedSentences.push("\n\n");
        chunkSize = 0;
      }
    }
  }
  return formattedSentences.join("").trim();
}
