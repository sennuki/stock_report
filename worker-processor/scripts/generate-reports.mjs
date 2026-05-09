#!/usr/bin/env node
/**
 * GitHub Actions runner 上で reports/*.json を生成する。
 *
 * 元々は worker-processor の processAllStocks (ctx.waitUntil) で行っていたが、
 * Cloudflare Workers の sub-request 制限 (paid 1000) を 1500 銘柄 × GET+PUT で
 * 大幅に超過するため、 batch がほぼ動かなかった。runner 上の Node.js なら
 * sub-request 制限がなく、 socket pool の concurrency 制御だけで完走する。
 *
 * worker-processor/src/index.ts の純粋ロジック群をそのまま JS に移植してある。
 * R2 binding (env.STOCK_DATA) 部分のみ Cloudflare REST API に差し替え。
 * (upload-raw-to-r2.mjs と同じ認証方式に揃え、 S3 互換キー無しで動かす)
 */
const ACCOUNT_ID = process.env.CLOUDFLARE_ACCOUNT_ID || process.env.R2_ACCOUNT_ID;
const API_TOKEN = process.env.CLOUDFLARE_API_TOKEN;
const BUCKET = process.env.R2_BUCKET_NAME || "stock-data-c1";
const CONCURRENCY = Number(process.env.GENERATE_REPORTS_CONCURRENCY || 50);

if (!ACCOUNT_ID || !API_TOKEN) {
  console.error(
    "ERROR: CLOUDFLARE_ACCOUNT_ID / CLOUDFLARE_API_TOKEN が必要です",
  );
  process.exit(1);
}

const API_BASE = `https://api.cloudflare.com/client/v4/accounts/${ACCOUNT_ID}/r2/buckets/${BUCKET}`;
const AUTH_HEADERS = { Authorization: `Bearer ${API_TOKEN}` };

async function fetchWithRetry(url, init = {}, attempts = 4) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    try {
      const resp = await fetch(url, init);
      // 5xx と 429 はリトライ対象
      if (resp.status >= 500 || resp.status === 429) {
        lastErr = new Error(`HTTP ${resp.status}`);
      } else {
        return resp;
      }
    } catch (e) {
      lastErr = e;
    }
    // 指数バックオフ: 250ms, 500ms, 1000ms
    await new Promise((r) => setTimeout(r, 250 * 2 ** i));
  }
  throw lastErr;
}

async function listAll(prefix) {
  const keys = [];
  let cursor;
  do {
    const url = new URL(`${API_BASE}/objects`);
    url.searchParams.set("prefix", prefix);
    url.searchParams.set("per_page", "1000");
    if (cursor) url.searchParams.set("cursor", cursor);
    const resp = await fetchWithRetry(url.toString(), { headers: AUTH_HEADERS });
    if (!resp.ok) {
      throw new Error(
        `List ${prefix} failed: HTTP ${resp.status}: ${(await resp.text()).slice(0, 300)}`,
      );
    }
    const data = await resp.json();
    for (const obj of data.result ?? []) keys.push(obj.key);
    // Cloudflare API は次ページがあるときのみ result_info.cursor を返す
    cursor = data.result_info?.is_truncated ? data.result_info.cursor : undefined;
  } while (cursor);
  return keys;
}

async function getJson(key) {
  const url = `${API_BASE}/objects/${encodeURIComponent(key)}`;
  const resp = await fetchWithRetry(url, { headers: AUTH_HEADERS });
  if (!resp.ok) {
    throw new Error(`Get ${key} failed: HTTP ${resp.status}`);
  }
  const text = await resp.text();
  return JSON.parse(
    text.replace(/\bNaN\b/g, "null").replace(/-?\bInfinity\b/g, "null"),
  );
}

async function putJson(key, value) {
  const url = `${API_BASE}/objects/${encodeURIComponent(key)}`;
  const resp = await fetchWithRetry(url, {
    method: "PUT",
    headers: { ...AUTH_HEADERS, "Content-Type": "application/json" },
    body: JSON.stringify(value),
  });
  if (!resp.ok) {
    throw new Error(
      `Put ${key} failed: HTTP ${resp.status}: ${(await resp.text()).slice(0, 300)}`,
    );
  }
}

async function pMap(items, fn, concurrency) {
  const results = new Array(items.length);
  let cursor = 0;
  let done = 0;
  const total = items.length;
  const workers = Array.from({ length: Math.min(concurrency, total) }, async () => {
    while (true) {
      const idx = cursor++;
      if (idx >= total) return;
      try {
        results[idx] = { ok: true, value: await fn(items[idx], idx) };
      } catch (e) {
        results[idx] = { ok: false, error: e };
      }
      done++;
      if (done % 50 === 0 || done === total) {
        process.stdout.write(`\r  ${done}/${total}`);
      }
    }
  });
  await Promise.all(workers);
  process.stdout.write("\n");
  return results;
}

// === 以下、worker-processor/src/index.ts の純粋ロジックを JS に移植 ===

function calculateDailyChange(history) {
  if (!history || history.length < 2) return 0;
  const last = history[history.length - 1].Close;
  const prev = history[history.length - 2].Close;
  return (last - prev) / prev;
}

function calculateRiskReturn(history, symbol) {
  if (!history || history.length < 5) return null;
  const lastQuotes = history.slice(-252);
  const returns = [];
  for (let i = 1; i < lastQuotes.length; i++) {
    returns.push(Math.log(lastQuotes[i].Close / lastQuotes[i - 1].Close));
  }
  if (returns.length === 0) return null;
  const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
  const variance =
    returns.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / (returns.length - 1);
  const hv = Math.sqrt(variance) * Math.sqrt(252);
  const totalReturn =
    lastQuotes[lastQuotes.length - 1].Close / lastQuotes[0].Close - 1;
  return { symbol, hv, ret: totalReturn };
}

function getSectorETF(sector) {
  const map = {
    "Communication Services": "XLC",
    "Consumer Discretionary": "XLY",
    "Consumer Staples": "XLP",
    Energy: "XLE",
    Financials: "XLF",
    "Health Care": "XLV",
    Industrials: "XLI",
    "Information Technology": "XLK",
    Materials: "XLB",
    "Real Estate": "XLRE",
    Utilities: "XLU",
  };
  return sector && map[sector] ? map[sector] : "SPY";
}

function extractHighlights(rawData) {
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
    payout_ratio:
      info.payoutRatio ||
      (info.dividendRate && info.trailingEps
        ? info.dividendRate / info.trailingEps
        : null),
    debt_to_equity: info.debtToEquity,
    current_ratio: info.currentRatio,
  };
}

function extractEarningsSurprise(rawData) {
  const datesObj = rawData.earnings_dates || {};
  const dates = Object.keys(datesObj).sort(
    (a, b) => new Date(b).getTime() - new Date(a).getTime(),
  );
  for (const date of dates) {
    const d = datesObj[date];
    if (d && d["Reported EPS"] !== null && d["Reported EPS"] !== undefined) {
      return {
        date: date.split(" ")[0],
        actual: d["Reported EPS"],
        estimate: d["EPS Estimate"],
        surprise_pct: d["Surprise(%)"],
      };
    }
  }
  return null;
}

function extractNextEarnings(rawData) {
  const datesObj = rawData.earnings_dates || {};
  const dates = Object.keys(datesObj).sort(
    (a, b) => new Date(a).getTime() - new Date(b).getTime(),
  );
  const now = new Date();
  for (const date of dates) {
    if (new Date(date) >= now) {
      const d = datesObj[date];
      if (d && (d["Reported EPS"] === null || d["Reported EPS"] === undefined)) {
        return {
          date: date.split(" ")[0],
          estimate: d["EPS Estimate"] || rawData.info?.earningsAverage || null,
        };
      }
    }
  }
  if (rawData.calendar?.["Earnings Date"]) {
    const cDates = Array.isArray(rawData.calendar["Earnings Date"])
      ? rawData.calendar["Earnings Date"]
      : [rawData.calendar["Earnings Date"]];
    if (cDates.length > 0 && new Date(cDates[0]) >= now) {
      return {
        date: cDates[0].split(" ")[0],
        estimate:
          rawData.calendar["Earnings Average"] ||
          rawData.calendar["EPS Estimate"] ||
          rawData.info?.earningsAverage ||
          null,
      };
    }
  }
  return null;
}

function extractConsensus(rawData) {
  const info = rawData.info || {};
  return {
    earnings: {
      "0q": {
        avg: info.earningsAverage,
        low: info.earningsLow,
        high: info.earningsHigh,
        growth: info.earningsGrowth,
        numberOfAnalysts: info.numberOfAnalystOpinions,
      },
    },
    revenue: {
      "0q": {
        avg: info.revenueAverage,
        low: info.revenueLow,
        high: info.revenueHigh,
        growth: info.revenueGrowth,
        numberOfAnalysts: info.numberOfAnalystOpinions,
      },
    },
  };
}

function extractRatingChanges(rawData) {
  const ud = rawData.upgrades_downgrades || {};
  const dates = Object.keys(ud).sort(
    (a, b) => new Date(b).getTime() - new Date(a).getTime(),
  );
  return dates.slice(0, 10).map((date) => ({
    GradeDate: date.split(" ")[0],
    Firm: ud[date]?.Firm || ud[date]?.firm,
    ToGrade: ud[date]?.ToGrade || ud[date]?.toGrade,
    FromGrade: ud[date]?.FromGrade || ud[date]?.fromGrade,
    Action: ud[date]?.Action || ud[date]?.action,
  }));
}

function extractAnalystRatings(rawData) {
  const info = rawData.info || {};
  const ratings = rawData.analyst_ratings || {};
  return {
    targetHighPrice: info.targetHighPrice,
    targetLowPrice: info.targetLowPrice,
    targetMeanPrice: info.targetMeanPrice,
    targetMedianPrice: info.targetMedianPrice,
    currentPrice: info.currentPrice,
    numberOfAnalystOpinions: info.numberOfAnalystOpinions,
    recommendationKey: info.recommendationKey,
    ...ratings,
  };
}

function generateRiskReturnChart(allMetrics, targetSymbol) {
  const others = allMetrics.filter((m) => m.symbol !== targetSymbol);
  const target = allMetrics.find((m) => m.symbol === targetSymbol);
  const datasets = [
    {
      label: "Other S&P 500",
      data: others.map((m) => ({ x: m.hv, y: m.ret, symbol: m.symbol })),
      backgroundColor: "rgba(200, 200, 200, 0.5)",
      pointRadius: 4,
    },
  ];
  if (target) {
    datasets.push({
      label: target.symbol,
      data: [{ x: target.hv, y: target.ret, symbol: target.symbol }],
      backgroundColor: "rgba(255, 0, 0, 0.9)",
      pointRadius: 8,
    });
  }
  return { datasets };
}

function getValFromArray(data, itemLabel, date) {
  const row = data.find((r) => r.index === itemLabel);
  return row ? Number(row[date]) || 0 : 0;
}

function generateFinancialChart(data, fields, type, barmode) {
  if (data.length === 0) return null;
  const dates = Object.keys(data[0])
    .filter((k) => k !== "index")
    .sort();
  if (dates.length === 0) return null;
  const colors = [
    { bg: "rgba(54, 162, 235, 0.5)", border: "rgb(54, 162, 235)" },
    { bg: "rgba(255, 99, 132, 0.5)", border: "rgb(255, 99, 132)" },
    { bg: "rgba(75, 192, 192, 0.5)", border: "rgb(75, 192, 192)" },
    { bg: "rgba(255, 159, 64, 0.5)", border: "rgb(255, 159, 64)" },
  ];
  return {
    labels: dates.map((d) => d.split(" ")[0]),
    datasets: fields.map((field, i) => ({
      label: field,
      data: dates.map((d) => getValFromArray(data, field, d)),
      backgroundColor: colors[i % colors.length].bg,
      borderColor: colors[i % colors.length].border,
      borderWidth: 1,
    })),
  };
}

function generatePerformanceChart(history, etfHistory, symbol, etfSymbol) {
  if (!history || history.length === 0) return null;
  const targetData = history.slice(-252);
  if (targetData.length === 0) return null;
  const startClose = targetData[0].Close;
  const dates = targetData.map((h) => (h.Date || h.index).split(" ")[0]);
  const targetReturns = targetData.map((h) => h.Close / startClose - 1);
  const datasets = [
    {
      label: symbol,
      data: targetReturns,
      borderColor: "#1f77b4",
      borderWidth: 2,
      fill: false,
      pointRadius: 0,
    },
  ];
  if (etfHistory && etfHistory.length > 0) {
    const etfMap = new Map(
      etfHistory.map((h) => [(h.Date || h.index).split(" ")[0], h.Close]),
    );
    let etfStartClose = etfMap.get(dates[0]);
    if (!etfStartClose) {
      for (const d of dates) {
        if (etfMap.has(d)) {
          etfStartClose = etfMap.get(d);
          break;
        }
      }
    }
    if (etfStartClose) {
      const etfReturns = dates.map((d) => {
        const close = etfMap.get(d) || etfStartClose;
        return close / etfStartClose - 1;
      });
      datasets.push({
        label: `Sector ETF (${etfSymbol})`,
        data: etfReturns,
        borderColor: "#ff7f0e",
        borderWidth: 2,
        borderDash: [5, 5],
        fill: false,
        pointRadius: 0,
      });
    }
  }
  return { labels: dates, datasets };
}

function generateTpChart(cf, is) {
  if (is.length === 0 && cf.length === 0) return null;
  const dates = Object.keys(is[0] || cf[0] || {})
    .filter((k) => k !== "index")
    .sort();
  if (dates.length === 0) return null;
  const niKeys = [
    "Net Income",
    "Net Income From Continuing Operations",
    "Net Income Common Stockholders",
  ];
  const divKeys = ["Cash Dividends Paid", "Common Stock Dividend Paid"];
  const repoKeys = [
    "Repurchase Of Capital Stock",
    "Repurchase Of Common Stock",
    "Common Stock Repurchased",
  ];
  const getAnyVal = (arr, keys, date) => {
    for (const k of keys) {
      const v = getValFromArray(arr, k, date);
      if (v !== 0) return v;
    }
    return 0;
  };
  const niData = dates.map(
    (d) => getAnyVal(is, niKeys, d) || getAnyVal(cf, niKeys, d),
  );
  const divData = dates.map((d) => Math.abs(getAnyVal(cf, divKeys, d)));
  const repoData = dates.map((d) => Math.abs(getAnyVal(cf, repoKeys, d)));
  const divRatio = niData.map((ni, i) => (ni > 0 ? divData[i] / ni : 0));
  const totalRatio = niData.map((ni, i) =>
    ni > 0 ? (divData[i] + repoData[i]) / ni : 0,
  );
  return {
    labels: dates.map((d) => d.split(" ")[0]),
    datasets: [
      {
        type: "bar",
        label: "Net Income",
        data: niData,
        backgroundColor: "rgba(44, 160, 44, 0.6)",
        yAxisID: "y",
      },
      {
        type: "bar",
        label: "Dividends Paid",
        data: divData,
        backgroundColor: "rgba(174, 199, 232, 0.6)",
        yAxisID: "y",
      },
      {
        type: "bar",
        label: "Stock Repurchase",
        data: repoData,
        backgroundColor: "rgba(31, 119, 180, 0.6)",
        yAxisID: "y",
      },
      {
        type: "line",
        label: "Dividend Payout Ratio",
        data: divRatio,
        borderColor: "#ffbb78",
        yAxisID: "y1",
      },
      {
        type: "line",
        label: "Total Payout Ratio",
        data: totalRatio,
        borderColor: "#ff7f0e",
        yAxisID: "y1",
      },
    ],
  };
}

function generateDpsEpsChart(dividends) {
  if (!dividends || !Array.isArray(dividends) || dividends.length === 0)
    return null;
  const annual = {};
  dividends.forEach((d) => {
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
    datasets: [
      {
        type: "bar",
        label: "Annual Dividends",
        data: labels.map((y) => annual[y]),
        backgroundColor: "rgba(31, 119, 180, 0.8)",
      },
    ],
  };
}

function generateSegmentChart(segmentData) {
  if (!segmentData || !Array.isArray(segmentData) || segmentData.length === 0)
    return null;
  const segmentsArr = Object.keys(segmentData[0]).filter(
    (k) => !["Date", "report_date", "symbol", "index"].includes(k),
  );
  if (segmentsArr.length === 0) return null;
  const labels = segmentData.map((row) =>
    String(row.Date || row.report_date || row.index).split(" ")[0],
  );
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
  const datasets = segmentsArr.map((seg, i) => ({
    label: seg,
    data: segmentData.map((row) => Number(row[seg]) || 0),
    backgroundColor: colors[i % colors.length],
  }));
  return { labels, datasets };
}

// === main ===

async function main() {
  const t0 = Date.now();
  console.log(`bucket=${BUCKET} concurrency=${CONCURRENCY}`);
  console.log("Listing raw/ keys...");
  const rawKeys = (await listAll("raw/")).filter(
    (k) => k.endsWith(".json") && k !== "raw/stocks_list.json",
  );
  console.log(`  found ${rawKeys.length} raw files`);

  let baseStocksList = [];
  try {
    baseStocksList = await getJson("raw/stocks_list.json");
  } catch {
    console.log("  raw/stocks_list.json not found, using empty metadata");
  }

  console.log(`Downloading ${rawKeys.length} raw objects...`);
  const rawDataMap = {};
  const dlResults = await pMap(
    rawKeys,
    async (key) => {
      const data = await getJson(key);
      const symbol = data.symbol || key.replace("raw/", "").replace(".json", "");
      rawDataMap[symbol] = data;
      return symbol;
    },
    CONCURRENCY,
  );
  const dlFails = dlResults.filter((r) => !r.ok).length;
  if (dlFails) console.log(`  ${dlFails} downloads failed`);

  console.log("Computing risk-return / sector / sub-industry maps...");
  const riskReturnMetrics = [];
  const sectorMap = {};
  const subIndustryMap = {};
  for (const symbol of Object.keys(rawDataMap)) {
    const rawData = rawDataMap[symbol];
    const rr = calculateRiskReturn(rawData.history, symbol);
    if (rr) riskReturnMetrics.push(rr);
    const metadata =
      baseStocksList.find(
        (s) => s.Symbol_YF === symbol || s.Symbol === symbol,
      ) || {};
    const sector = metadata["GICS Sector"] || rawData.info?.sector || "Unknown";
    const subInd =
      metadata["GICS Sub-Industry"] || rawData.info?.industry || "Unknown";
    const dailyChange = calculateDailyChange(rawData.history);
    const peerInfo = {
      Symbol: metadata.Symbol || symbol,
      Symbol_YF: symbol,
      Daily_Change: dailyChange,
    };
    if (!sectorMap[sector]) sectorMap[sector] = [];
    sectorMap[sector].push(peerInfo);
    if (!subIndustryMap[subInd]) subIndustryMap[subInd] = [];
    subIndustryMap[subInd].push(peerInfo);
  }

  const movementReasons = {};
  const ETFS = [
    "SPY",
    "XLC",
    "XLY",
    "XLP",
    "XLE",
    "XLF",
    "XLV",
    "XLI",
    "XLK",
    "XLB",
    "XLRE",
    "XLU",
  ];

  console.log("Generating reports/*.json...");
  const symbols = Object.keys(rawDataMap);
  const updatedStocksList = [];
  const updatedLock = []; // 並列 push の安全化のため append-only にする
  const putResults = await pMap(
    symbols,
    async (symbol) => {
      const rawData = rawDataMap[symbol];
      const isETF = ETFS.includes(symbol);
      const metadata =
        baseStocksList.find(
          (s) => s.Symbol_YF === symbol || s.Symbol === symbol,
        ) || {};
      const sectorEtf = getSectorETF(
        metadata["GICS Sector"] || rawData.info?.sector,
      );
      const etfRawData = rawDataMap[sectorEtf];

      const highlights = extractHighlights(rawData);
      const earningsSurprise = extractEarningsSurprise(rawData);
      const nextEarnings = extractNextEarnings(rawData);
      const consensus = extractConsensus(rawData);
      const ratingChanges = extractRatingChanges(rawData);
      const analystRatings = extractAnalystRatings(rawData);

      const riskReturnChart = generateRiskReturnChart(riskReturnMetrics, symbol);
      const isChart = generateFinancialChart(
        rawData.income_stmt || [],
        ["Total Revenue", "Gross Profit", "Operating Income", "Net Income"],
        "bar",
        "group",
      );
      const bsChart = generateFinancialChart(
        rawData.balancesheet || [],
        [
          "Total Assets",
          "Total Liabilities Net Minority Interest",
          "Stockholders Equity",
        ],
        "bar",
        "stack",
      );
      const cfChart = generateFinancialChart(
        rawData.cashflow || [],
        [
          "Operating Cash Flow",
          "Investing Cash Flow",
          "Financing Cash Flow",
          "Free Cash Flow",
        ],
        "bar",
        "group",
      );
      const perfChart = generatePerformanceChart(
        rawData.history,
        etfRawData?.history,
        symbol,
        sectorEtf,
      );
      const tpChart = generateTpChart(
        rawData.cashflow || [],
        rawData.income_stmt || [],
      );
      const dpsChart = generateDpsEpsChart(rawData.dividends);
      const segmentChart = generateSegmentChart(rawData.revenue_by_segment);
      const geoChart = generateSegmentChart(rawData.revenue_by_geography);

      const sector =
        metadata["GICS Sector"] || rawData.info?.sector || "Unknown";
      const subInd =
        metadata["GICS Sub-Industry"] || rawData.info?.industry || "Unknown";

      const reportData = {
        symbol: metadata.Symbol || symbol,
        symbol_yf: symbol,
        security:
          metadata.Security ||
          rawData.info?.longName ||
          rawData.info?.shortName ||
          symbol,
        security_ja: metadata.Security_JA || null,
        business_summary_ja: rawData.info?.longBusinessSummary || null,
        sector,
        sub_industry: subInd,
        exchange: rawData.info?.exchange || "NASDAQ",
        full_symbol: `${rawData.info?.exchange || "NASDAQ"}:${symbol.replace("-", ".")}`,
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
          eps_revisions: rawData.info?.epsRevisions || null,
        },
        rating_changes: ratingChanges,
        analyst_ratings: analystRatings,
        peers: {
          sub_industry: (subIndustryMap[subInd] || []).filter(
            (s) => s.Symbol_YF !== symbol,
          ),
          sector: (sectorMap[sector] || []).filter(
            (s) =>
              s.Symbol_YF !== symbol &&
              !subIndustryMap[subInd]?.find((si) => si.Symbol_YF === s.Symbol_YF),
          ),
        },
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
          geo: geoChart,
        },
        last_updated: new Date().toISOString(),
      };

      await putJson(`reports/${symbol}.json`, reportData);

      if (!isETF || metadata.Symbol) {
        updatedLock.push({
          ...metadata,
          Symbol_YF: symbol,
          Daily_Change: calculateDailyChange(rawData.history),
          Has_Movement_Reason: !!movementReasons[symbol],
        });
      }
      return symbol;
    },
    CONCURRENCY,
  );
  const putFails = putResults.filter((r) => !r.ok).length;
  if (putFails) {
    console.log(`  ${putFails} report puts failed`);
    for (const r of putResults.filter((x) => !x.ok)) {
      console.log(`    error: ${r.error?.message || r.error}`);
    }
  }

  updatedStocksList.push(...updatedLock);
  if (updatedStocksList.length > 0) {
    await putJson("reports/stocks.json", updatedStocksList);
    console.log(
      `Saved reports/stocks.json with ${updatedStocksList.length} items.`,
    );
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`完了: reports=${symbols.length - putFails}, failed=${putFails}, elapsed=${elapsed}s`);
}

main().catch((e) => {
  console.error("FATAL:", e);
  process.exit(1);
});
