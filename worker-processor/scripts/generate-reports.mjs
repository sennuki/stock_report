#!/usr/bin/env node
/**
 * GitHub Actions runner 上で reports/*.json を生成する。
 *
 * 元々は worker-processor の processAllStocks (ctx.waitUntil) で行っていたが、
 * Cloudflare Workers の sub-request 制限 (paid 1000) を 1500 銘柄 × GET+PUT で
 * 大幅に超過するため、外部（GitHub Actions）で一括生成して R2 に書き込む方式に変更。
 */

import "dotenv/config";
import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import pMap from "p-map";
import YahooFinance from "yahoo-finance2";

const yahooFinance = new YahooFinance();
const BUCKET = process.env.R2_BUCKET_NAME || "defeat-beta-stock-data";
const CONCURRENCY = 20;

const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  },
});

const sectorEtfMap = {
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

const broadSectorEtfMap = {
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

const marketIndexMap = {
  'S&P 500': 'SPY',
  'S&P 400': 'MDY',
  'S&P 600': 'IJR'
};

const etfFullNameMap = {
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

async function listAll(prefix) {
  let keys = [];
  let token = null;
  do {
    const res = await s3.send(
      new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: prefix,
        ContinuationToken: token,
      }),
    );
    if (res.Contents) {
      keys.push(...res.Contents.map((c) => c.Key));
    }
    token = res.NextContinuationToken;
  } while (token);
  return keys;
}

async function getJson(key) {
  const res = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
  const body = await res.Body.transformToString();
  const safe = body
    .replace(/\bNaN\b/g, "null")
    .replace(/\b-?Infinity\b/g, "null");
  return JSON.parse(safe);
}

async function putJson(key, data) {
  await s3.send(
    new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: JSON.stringify(data),
      ContentType: "application/json",
    }),
  );
}

// === helpers ===

// master の risk_return.py / performance_comparison.py に合わせた期間定義。
// days は履歴の末尾から何営業日分 (1Y=252) を使うかの目安 (YTD は年初から)。
const RR_PERIOD_CONFIGS = [
  { key: "1M", label: "1ヶ月", days: 21 },
  { key: "3M", label: "3ヶ月", days: 63 },
  { key: "6M", label: "6ヶ月", days: 126 },
  { key: "YTD", label: "年初来", days: "YTD" },
  { key: "1Y", label: "1年", days: 252 },
  { key: "3Y", label: "3年", days: 756 },
  { key: "5Y", label: "5年", days: 1260 },
  { key: "10Y", label: "10年", days: 2520 },
];

function calculateRiskReturn(history, symbol) {
  if (!history || !Array.isArray(history) || history.length < 5) return null;
  const safe = history.filter((h) => h && h.Close > 0);
  if (safe.length < 5) return null;

  // 全期間の log return を 1 度だけ計算しておき、 各期間で slice する。
  const logReturns = [0];
  for (let i = 1; i < safe.length; i++) {
    logReturns.push(Math.log(safe[i].Close / safe[i - 1].Close));
  }

  const result = { symbol };
  const lastDate = new Date(
    safe[safe.length - 1].Date || safe[safe.length - 1].index,
  );

  for (const p of RR_PERIOD_CONFIGS) {
    let startIdx;
    let isValid = true;
    if (p.days === "YTD") {
      const ytdStart = new Date(Date.UTC(lastDate.getUTCFullYear(), 0, 1));
      startIdx = safe.findIndex(
        (h) => new Date(h.Date || h.index) >= ytdStart,
      );
      if (startIdx === -1) startIdx = Math.max(0, safe.length - 21);
    } else {
      // 要求期間の 80% 以上のデータが必要 (上場直後の銘柄を長期から除外)
      if (safe.length < p.days * 0.8) {
        isValid = false;
        startIdx = -1;
      } else {
        startIdx = Math.max(0, safe.length - p.days);
      }
    }
    if (!isValid || safe.length - startIdx < 5) {
      result[`HV_${p.key}`] = null;
      result[`Ret_${p.key}`] = null;
      continue;
    }

    // 年率リスク (log return の標準偏差 × √252)
    const subRet = logReturns.slice(startIdx + 1);
    const mean = subRet.reduce((a, b) => a + b, 0) / subRet.length;
    const variance =
      subRet.reduce((a, b) => a + (b - mean) ** 2, 0) / (subRet.length - 1);
    const hv = Math.sqrt(variance * 252);

    // 年率換算リターン
    const startClose = safe[startIdx].Close;
    const lastClose = safe[safe.length - 1].Close;
    const totalRet = lastClose / startClose - 1;
    const startDate = new Date(
      safe[startIdx].Date || safe[startIdx].index,
    );
    const daysDiff = (lastDate - startDate) / (1000 * 60 * 60 * 24);
    let annRet;
    if (daysDiff > 5) {
      annRet = Math.pow(1 + totalRet, 365 / daysDiff) - 1;
    } else {
      annRet = totalRet;
    }
    result[`HV_${p.key}`] = Number.isFinite(hv) ? hv : null;
    result[`Ret_${p.key}`] = Number.isFinite(annRet) ? annRet : null;
  }

  // 1Y を従来通り ret / hv トップレベルにも入れて後方互換にしておく
  result.hv = result.HV_1Y ?? null;
  result.ret = result.Ret_1Y ?? null;
  return result;
}

function calculateDailyChange(history) {
  if (!history || history.length < 2) return 0;
  const last = history[history.length - 1].Close;
  const prev = history[history.length - 2].Close;
  return (last - prev) / prev;
}

function getSectorETF(sector, subIndustry) {
  return sectorEtfMap[subIndustry] || sectorEtfMap[sector] || 'SPY';
}

function getBenchmarkInfo(metadata) {
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

function toTradingViewExchange(ex) {
  const map = {
    NMS: "NASDAQ", NGM: "NASDAQ", NCM: "NASDAQ", NAS: "NASDAQ",
    NYQ: "NYSE", NYS: "NYSE",
    ASE: "AMEX", AMX: "AMEX",
    PCX: "NYSEARCA", BATS: "BATS", BTS: "BATS",
  };
  if (!ex) return "NASDAQ";
  return map[ex] || ex;
}

function extractHighlights(rawData) {
  const info = rawData.info || {};
  return {
    revenue_growth: info.revenueGrowth || null,
    roe: info.returnOnEquity || null,
    operating_margins: info.operatingMargins || null,
    pe_forward: info.forwardPE || null,
    pe_ttm: info.trailingPE || null,
    dividend_yield: info.dividendYield || null,
    debt_to_equity: info.debtToEquity || null,
    earnings_growth: info.earningsGrowth || null,
    profit_margins: info.profitMargins || null,
    current_ratio: info.currentRatio || null,
    eps_ttm: info.trailingEps || null,
    eps_forward: info.forwardEps || null,
    payout_ratio: info.payoutRatio || null,
  };
}

function extractEarningsSurprise(rawData) {
  const ed = rawData.earnings_dates;
  if (!ed || !Array.isArray(ed) || ed.length === 0) return null;
  // yfinance provides a list where each element is a date's data
  // We need to find the latest one that has a "Reported EPS"
  const sorted = [...ed].sort((a, b) => new Date(b.index || b.Date).getTime() - new Date(a.index || a.Date).getTime());
  for (const item of sorted) {
    if (item["Reported EPS"] !== null && item["Reported EPS"] !== undefined) {
      return {
        date: String(item.index || item.Date).split(" ")[0],
        actual: item["Reported EPS"],
        estimate: item["EPS Estimate"],
        surprise_pct: item["Surprise(%)"],
      };
    }
  }
  return null;
}

function extractNextEarnings(rawData) {
  const ed = rawData.earnings_dates;
  if (!ed || !Array.isArray(ed) || ed.length === 0) return null;
  const now = new Date();
  const sorted = [...ed].sort((a, b) => new Date(a.index || a.Date).getTime() - new Date(b.index || b.Date).getTime());
  for (const item of sorted) {
    const d = new Date(item.index || item.Date);
    if (d >= now && (item["Reported EPS"] === null || item["Reported EPS"] === undefined)) {
      return {
        date: String(item.index || item.Date).split(" ")[0],
        estimate: item["EPS Estimate"] || null,
      };
    }
  }
  const cal = rawData.calendar;
  if (cal && cal["Earnings Date"] && cal["Earnings Date"][0]) {
    return {
      date: cal["Earnings Date"][0],
      estimate: cal["Earnings Average"] || null,
    };
  }
  return null;
}

function extractConsensus(rawData) {
  const info = rawData.info || {};
  const consensus = {
    earnings: {},
    revenue: {}
  };

  // 0q from info dict (current quarter consensus, present even when estimate
  // DataFrames are empty).
  consensus.earnings["0q"] = {
    avg: info.earningsAverage ?? null,
    low: info.earningsLow ?? null,
    high: info.earningsHigh ?? null,
    growth: info.earningsGrowth ?? null,
    numberOfAnalysts: info.numberOfAnalystOpinions ?? 0
  };
  consensus.revenue["0q"] = {
    avg: info.revenueAverage ?? null,
    low: info.revenueLow ?? null,
    high: info.revenueHigh ?? null,
    growth: info.revenueGrowth ?? null,
    numberOfAnalysts: info.numberOfAnalystOpinions ?? 0
  };

  // yfinance's earnings_estimate / revenue_estimate DataFrames carry the
  // forecast periods. The DataFrame index is named 'period' so after
  // reset_index() each record has a 'period' field. Some yfinance versions
  // may emit 'Period' or carry the period as the integer index column instead.
  const eEst = Array.isArray(rawData.earnings_estimate) ? rawData.earnings_estimate : [];
  const rEst = Array.isArray(rawData.revenue_estimate) ? rawData.revenue_estimate : [];
  const getPeriod = (row) => String(row.period ?? row.Period ?? row.index ?? "").trim();
  const periodMatch = (row, target) => {
    const p = getPeriod(row).toLowerCase();
    return p === target.toLowerCase();
  };

  // master's get_row_val tries multiple case variants (original, lower, capitalize).
  const pickField = (row, base) => {
    const variants = [base, base.toLowerCase(), base.charAt(0).toUpperCase() + base.slice(1).toLowerCase()];
    for (const v of variants) {
      if (row[v] !== undefined && row[v] !== null) return row[v];
    }
    return null;
  };

  const buildEntry = (row) => ({
    avg: pickField(row, "avg"),
    low: pickField(row, "low"),
    high: pickField(row, "high"),
    growth: pickField(row, "growth"),
    numberOfAnalysts: pickField(row, "numberOfAnalysts") ?? 0,
  });

  // master replicates the same loop for all 4 periods including 0q. We override
  // 0q only when info-derived values are missing (avg == null), matching master's
  // `if p not in consensus["earnings"] or consensus["earnings"][p].get("avg") is None`.
  for (const period of ["0q", "+1q", "0y", "+1y"]) {
    const eRow = eEst.find((r) => periodMatch(r, period));
    if (eRow) {
      const existing = consensus.earnings[period];
      if (!existing || existing.avg == null) {
        consensus.earnings[period] = buildEntry(eRow);
      } else {
        // info had avg but may have missed low/high; fill those from DataFrame.
        if (existing.low == null) existing.low = pickField(eRow, "low");
        if (existing.high == null) existing.high = pickField(eRow, "high");
      }
    }
    const rRow = rEst.find((r) => periodMatch(r, period));
    if (rRow) {
      const existing = consensus.revenue[period];
      if (!existing || existing.avg == null) {
        consensus.revenue[period] = buildEntry(rRow);
      } else {
        if (existing.low == null) existing.low = pickField(rRow, "low");
        if (existing.high == null) existing.high = pickField(rRow, "high");
      }
    }
  }

  // growth_estimates DataFrame fallback (yfinance separate endpoint).
  // Rows look like {period:"0q"|"+1y"|..., stockTrend:number, indexTrend:number}.
  const gEst = Array.isArray(rawData.growth_estimates) ? rawData.growth_estimates : [];
  const growthByPeriod = new Map();
  for (const r of gEst) {
    const p = getPeriod(r).toLowerCase();
    const v = r.stockTrend ?? r.StockTrend ?? r.stocktrend ?? null;
    if (p && typeof v === "number") growthByPeriod.set(p, v);
  }

  // Fallback: yfinance's info dict has forwardEps for forward 12-month EPS.
  // If +1y is still missing but forwardEps exists, use it as a coarse proxy.
  if (consensus.earnings["+1y"] == null && typeof info.forwardEps === "number") {
    consensus.earnings["+1y"] = {
      avg: info.forwardEps,
      low: null,
      high: null,
      growth: growthByPeriod.get("+1y") ?? info.earningsGrowth ?? null,
      numberOfAnalysts: info.numberOfAnalystOpinions ?? 0,
    };
  }

  // Fallback for revenue +1y: yfinance info has no forward revenue field, but
  // we can approximate it as totalRevenue (TTM) × (1 + growth). Prefer the
  // growth_estimates +1y row if available, otherwise fall back to revenueGrowth
  // (which yfinance typically reports as the most recent quarterly YoY).
  if (consensus.revenue["+1y"] == null && typeof info.totalRevenue === "number") {
    const g = growthByPeriod.get("+1y") ?? info.revenueGrowth ?? null;
    if (typeof g === "number") {
      consensus.revenue["+1y"] = {
        avg: info.totalRevenue * (1 + g),
        low: null,
        high: null,
        growth: g,
        numberOfAnalysts: info.numberOfAnalystOpinions ?? 0,
        _estimated: true,
      };
    }
  }

  // Fallback for revenue 0q: when info.revenueAverage is missing but quarterly
  // income statement carries Total Revenue, derive an approximate quarterly
  // forecast as last_quarter_revenue × (1 + revenueGrowth).
  if (
    (consensus.revenue["0q"] == null || consensus.revenue["0q"].avg == null) &&
    typeof info.revenueGrowth === "number"
  ) {
    const q = rawData.quarterly_income_stmt;
    const lastQRevenue = (() => {
      if (!Array.isArray(q) || q.length === 0) return null;
      const row = q.find((r) => /total\s*revenue/i.test(String(r.index || "")));
      if (!row) return null;
      const dateKeys = Object.keys(row).filter((k) => /^\d{4}-\d{2}-\d{2}/.test(k));
      if (!dateKeys.length) return null;
      dateKeys.sort();
      const latest = row[dateKeys[dateKeys.length - 1]];
      return typeof latest === "number" ? latest : null;
    })();
    if (lastQRevenue != null) {
      consensus.revenue["0q"] = {
        avg: lastQRevenue * (1 + info.revenueGrowth),
        low: null,
        high: null,
        growth: info.revenueGrowth,
        numberOfAnalysts: info.numberOfAnalystOpinions ?? 0,
        _estimated: true,
      };
    }
  }

  return consensus;
}

function extractRatingChanges(rawData) {
  const ud = rawData.upgrades_downgrades;
  if (!ud || !Array.isArray(ud)) return [];

  // Build a date → close map from history so we can attach PriceAtRating.
  // history rows from df_to_dict_safe look like {Date:"YYYY-MM-DD HH:MM:SS+00:00", Close:N, ...}
  const history = Array.isArray(rawData.history) ? rawData.history : [];
  const priceByDate = new Map();
  for (const h of history) {
    const d = String(h.Date || h.index || "").split("T")[0].split(" ")[0];
    const c = typeof h.Close === "number" ? h.Close : null;
    if (d && c != null) priceByDate.set(d, c);
  }
  const sortedDates = [...priceByDate.keys()].sort();
  const priceAt = (date) => {
    if (priceByDate.has(date)) return priceByDate.get(date);
    // Walk back to the nearest trading day (weekends/holidays).
    let last = null;
    for (const d of sortedDates) {
      if (d <= date) last = priceByDate.get(d);
      else break;
    }
    return last;
  };

  return ud.slice(0, 10).map((x) => {
    const gradeDate = String(x.GradeDate || x.index || x.Date || "").split("T")[0].split(" ")[0];
    return {
      GradeDate: gradeDate,
      Firm: x.Firm || x.firm,
      // yfinance returns ToGrade/FromGrade (no spaces). Older variants used "To Grade".
      ToGrade: x.ToGrade || x["To Grade"] || x.toGrade || "",
      FromGrade: x.FromGrade || x["From Grade"] || x.fromGrade || "",
      Action: x.Action || x.action || "",
      PriceAtRating: priceAt(gradeDate),
      currentPriceTarget: 0,
      priorPriceTarget: 0,
      priceTargetAction: "",
    };
  });
}

/**
 * Fetch upgrade/downgrade history from Yahoo Finance (via yahoo-finance2)
 * since yfinance's upgradeDowngradeHistory module returns currentPriceTarget /
 * priorPriceTarget / priceTargetAction columns that aren't exposed in the
 * Python yfinance package.
 *
 * Returns an array of { Firm, ToGrade, FromGrade, Action, GradeDate, currentPriceTarget,
 * priorPriceTarget, priceTargetAction } records, or null on failure.
 */
async function fetchUpgradeDowngradeHistory(symbol) {
  try {
    const summary = await yahooFinance.quoteSummary(symbol, {
      modules: ["upgradeDowngradeHistory"],
    });
    const history = summary?.upgradeDowngradeHistory?.history;
    if (!Array.isArray(history)) return null;
    return history.map((h) => {
      const d = h.epochGradeDate instanceof Date ? h.epochGradeDate : new Date(h.epochGradeDate);
      return {
        GradeDate: isNaN(d.getTime()) ? null : d.toISOString().split("T")[0],
        Firm: h.firm || "",
        ToGrade: h.toGrade || "",
        FromGrade: h.fromGrade || "",
        Action: h.action || "",
        currentPriceTarget: typeof h.currentPriceTarget === "number" ? h.currentPriceTarget : 0,
        priorPriceTarget: typeof h.priorPriceTarget === "number" ? h.priorPriceTarget : 0,
        priceTargetAction: h.priceTargetAction || "",
      };
    });
  } catch (e) {
    return null;
  }
}

/**
 * Merge target-price fields from yahoo-finance2's history into the rating_changes
 * list produced by extractRatingChanges. Match by (GradeDate, Firm) since both
 * sources should agree on those.
 */
function mergeTargetPrices(ratingChanges, yfHistory) {
  if (!Array.isArray(yfHistory) || yfHistory.length === 0) return ratingChanges;
  const key = (r) => `${r.GradeDate}|${(r.Firm || "").toLowerCase()}`;
  const byKey = new Map(yfHistory.map((h) => [key(h), h]));
  for (const rc of ratingChanges) {
    const match = byKey.get(key(rc));
    if (match) {
      rc.currentPriceTarget = match.currentPriceTarget || 0;
      rc.priorPriceTarget = match.priorPriceTarget || 0;
      rc.priceTargetAction = match.priceTargetAction || "";
    }
  }
  return ratingChanges;
}

function extractAnalystRatings(rawData) {
  const info = rawData.info || {};
  // analyst_ratings from fetch_raw_data.py is an array of period records
  // [{index:"0", period:"0m", strongBuy:7, buy:24, hold:15, sell:1, strongSell:1}, ...]
  const ratingsRaw = rawData.analyst_ratings;
  let currentRating = {};
  if (Array.isArray(ratingsRaw)) {
    currentRating = ratingsRaw.find((r) => r.period === "0m") || ratingsRaw[0] || {};
  } else if (ratingsRaw && typeof ratingsRaw === "object") {
    currentRating = ratingsRaw;
  }
  return {
    recommendationKey: (info.recommendationKey || "hold").toLowerCase(),
    targetMeanPrice: info.targetMeanPrice || null,
    targetHighPrice: info.targetHighPrice || null,
    targetLowPrice: info.targetLowPrice || null,
    targetMedianPrice: info.targetMedianPrice || null,
    numberOfAnalystOpinions: info.numberOfAnalystOpinions || 0,
    currentPrice: info.currentPrice || info.regularMarketPrice || null,
    strongBuy: currentRating.strongBuy || 0,
    buy: currentRating.buy || 0,
    hold: currentRating.hold || 0,
    sell: currentRating.sell || 0,
    strongSell: currentRating.strongSell || 0,
  };
}

function getValFromArray(arr, field, date) {
  const row = arr.find((r) => r.index === field);
  return row ? row[date] : null;
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
  
  const labelMap = {
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
    labels: dates.map((d) => d.split(" ")[0]),
    datasets: fields.map((field, i) => ({
      label: labelMap[field] || field,
      data: dates.map((d) => getValFromArray(data, field, d)),
      backgroundColor: colors[i % colors.length].bg,
      borderColor: colors[i % colors.length].border,
      borderWidth: 1,
    })),
  };
}

// 損益計算書: 売上高 / 売上総利益 / 営業利益 / 純利益 の 4 本並列棒 +
// 売上総利益率 / 営業利益率 / 純利益率 を右側軸の折れ線で表示する。
// master の Python fundamentals.get_is_chart_data に揃えた構造。
function _isDatasets(stmt, suffix, hidden) {
  if (!stmt || stmt.length === 0) return null;
  const dates = Object.keys(stmt[0])
    .filter((k) => k !== "index" && !k.includes("TTM"))
    .sort();
  if (dates.length === 0) return null;
  const get = (label) =>
    dates.map((d) => Number(getValFromArray(stmt, label, d)) || 0);

  const revenue = get("Total Revenue");
  // 通年は最後の 6 期、四半期は最後の 8 期を使う (master の挙動に合わせる)
  const sliceCount = suffix === " (四半期)" ? 8 : 6;
  const validIdx = revenue
    .map((v, i) => (v > 0 ? i : -1))
    .filter((i) => i !== -1)
    .slice(-sliceCount);
  if (validIdx.length === 0) return null;
  const pick = (arr) => validIdx.map((i) => arr[i]);
  const labels = validIdx.map((i) => dates[i].split(" ")[0]);

  const rev = pick(revenue);
  const grossProfit = pick(get("Gross Profit"));
  const operatingIncome = pick(get("Operating Income"));
  const netIncome = pick(get("Net Income"));
  const ratio = (num, den) =>
    num.map((v, i) => (den[i] ? v / den[i] : null));

  const datasets = [
    { type: "bar", label: `売上高${suffix}`, data: rev,
      backgroundColor: "rgba(174, 199, 232, 0.85)", yAxisID: "y", hidden },
    { type: "bar", label: `売上総利益${suffix}`, data: grossProfit,
      backgroundColor: "rgba(31, 119, 180, 0.85)", yAxisID: "y", hidden },
    { type: "bar", label: `営業利益${suffix}`, data: operatingIncome,
      backgroundColor: "rgba(255, 187, 120, 0.85)", yAxisID: "y", hidden },
    { type: "bar", label: `純利益${suffix}`, data: netIncome,
      backgroundColor: "rgba(44, 160, 44, 0.85)", yAxisID: "y", hidden },
    { type: "line", label: `売上総利益率${suffix}`, data: ratio(grossProfit, rev),
      borderColor: "#1f77b4", backgroundColor: "#1f77b4",
      borderWidth: 2, fill: false, pointRadius: 4, yAxisID: "y1", hidden },
    { type: "line", label: `営業利益率${suffix}`, data: ratio(operatingIncome, rev),
      borderColor: "#ffbb78", backgroundColor: "#ffbb78",
      borderWidth: 2, fill: false, pointRadius: 4, yAxisID: "y1", hidden },
    { type: "line", label: `純利益率${suffix}`, data: ratio(netIncome, rev),
      borderColor: "#2ca02c", backgroundColor: "#2ca02c",
      borderWidth: 2, fill: false, pointRadius: 4, yAxisID: "y1", hidden },
  ];
  return { labels, datasets };
}

// 各データセットに自分の x軸ラベル と 元データ を埋め込んで、
// setupControls がグループ切替時に正しいラベル列に揃えられるようにする。
function _tagGroupLabels(group) {
  if (!group) return null;
  group.datasets.forEach((ds) => {
    ds._xLabels = group.labels;
    ds._originalData = [...ds.data];
  });
  return group;
}

function _mergeGroups(annual, quarterly) {
  if (!annual && !quarterly) return null;
  const base = annual || quarterly;
  return {
    labels: base.labels,
    datasets: [
      ...(annual?.datasets || []),
      ...(quarterly?.datasets || []),
    ],
  };
}

function generateIsChart(incomeStmt, quarterlyIncomeStmt) {
  const annual = _tagGroupLabels(_isDatasets(incomeStmt, " (通年)", false));
  const quarterly = _tagGroupLabels(_isDatasets(quarterlyIncomeStmt, " (四半期)", true));
  return _mergeGroups(annual, quarterly);
}

// 貸借対照表: 資産側 (固定+流動) と負債・純資産側 (純資産+固定負債+流動負債)
// を別 stack に積み上げて並列表示する。
// master の Python fundamentals.get_bs_chart_data に揃えた構造。
function _bsDatasets(balanceSheet, suffix, hidden) {
  if (!balanceSheet || balanceSheet.length === 0) return null;
  const dates = Object.keys(balanceSheet[0])
    .filter((k) => k !== "index")
    .sort();
  if (dates.length === 0) return null;
  const get = (label) =>
    dates.map((d) => Number(getValFromArray(balanceSheet, label, d)) || 0);
  const sum = (arr) => arr.reduce((a, b) => a + b, 0);

  const totalAssets = get("Total Assets");
  const sliceCount = suffix === " (四半期)" ? 8 : 6;
  const validIdx = totalAssets
    .map((v, i) => (v > 0 ? i : -1))
    .filter((i) => i !== -1)
    .slice(-sliceCount);
  if (validIdx.length === 0) return null;
  const pick = (arr) => validIdx.map((i) => arr[i]);
  const labels = validIdx.map((i) => dates[i].split(" ")[0]);

  const currentAssets = pick(get("Current Assets"));
  const nonCurrentAssets = pick(get("Total Non Current Assets"));
  const currentLiab = pick(get("Current Liabilities"));
  const nonCurrentLiab = pick(
    get("Total Non Current Liabilities Net Minority Interest"),
  );
  let equity = pick(get("Total Equity Gross Minority Interest"));
  if (sum(equity) === 0) equity = pick(get("Stockholders Equity"));
  const totalAssetsValid = pick(totalAssets);
  const totalLiabValid = pick(get("Total Liabilities Net Minority Interest"));

  const hasBreakdown = sum(currentAssets) !== 0 && sum(currentLiab) !== 0;

  const datasets = hasBreakdown
    ? [
        { label: `固定資産${suffix}`, data: nonCurrentAssets,
          backgroundColor: "rgba(31, 119, 180, 0.85)", stack: `assets${suffix}`, hidden },
        { label: `流動資産${suffix}`, data: currentAssets,
          backgroundColor: "rgba(174, 199, 232, 0.85)", stack: `assets${suffix}`, hidden },
        { label: `純資産${suffix}`, data: equity,
          backgroundColor: "rgba(44, 160, 44, 0.85)", stack: `liabilities${suffix}`, hidden },
        { label: `固定負債${suffix}`, data: nonCurrentLiab,
          backgroundColor: "rgba(255, 127, 14, 0.85)", stack: `liabilities${suffix}`, hidden },
        { label: `流動負債${suffix}`, data: currentLiab,
          backgroundColor: "rgba(255, 187, 120, 0.85)", stack: `liabilities${suffix}`, hidden },
      ]
    : [
        { label: `総資産${suffix}`, data: totalAssetsValid,
          backgroundColor: "rgba(31, 119, 180, 0.85)", stack: `assets${suffix}`, hidden },
        { label: `純資産${suffix}`, data: equity,
          backgroundColor: "rgba(44, 160, 44, 0.85)", stack: `liabilities${suffix}`, hidden },
        { label: `総負債${suffix}`, data: totalLiabValid,
          backgroundColor: "rgba(255, 127, 14, 0.85)", stack: `liabilities${suffix}`, hidden },
      ];
  return { labels, datasets };
}

function generateBsChart(balanceSheet, quarterlyBalanceSheet) {
  const annual = _tagGroupLabels(_bsDatasets(balanceSheet, " (通年)", false));
  const quarterly = _tagGroupLabels(_bsDatasets(quarterlyBalanceSheet, " (四半期)", true));
  return _mergeGroups(annual, quarterly);
}

// キャッシュフロー: 純利益 + 営業/投資/財務/フリー CF の 5 本並列棒。
// master の Python fundamentals.get_cf_chart_data に揃えた構造。
function _cfDatasets(cashflow, incomeStmt, suffix, hidden) {
  if (!cashflow || cashflow.length === 0) return null;
  const dates = Object.keys(cashflow[0])
    .filter((k) => k !== "index" && !k.includes("TTM"))
    .sort();
  if (dates.length === 0) return null;
  const getCf = (label) =>
    dates.map((d) => Number(getValFromArray(cashflow, label, d)) || 0);
  const getIs = (label) =>
    dates.map((d) =>
      Number(getValFromArray(incomeStmt || [], label, d)) || 0,
    );

  const opCf = getCf("Operating Cash Flow");
  const sliceCount = suffix === " (四半期)" ? 8 : 6;
  const validIdx = opCf
    .map((v, i) => (v !== 0 ? i : -1))
    .filter((i) => i !== -1)
    .slice(-sliceCount);
  if (validIdx.length === 0) return null;
  const pick = (arr) => validIdx.map((i) => arr[i]);
  const labels = validIdx.map((i) => dates[i].split(" ")[0]);

  return {
    labels,
    datasets: [
      { type: "bar", label: `純利益${suffix}`, data: pick(getIs("Net Income")),
        backgroundColor: "rgba(44, 160, 44, 0.85)", hidden },
      { type: "bar", label: `営業CF${suffix}`, data: pick(opCf),
        backgroundColor: "rgba(174, 199, 232, 0.85)", hidden },
      { type: "bar", label: `投資CF${suffix}`, data: pick(getCf("Investing Cash Flow")),
        backgroundColor: "rgba(31, 119, 180, 0.85)", hidden },
      { type: "bar", label: `財務CF${suffix}`, data: pick(getCf("Financing Cash Flow")),
        backgroundColor: "rgba(255, 187, 120, 0.85)", hidden },
      { type: "bar", label: `フリーCF${suffix}`, data: pick(getCf("Free Cash Flow")),
        backgroundColor: "rgba(148, 103, 189, 0.85)", hidden },
    ],
  };
}

function generateCfChart(cashflow, incomeStmt, quarterlyCashflow, quarterlyIncomeStmt) {
  const annual = _tagGroupLabels(_cfDatasets(cashflow, incomeStmt, " (通年)", false));
  const quarterly = _tagGroupLabels(_cfDatasets(quarterlyCashflow, quarterlyIncomeStmt, " (四半期)", true));
  return _mergeGroups(annual, quarterly);
}

// パフォーマンス比較: 対象銘柄 / セクター ETF / S&P 500 (SPY) の累積リターンを
// 8 期間で表示。 各期間で 2 〜 3 trace、 ラベル末尾の "(1年)" などで ChartJs.astro
// が期間切替タブを描画する。 初期表示は 1年。
// 各期間で日付範囲が異なるため Plotly 形式 ({data: [{name,x,y,...}], layout})
// で出力し、 ChartJs.astro の transformPlotlyToChartJs に処理させる。
// master の performance_comparison.generate_performance_chart_fig に揃えた構造。
function generatePerformanceChart(history, etfHistory, spyHistory, symbol, etfSymbol) {
  if (!history || history.length === 0) return null;
  const lastDate = new Date(
    history[history.length - 1].Date || history[history.length - 1].index,
  );

  const normalise = (h) =>
    (h || [])
      .filter((r) => r && r.Close > 0)
      .map((r) => ({ date: new Date(r.Date || r.index), close: r.Close }));
  const targetSeries = normalise(history);
  const etfSeries = normalise(etfHistory);
  const spySeries = normalise(spyHistory);

  const meta = {
    [symbol]: { name: symbol, color: "#ff6b01", series: targetSeries },
    [etfSymbol]: { name: etfSymbol, color: "#006cac", series: etfSeries },
    SPY: { name: "S&P 500", color: "#22c55e", series: spySeries },
  };
  const symbolOrder =
    etfSymbol === "SPY" ? [symbol, "SPY"] : [symbol, etfSymbol, "SPY"];

  const data = [];
  for (const p of RR_PERIOD_CONFIGS) {
    const isDefault = p.key === "1Y";
    let cutoff;
    if (p.days === "YTD") {
      cutoff = new Date(Date.UTC(lastDate.getUTCFullYear(), 0, 1));
    } else {
      const daysAgo = typeof p.days === "number" ? p.days * (365 / 252) : 365;
      cutoff = new Date(lastDate.getTime() - daysAgo * 86400 * 1000);
    }

    const sliced = {};
    for (const sym of symbolOrder) {
      const series = meta[sym]?.series;
      if (!series || !series.length) continue;
      const idx = series.findIndex((r) => r.date >= cutoff);
      if (idx === -1) continue;
      sliced[sym] = series.slice(idx);
    }
    const startTimes = Object.values(sliced).map((s) => s[0].date.getTime());
    if (startTimes.length === 0) continue;
    // 全銘柄を最遅の開始日にそろえる (グラフが共通の基準点から始まるように)
    const commonStart = Math.max(...startTimes);
    for (const sym of symbolOrder) {
      if (!sliced[sym]) continue;
      sliced[sym] = sliced[sym].filter((r) => r.date.getTime() >= commonStart);
    }
    const target = sliced[symbol];
    if (!target || target.length < 2) continue;

    for (const sym of symbolOrder) {
      const s = sliced[sym];
      if (!s || s.length === 0) continue;
      const base = s[0].close;
      const m = meta[sym];
      data.push({
        name: `${m.name} (${p.label})`,
        type: "scatter",
        mode: "lines",
        x: s.map((r) => r.date.toISOString().slice(0, 10)),
        y: s.map((r) => r.close / base - 1),
        line: { color: m.color, width: 2 },
        visible: isDefault,
      });
    }
  }

  if (data.length === 0) return null;
  return {
    data,
    layout: {
      xaxis: { title: "日付" },
      yaxis: { title: "累積リターン", tickformat: ".0%" },
    },
  };
}

// リスク・リターン散布図: 8 期間 × 4 トレース (target / sectorETF / S&P 500
// / その他 S&P 銘柄) = 32 dataset。 ラベル末尾の "(1年)" などの期間サフィックス
// は ChartJs.astro の hasGroups 機能でタブ切替に変換される。 初期表示は 1年。
// master の risk_return.generate_scatter_fig に揃えた構造。
function generateRiskReturnChart(allMetrics, targetSymbol, sectorEtf) {
  if (!allMetrics || allMetrics.length === 0) return null;
  const datasets = [];

  for (const p of RR_PERIOD_CONFIGS) {
    const hvKey = `HV_${p.key}`;
    const retKey = `Ret_${p.key}`;
    const isDefault = p.key === "1Y";

    const hasValue = (m) =>
      m[hvKey] != null &&
      m[retKey] != null &&
      Number.isFinite(m[hvKey]) &&
      Number.isFinite(m[retKey]);

    const target = allMetrics.find(
      (m) => m.symbol === targetSymbol && hasValue(m),
    );
    const sector = allMetrics.find(
      (m) => m.symbol === sectorEtf && hasValue(m),
    );
    const market = allMetrics.find(
      (m) => m.symbol === "SPY" && hasValue(m),
    );
    const others = allMetrics.filter(
      (m) =>
        m.symbol !== targetSymbol &&
        m.symbol !== sectorEtf &&
        m.symbol !== "SPY" &&
        hasValue(m),
    );

    // 描画順: その他 (背景) -> 市場 -> セクター -> ターゲット (前面)
    datasets.push({
      label: `S&P銘柄 (${p.label})`,
      data: others.map((m) => ({ x: m[hvKey], y: m[retKey], symbol: m.symbol })),
      backgroundColor: "rgba(114, 119, 123, 0.4)",
      pointRadius: 4,
      visible: isDefault,
    });
    datasets.push({
      label: `S&P 500 (${p.label})`,
      data: market
        ? [{ x: market[hvKey], y: market[retKey], symbol: "S&P 500" }]
        : [],
      backgroundColor: "rgba(0, 0, 0, 0.85)",
      pointRadius: 7,
      visible: isDefault,
    });
    if (sectorEtf && sectorEtf !== "SPY") {
      datasets.push({
        label: `${sectorEtf} (${p.label})`,
        data: sector
          ? [{ x: sector[hvKey], y: sector[retKey], symbol: sectorEtf }]
          : [],
        backgroundColor: "rgba(0, 108, 172, 0.9)",
        pointRadius: 7,
        visible: isDefault,
      });
    }
    datasets.push({
      label: `${targetSymbol} (${p.label})`,
      data: target
        ? [{ x: target[hvKey], y: target[retKey], symbol: targetSymbol }]
        : [],
      backgroundColor: "rgba(255, 0, 0, 0.9)",
      pointRadius: 9,
      visible: isDefault,
    });
  }
  // ChartJs.astro の "labels && datasets" 判定で Chart.js native 経路を通すため
  // 空配列の labels を付ける (scatter なので軸ラベルとしては未使用)。
  // canvas.id === 'chart-risk-return' で isScatterChart として正しく判定される。
  return { labels: [], datasets };
}

// 株主還元: 純利益 (1 列目) と 配当金+自社株買い (2 列目に積み上げ) の
// 2 並列スタック + 配当性向 / 総還元性向の右軸折れ線。
// master の Python fundamentals.get_tp_chart_data に揃えた構造。
function _tpDatasets(cfData, isData, suffix, hidden) {
  if ((isData?.length ?? 0) === 0 && (cfData?.length ?? 0) === 0) return null;
  const sourceDates = Object.keys(isData[0] || cfData[0] || {})
    .filter((k) => k !== "index" && !k.includes("TTM"))
    .sort();
  if (sourceDates.length === 0) return null;

  const niKeys = [
    "Net Income From Continuing Operations",
    "Net Income",
    "Net Income Common Stockholders",
  ];
  const divKeys = ["Cash Dividends Paid", "Common Stock Dividend Paid"];
  const repoKeys = [
    "Repurchase Of Capital Stock",
    "Repurchase of Capital Stock",
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

  const allNi = sourceDates.map(
    (d) => getAnyVal(isData, niKeys, d) || getAnyVal(cfData, niKeys, d),
  );
  const sliceCount = suffix === " (四半期)" ? 8 : 6;
  const validIdx = allNi
    .map((v, i) => (v > 0 ? i : -1))
    .filter((i) => i !== -1)
    .slice(-sliceCount);
  if (validIdx.length === 0) return null;
  const pick = (arr) => validIdx.map((i) => arr[i]);
  const labels = validIdx.map((i) => sourceDates[i].split(" ")[0]);

  const niData = pick(allNi);
  const divData = pick(
    sourceDates.map((d) => Math.abs(getAnyVal(cfData, divKeys, d))),
  );
  const repoData = pick(
    sourceDates.map((d) => Math.abs(getAnyVal(cfData, repoKeys, d))),
  );
  const divRatio = niData.map((ni, i) => (ni > 0 ? divData[i] / ni : null));
  const totalRatio = niData.map((ni, i) =>
    ni > 0 ? (divData[i] + repoData[i]) / ni : null,
  );

  return {
    labels,
    datasets: [
      { type: "bar", label: `純利益${suffix}`, data: niData,
        backgroundColor: "rgba(44, 160, 44, 0.85)",
        stack: `income${suffix}`, yAxisID: "y", hidden },
      { type: "bar", label: `配当金${suffix}`, data: divData,
        backgroundColor: "rgba(174, 199, 232, 0.85)",
        stack: `payout${suffix}`, yAxisID: "y", hidden },
      { type: "bar", label: `自社株買い${suffix}`, data: repoData,
        backgroundColor: "rgba(31, 119, 180, 0.85)",
        stack: `payout${suffix}`, yAxisID: "y", hidden },
      { type: "line", label: `配当性向${suffix}`, data: divRatio,
        borderColor: "#ffbb78", backgroundColor: "#ffbb78",
        borderWidth: 2, fill: false, pointRadius: 4, yAxisID: "y1", hidden },
      { type: "line", label: `総還元性向${suffix}`, data: totalRatio,
        borderColor: "#ff7f0e", backgroundColor: "#ff7f0e",
        borderWidth: 2, fill: false, pointRadius: 4, yAxisID: "y1", hidden },
    ],
  };
}

function generateTpChart(cfData, isData, quarterlyCfData, quarterlyIsData) {
  const annual = _tagGroupLabels(_tpDatasets(cfData, isData, " (通年)", false));
  const quarterly = _tagGroupLabels(_tpDatasets(quarterlyCfData, quarterlyIsData, " (四半期)", true));
  return _mergeGroups(annual, quarterly);
}

// 1株あたり配当金: 年間配当 (bar) + 配当利回り (line, 右軸 y1)。
// 利回りは history から各年の年初取引日終値を取り、 その年の配当合計を割って算出。
// 直近 10 年に制限。
// master の fundamentals.get_dps_eps_chart_data の "年間推移" 表示に揃えた構造。
// (権利落日別タブは別 PR で対応する)
function generateDpsEpsChart(dividends, history) {
  if (!dividends || !Array.isArray(dividends) || dividends.length === 0)
    return null;

  const annual = {};
  dividends.forEach((d) => {
    const date = new Date(d.Date || d.index);
    if (Number.isNaN(date.getTime())) return;
    const y = date.getFullYear();
    annual[y] = (annual[y] || 0) + (d.Dividends || d.Value || 0);
  });
  const allYears = Object.keys(annual)
    .map(Number)
    .sort((a, b) => a - b);
  if (allYears.length === 0) return null;
  const years = allYears.slice(-10);
  const labels = years.map(String);
  const divs = years.map((y) => annual[y]);

  // history から各年の最初の取引日終値を求めて利回りを算出
  const yieldData = years.map((y) => {
    if (!history || history.length === 0) return null;
    const firstOfYear = history.find((h) => {
      const dt = new Date(h.Date || h.index);
      return dt.getFullYear() === y;
    });
    if (!firstOfYear || !(firstOfYear.Close > 0)) return null;
    return annual[y] / firstOfYear.Close;
  });
  const hasYield = yieldData.some((v) => v != null && Number.isFinite(v));

  const datasets = [
    {
      type: "bar",
      label: "年間配当金",
      data: divs,
      backgroundColor: "rgba(31, 119, 180, 0.85)",
      yAxisID: "y",
    },
  ];
  if (hasYield) {
    datasets.push({
      type: "line",
      label: "配当利回り",
      data: yieldData,
      borderColor: "#ff7f0e",
      backgroundColor: "#ff7f0e",
      borderWidth: 2,
      borderDash: [4, 4],
      fill: false,
      pointRadius: 4,
      yAxisID: "y1",
    });
  }
  return { labels, datasets };
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

function formatSummary(text) {
  if (!text) return text;
  
  // 正規化: 既存の改行を削除
  const normalized = text.replace(/\n\n/g, "\n").replace(/\n/g, "");
  
  // 句点で分割
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
      
      if (chunkSize > 150) {
        shouldBreak = true;
      }
      
      const startKeywords = ["また、", "さらに", "加えて", "同社は", "同社の", "主要な", "事業は"];
      if (startKeywords.some(word => nextSentence.startsWith(word))) {
        if (chunkSize > 40) {
          shouldBreak = true;
        }
      }
      
      if (shouldBreak) {
        formattedSentences.push("\n\n");
        chunkSize = 0;
      }
    }
  }
  
  return formattedSentences.join("").trim();
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
      try {
        const data = await getJson(key);
        const symbol = data.symbol || key.replace("raw/", "").replace(".json", "");
        rawDataMap[symbol] = data;
        return { ok: true, symbol };
      } catch (e) {
        return { ok: false, item: key, error: e };
      }
    },
    CONCURRENCY,
  );

  let translations = {};
  try {
    console.log("Downloading translations/business_summaries.json...");
    translations = await getJson("translations/business_summaries.json");
  } catch (e) {
    console.log("  Translations not found or failed to load.");
  }

  const dlFails = dlResults.filter((r) => !r.ok).length;
  if (dlFails) {
    console.log(`  ${dlFails} downloads failed`);
    for (const r of dlResults.filter((x) => !x.ok)) {
      console.log(`    key=${r.item}: error: ${r.error?.message || r.error}`);
    }
  }

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

  // 処理する銘柄を決定する。
  //   - TEST_SYMBOLS 環境変数 (例: "MSFT,AAPL,NVDA") があればそのリストを優先
  //   - "all" を指定するとフィルタを掛けず raw データにある全銘柄を処理する
  //   - 未指定の場合はテスト用デフォルト ["MSFT", "AAPL", "NVDA"]
  // テスト時は数銘柄に絞ることで Actions の所要時間を 30 秒程度に抑えられる。
  console.log("Generating reports/*.json...");
  const testSymbolsEnv = (process.env.TEST_SYMBOLS || "").trim();
  let symbols;
  if (testSymbolsEnv.toLowerCase() === "all") {
    symbols = Object.keys(rawDataMap);
  } else if (testSymbolsEnv) {
    symbols = testSymbolsEnv
      .split(",")
      .map((s) => s.trim().toUpperCase())
      .filter((s) => s && rawDataMap[s]);
  } else {
    symbols = ["MSFT", "AAPL", "NVDA"].filter((s) => rawDataMap[s]);
  }
  console.log(`Processing ${symbols.length} symbol(s): ${symbols.join(", ")}`);
  const updatedStocksList = [];
  const updatedLock = []; // 並列 push の安全化のため append-only にする
  const putResults = await pMap(
    symbols,
    async (symbol) => {
      try {
        const rawData = rawDataMap[symbol];
        const isETF = ETFS.includes(symbol);
        const metadata =
          baseStocksList.find(
            (s) => s.Symbol_YF === symbol || s.Symbol === symbol,
          ) || {};
        const sectorEtf = getSectorETF(
          metadata["GICS Sector"] || rawData.info?.sector,
          metadata["GICS Sub-Industry"] || rawData.info?.industry,
        );
        const etfRawData = rawDataMap[sectorEtf];

        const highlights = extractHighlights(rawData);
        const earningsSurprise = extractEarningsSurprise(rawData);
        const nextEarnings = extractNextEarnings(rawData);
        const consensus = extractConsensus(rawData);
        const ratingChanges = extractRatingChanges(rawData);
        // Enrich ratings with currentPriceTarget/priorPriceTarget/priceTargetAction
        // via yahoo-finance2 (the Python yfinance package doesn't expose these
        // fields even though Yahoo's API returns them).
        if (ratingChanges.length > 0) {
          const yfHistory = await fetchUpgradeDowngradeHistory(symbol);
          if (yfHistory) mergeTargetPrices(ratingChanges, yfHistory);
        }
        const analystRatings = extractAnalystRatings(rawData);

        const riskReturnChart = generateRiskReturnChart(
          riskReturnMetrics,
          symbol,
          sectorEtf,
        );
        // BS / IS / CF は master の Plotly レイアウトに合わせた専用関数を使う。
        // generateFinancialChart は単純スタックしか作らないため使用しない。
        const isChart = generateIsChart(
          rawData.income_stmt || [],
          rawData.quarterly_income_stmt || [],
        );
        const bsChart = generateBsChart(
          rawData.balancesheet || [],
          rawData.quarterly_balancesheet || [],
        );
        const cfChart = generateCfChart(
          rawData.cashflow || [],
          rawData.income_stmt || [],
          rawData.quarterly_cashflow || [],
          rawData.quarterly_income_stmt || [],
        );
        const perfChart = generatePerformanceChart(
          rawData.history,
          etfRawData?.history,
          rawDataMap["SPY"]?.history,
          symbol,
          sectorEtf,
        );
        const tpChart = generateTpChart(
          rawData.cashflow || [],
          rawData.income_stmt || [],
          rawData.quarterly_cashflow || [],
          rawData.quarterly_income_stmt || [],
        );
        const dpsChart = generateDpsEpsChart(rawData.dividends, rawData.history);
        const segmentChart = generateSegmentChart(rawData.revenue_by_segment);
        const geoChart = generateSegmentChart(rawData.revenue_by_geography);

        const sector =
          metadata["GICS Sector"] || rawData.info?.sector || "Unknown";
        const subInd =
          metadata["GICS Sub-Industry"] || rawData.info?.industry || "Unknown";

        let summary_ja = translations[symbol]?.business_summary_ja || translations[symbol] || null;
        if (summary_ja) {
          summary_ja = formatSummary(summary_ja);
        }

        const reportData = {
          symbol: metadata.Symbol || symbol,
          symbol_yf: symbol,
          security:
            metadata.Security ||
            rawData.info?.longName ||
            rawData.info?.shortName ||
            symbol,
          security_ja: metadata.Security_JA || null,
          business_summary_ja: summary_ja,
          sector,
          sub_industry: subInd,
          exchange: toTradingViewExchange(rawData.info?.exchange),
          full_symbol: `${toTradingViewExchange(rawData.info?.exchange)}:${symbol.replace("-", ".")}`,
          sector_etf: sectorEtf,
          is_financial: ["Financials", "Real Estate"].includes(sector),
          
          benchmark_info: getBenchmarkInfo(metadata),

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
        return { ok: true, symbol };
      } catch (e) {
        return { ok: false, symbol, error: e };
      }
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
