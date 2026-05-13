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

function calculateRiskReturn(history, symbol) {
  if (!history || !Array.isArray(history) || history.length < 252) return null;
  // returns: (Price(T) - Price(T-252)) / Price(T-252)
  const last = history[history.length - 1].Close;
  const first = history[history.length - 252].Close;
  const ret = (last - first) / first;

  // volatility (std of log returns)
  const logReturns = [];
  for (let i = history.length - 251; i < history.length; i++) {
    logReturns.push(Math.log(history[i].Close / history[i - 1].Close));
  }
  const mean = logReturns.reduce((a, b) => a + b, 0) / logReturns.length;
  const variance =
    logReturns.reduce((a, b) => a + Math.pow(b - mean, 2), 0) /
    (logReturns.length - 1);
  const hv = Math.sqrt(variance * 252);

  return { symbol, ret, hv };
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

function extractRatingChanges(rawData) {
  const ud = rawData.upgrades_downgrades;
  if (!ud || !Array.isArray(ud)) return [];
  return ud.slice(0, 10).map((x) => ({
    GradeDate: String(x.index || x.Date).split(" ")[0],
    Firm: x.Firm || x.firm,
    ToGrade: x["To Grade"] || x.toGrade,
    FromGrade: x["From Grade"] || x.fromGrade,
    Action: x.Action || x.action,
  }));
}

function extractAnalystRatings(rawData) {
  const info = rawData.info || {};
  const ratings = rawData.analyst_ratings || {};
  return {
    recommendationKey: (info.recommendationKey || "hold").toLowerCase(),
    targetMeanPrice: info.targetMeanPrice || null,
    targetHighPrice: info.targetHighPrice || null,
    targetLowPrice: info.targetLowPrice || null,
    targetMedianPrice: info.targetMedianPrice || null,
    numberOfAnalystOpinions: info.numberOfAnalystOpinions || 0,
    currentPrice: info.currentPrice || info.regularMarketPrice || null,
    ...ratings
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
function generateIsChart(incomeStmt) {
  if (!incomeStmt || incomeStmt.length === 0) return null;
  const dates = Object.keys(incomeStmt[0])
    .filter((k) => k !== "index" && !k.includes("TTM"))
    .sort();
  if (dates.length === 0) return null;
  const get = (label) =>
    dates.map((d) => Number(getValFromArray(incomeStmt, label, d)) || 0);

  const revenue = get("Total Revenue");
  const validIdx = revenue
    .map((v, i) => (v > 0 ? i : -1))
    .filter((i) => i !== -1)
    .slice(-6);
  if (validIdx.length === 0) return null;
  const pick = (arr) => validIdx.map((i) => arr[i]);
  const labels = validIdx.map((i) => dates[i].split(" ")[0]);

  const rev = pick(revenue);
  const grossProfit = pick(get("Gross Profit"));
  const operatingIncome = pick(get("Operating Income"));
  const netIncome = pick(get("Net Income"));
  const ratio = (num, den) =>
    num.map((v, i) => (den[i] ? v / den[i] : null));

  return {
    labels,
    datasets: [
      { type: "bar", label: "売上高", data: rev,
        backgroundColor: "rgba(174, 199, 232, 0.85)", yAxisID: "y" },
      { type: "bar", label: "売上総利益", data: grossProfit,
        backgroundColor: "rgba(31, 119, 180, 0.85)", yAxisID: "y" },
      { type: "bar", label: "営業利益", data: operatingIncome,
        backgroundColor: "rgba(255, 187, 120, 0.85)", yAxisID: "y" },
      { type: "bar", label: "純利益", data: netIncome,
        backgroundColor: "rgba(44, 160, 44, 0.85)", yAxisID: "y" },
      { type: "line", label: "売上総利益率", data: ratio(grossProfit, rev),
        borderColor: "#1f77b4", backgroundColor: "#1f77b4",
        borderWidth: 2, fill: false, pointRadius: 4, yAxisID: "y1" },
      { type: "line", label: "営業利益率", data: ratio(operatingIncome, rev),
        borderColor: "#ffbb78", backgroundColor: "#ffbb78",
        borderWidth: 2, fill: false, pointRadius: 4, yAxisID: "y1" },
      { type: "line", label: "純利益率", data: ratio(netIncome, rev),
        borderColor: "#2ca02c", backgroundColor: "#2ca02c",
        borderWidth: 2, fill: false, pointRadius: 4, yAxisID: "y1" },
    ],
  };
}

// 貸借対照表: 資産側 (固定+流動) と負債・純資産側 (純資産+固定負債+流動負債)
// を別 stack に積み上げて並列表示する。
// master の Python fundamentals.get_bs_chart_data に揃えた構造。
function generateBsChart(balanceSheet) {
  if (!balanceSheet || balanceSheet.length === 0) return null;
  const dates = Object.keys(balanceSheet[0])
    .filter((k) => k !== "index")
    .sort();
  if (dates.length === 0) return null;
  const get = (label) =>
    dates.map((d) => Number(getValFromArray(balanceSheet, label, d)) || 0);
  const sum = (arr) => arr.reduce((a, b) => a + b, 0);

  const totalAssets = get("Total Assets");
  const validIdx = totalAssets
    .map((v, i) => (v > 0 ? i : -1))
    .filter((i) => i !== -1)
    .slice(-6);
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
        { label: "固定資産", data: nonCurrentAssets,
          backgroundColor: "rgba(31, 119, 180, 0.85)", stack: "assets" },
        { label: "流動資産", data: currentAssets,
          backgroundColor: "rgba(174, 199, 232, 0.85)", stack: "assets" },
        { label: "純資産", data: equity,
          backgroundColor: "rgba(44, 160, 44, 0.85)", stack: "liabilities" },
        { label: "固定負債", data: nonCurrentLiab,
          backgroundColor: "rgba(255, 127, 14, 0.85)", stack: "liabilities" },
        { label: "流動負債", data: currentLiab,
          backgroundColor: "rgba(255, 187, 120, 0.85)", stack: "liabilities" },
      ]
    : [
        { label: "総資産", data: totalAssetsValid,
          backgroundColor: "rgba(31, 119, 180, 0.85)", stack: "assets" },
        { label: "純資産", data: equity,
          backgroundColor: "rgba(44, 160, 44, 0.85)", stack: "liabilities" },
        { label: "総負債", data: totalLiabValid,
          backgroundColor: "rgba(255, 127, 14, 0.85)", stack: "liabilities" },
      ];
  return { labels, datasets };
}

// キャッシュフロー: 純利益 + 営業/投資/財務/フリー CF の 5 本並列棒。
// master の Python fundamentals.get_cf_chart_data に揃えた構造。
function generateCfChart(cashflow, incomeStmt) {
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
  const validIdx = opCf
    .map((v, i) => (v !== 0 ? i : -1))
    .filter((i) => i !== -1)
    .slice(-6);
  if (validIdx.length === 0) return null;
  const pick = (arr) => validIdx.map((i) => arr[i]);
  const labels = validIdx.map((i) => dates[i].split(" ")[0]);

  return {
    labels,
    datasets: [
      { type: "bar", label: "純利益", data: pick(getIs("Net Income")),
        backgroundColor: "rgba(44, 160, 44, 0.85)" },
      { type: "bar", label: "営業CF", data: pick(opCf),
        backgroundColor: "rgba(174, 199, 232, 0.85)" },
      { type: "bar", label: "投資CF", data: pick(getCf("Investing Cash Flow")),
        backgroundColor: "rgba(31, 119, 180, 0.85)" },
      { type: "bar", label: "財務CF", data: pick(getCf("Financing Cash Flow")),
        backgroundColor: "rgba(255, 187, 120, 0.85)" },
      { type: "bar", label: "フリーCF", data: pick(getCf("Free Cash Flow")),
        backgroundColor: "rgba(148, 103, 189, 0.85)" },
    ],
  };
}

function generatePerformanceChart(history, etfHistory, symbol, etfSymbol) {
  if (!history || history.length === 0) return null;
  const targetData = history.slice(-252);
  const etfData = etfHistory ? etfHistory.slice(-252) : [];
  const spyData = history.find((h) => h.symbol === "SPY") ? [] : []; // simplified

  // In Action, we don't easily have other histories.
  // We'll just provide the target's relative performance.
  const dates = targetData.map((d) => String(d.Date || d.index).split(" ")[0]);
  const base = targetData[0].Close;
  const targetPerf = targetData.map((d) => (d.Close - base) / base);

  const datasets = [
    {
      label: symbol,
      data: targetPerf,
      borderColor: "rgb(255, 99, 132)",
      borderWidth: 2,
      fill: false,
      pointRadius: 0,
    },
  ];

  if (etfData.length === targetData.length) {
    const eBase = etfData[0].Close;
    datasets.push({
      label: etfSymbol,
      data: etfData.map((d) => (d.Close - eBase) / eBase),
      borderColor: "rgb(54, 162, 235)",
      borderWidth: 2,
      fill: false,
      pointRadius: 0,
    });
  }

  return { labels: dates, datasets };
}

function generateRiskReturnChart(allMetrics, targetSymbol) {
  const others = allMetrics.filter((m) => m.symbol !== targetSymbol);
  const target = allMetrics.find((m) => m.symbol === targetSymbol);
  const datasets = [
    {
      label: "その他のS&P 500銘柄",
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

// 株主還元: 純利益 (1 列目) と 配当金+自社株買い (2 列目に積み上げ) の
// 2 並列スタック + 配当性向 / 総還元性向の右軸折れ線。
// master の Python fundamentals.get_tp_chart_data に揃えた構造。
function generateTpChart(cfData, isData) {
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
  const validIdx = allNi
    .map((v, i) => (v > 0 ? i : -1))
    .filter((i) => i !== -1)
    .slice(-6);
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
      { type: "bar", label: "純利益", data: niData,
        backgroundColor: "rgba(44, 160, 44, 0.85)",
        stack: "income", yAxisID: "y" },
      { type: "bar", label: "配当金", data: divData,
        backgroundColor: "rgba(174, 199, 232, 0.85)",
        stack: "payout", yAxisID: "y" },
      { type: "bar", label: "自社株買い", data: repoData,
        backgroundColor: "rgba(31, 119, 180, 0.85)",
        stack: "payout", yAxisID: "y" },
      { type: "line", label: "配当性向", data: divRatio,
        borderColor: "#ffbb78", backgroundColor: "#ffbb78",
        borderWidth: 2, fill: false, pointRadius: 4, yAxisID: "y1" },
      { type: "line", label: "総還元性向", data: totalRatio,
        borderColor: "#ff7f0e", backgroundColor: "#ff7f0e",
        borderWidth: 2, fill: false, pointRadius: 4, yAxisID: "y1" },
    ],
  };
}

function generateDpsEpsChart(dividends) {
  if (!dividends || !Array.isArray(dividends) || dividends.length === 0)
    return null;
  const annual = {};
  dividends.forEach((d) => {
    const y = new Date(d.Date || d.index).getFullYear();
    annual[y] = (annual[y] || 0) + (d.Dividends || d.Value || 0);
  });
  const labels = Object.keys(annual).sort();
  return {
    labels,
    datasets: [
      {
        type: "bar",
        label: "年間配当金",
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
        const analystRatings = extractAnalystRatings(rawData);

        const riskReturnChart = generateRiskReturnChart(riskReturnMetrics, symbol);
        // BS / IS / CF は master の Plotly レイアウトに合わせた専用関数を使う。
        // generateFinancialChart は単純スタックしか作らないため使用しない。
        const isChart = generateIsChart(rawData.income_stmt || []);
        const bsChart = generateBsChart(rawData.balancesheet || []);
        const cfChart = generateCfChart(
          rawData.cashflow || [],
          rawData.income_stmt || [],
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
