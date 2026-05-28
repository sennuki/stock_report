#!/usr/bin/env node
/**
 * GitHub Actions runner 上で reports/*.json を生成する。
 *
 * 元々は worker-processor の processAllStocks (ctx.waitUntil) で行っていたが、
 * Cloudflare Workers の sub-request 制限 (paid 1000) を 1500 銘柄 × GET+PUT で
 * 大幅に超過するため、外部（GitHub Actions）で一括生成して R2 に書き込む方式に変更。
 */

import "dotenv/config";
import { normalizeDividendYield } from './highlights-utils.mjs';
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pMap from "p-map";
import YahooFinance from "yahoo-finance2";

const yahooFinance = new YahooFinance();
const BUCKET = process.env.R2_BUCKET_NAME || "defeat-beta-stock-data";
const CONCURRENCY = 20;

// LOCAL_MODE: R2 を使わず、ローカル FS から生データを読み reports/*.json を
// public/reports/ に書き出す。code/main.py を実行して code/raw_data/ に生データ
// を揃えた後、 worker-processor/ から `node scripts/generate-reports.mjs` で
// 本番と同じ出力スキーマの reports/*.json をローカルに生成できる。
// R2_ACCOUNT_ID 等が未設定なら自動的に LOCAL_MODE になる。
const LOCAL_MODE =
  process.env.LOCAL_MODE === "true" ||
  !process.env.R2_ACCOUNT_ID ||
  !process.env.R2_ACCESS_KEY_ID ||
  !process.env.R2_SECRET_ACCESS_KEY;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "..", "..");
const LOCAL_RAW_DIR = path.join(REPO_ROOT, "code", "raw_data");
const LOCAL_STOCKS_JSON = path.join(REPO_ROOT, "stock-blog", "src", "data", "stocks.json");
const LOCAL_REPORTS_DIR = path.join(REPO_ROOT, "stock-blog", "public", "reports");
const LOCAL_TRANSLATIONS_PATH = path.join(
  REPO_ROOT,
  "worker-processor",
  "translations",
  "business_summaries.json",
);
const LOCAL_BROKER_AVAILABILITY_PATH = path.join(
  REPO_ROOT,
  "code",
  "data",
  "broker_availability.json",
);

// S3 クライアントは R2 アクセスが必要な時のみ動的に初期化する (LOCAL_MODE 時は不要)。
let s3Lazy = null;
async function getS3() {
  if (!s3Lazy) {
    const mod = await import("@aws-sdk/client-s3");
    s3Lazy = {
      client: new mod.S3Client({
        region: "auto",
        endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
        credentials: {
          accessKeyId: process.env.R2_ACCESS_KEY_ID,
          secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
        },
      }),
      ListObjectsV2Command: mod.ListObjectsV2Command,
      GetObjectCommand: mod.GetObjectCommand,
      PutObjectCommand: mod.PutObjectCommand,
      DeleteObjectCommand: mod.DeleteObjectCommand,
    };
  }
  return s3Lazy;
}

// LOCAL_MODE 時の R2 key → ローカルパス変換。
// raw/{sym}.json は code/raw_data/{sym}_raw.json に対応 (Python 側の命名規約)。
function localPathForKey(key) {
  if (key === "raw/stocks_list.json") return LOCAL_STOCKS_JSON;
  if (key === "raw/broker_availability.json") return LOCAL_BROKER_AVAILABILITY_PATH;
  if (key === "translations/business_summaries.json") return LOCAL_TRANSLATIONS_PATH;
  if (key === "reports/stocks.json") return LOCAL_STOCKS_JSON;
  if (key.startsWith("raw/")) {
    const sym = key.slice("raw/".length).replace(/\.json$/, "");
    return path.join(LOCAL_RAW_DIR, `${sym}_raw.json`);
  }
  if (key.startsWith("reports/")) {
    const sym = key.slice("reports/".length).replace(/\.json$/, "");
    return path.join(LOCAL_REPORTS_DIR, `${sym}.json`);
  }
  throw new Error(`Unmapped R2 key for LOCAL_MODE: ${key}`);
}

// raw/broker_availability.json の取扱銘柄 Set に対し、ある銘柄が買えるか判定する。
// Python 側 generate_json_reports.py の check_availability と同じく、
// BRK-B ⇔ BRKB / BRK.B のような記号ゆれを吸収する。
// 第二引数以降はフォールバックシンボル (Symbol_YF など) で、
// 表示用シンボルが見つからない場合にも確認する (BNY/BK 等のティッカー変更対応)。
function isAvailableAt(brokerSet, ...symbols) {
  if (!brokerSet || brokerSet.size === 0) return false;
  for (const symbol of symbols) {
    if (!symbol) continue;
    if (brokerSet.has(symbol)) return true;
    // 記号なし (BRK-B / BRK.B → BRKB)
    if (brokerSet.has(symbol.replace(/[-\.]/g, ""))) return true;
    // ハイフン → ドット (BRK-B → BRK.B)
    if (brokerSet.has(symbol.replace(/-/g, "."))) return true;
    // ドット → ハイフン (BRK.B → BRK-B)
    if (brokerSet.has(symbol.replace(/\./g, "-"))) return true;
  }
  return false;
}

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

// yfinance の sector / industry 名は GICS と微妙に異なる (例: yfinance は
// "Technology" / "Financial Services" / "Consumer Cyclical" を返すが GICS は
// "Information Technology" / "Financials" / "Consumer Discretionary")。
// stocks_list.json が無く rawData.info.sector にフォールバックするケースで
// sectorEtfMap に当たらず SPY に落ちてしまうのを防ぐため、ここで GICS 名に
// 正規化する。
const yfToGicsSector = {
  'Technology': 'Information Technology',
  'Financial Services': 'Financials',
  'Financial': 'Financials',
  'Consumer Cyclical': 'Consumer Discretionary',
  'Consumer Defensive': 'Consumer Staples',
  'Healthcare': 'Health Care',
  'Basic Materials': 'Materials',
  // 以下はそのまま使えるので恒等写像 (記録のため明示):
  // 'Industrials', 'Energy', 'Utilities', 'Real Estate', 'Communication Services'
};
function normalizeSector(s) {
  if (!s) return s;
  return yfToGicsSector[s] || s;
}

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
  if (LOCAL_MODE) {
    // 現状 prefix=="raw/" のみ呼ばれる。code/raw_data/*_raw.json を列挙して
    // R2 key 形式 (raw/{sym}.json) に正規化する。
    if (prefix !== "raw/") {
      throw new Error(`LOCAL_MODE listAll: unsupported prefix ${prefix}`);
    }
    if (!fs.existsSync(LOCAL_RAW_DIR)) return [];
    const files = await fs.promises.readdir(LOCAL_RAW_DIR);
    return files
      .filter((f) => f.endsWith("_raw.json"))
      .map((f) => `raw/${f.replace(/_raw\.json$/, ".json")}`);
  }
  const { client, ListObjectsV2Command } = await getS3();
  let keys = [];
  let token = null;
  do {
    const res = await client.send(
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
  let body;
  if (LOCAL_MODE) {
    const p = localPathForKey(key);
    body = await fs.promises.readFile(p, "utf-8");
  } else {
    const { client, GetObjectCommand } = await getS3();
    const res = await client.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
    body = await res.Body.transformToString();
  }
  const safe = body
    .replace(/\bNaN\b/g, "null")
    .replace(/\b-?Infinity\b/g, "null");
  return JSON.parse(safe);
}

async function putJson(key, data) {
  if (LOCAL_MODE) {
    const p = localPathForKey(key);
    await fs.promises.mkdir(path.dirname(p), { recursive: true });
    await fs.promises.writeFile(p, JSON.stringify(data, null, 2));
    return;
  }
  const { client, PutObjectCommand } = await getS3();
  await client.send(
    new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: JSON.stringify(data),
      ContentType: "application/json",
    }),
  );
}

async function deleteObject(key) {
  if (LOCAL_MODE) return;
  const { client, DeleteObjectCommand } = await getS3();
  try {
    await client.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: key }));
  } catch (e) {
    // 既に存在しない等は無視 (reports/{sym}.json が未生成のケースなど)。
    console.warn(`  delete ${key} failed (ignored): ${e.message}`);
  }
}

// S&P 入れ替えで「除外」された銘柄の残骸を R2 から削除する。
// 銘柄ユニバースは raw/stocks_list.json (= Wikipedia スクレイプ) が毎回上書き
// するが、パイプラインは raw/ に書き込むだけで削除しないため、除外銘柄の
// raw/{sym}.json が残り続けレポートも生成され続けてしまう。これを防ぐ。
// 戻り値: 削除した銘柄の Set (呼び出し側で rawKeys から除外する)。
async function pruneRemovedSymbols(rawKeys, baseStocksList) {
  // ローカル/テスト実行 (raw/stocks_list.json が部分的) では prune しない。
  if (LOCAL_MODE) return new Set();

  const MIN_UNIVERSE = Number(process.env.PRUNE_MIN_UNIVERSE || 1400);
  const MAX_PRUNE = Number(process.env.PRUNE_MAX || 60);

  const universe = new Set(
    baseStocksList.map((s) => s.Symbol_YF || s.Symbol).filter(Boolean),
  );

  // 安全策 1: Wikipedia スクレイプが失敗・部分取得でユニバースが極端に
  // 小さいときは、正常銘柄を誤って消さないよう prune 全体をスキップする。
  if (universe.size < MIN_UNIVERSE) {
    console.warn(
      `  prune skipped: universe too small (${universe.size} < ${MIN_UNIVERSE})`,
    );
    return new Set();
  }

  // ETF / 指数は S&P 構成銘柄リストに載らないため prune 対象から除外する。
  const protectedSyms = new Set([
    ...Object.values(sectorEtfMap),
    ...Object.values(broadSectorEtfMap),
    ...Object.values(marketIndexMap),
    "^GSPC",
  ]);

  const rawSymbols = rawKeys.map((k) =>
    k.slice("raw/".length).replace(/\.json$/, ""),
  );
  const stale = rawSymbols.filter(
    (s) => !universe.has(s) && !protectedSyms.has(s) && !s.startsWith("^"),
  );

  if (stale.length === 0) {
    console.log("  prune: no removed symbols");
    return new Set();
  }
  // 安全策 2: 一度に大量に消そうとする場合は異常とみなしスキップする。
  if (stale.length > MAX_PRUNE) {
    console.warn(
      `  prune skipped: too many stale symbols (${stale.length} > ${MAX_PRUNE}): ` +
        `${stale.slice(0, 20).join(", ")}...`,
    );
    return new Set();
  }

  console.log(`  prune: removing ${stale.length} symbol(s): ${stale.join(", ")}`);
  for (const sym of stale) {
    await deleteObject(`raw/${sym}.json`);
    await deleteObject(`reports/${sym}.json`);
  }
  return new Set(stale);
}

// S&P 500/400/600 の構成銘柄入れ替えを検出し、変更履歴
// (reports/change_history.json) に追記する。現ユニバースを前回スナップショット
// と比較し、追加/除外を日付付きで記録する。更新履歴ページ (/updates) が読む。
async function recordIndexChanges(baseStocksList) {
  if (LOCAL_MODE) return;
  const KEY = "reports/change_history.json";
  const MIN_UNIVERSE = Number(process.env.PRUNE_MIN_UNIVERSE || 1400);
  // スクレイプ揺れで一度に大量の増減が出たら異常とみなす上限。
  const MAX_CHANGES = Number(process.env.INDEX_CHANGE_MAX || 80);
  const MAX_STORED_EVENTS = 500;

  const current = new Map();
  for (const s of baseStocksList) {
    const sym = s.Symbol_YF || s.Symbol;
    if (!sym || !s.Index) continue;
    current.set(sym, { symbol: sym, security: s.Security || sym, index: s.Index });
  }
  if (current.size < MIN_UNIVERSE) {
    console.log(
      `  change history skipped: universe too small (${current.size} < ${MIN_UNIVERSE})`,
    );
    return;
  }

  let history = { universe: [], events: [] };
  try {
    const loaded = await getJson(KEY);
    if (loaded && typeof loaded === "object") {
      if (Array.isArray(loaded.universe)) history.universe = loaded.universe;
      if (Array.isArray(loaded.events)) history.events = loaded.events;
    }
  } catch {
    // 初回実行: ファイルが無い。
  }

  const prev = new Map(
    history.universe.filter((e) => e && e.symbol).map((e) => [e.symbol, e]),
  );

  // 初回 (前回スナップショット無し): ベースラインのみ保存しイベントは作らない。
  if (prev.size === 0) {
    history.universe = [...current.values()];
    await putJson(KEY, history);
    console.log(`  change history: baseline established (${current.size} symbols)`);
    return;
  }

  const added = [...current.values()].filter((e) => !prev.has(e.symbol));
  const removed = [...prev.values()].filter((e) => !current.has(e.symbol));

  // 安全策: 大量の増減はスクレイプ失敗とみなし記録しない (ベースラインも
  // 更新せず、次の正常実行で再判定させる)。
  if (added.length + removed.length > MAX_CHANGES) {
    console.warn(
      `  change history skipped: ${added.length + removed.length} changes ` +
        `exceed ${MAX_CHANGES} (likely a scrape glitch)`,
    );
    return;
  }

  if (added.length > 0 || removed.length > 0) {
    const date = new Date().toISOString().slice(0, 10);
    const toEvent = (type) => (e) => ({
      date,
      type,
      symbol: e.symbol,
      security: e.security,
      index: e.index,
    });
    const newEvents = [
      ...added.map(toEvent("index_added")),
      ...removed.map(toEvent("index_removed")),
    ];
    history.events = [...newEvents, ...history.events].slice(0, MAX_STORED_EVENTS);
    console.log(
      `  change history: ${added.length} added, ${removed.length} removed`,
    );
  }

  // ユニバースのスナップショットを最新化 (security 名の更新も取り込む)。
  history.universe = [...current.values()];
  await putJson(KEY, history);
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

// 直近 N 営業日における最大ドローダウンを計算。返値は負の小数 (-0.25 = -25%)。
function calculateMaxDrawdown(history, days) {
  if (!Array.isArray(history) || history.length < 5) return null;
  const safe = history.filter((h) => h && h.Close > 0);
  if (safe.length < 5) return null;
  const start = Math.max(0, safe.length - days);
  const slice = safe.slice(start);
  let peak = slice[0].Close;
  let maxDD = 0;
  for (const h of slice) {
    if (h.Close > peak) peak = h.Close;
    const dd = (h.Close - peak) / peak;
    if (dd < maxDD) maxDD = dd;
  }
  return Number.isFinite(maxDD) ? maxDD : null;
}

// 財務諸表 (income_stmt / balancesheet / cashflow) の row から日付キーだけを抽出し、
// 新しい順 (降順) でソートして返す。Piotroski の YoY 比較で最新 2 期を取り出す用途。
function getStmtLatestDates(stmt) {
  if (!Array.isArray(stmt) || stmt.length === 0) return [];
  const dates = new Set();
  for (const row of stmt) {
    for (const k of Object.keys(row)) {
      if (/^\d{4}-\d{2}-\d{2}/.test(k)) dates.add(k);
    }
  }
  return [...dates].sort().reverse();
}

// Piotroski F-Score (0-9 点)。年次決算 2 期の比較が必要。
// 9 項目のうち最低 7 項目を評価できないと null を返す。
function calcPiotroskiScore(rawData) {
  const incomeStmt = rawData.income_stmt;
  const bs = rawData.balancesheet;
  const cf = rawData.cashflow;
  if (!Array.isArray(incomeStmt) || !Array.isArray(bs) || !Array.isArray(cf)) return null;
  const isDates = getStmtLatestDates(incomeStmt);
  const bsDates = getStmtLatestDates(bs);
  const cfDates = getStmtLatestDates(cf);
  if (isDates.length < 2 || bsDates.length < 2 || cfDates.length < 2) return null;
  const [d0, d1] = isDates;
  const [bd0, bd1] = bsDates;
  const [cd0] = cfDates;
  const get = (stmt, field, date) => {
    const v = getValFromArray(stmt, field, date);
    return typeof v === "number" && Number.isFinite(v) ? v : null;
  };
  const ni0 = get(incomeStmt, "Net Income", d0);
  const ni1 = get(incomeStmt, "Net Income", d1);
  const cfo0 = get(cf, "Operating Cash Flow", cd0);
  const ta0 = get(bs, "Total Assets", bd0);
  const ta1 = get(bs, "Total Assets", bd1);
  const ltd0 = get(bs, "Long Term Debt", bd0);
  const ltd1 = get(bs, "Long Term Debt", bd1);
  const ca0 = get(bs, "Current Assets", bd0);
  const ca1 = get(bs, "Current Assets", bd1);
  const cl0 = get(bs, "Current Liabilities", bd0);
  const cl1 = get(bs, "Current Liabilities", bd1);
  const sh0 = get(bs, "Ordinary Shares Number", bd0);
  const sh1 = get(bs, "Ordinary Shares Number", bd1);
  const rev0 = get(incomeStmt, "Total Revenue", d0);
  const rev1 = get(incomeStmt, "Total Revenue", d1);
  const gp0 = get(incomeStmt, "Gross Profit", d0);
  const gp1 = get(incomeStmt, "Gross Profit", d1);
  const roa0 = (ni0 != null && ta0 != null && ta0 > 0) ? ni0 / ta0 : null;
  const roa1 = (ni1 != null && ta1 != null && ta1 > 0) ? ni1 / ta1 : null;
  const cr0 = (ca0 != null && cl0 != null && cl0 > 0) ? ca0 / cl0 : null;
  const cr1 = (ca1 != null && cl1 != null && cl1 > 0) ? ca1 / cl1 : null;
  const gm0 = (gp0 != null && rev0 != null && rev0 > 0) ? gp0 / rev0 : null;
  const gm1 = (gp1 != null && rev1 != null && rev1 > 0) ? gp1 / rev1 : null;
  const at0 = (rev0 != null && ta0 != null && ta0 > 0) ? rev0 / ta0 : null;
  const at1 = (rev1 != null && ta1 != null && ta1 > 0) ? rev1 / ta1 : null;
  let score = 0;
  let evaluated = 0;
  const check = (cond) => { if (cond !== null) { evaluated++; if (cond === true) score++; } };
  check(ni0 != null ? ni0 > 0 : null);                                  // 1. 純利益 > 0
  check(cfo0 != null ? cfo0 > 0 : null);                                // 2. 営業 CF > 0
  check(roa0 != null && roa1 != null ? roa0 > roa1 : null);             // 3. ΔROA > 0
  check(cfo0 != null && ni0 != null ? cfo0 > ni0 : null);               // 4. CFO > NI (アクルーアル)
  check(ltd0 != null && ltd1 != null ? ltd0 < ltd1 : null);             // 5. 長期負債減少
  check(cr0 != null && cr1 != null ? cr0 > cr1 : null);                 // 6. 流動比率上昇
  check(sh0 != null && sh1 != null ? sh0 <= sh1 * 1.001 : null);        // 7. 株式希薄化なし (0.1% 許容)
  check(gm0 != null && gm1 != null ? gm0 > gm1 : null);                 // 8. 粗利率上昇
  check(at0 != null && at1 != null ? at0 > at1 : null);                 // 9. 総資産回転率上昇
  if (evaluated < 7) return null;
  return score;
}

// 配当の継続性・安全性を 0-5 点でスコア化。
function calcDividendStability(rawData) {
  const info = rawData.info || {};
  let score = 0;
  // (1) 配当を払っているか
  const divRate = info.dividendRate;
  if (typeof divRate !== "number" || divRate <= 0) return null;
  score++;
  // (2) Payout Ratio が健全な範囲 (10%-80%)
  const pr = info.payoutRatio;
  if (typeof pr === "number" && pr > 0.1 && pr < 0.8) score++;
  // (3) FCF カバー率 ≥ 1.5x
  const fcf = info.freeCashflow;
  const shares = info.sharesOutstanding;
  if (typeof fcf === "number" && typeof shares === "number" && shares > 0) {
    const totalDiv = divRate * shares;
    if (totalDiv > 0 && fcf / totalDiv >= 1.5) score++;
  }
  // (4) 5 年平均利回りが存在する (= 配当履歴が一定期間ある)
  if (typeof info.fiveYearAvgDividendYield === "number" && info.fiveYearAvgDividendYield > 0) score++;
  // (5) 利益が出ている (trailing EPS > 0)
  if (typeof info.trailingEps === "number" && info.trailingEps > 0) score++;
  return score;
}

// Magic Formula (Greenblatt) 用: Earnings Yield = EBIT / EV。
// EBIT が income_stmt に無い場合は EBITDA で代替。
function calcEarningsYield(rawData) {
  const ev = rawData.info?.enterpriseValue;
  if (typeof ev !== "number" || ev <= 0) return null;
  const incomeStmt = rawData.income_stmt;
  if (Array.isArray(incomeStmt)) {
    const dates = getStmtLatestDates(incomeStmt);
    if (dates.length > 0) {
      const ebit = getValFromArray(incomeStmt, "EBIT", dates[0]);
      if (typeof ebit === "number" && Number.isFinite(ebit)) return ebit / ev;
    }
  }
  const ebitda = rawData.info?.ebitda;
  if (typeof ebitda === "number" && Number.isFinite(ebitda)) return ebitda / ev;
  return null;
}

// Magic Formula 用 ROIC ≈ EBIT / (総資産 - 流動負債)。データ不足時は ROA 代替。
function calcROIC(rawData) {
  const incomeStmt = rawData.income_stmt;
  const bs = rawData.balancesheet;
  if (Array.isArray(incomeStmt) && Array.isArray(bs)) {
    const isDates = getStmtLatestDates(incomeStmt);
    const bsDates = getStmtLatestDates(bs);
    if (isDates.length > 0 && bsDates.length > 0) {
      const ebit = getValFromArray(incomeStmt, "EBIT", isDates[0]);
      const ta = getValFromArray(bs, "Total Assets", bsDates[0]);
      const cl = getValFromArray(bs, "Current Liabilities", bsDates[0]);
      if (typeof ebit === "number" && typeof ta === "number" && typeof cl === "number" && ta - cl > 0) {
        return ebit / (ta - cl);
      }
    }
  }
  const roa = rawData.info?.returnOnAssets;
  return (typeof roa === "number" && Number.isFinite(roa)) ? roa : null;
}

// yfinance の info.regularMarketPrice はキャッシュで古い値を返すことがある。
// history の最終 Close は毎回フレッシュに取得されるため、こちらをプライマリとする。
function getLastClosePrice(rawData) {
  const hist = rawData.history;
  if (Array.isArray(hist) && hist.length > 0) {
    const last = hist[hist.length - 1];
    if (typeof last?.Close === "number" && last.Close > 0) return last.Close;
  }
  const info = rawData.info || {};
  return info.currentPrice || info.regularMarketPrice || null;
}

function getSectorETF(sector, subIndustry) {
  return sectorEtfMap[subIndustry] || sectorEtfMap[sector] || 'SPY';
}

function getBenchmarkInfo(metadata, rawInfo) {
  // metadata (stocks_list.json) を優先しつつ、未取得時は yfinance info に
  // フォールバック (sector は yf 名 → GICS 名に正規化)。
  const sector = metadata['GICS Sector'] || normalizeSector(rawInfo?.sector);
  const subInd = metadata['GICS Sub-Industry'] || rawInfo?.industry;
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
    dividend_yield: normalizeDividendYield(info.dividendYield),
    debt_to_equity: info.debtToEquity || null,
    earnings_growth: info.earningsGrowth || null,
    profit_margins: info.profitMargins || null,
    current_ratio: info.currentRatio || null,
    eps_ttm: info.trailingEps || null,
    eps_forward: info.forwardEps || null,
    payout_ratio: info.payoutRatio || null,
  };
}

function extractFcfMetrics(rawData) {
  const info = rawData.info || {};
  const marketCap = info.marketCap || null;
  const sharesOutstanding = info.sharesOutstanding || null;

  // Most recent annual FCF
  let fcfAnnual = null;
  const cf = rawData.cashflow || [];
  if (Array.isArray(cf) && cf.length > 0) {
    const fcfRow = cf.find((r) => r.index === "Free Cash Flow");
    if (fcfRow) {
      const dates = Object.keys(fcfRow).filter((k) => k !== "index").sort().reverse();
      if (dates.length > 0) fcfAnnual = Number(fcfRow[dates[0]]) || null;
    }
  }

  // TTM FCF from 4 most recent quarters
  let fcfTtm = null;
  const qcf = rawData.quarterly_cashflow || [];
  if (Array.isArray(qcf) && qcf.length > 0) {
    const fcfRow = qcf.find((r) => r.index === "Free Cash Flow");
    if (fcfRow) {
      const dates = Object.keys(fcfRow).filter((k) => k !== "index").sort().reverse().slice(0, 4);
      if (dates.length >= 4) {
        const sum = dates.reduce((acc, d) => acc + (Number(fcfRow[d]) || 0), 0);
        if (sum !== 0) fcfTtm = sum;
      }
    }
  }

  const fcf = fcfTtm ?? fcfAnnual;
  return {
    fcf_ttm: fcf,
    fcf_per_share: fcf != null && sharesOutstanding ? fcf / sharesOutstanding : null,
    fcf_yield: fcf != null && marketCap && marketCap > 0 ? fcf / marketCap : null,
  };
}

function extractDividendStreak(dividends) {
  if (!Array.isArray(dividends) || dividends.length === 0)
    return { streak: 0, has_dividend: false };

  const byYear = {};
  for (const d of dividends) {
    const dateStr = String(d.index || d.Date || d.date || "");
    const year = dateStr.substring(0, 4);
    if (!year || isNaN(parseInt(year))) continue;
    const val = Number(d.Dividends || d.dividends || d.Value || d.value || 0);
    byYear[year] = (byYear[year] || 0) + val;
  }

  const years = Object.keys(byYear).filter((y) => byYear[y] > 0).sort();
  if (years.length === 0) return { streak: 0, has_dividend: false };
  if (years.length === 1) return { streak: 0, has_dividend: true };

  // Exclude current year if incomplete
  const currentYear = new Date().getFullYear().toString();
  const compareYears = years.filter((y) => y < currentYear);
  if (compareYears.length < 2) return { streak: 0, has_dividend: true };

  let streak = 0;
  for (let i = compareYears.length - 1; i > 0; i--) {
    const curr = compareYears[i];
    const prev = compareYears[i - 1];
    if (parseInt(curr) - parseInt(prev) === 1 && byYear[curr] >= byYear[prev] * 0.99) {
      streak++;
    } else {
      break;
    }
  }

  return { streak, has_dividend: true };
}

function extractShortInterest(rawData) {
  const info = rawData.info || {};
  const shortPct = info.shortPercentOfFloat ?? null;
  const shortRatio = info.shortRatio ?? null;
  if (shortPct == null && shortRatio == null) return null;

  const sharesShort = info.sharesShort ?? null;
  const sharesShortPrior = info.sharesShortPriorMonth ?? null;
  return {
    short_percent_float: shortPct,
    short_ratio: shortRatio,
    shares_short: sharesShort,
    shares_short_prior_month: sharesShortPrior,
    change_pct:
      sharesShort != null && sharesShortPrior && sharesShortPrior > 0
        ? (sharesShort - sharesShortPrior) / sharesShortPrior
        : null,
  };
}

function extractInsiderTransactions(rawData) {
  const raw = rawData.insider_transactions;
  if (!Array.isArray(raw) || raw.length === 0) return [];

  return raw
    .map((t) => {
      const dateStr = String(t.index || t["Start Date"] || t.date || "").split(" ")[0];
      return {
        date: dateStr,
        insider: String(t.Insider || t.insider || ""),
        title: String(t.Position || t.position || ""),
        transaction: String(t.Text || t.Transaction || t.transaction || ""),
        shares: typeof t.Shares === "number" ? t.Shares : null,
        value: typeof t.Value === "number" ? t.Value : null,
      };
    })
    .filter((t) => t.date && t.date.length === 10 && t.insider)
    .sort((a, b) => b.date.localeCompare(a.date))
    .slice(0, 20);
}

function extractInstitutionalOwnership(rawData) {
  const info = rawData.info || {};
  const raw = rawData.institutional_holders;
  const pctInst = info.heldPercentInstitutions ?? null;
  const pctIns = info.heldPercentInsiders ?? null;

  if ((!Array.isArray(raw) || raw.length === 0) && pctInst == null && pctIns == null)
    return null;

  const holders = Array.isArray(raw)
    ? raw
        .filter((h) => h.Holder || h.holder)
        .map((h) => ({
          holder: String(h.Holder || h.holder || ""),
          shares: typeof h.Shares === "number" ? h.Shares : null,
          date_reported: String(h["Date Reported"] || h.dateReported || "").split(" ")[0],
          pct_out: typeof h["% Out"] === "number" ? h["% Out"] : null,
          value: typeof h.Value === "number" ? h.Value : null,
        }))
        .sort((a, b) => (b.pct_out || 0) - (a.pct_out || 0))
        .slice(0, 10)
    : [];

  return { holders, pct_held_institutions: pctInst, pct_held_insiders: pctIns };
}

function extractEsgScores(rawData) {
  const sust = rawData.sustainability;
  if (!sust || typeof sust !== "object" || Array.isArray(sust)) return null;

  const get = (key) => {
    const v = sust[key];
    return typeof v === "number" ? v : v != null ? Number(v) || null : null;
  };

  const total = get("totalEsg");
  const env = get("environmentScore");
  const social = get("socialScore");
  const gov = get("governanceScore");

  if (total == null && env == null && social == null && gov == null) return null;

  return {
    total_esg: total,
    environment_score: env,
    social_score: social,
    governance_score: gov,
    peer_group: sust.peerGroup != null ? String(sust.peerGroup) : null,
    highest_controversy: get("highestControversy"),
  };
}

function extractEarningsSurprise(rawData) {
  const ed = rawData.earnings_dates;
  if (!ed || !Array.isArray(ed) || ed.length === 0) return null;
  // yfinance provides a list where each element is a date's data
  // We need to find the latest one that has a "Reported EPS"
  const sorted = [...ed].sort((a, b) => new Date(b["Earnings Date"] || b.index || b.Date).getTime() - new Date(a["Earnings Date"] || a.index || a.Date).getTime());
  for (const item of sorted) {
    if (item["Reported EPS"] !== null && item["Reported EPS"] !== undefined) {
      return {
        date: String(item["Earnings Date"] || item.index || item.Date).split(" ")[0],
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
  const sorted = [...ed].sort((a, b) => new Date(a["Earnings Date"] || a.index || a.Date).getTime() - new Date(b["Earnings Date"] || b.index || b.Date).getTime());
  for (const item of sorted) {
    const d = new Date(item["Earnings Date"] || item.index || item.Date);
    if (d >= now && (item["Reported EPS"] === null || item["Reported EPS"] === undefined)) {
      return {
        date: String(item["Earnings Date"] || item.index || item.Date).split(" ")[0],
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
    currentPrice: getLastClosePrice(rawData),
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
  // 欠損 (null / undefined) と 0 を区別するため、生値配列と数値配列を別々に保持する。
  // 金融セクター等は損益計算書に Gross Profit / Operating Income が無く、
  // yfinance が null を返す。これを 0 として描画すると棒が消えるだけでなく
  // 凡例・率の折れ線も無意味に並ぶため、全期間欠損のシリーズは丸ごと省略する。
  const getRaw = (label) => dates.map((d) => getValFromArray(stmt, label, d));
  const toNum = (raw) => raw.map((v) => Number(v) || 0);
  const allMissing = (raw) =>
    raw.every((v) => v === null || v === undefined || Number(v) === 0);

  const revenueRaw = getRaw("Total Revenue");
  const revenue = toNum(revenueRaw);
  // 通年は最後の 6 期、四半期は最後の 8 期を使う (master の挙動に合わせる)
  const sliceCount = suffix === " (四半期)" ? 8 : 6;
  const validIdx = revenue
    .map((v, i) => (v > 0 ? i : -1))
    .filter((i) => i !== -1)
    .slice(-sliceCount);
  if (validIdx.length === 0) return null;
  const pick = (arr) => validIdx.map((i) => arr[i]);
  const labels = validIdx.map((i) => dates[i].split(" ")[0]);

  const grossRaw = getRaw("Gross Profit");
  const opIncomeRaw = getRaw("Operating Income");
  const netRaw = getRaw("Net Income");

  const rev = pick(revenue);
  const grossProfit = pick(toNum(grossRaw));
  const operatingIncome = pick(toNum(opIncomeRaw));
  const netIncome = pick(toNum(netRaw));
  const ratio = (num, den) =>
    num.map((v, i) => (den[i] ? v / den[i] : null));

  // 表示対象期間 (validIdx) で値がすべて欠損/0 のシリーズは棒も率も省略。
  // 金融・不動産セクター等で Gross Profit / Operating Income が未報告のときに発動。
  const grossMissing = allMissing(validIdx.map((i) => grossRaw[i]));
  const opMissing = allMissing(validIdx.map((i) => opIncomeRaw[i]));

  const datasets = [
    { type: "bar", label: `売上高${suffix}`, data: rev,
      backgroundColor: "rgba(174, 199, 232, 0.85)", yAxisID: "y", order: 2, hidden },
  ];
  if (!grossMissing) {
    datasets.push({ type: "bar", label: `売上総利益${suffix}`, data: grossProfit,
      backgroundColor: "rgba(31, 119, 180, 0.85)", yAxisID: "y", order: 2, hidden });
  }
  if (!opMissing) {
    datasets.push({ type: "bar", label: `営業利益${suffix}`, data: operatingIncome,
      backgroundColor: "rgba(255, 187, 120, 0.85)", yAxisID: "y", order: 2, hidden });
  }
  datasets.push({ type: "bar", label: `純利益${suffix}`, data: netIncome,
    backgroundColor: "rgba(44, 160, 44, 0.85)", yAxisID: "y", order: 2, hidden });
  if (!grossMissing) {
    datasets.push({ type: "line", label: `売上総利益率${suffix}`, data: ratio(grossProfit, rev),
      borderColor: "#1f77b4", backgroundColor: "#1f77b4",
      borderWidth: 2, fill: false, pointRadius: 4, yAxisID: "y1", order: 1, hidden });
  }
  if (!opMissing) {
    datasets.push({ type: "line", label: `営業利益率${suffix}`, data: ratio(operatingIncome, rev),
      borderColor: "#ffbb78", backgroundColor: "#ffbb78",
      borderWidth: 2, fill: false, pointRadius: 4, yAxisID: "y1", order: 1, hidden });
  }
  datasets.push({ type: "line", label: `純利益率${suffix}`, data: ratio(netIncome, rev),
    borderColor: "#2ca02c", backgroundColor: "#2ca02c",
    borderWidth: 2, fill: false, pointRadius: 4, yAxisID: "y1", order: 1, hidden });
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
  const currentLiabRaw = get("Current Liabilities");
  const currentLiab = pick(currentLiabRaw);
  // yfinance が四半期によってフィールド名を変えるためフォールバック付きで取得する。
  // 優先順: Net Minority Interest 付き → 別名 → 総負債 - 流動負債で導出
  const nclRaw = get("Total Non Current Liabilities Net Minority Interest");
  const nclFallback = get("Total Non Current Liabilities");
  const totalLiabRaw = get("Total Liabilities Net Minority Interest");
  const nclMerged = nclRaw.map((v, i) =>
    v !== 0 ? v :
    nclFallback[i] !== 0 ? nclFallback[i] :
    Math.max(0, totalLiabRaw[i] - currentLiabRaw[i])
  );
  const nonCurrentLiab = pick(nclMerged);
  let equity = pick(get("Total Equity Gross Minority Interest"));
  if (sum(equity) === 0) equity = pick(get("Stockholders Equity"));
  const totalAssetsValid = pick(totalAssets);
  const totalLiabValid = pick(totalLiabRaw);

  const hasBreakdown = sum(currentAssets) !== 0 && sum(currentLiab) !== 0;

  // 配列順: 先に登場した stack が左、各 stack 内では先の要素が下に積まれる。
  // 左 (資産): 固定資産(下) → 流動資産(上)
  // 右 (負債・純資産): 純資産(下) → 固定負債 → 流動負債(上)
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

function _percentile(sorted, q) {
  if (sorted.length === 0) return null;
  const idx = (sorted.length - 1) * q;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo];
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
}

// IQR ベースで散布図プロットの表示位置をキャップする境界を求める。
// Q1-3*IQR / Q3+3*IQR を境界とし、SNDK のような急騰銘柄の極端値で
// 軸が引き伸ばされて他銘柄が潰れるのを防ぐ。境界外の点は境界に固定し、
// 実数値は origX / origY に保持する (ツールチップは原値を表示)。
function _clampBounds(values) {
  const s = values.filter(Number.isFinite).sort((a, b) => a - b);
  if (s.length < 4) return null;
  const q1 = _percentile(s, 0.25);
  const q3 = _percentile(s, 0.75);
  const iqr = q3 - q1;
  if (!(iqr > 0)) return null;
  return { lo: q1 - 3 * iqr, hi: q3 + 3 * iqr };
}

// 散布図の 1 点を生成。境界外なら表示位置 (x/y) を境界に丸め、
// 元の値を origX / origY に退避する。
function _rrPoint(symbol, x, y, xBounds, yBounds) {
  const point = { x, y, symbol };
  if (xBounds && Number.isFinite(x)) {
    const cx = Math.min(Math.max(x, xBounds.lo), xBounds.hi);
    if (cx !== x) {
      point.x = cx;
      point.origX = x;
    }
  }
  if (yBounds && Number.isFinite(y)) {
    const cy = Math.min(Math.max(y, yBounds.lo), yBounds.hi);
    if (cy !== y) {
      point.y = cy;
      point.origY = y;
    }
  }
  return point;
}

// リスク・リターン散布図: 8 期間 × 最大 6 トレース (target / sectorETF /
// broadSectorEtf / S&P 500 / 自インデックス (S&P 400/600) / その他 S&P 銘柄)。
// ラベル末尾の "(1年)" などの期間サフィックスは ChartJs.astro の hasGroups
// 機能でタブ切替に変換される。 初期表示は 1年。
// marketIndexEtf は対象銘柄が属する指数の ETF (S&P 400→MDY / S&P 600→IJR)。
// S&P 500 銘柄では "SPY" となり、SPY は常に描画されるため追加プロットしない。
function generateRiskReturnChart(allMetrics, targetSymbol, sectorEtf, broadSectorEtf, marketIndexEtf, peerSet) {
  if (!allMetrics || allMetrics.length === 0) return null;
  const datasets = [];
  // "その他銘柄" は対象銘柄が属する指数 (S&P 500/400/600) の構成銘柄に限定する。
  const limitToPeers = peerSet && peerSet.size > 0;
  // 対象銘柄が属する指数のラベル (凡例表示用。peerSet の指数に対応)。
  const peerIndexLabel =
    ({ SPY: "S&P 500", MDY: "S&P 400", IJR: "S&P 600" })[marketIndexEtf] ||
    "S&P 500";
  // 対象銘柄が S&P 400/600 のとき、その指数 ETF を SPY に加えて描画する。
  const indexEtfLabels = { MDY: "S&P 400", IJR: "S&P 600" };
  const ownIndexEtf =
    marketIndexEtf && marketIndexEtf !== "SPY" && indexEtfLabels[marketIndexEtf]
      ? marketIndexEtf
      : null;

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
    const broadSector = allMetrics.find(
      (m) => m.symbol === broadSectorEtf && hasValue(m),
    );
    const market = allMetrics.find(
      (m) => m.symbol === "SPY" && hasValue(m),
    );
    const ownIndex = ownIndexEtf
      ? allMetrics.find((m) => m.symbol === ownIndexEtf && hasValue(m))
      : null;
    const others = allMetrics.filter(
      (m) =>
        m.symbol !== targetSymbol &&
        m.symbol !== sectorEtf &&
        m.symbol !== broadSectorEtf &&
        m.symbol !== "SPY" &&
        m.symbol !== ownIndexEtf &&
        (!limitToPeers || peerSet.has(m.symbol)) &&
        hasValue(m),
    );

    // この期間で実際に描画する全銘柄からプロット位置のキャップ境界を算出。
    const displayed = [...others];
    if (target) displayed.push(target);
    if (sector) displayed.push(sector);
    if (broadSector) displayed.push(broadSector);
    if (market) displayed.push(market);
    if (ownIndex) displayed.push(ownIndex);
    const xBounds = _clampBounds(displayed.map((m) => m[hvKey]));
    const yBounds = _clampBounds(displayed.map((m) => m[retKey]));

    // 描画順 (配列の後ほど前面): その他 S&P 銘柄 (背景) -> Vanguard セクター ETF
    // -> セクター ETF (SPDR) -> S&P 500 ETF -> ターゲット (最前面)。
    // 優先表示は ターゲット > S&P 500 ETF > セクター ETF > Vanguard ETF > その他 S&P 銘柄。
    // order: 小さい値ほど前面 (Chart.js の描画順)。
    // ターゲット=0 (最前面), ETF類=1-2, S&P銘柄=3 (最背面)。
    datasets.push({
      label: `${peerIndexLabel}銘柄 (${p.label})`,
      data: others.map((m) =>
        _rrPoint(m.symbol, m[hvKey], m[retKey], xBounds, yBounds),
      ),
      backgroundColor: "rgba(114, 119, 123, 0.4)",
      pointRadius: 2.5,
      order: 3,
      visible: isDefault,
    });
    if (broadSectorEtf && broadSectorEtf !== "SPY") {
      datasets.push({
        label: `${broadSectorEtf} (${p.label})`,
        data: broadSector
          ? [_rrPoint(broadSectorEtf, broadSector[hvKey], broadSector[retKey], xBounds, yBounds)]
          : [],
        backgroundColor: "rgba(100, 149, 237, 0.7)",
        pointRadius: 7,
        pointBorderColor: "#ffffff",
        pointBorderWidth: 1.5,
        order: 2,
        visible: isDefault,
      });
    }
    if (sectorEtf && sectorEtf !== "SPY") {
      datasets.push({
        label: `${sectorEtf} (${p.label})`,
        data: sector
          ? [_rrPoint(sectorEtf, sector[hvKey], sector[retKey], xBounds, yBounds)]
          : [],
        backgroundColor: "rgba(0, 108, 172, 0.9)",
        pointRadius: 8,
        pointBorderColor: "#ffffff",
        pointBorderWidth: 1.5,
        order: 2,
        visible: isDefault,
      });
    }
    datasets.push({
      label: `S&P 500 (${p.label})`,
      data: market
        ? [_rrPoint("S&P 500", market[hvKey], market[retKey], xBounds, yBounds)]
        : [],
      backgroundColor: "rgba(34, 197, 94, 0.95)",
      pointRadius: 8,
      pointBorderColor: "#ffffff",
      pointBorderWidth: 1.5,
      order: 1,
      visible: isDefault,
    });
    if (ownIndexEtf) {
      const indexLabel = indexEtfLabels[ownIndexEtf];
      datasets.push({
        label: `${indexLabel} (${p.label})`,
        data: ownIndex
          ? [_rrPoint(indexLabel, ownIndex[hvKey], ownIndex[retKey], xBounds, yBounds)]
          : [],
        backgroundColor: "rgba(168, 85, 247, 0.95)",
        pointRadius: 8,
        pointBorderColor: "#ffffff",
        pointBorderWidth: 1.5,
        order: 1,
        visible: isDefault,
      });
    }
    datasets.push({
      label: `${targetSymbol} (${p.label})`,
      data: target
        ? [_rrPoint(targetSymbol, target[hvKey], target[retKey], xBounds, yBounds)]
        : [],
      backgroundColor: "rgba(255, 0, 0, 0.9)",
      pointRadius: 10,
      pointBorderColor: "#ffffff",
      pointBorderWidth: 1.5,
      order: 0,
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
        stack: `income${suffix}`, yAxisID: "y", order: 2, hidden },
      { type: "bar", label: `配当金${suffix}`, data: divData,
        backgroundColor: "rgba(174, 199, 232, 0.85)",
        stack: `payout${suffix}`, yAxisID: "y", order: 2, hidden },
      { type: "bar", label: `自社株買い${suffix}`, data: repoData,
        backgroundColor: "rgba(31, 119, 180, 0.85)",
        stack: `payout${suffix}`, yAxisID: "y", order: 2, hidden },
      { type: "line", label: `配当性向${suffix}`, data: divRatio,
        borderColor: "#ffbb78", backgroundColor: "#ffbb78",
        borderWidth: 2, fill: false, pointRadius: 4, yAxisID: "y1", order: 1, hidden },
      { type: "line", label: `総還元性向${suffix}`, data: totalRatio,
        borderColor: "#ff7f0e", backgroundColor: "#ff7f0e",
        borderWidth: 2, fill: false, pointRadius: 4, yAxisID: "y1", order: 1, hidden },
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
// 年間推移タブ: 年単位で配当を集計し、年初株価との比で利回りを算出。
function _dpsAnnualDatasets(dividends, history, suffix, hidden) {
  if (!dividends || !Array.isArray(dividends) || dividends.length === 0)
    return null;

  // 年ごとの配当合計・支払回数を集計
  const annual = {};
  const countByYear = {};
  const dated = [];
  dividends.forEach((d) => {
    const date = new Date(d.Date || d.index);
    if (Number.isNaN(date.getTime())) return;
    const amt = d.Dividends || d.Value || 0;
    const y = date.getFullYear();
    annual[y] = (annual[y] || 0) + amt;
    countByYear[y] = (countByYear[y] || 0) + 1;
    dated.push({ time: date.getTime(), amount: amt });
  });

  const allYears = Object.keys(annual).map(Number).sort((a, b) => a - b);
  if (allYears.length === 0) return null;
  const years = allYears.slice(-10);
  const labels = years.map(String);

  // 進行中の年度は「直近1回の配当 × 昨年の支払回数」で年間配当を推定し、
  // 実績分 (actual) の上に推定上乗せ分 (estimate) を積み上げて表示する。
  // (master の fundamentals.get_dps_eps_chart_data の推定ロジックに準拠)
  const currentYear = new Date().getFullYear();
  const actualByYear = { ...annual }; // バー (実績) に使う値
  const estimatedPart = {};           // バー (推定上乗せ) に使う値
  const totalByYear = { ...annual };  // 利回り計算に使う年間総額 (推定込み)

  if (annual[currentYear] != null) {
    let freq = countByYear[currentYear - 1] || 0;
    if (freq <= 0) {
      // 昨年データが無い場合は直近 365 日の支払回数で代用
      const yearAgo = Date.now() - 365 * 24 * 3600 * 1000;
      freq = dated.filter((d) => d.time >= yearAgo).length;
    }
    if (freq <= 0) freq = 4;   // 四半期配当をデフォルトと仮定
    if (freq > 12) freq = 12;  // 異常値ガード

    dated.sort((a, b) => a.time - b.time);
    const latestDiv = dated.length ? dated[dated.length - 1].amount : 0;
    const estTotal = latestDiv * freq;
    const actualPaid = annual[currentYear];
    // 推定総額が実績支払額を上回るときだけ「推定上乗せ」を出す
    if (estTotal > actualPaid) {
      actualByYear[currentYear] = actualPaid;
      estimatedPart[currentYear] = estTotal - actualPaid;
      totalByYear[currentYear] = estTotal;
    }
  }

  const hasEstimate = years.some((y) => estimatedPart[y] > 0);
  const actualData = years.map((y) => actualByYear[y]);
  const estimateData = years.map((y) => estimatedPart[y] || null);

  const yieldData = years.map((y) => {
    if (!history || history.length === 0) return null;
    const firstOfYear = history.find((h) => {
      const dt = new Date(h.Date || h.index);
      return dt.getFullYear() === y;
    });
    if (!firstOfYear || !(firstOfYear.Close > 0)) return null;
    return totalByYear[y] / firstOfYear.Close;
  });

  const hasYield = yieldData.some((v) => v != null && Number.isFinite(v));

  const datasets = [
    {
      type: "bar",
      label: `${hasEstimate ? "実績配当" : "配当金"}${suffix}`,
      data: actualData,
      backgroundColor: "rgba(31, 119, 180, 0.85)",
      yAxisID: "y",
      order: 2,
      hidden,
    },
  ];

  if (hasEstimate) {
    datasets.push({
      type: "bar",
      label: `推定配当${suffix}`,
      data: estimateData,
      backgroundColor: "#aec7e8",
      yAxisID: "y",
      order: 2,
      hidden,
    });
  }

  if (hasYield) {
    datasets.push({
      type: "line",
      label: `配当利回り${suffix}`,
      data: yieldData,
      borderColor: "#ff7f0e",
      backgroundColor: "#ff7f0e",
      borderWidth: 2,
      fill: false,
      pointRadius: 4,
      yAxisID: "y1",
      order: 1,
      hidden,
    });
  }

  return { labels, datasets };
}

// 権利落日別タブ: 配当ごとの支払日を X 軸にし、各配当の年間換算利回り
// (= 配当金 × その時点での年間支払回数 ÷ 支払日終値) を折れ線で表示。
function _dpsPerPaymentDatasets(dividends, history, suffix, hidden) {
  if (!dividends || !Array.isArray(dividends) || dividends.length === 0)
    return null;

  const validDivs = dividends
    .map((d) => ({
      date: new Date(d.Date || d.index),
      amount: d.Dividends || d.Value || 0,
    }))
    .filter((d) => !Number.isNaN(d.date.getTime()) && d.amount > 0)
    .sort((a, b) => a.date - b.date);

  if (validDivs.length === 0) return null;

  // 直近 24 支払を表示 (四半期配当なら 6 年分)
  const selected = validDivs.slice(-24);

  // 各配当の年間支払回数 = その配当日から遡って 365 日以内に発生した
  // 配当の件数 (自身を含む)。新規配当銘柄や頻度変化にも追随する。
  const ONE_YEAR_MS = 365 * 24 * 60 * 60 * 1000;
  const annualizationFactors = selected.map((d) => {
    const cutoff = d.date.getTime() - ONE_YEAR_MS;
    return validDivs.filter(
      (o) => o.date.getTime() > cutoff && o.date.getTime() <= d.date.getTime(),
    ).length;
  });

  const labels = selected.map((d) => {
    const y = d.date.getFullYear();
    const m = String(d.date.getMonth() + 1).padStart(2, "0");
    const day = String(d.date.getDate()).padStart(2, "0");
    return `${y}-${m}-${day}`;
  });

  const divs = selected.map((d) => d.amount);

  // 支払日の終値 (なければ前後 7 日以内の最も近い取引日)
  const priceOnDate = (targetDate) => {
    if (!history || history.length === 0) return null;
    const targetTime = targetDate.getTime();
    const WINDOW = 7 * 24 * 60 * 60 * 1000;
    let best = null;
    let bestDiff = Infinity;
    for (const h of history) {
      const ht = new Date(h.Date || h.index).getTime();
      if (Number.isNaN(ht)) continue;
      const diff = Math.abs(ht - targetTime);
      if (diff <= WINDOW && diff < bestDiff && h.Close > 0) {
        best = h;
        bestDiff = diff;
      }
    }
    return best ? best.Close : null;
  };

  const yieldData = selected.map((d, i) => {
    const price = priceOnDate(d.date);
    if (!price) return null;
    const factor = annualizationFactors[i] || 1;
    return (d.amount * factor) / price;
  });

  const hasYield = yieldData.some((v) => v != null && Number.isFinite(v));

  const datasets = [
    {
      type: "bar",
      label: `配当金${suffix}`,
      data: divs,
      backgroundColor: "rgba(31, 119, 180, 0.85)",
      yAxisID: "y",
      order: 2,
      hidden,
    },
  ];

  if (hasYield) {
    datasets.push({
      type: "line",
      label: `年間換算配当利回り${suffix}`,
      data: yieldData,
      borderColor: "#ff7f0e",
      backgroundColor: "#ff7f0e",
      borderWidth: 2,
      fill: false,
      pointRadius: 4,
      yAxisID: "y1",
      order: 1,
      hidden,
    });
  }

  return { labels, datasets };
}

function generateDpsEpsChart(dividends, history) {
  const annual = _tagGroupLabels(
    _dpsAnnualDatasets(dividends, history, " (年間推移)", false),
  );
  const perPayment = _tagGroupLabels(
    _dpsPerPaymentDatasets(dividends, history, " (権利落日別)", true),
  );
  return _mergeGroups(annual, perPayment);
}

const _SEG_COLORS = [
  "rgba(31, 119, 180, 0.8)",
  "rgba(255, 127, 14, 0.8)",
  "rgba(44, 160, 44, 0.8)",
  "rgba(214, 39, 40, 0.8)",
  "rgba(148, 103, 189, 0.8)",
  "rgba(140, 86, 75, 0.8)",
  "rgba(227, 119, 194, 0.8)",
  "rgba(127, 127, 127, 0.8)",
  "rgba(188, 189, 34, 0.8)",
  "rgba(23, 190, 207, 0.8)",
];
const _DATE_KEYS = ["Date", "report_date", "symbol", "index"];

function _segmentDatasets(data, suffix, hidden, stackName, orderedCols, colorMap) {
  if (!data || data.length === 0) return null;
  const segCols = orderedCols || Object.keys(data[0]).filter((k) => !_DATE_KEYS.includes(k));
  if (segCols.length === 0) return null;
  const labels = data.map((row) =>
    String(row.Date || row.report_date || row.index).split(" ")[0],
  );
  const datasets = segCols.map((seg, i) => ({
    label: `${seg}${suffix}`,
    data: data.map((row) => Number(row[seg]) || 0),
    backgroundColor: colorMap ? (colorMap[seg] ?? _SEG_COLORS[i % _SEG_COLORS.length]) : _SEG_COLORS[i % _SEG_COLORS.length],
    hidden,
    stack: stackName,
  }));
  return { labels, datasets };
}

// segmentData: records 配列 (defeatbeta-api から取得した生データ)
// stackName:   Chart.js の stack 識別子 ('segment' | 'geography')
// errorLabel:  エラーメッセージ用ラベル ('セグメント収益' | '地域別収益')
function generateSegmentChart(segmentData, stackName = 'segment', errorLabel = 'セグメント収益') {
  const NO_DATA = { error: `詳細な${errorLabel}のデータが取得できませんでした（企業による未開示、またはデータソースの制約によるもの）。` };
  if (!segmentData || !Array.isArray(segmentData) || segmentData.length === 0)
    return NO_DATA;
  const segCols = Object.keys(segmentData[0]).filter((k) => !_DATE_KEYS.includes(k));
  if (segCols.length === 0) return NO_DATA;

  // 日付昇順ソート
  const sorted = [...segmentData].sort((a, b) =>
    String(a.Date || a.report_date || '').localeCompare(String(b.Date || b.report_date || '')),
  );

  // 最新行の値を基準にセグメント列を並び替え: 値降順、"other"含む項目は末尾
  const latestRow = sorted[sorted.length - 1] ?? {};
  const orderedCols = [...segCols].sort((a, b) => {
    const aOther = a.toLowerCase().includes('other');
    const bOther = b.toLowerCase().includes('other');
    if (aOther !== bOther) return aOther ? 1 : -1;
    return (Number(latestRow[b]) || 0) - (Number(latestRow[a]) || 0);
  });

  // セグメント名→色を固定（通年・四半期タブ間で色を統一）
  const colorMap = Object.fromEntries(
    orderedCols.map((col, i) => [col, _SEG_COLORS[i % _SEG_COLORS.length]]),
  );

  // 四半期: 直近40四半期（通年10年に合わせた期間）
  const quarterlyData = sorted.slice(-40);

  // 通年: 年（決算期）ごとに集計して直近10年
  const byYear = {};
  for (const row of sorted) {
    const year = String(row.Date || row.report_date || '').slice(0, 4);
    if (year.length < 4) continue;
    if (!byYear[year]) { byYear[year] = { Date: year }; for (const c of segCols) byYear[year][c] = 0; }
    for (const c of segCols) byYear[year][c] += Number(row[c]) || 0;
  }
  const annualData = Object.values(byYear)
    .sort((a, b) => a.Date.localeCompare(b.Date))
    .slice(-10);

  const annual    = _tagGroupLabels(_segmentDatasets(annualData,    " (通年)",  false, stackName, orderedCols, colorMap));
  const quarterly = _tagGroupLabels(_segmentDatasets(quarterlyData, " (四半期)", true,  stackName, orderedCols, colorMap));
  return _mergeGroups(annual, quarterly) || NO_DATA;
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

// === ranking ===

// 各銘柄を「全体」「セクター内」で順位付けする指標定義。
// group: "analysis" は分析寄り (常時表示)、"facts" は雑学/規模感寄り (展開表示)。
// dir: 値が大きい方が「上位」(rank 1) になるよう正規化するための符号。
//   "desc" は値が大きい方が上位 (時価総額・ROE・売上など多数派)
//   "asc" は値が小さい方が上位 (PER のような割安系)
// get: rawData (raw_payload) と info を受け取り、値 (number) または null を返す。
const RANK_METRICS = [
  // --- 分析寄り ---
  { key: "market_cap",          label: "時価総額",                group: "analysis", dir: "desc", unit: "currency", get: (r) => r.info?.marketCap ?? null },
  { key: "roe",                 label: "ROE",                     group: "analysis", dir: "desc", unit: "percent",  get: (r) => r.info?.returnOnEquity ?? null },
  { key: "roa",                 label: "ROA",                     group: "analysis", dir: "desc", unit: "percent",  get: (r) => r.info?.returnOnAssets ?? null },
  { key: "operating_margin",    label: "営業利益率",              group: "analysis", dir: "desc", unit: "percent",  get: (r) => r.info?.operatingMargins ?? null },
  { key: "gross_margin",        label: "粗利率",                  group: "analysis", dir: "desc", unit: "percent",  get: (r) => r.info?.grossMargins ?? null },
  { key: "forward_pe",          label: "PER 予想",                group: "analysis", dir: "asc",  unit: "ratio",    get: (r) => (typeof r.info?.forwardPE === "number" && r.info.forwardPE > 0) ? r.info.forwardPE : null },
  { key: "pbr",                 label: "PBR",                     group: "analysis", dir: "asc",  unit: "ratio",    get: (r) => (typeof r.info?.priceToBook === "number" && r.info.priceToBook > 0) ? r.info.priceToBook : null },
  { key: "psr",                 label: "PSR",                     group: "analysis", dir: "asc",  unit: "ratio",    get: (r) => (typeof r.info?.priceToSalesTrailing12Months === "number" && r.info.priceToSalesTrailing12Months > 0) ? r.info.priceToSalesTrailing12Months : null },
  { key: "revenue_cagr_3y",     label: "売上 3Y CAGR",            group: "analysis", dir: "desc", unit: "percent",  get: (r) => r.dcf_valuation?.cagr_details?.revenue ?? null },
  { key: "eps_9y_cagr",         label: "EPS 9Y CAGR",             group: "analysis", dir: "desc", unit: "percent",  get: (r) => r.dcf_valuation?.cagr_details?.eps_9y_cagr ?? null },
  { key: "dividend_yield",      label: "配当利回り",              group: "analysis", dir: "desc", unit: "percent",  get: (r) => {
    // yfinance の dividendYield はバージョンによって percent 単位 (0.36 = 0.36%) で
    // 返ってくる。他の percent 指標 (ROE, margins) は小数 (0.34 = 34%) で返るので
    // 単位を揃えるため 100 で割って小数化する。
    const y = r.info?.dividendYield;
    return typeof y === "number" ? y / 100 : null;
  }},
  // --- ファクト寄り (規模感・雑学) ---
  { key: "stock_price",         label: "株価 (1株あたり)",        group: "facts",    dir: "desc", unit: "price",    get: (r) => getLastClosePrice(r) },
  { key: "revenue_ttm",         label: "TTM 売上",                group: "facts",    dir: "desc", unit: "currency", get: (r) => r.info?.totalRevenue ?? null },
  { key: "employees",           label: "従業員数",                group: "facts",    dir: "desc", unit: "count",    get: (r) => r.info?.fullTimeEmployees ?? null },
  { key: "total_cash",          label: "現金保有額",              group: "facts",    dir: "desc", unit: "currency", get: (r) => r.info?.totalCash ?? null },
  { key: "revenue_per_employee", label: "売上 / 従業員",          group: "facts",    dir: "desc", unit: "currency", get: (r) => {
    const rev = r.info?.totalRevenue, emp = r.info?.fullTimeEmployees;
    return (typeof rev === "number" && typeof emp === "number" && emp > 0) ? rev / emp : null;
  }},
  { key: "mcap_per_employee",   label: "時価総額 / 従業員",       group: "facts",    dir: "desc", unit: "currency", get: (r) => {
    const m = r.info?.marketCap, emp = r.info?.fullTimeEmployees;
    return (typeof m === "number" && typeof emp === "number" && emp > 0) ? m / emp : null;
  }},
  { key: "range_position_52w",  label: "52週レンジ内位置",        group: "facts",    dir: "desc", unit: "percent",  get: (r) => {
    const cur = getLastClosePrice(r), hi = r.info?.fiftyTwoWeekHigh, lo = r.info?.fiftyTwoWeekLow;
    if (typeof cur !== "number" || typeof hi !== "number" || typeof lo !== "number" || hi <= lo) return null;
    return (cur - lo) / (hi - lo); // 0=安値、1=高値
  }},
  { key: "dividend_rate",       label: "1株あたり配当 (年)",      group: "facts",    dir: "desc", unit: "price",    get: (r) => r.info?.dividendRate ?? null },
  { key: "beta",                label: "ベータ",                  group: "facts",    dir: "desc", unit: "ratio",    get: (r) => r.dcf_valuation?.wacc_details?.beta ?? r.info?.beta ?? null },
  // --- アナリスト評価 ---
  { key: "analyst_count",       label: "アナリスト数",            group: "analysis", dir: "desc", unit: "count",    get: (r) => {
    const n = r.info?.numberOfAnalystOpinions;
    return (typeof n === "number" && n > 0) ? n : null;
  }},
  { key: "target_upside",       label: "目標株価乖離率",          group: "analysis", dir: "desc", unit: "percent",  get: (r) => {
    const target = r.info?.targetMeanPrice;
    const cur = getLastClosePrice(r);
    if (typeof target !== "number" || typeof cur !== "number" || cur <= 0 || target <= 0) return null;
    return (target - cur) / cur;
  }},
  { key: "analyst_buy_ratio",   label: "Buy評価比率",             group: "analysis", dir: "desc", unit: "percent",  get: (r) => {
    // 直近 (0m) のアナリスト推奨を集計し、(strongBuy + buy) / 総数 を返す。
    // 母集団が小さいと比率が暴れるため、最低 5 件以上の評価がある銘柄のみ対象。
    const ratingsRaw = r.analyst_ratings;
    let cur = {};
    if (Array.isArray(ratingsRaw)) {
      cur = ratingsRaw.find((x) => x.period === "0m") || ratingsRaw[0] || {};
    } else if (ratingsRaw && typeof ratingsRaw === "object") {
      cur = ratingsRaw;
    }
    const sb = cur.strongBuy || 0, b = cur.buy || 0, h = cur.hold || 0, s = cur.sell || 0, ss = cur.strongSell || 0;
    const total = sb + b + h + s + ss;
    if (total < 5) return null;
    return (sb + b) / total;
  }},
  { key: "recommendation_mean", label: "コンセンサス推奨スコア",  group: "analysis", dir: "asc",  unit: "ratio",    get: (r) => {
    const v = r.info?.recommendationMean;
    return (typeof v === "number" && v > 0 && v <= 5) ? v : null;
  }},
  { key: "target_price_dispersion", label: "目標株価レンジ幅",    group: "analysis", dir: "desc", unit: "percent",  get: (r) => {
    const hi = r.info?.targetHighPrice, lo = r.info?.targetLowPrice, mean = r.info?.targetMeanPrice;
    const n = r.info?.numberOfAnalystOpinions || 0;
    if (n < 3 || typeof hi !== "number" || typeof lo !== "number" || typeof mean !== "number" || mean <= 0) return null;
    return (hi - lo) / mean;
  }},
  { key: "analyst_strong_buy_ratio", label: "Strong Buy 専一率", group: "analysis", dir: "desc", unit: "percent",  get: (r) => {
    const ratingsRaw = r.analyst_ratings;
    let cur = {};
    if (Array.isArray(ratingsRaw)) cur = ratingsRaw.find((x) => x.period === "0m") || ratingsRaw[0] || {};
    else if (ratingsRaw && typeof ratingsRaw === "object") cur = ratingsRaw;
    const sb = cur.strongBuy || 0, b = cur.buy || 0, h = cur.hold || 0, s = cur.sell || 0, ss = cur.strongSell || 0;
    const total = sb + b + h + s + ss;
    if (total < 5) return null;
    return sb / total;
  }},
  { key: "analyst_bear_ratio",  label: "弱気評価比率",            group: "analysis", dir: "desc", unit: "percent",  get: (r) => {
    const ratingsRaw = r.analyst_ratings;
    let cur = {};
    if (Array.isArray(ratingsRaw)) cur = ratingsRaw.find((x) => x.period === "0m") || ratingsRaw[0] || {};
    else if (ratingsRaw && typeof ratingsRaw === "object") cur = ratingsRaw;
    const sb = cur.strongBuy || 0, b = cur.buy || 0, h = cur.hold || 0, s = cur.sell || 0, ss = cur.strongSell || 0;
    const total = sb + b + h + s + ss;
    if (total < 5) return null;
    return (s + ss) / total;
  }},
  // --- 財務健全性 ---
  { key: "debt_to_equity_low",  label: "D/E 比率 (低)",           group: "analysis", dir: "asc",  unit: "ratio",    get: (r) => {
    // yfinance の debtToEquity は % 表記 (154.5 = 1.545x) で返るので 100 で割る。
    const v = r.info?.debtToEquity;
    return (typeof v === "number" && v >= 0) ? v / 100 : null;
  }},
  { key: "current_ratio",       label: "流動比率",                group: "analysis", dir: "desc", unit: "ratio",    get: (r) => {
    const v = r.info?.currentRatio;
    return (typeof v === "number" && v > 0) ? v : null;
  }},
  { key: "quick_ratio",         label: "当座比率",                group: "analysis", dir: "desc", unit: "ratio",    get: (r) => {
    const v = r.info?.quickRatio;
    return (typeof v === "number" && v > 0) ? v : null;
  }},
  // --- キャッシュフロー ---
  { key: "free_cashflow",       label: "フリーキャッシュフロー",  group: "analysis", dir: "desc", unit: "currency", get: (r) => {
    const v = r.info?.freeCashflow;
    return (typeof v === "number" && Number.isFinite(v)) ? v : null;
  }},
  { key: "fcf_margin",          label: "FCF マージン",            group: "analysis", dir: "desc", unit: "percent",  get: (r) => {
    const fcf = r.info?.freeCashflow, rev = r.info?.totalRevenue;
    return (typeof fcf === "number" && typeof rev === "number" && rev > 0) ? fcf / rev : null;
  }},
  // --- 株主構成 ---
  { key: "held_pct_institutions", label: "機関投資家保有率",      group: "analysis", dir: "desc", unit: "percent",  get: (r) => {
    const v = r.info?.heldPercentInstitutions;
    return (typeof v === "number" && v > 0 && v <= 1.5) ? v : null;
  }},
  { key: "held_pct_insiders",   label: "インサイダー保有率",      group: "analysis", dir: "desc", unit: "percent",  get: (r) => {
    const v = r.info?.heldPercentInsiders;
    return (typeof v === "number" && v >= 0 && v <= 1) ? v : null;
  }},
  { key: "short_pct_float",     label: "空売り比率 (浮動株比)",   group: "analysis", dir: "desc", unit: "percent",  get: (r) => {
    const v = r.info?.shortPercentOfFloat;
    return (typeof v === "number" && v >= 0) ? v : null;
  }},
  // --- 直近モメンタム ---
  { key: "earnings_growth",     label: "EPS 成長率 (YoY)",        group: "analysis", dir: "desc", unit: "percent",  get: (r) => {
    const v = r.info?.earningsGrowth;
    return (typeof v === "number" && Number.isFinite(v)) ? v : null;
  }},
  { key: "earnings_quarterly_growth", label: "EPS 成長率 (四半期 YoY)", group: "analysis", dir: "desc", unit: "percent", get: (r) => {
    const v = r.info?.earningsQuarterlyGrowth;
    return (typeof v === "number" && Number.isFinite(v)) ? v : null;
  }},
  // --- バリュエーション拡張 ---
  { key: "high_forward_pe",     label: "PER 予想 (高)",           group: "analysis", dir: "desc", unit: "ratio",    get: (r) => {
    const v = r.info?.forwardPE;
    return (typeof v === "number" && v > 0 && v < 1000) ? v : null;
  }},
  { key: "peg_ratio_low",       label: "PEG レシオ (低)",         group: "analysis", dir: "asc",  unit: "ratio",    get: (r) => {
    const v = r.info?.pegRatio ?? r.info?.trailingPegRatio;
    return (typeof v === "number" && v > 0 && v < 100) ? v : null;
  }},
  { key: "enterprise_to_revenue", label: "EV / Revenue (低)",     group: "analysis", dir: "asc",  unit: "ratio",    get: (r) => {
    const v = r.info?.enterpriseToRevenue;
    return (typeof v === "number" && v > 0) ? v : null;
  }},
  { key: "ebitda_margin",       label: "EBITDA マージン",         group: "analysis", dir: "desc", unit: "percent",  get: (r) => {
    const v = r.info?.ebitdaMargins;
    return (typeof v === "number" && Number.isFinite(v)) ? v : null;
  }},
  { key: "earnings_yield",      label: "Earnings Yield",          group: "analysis", dir: "desc", unit: "percent",  get: (r) => calcEarningsYield(r) },
  { key: "roic_proxy",          label: "ROIC (推定)",             group: "analysis", dir: "desc", unit: "percent",  get: (r) => calcROIC(r) },
  // --- 低ベータ (defensive) ---
  { key: "beta_low",            label: "ベータ (低)",             group: "facts",    dir: "asc",  unit: "ratio",    get: (r) => {
    const v = r.dcf_valuation?.wacc_details?.beta ?? r.info?.beta;
    return (typeof v === "number" && Number.isFinite(v) && v > 0) ? v : null;
  }},
  // --- パフォーマンス (history からの事前計算結果を使う) ---
  { key: "return_1y",           label: "1年リターン (年率)",      group: "facts",    dir: "desc", unit: "percent",  get: (r) => {
    const v = r._risk_return?.Ret_1Y;
    return (typeof v === "number" && Number.isFinite(v)) ? v : null;
  }},
  { key: "return_ytd",          label: "年初来リターン (年率)",   group: "facts",    dir: "desc", unit: "percent",  get: (r) => {
    const v = r._risk_return?.Ret_YTD;
    return (typeof v === "number" && Number.isFinite(v)) ? v : null;
  }},
  { key: "max_drawdown_1y",     label: "最大ドローダウン (1年)",  group: "facts",    dir: "desc", unit: "percent",  get: (r) => {
    const v = r._max_drawdown_1y;
    return (typeof v === "number" && Number.isFinite(v)) ? v : null;
  }},
  // --- コンポジット (per-symbol で計算できるもの) ---
  { key: "piotroski_score",     label: "Piotroski F-Score",       group: "analysis", dir: "desc", unit: "count",    get: (r) => calcPiotroskiScore(r) },
  { key: "dividend_stability",  label: "配当安定性スコア",        group: "analysis", dir: "desc", unit: "count",    get: (r) => calcDividendStability(r) },
];

// クロス銘柄の順位合成が必要なコンポジット指標。
// 各銘柄の per-metric ランクが揃ってから 2 パス目で計算するため、
// 通常の RANK_METRICS とは別ループで処理する。
// value: 表示する数値 (低いほど良い場合は asc、高いほど良いなら desc)。
// compose: (out, universe, sectorBySymbol) => { [sym]: number | null }
const COMPOSITE_METRICS = [
  {
    key: "magic_formula",
    label: "Magic Formula スコア",
    dir: "asc",
    unit: "count",
    // 値は「Earnings Yield のランク + ROIC のランク」の合計。低いほど両方上位。
    compose: (out, universe) => {
      const result = {};
      for (const sym of universe) {
        const ey = out[sym]?.earnings_yield?.overall?.rank;
        const roic = out[sym]?.roic_proxy?.overall?.rank;
        result[sym] = (ey && roic) ? (ey + roic) : null;
      }
      return result;
    },
  },
];

// 配列をソートして rank を付与する。同値は同順位 (1, 2, 2, 4 形式) ではなく
// 並び順そのまま (1, 2, 3, 4) でいい — 順位の細かい揺れより全体感が大事なので。
// 戻り値: { [symbol]: { rank, total, percentile } }  percentile は 0..1 の値、 0 が最上位。
function rankSymbols(entries, dir) {
  // entries: [{ symbol, value }]  (value は非 null 前提)
  const sorted = [...entries].sort((a, b) =>
    dir === "asc" ? a.value - b.value : b.value - a.value,
  );
  const total = sorted.length;
  const result = {};
  sorted.forEach((e, i) => {
    result[e.symbol] = {
      rank: i + 1,
      total,
      percentile: total > 1 ? i / (total - 1) : 0,
    };
  });
  return result;
}

// ランキング一覧ページで取り上げる指標。
// 銘柄レポート内には全 RANK_METRICS が保存されるが、一覧ページ用には
// 別ファイル (rankings.json) に上位 100 銘柄だけ抽出する。
const RANKING_PAGE_METRICS = [
  // 規模感
  "market_cap",
  "revenue_ttm",
  "employees",
  "total_cash",
  // 収益性
  "roe",
  "roa",
  "operating_margin",
  "gross_margin",
  // バリュエーション
  "forward_pe",
  "pbr",
  "psr",
  // 成長性
  "revenue_cagr_3y",
  "eps_9y_cagr",
  // 配当
  "dividend_yield",
  "dividend_rate",
  // 生産性
  "revenue_per_employee",
  "mcap_per_employee",
  // 株価・リスク
  "stock_price",
  "range_position_52w",
  "beta",
  // アナリスト評価
  "analyst_count",
  "target_upside",
  "analyst_buy_ratio",
  "recommendation_mean",
  "target_price_dispersion",
  "analyst_strong_buy_ratio",
  "analyst_bear_ratio",
  // 財務健全性
  "debt_to_equity_low",
  "current_ratio",
  "quick_ratio",
  // キャッシュフロー
  "free_cashflow",
  "fcf_margin",
  // 株主構成
  "held_pct_institutions",
  "held_pct_insiders",
  "short_pct_float",
  // 直近モメンタム
  "earnings_growth",
  "earnings_quarterly_growth",
  // バリュエーション拡張
  "high_forward_pe",
  "peg_ratio_low",
  "enterprise_to_revenue",
  "ebitda_margin",
  "earnings_yield",
  "roic_proxy",
  // 低ベータ
  "beta_low",
  // パフォーマンス
  "return_1y",
  "return_ytd",
  "max_drawdown_1y",
  // コンポジット
  "piotroski_score",
  "dividend_stability",
  "magic_formula",
];

// 同会社の複数株式クラスのうち、ランキングで非表示にする銘柄。
// yfinance.info.marketCap は会社全体の時価総額を両クラスに返すため、
// そのままだと「同じ会社が同じ値で 2 回」一覧に出る。代表クラスのみ残す。
const RANKING_DUPLICATE_CLASS_EXCLUDED = new Set([
  "GOOG",   // Alphabet Class C (議決権なし): GOOGL (Class A) を残す
  "NWS",    // News Corp Class B: NWSA (Class A) を残す
  "FOX",    // Fox Class B: FOXA (Class A) を残す
  "LBRDK",  // Liberty Broadband K: LBRDA (A) を残す
  "LSXMK",  // Liberty Media SiriusXM K: LSXMA (A) を残す
  "BATRK",  // Liberty Media Braves K: BATRA (A) を残す
  "FWONK",  // Liberty Media F1 K: FWONA (A) を残す
]);

// 各指標について上位 100 銘柄を抽出して { metric_key: { label, unit, top: [...] } } を返す。
// 表示に必要な最小限の情報 (シンボル、社名、セクター、値、順位) だけ含める。
function buildRankings(ranksBySymbol, metadataBySymbol) {
  const metricMap = Object.fromEntries([
    ...RANK_METRICS.map((m) => [m.key, m]),
    ...COMPOSITE_METRICS.map((m) => [m.key, m]),
  ]);
  const out = {};
  for (const key of RANKING_PAGE_METRICS) {
    const metric = metricMap[key];
    if (!metric) continue;
    const entries = [];
    for (const sym of Object.keys(ranksBySymbol)) {
      if (RANKING_DUPLICATE_CLASS_EXCLUDED.has(sym)) continue;
      const r = ranksBySymbol[sym]?.[key];
      if (!r || !r.overall || r.value == null) continue;
      const meta = metadataBySymbol[sym] || {};
      entries.push({
        symbol: meta.Symbol || sym,
        symbol_yf: sym,
        security: meta.Security || sym,
        security_ja: meta.Security_JA || null,
        sector: meta["GICS Sector"] || null,
        value: r.value,
        rank: r.overall.rank,
        total: r.overall.total,
      });
    }
    entries.sort((a, b) => a.rank - b.rank);
    // 除外銘柄により順位に欠番が生じるため、ランキング一覧表示用に
    // 連番 (1, 2, 3, ...) に振り直す。元の overall.rank は個別レポート側に残る。
    entries.forEach((e, i) => { e.rank = i + 1; e.total = entries.length; });
    out[key] = {
      key,
      label: metric.label,
      unit: metric.unit,
      dir: metric.dir,
      total: entries.length,
      top: entries.slice(0, 100),
    };
  }
  return out;
}

// すべての銘柄を横断して全指標のランキングを計算する。
// rawDataMap には ETF (SPY/XLK/...) も含まれるので S&P 1500 個別株のみを母集団にする。
// sectorBySymbol: 各銘柄の (正規化済) GICS Sector。Unknown はセクター順位の母集団から除外。
function computeRanks(rawDataMap, sectorBySymbol, isEtf) {
  const universe = Object.keys(rawDataMap).filter((s) => !isEtf(s));

  const out = {};
  for (const sym of universe) out[sym] = {};

  for (const metric of RANK_METRICS) {
    // (a) 全体順位
    const overallEntries = [];
    for (const sym of universe) {
      const v = metric.get(rawDataMap[sym]);
      if (typeof v === "number" && Number.isFinite(v)) {
        overallEntries.push({ symbol: sym, value: v });
      }
    }
    const overallRanks = rankSymbols(overallEntries, metric.dir);

    // (b) セクター順位 (セクターごとに sort)
    const bySector = {};
    for (const e of overallEntries) {
      const sec = sectorBySymbol[e.symbol];
      if (!sec || sec === "Unknown") continue;
      (bySector[sec] = bySector[sec] || []).push(e);
    }
    const sectorRanks = {};
    for (const [sec, entries] of Object.entries(bySector)) {
      const ranked = rankSymbols(entries, metric.dir);
      for (const sym of Object.keys(ranked)) {
        sectorRanks[sym] = { ...ranked[sym], sector: sec };
      }
    }

    for (const sym of universe) {
      const value = metric.get(rawDataMap[sym]);
      out[sym][metric.key] = {
        value: typeof value === "number" && Number.isFinite(value) ? value : null,
        overall: overallRanks[sym] || null,
        sector: sectorRanks[sym]
          ? {
              rank: sectorRanks[sym].rank,
              total: sectorRanks[sym].total,
              percentile: sectorRanks[sym].percentile,
            }
          : null,
      };
    }
  }

  // コンポジット指標 (Magic Formula 等) は他指標のランクを参照するため
  // 通常の per-metric ループが終わった後に 2 パス目で計算する。
  for (const composite of COMPOSITE_METRICS) {
    const valueBySymbol = composite.compose(out, universe, sectorBySymbol);
    const overallEntries = [];
    for (const sym of universe) {
      const v = valueBySymbol[sym];
      if (typeof v === "number" && Number.isFinite(v)) {
        overallEntries.push({ symbol: sym, value: v });
      }
    }
    const overallRanks = rankSymbols(overallEntries, composite.dir);
    const bySector = {};
    for (const e of overallEntries) {
      const sec = sectorBySymbol[e.symbol];
      if (!sec || sec === "Unknown") continue;
      (bySector[sec] = bySector[sec] || []).push(e);
    }
    const sectorRanks = {};
    for (const [sec, entries] of Object.entries(bySector)) {
      const ranked = rankSymbols(entries, composite.dir);
      for (const sym of Object.keys(ranked)) {
        sectorRanks[sym] = { ...ranked[sym], sector: sec };
      }
    }
    for (const sym of universe) {
      const v = valueBySymbol[sym];
      out[sym][composite.key] = {
        value: typeof v === "number" && Number.isFinite(v) ? v : null,
        overall: overallRanks[sym] || null,
        sector: sectorRanks[sym]
          ? {
              rank: sectorRanks[sym].rank,
              total: sectorRanks[sym].total,
              percentile: sectorRanks[sym].percentile,
            }
          : null,
      };
    }
  }
  return out;
}

// === main ===

async function main() {
  const t0 = Date.now();
  if (LOCAL_MODE) {
    console.log(`mode=LOCAL raw=${LOCAL_RAW_DIR} reports=${LOCAL_REPORTS_DIR}`);
  } else {
    console.log(`mode=R2 bucket=${BUCKET} concurrency=${CONCURRENCY}`);
  }
  console.log("Listing raw/ keys...");
  let rawKeys = (await listAll("raw/")).filter(
    (k) =>
      k.endsWith(".json") &&
      k !== "raw/stocks_list.json" &&
      k !== "raw/broker_availability.json",
  );
  console.log(`  found ${rawKeys.length} raw files`);

  let baseStocksList = [];
  try {
    baseStocksList = await getJson("raw/stocks_list.json");
  } catch {
    console.log("  raw/stocks_list.json not found, using empty metadata");
  }

  // S&P 入れ替えで「除外」された銘柄の残骸 (raw/reports) を削除し、
  // 以降の処理対象 (rawKeys) からも外す。
  const prunedSymbols = await pruneRemovedSymbols(rawKeys, baseStocksList);
  if (prunedSymbols.size > 0) {
    rawKeys = rawKeys.filter(
      (k) => !prunedSymbols.has(k.slice("raw/".length).replace(/\.json$/, "")),
    );
  }

  // risk-return チャートの "その他銘柄" は対象銘柄が属する指数の構成銘柄に
  // 限定する。stocks_list.json の Index 列で S&P 500/400/600 ごとの集合を作る。
  const indexSymbolSets = {};
  for (const s of baseStocksList) {
    const idx = s.Index;
    const sym = s.Symbol_YF || s.Symbol;
    if (!idx || !sym) continue;
    (indexSymbolSets[idx] ??= new Set()).add(sym);
  }

  // 指数構成銘柄の入れ替えを検出して変更履歴に記録する。
  // 失敗してもレポート生成は止めない。
  try {
    await recordIndexChanges(baseStocksList);
  } catch (e) {
    console.error(`  change history update failed (ignored): ${e.message}`);
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

  // 日本の証券会社の取扱銘柄リスト (main.py がアップロード)。
  // broker 名 -> 取扱シンボルの Set。見つからない場合は全銘柄「取扱なし」扱い。
  const brokerAvailability = {};
  try {
    console.log("Downloading raw/broker_availability.json...");
    const rawBrokers = await getJson("raw/broker_availability.json");
    for (const [name, syms] of Object.entries(rawBrokers || {})) {
      brokerAvailability[name] = new Set(Array.isArray(syms) ? syms : []);
    }
    const counts = Object.entries(brokerAvailability)
      .map(([n, s]) => `${n}:${s.size}`)
      .join(" ");
    console.log(`  broker availability loaded (${counts})`);
  } catch (e) {
    console.log(
      "  broker_availability.json not found — is_available_* will all be false.",
    );
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
  const sectorBySymbol = {}; // computeRanks 用
  for (const symbol of Object.keys(rawDataMap)) {
    const rawData = rawDataMap[symbol];
    const rr = calculateRiskReturn(rawData.history, symbol);
    if (rr) {
      riskReturnMetrics.push(rr);
      // ランキング計算 (computeRanks) で参照するため rawData に貼り付ける。
      // 後段の個別レポート生成にも影響しない (rr は読み取り専用扱い)。
      rawData._risk_return = rr;
    }
    rawData._max_drawdown_1y = calculateMaxDrawdown(rawData.history, 252);
    const metadata =
      baseStocksList.find(
        (s) => s.Symbol_YF === symbol || s.Symbol === symbol,
      ) || {};
    const sector =
      metadata["GICS Sector"] || normalizeSector(rawData.info?.sector) || "Unknown";
    const subInd =
      metadata["GICS Sub-Industry"] || rawData.info?.industry || "Unknown";
    sectorBySymbol[symbol] = sector;
    const dailyChange = calculateDailyChange(rawData.history);
    const peerInfo = {
      Symbol: metadata.Symbol || symbol,
      Symbol_YF: symbol,
      // 同セクター他社のフィルタ用に各銘柄の所属指数を持たせる。
      // metadata が無い銘柄 (ETF など) は null。
      Index: metadata["Index"] || null,
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
    "MDY",
    "IJR",
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
    // Vanguard セクター ETF（中小含む）
    "VOX",
    "VCR",
    "VDC",
    "VDE",
    "VFH",
    "VHT",
    "VIS",
    "VGT",
    "VAW",
    "VNQ",
    "VPU",
    "ITB",
  ];

  // 全銘柄を横断したランキングを 1 回だけ計算 (大したコストにはならない)。
  console.log("Computing ranks (overall + sector)...");
  const isEtf = (s) => ETFS.includes(s);
  const ranksBySymbol = computeRanks(rawDataMap, sectorBySymbol, isEtf);

  // ランキング一覧ページ用のメタデータマップだけここで構築し、
  // 実際の rankings.json 書き出しはレポート生成後に行う。
  // 書き出しを最後にずらすことで R2 書込みの transient error が起きても
  // 個別レポート / stocks.json の更新が阻害されないようにする。
  const metadataBySymbol = {};
  for (const s of baseStocksList) {
    if (s.Symbol_YF) metadataBySymbol[s.Symbol_YF] = s;
    if (s.Symbol && !metadataBySymbol[s.Symbol]) metadataBySymbol[s.Symbol] = s;
  }

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
  } else if (LOCAL_MODE) {
    // LOCAL_MODE のデフォルトは raw_data に存在する全銘柄を処理。
    // ローカルでは fetch 済みの少数銘柄しか raw に無いことが多いので、
    // 「揃ってる分は全部レポート出す」が直感的。
    symbols = Object.keys(rawDataMap);
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
          metadata["GICS Sector"] || normalizeSector(rawData.info?.sector),
          metadata["GICS Sub-Industry"] || rawData.info?.industry,
        );
        const broadSectorEtf = broadSectorEtfMap[metadata["GICS Sub-Industry"]] ||
          broadSectorEtfMap[metadata["GICS Sector"]] ||
          broadSectorEtfMap[rawData.info?.industry] ||
          broadSectorEtfMap[normalizeSector(rawData.info?.sector)] ||
          'SPY';
        // 対象銘柄が属する指数の ETF (S&P 400→MDY / S&P 600→IJR / 既定 SPY)。
        const marketIndexEtf = marketIndexMap[metadata["Index"]] || 'SPY';
        // "その他銘柄" は対象銘柄が属する指数の構成銘柄に限定する
        // (S&P 400 銘柄なら S&P 400 銘柄をプロットする)。指数不明時は S&P 500。
        const peerIndexSet =
          indexSymbolSets[metadata["Index"]] ||
          indexSymbolSets["S&P 500"] ||
          null;
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
        const fcfMetrics = extractFcfMetrics(rawData);
        const dividendStreak = extractDividendStreak(rawData.dividends || []);
        const shortInterest = extractShortInterest(rawData);
        const insiderTransactions = extractInsiderTransactions(rawData);
        const institutionalOwnership = extractInstitutionalOwnership(rawData);
        const esgScores = extractEsgScores(rawData);

        const riskReturnChart = generateRiskReturnChart(
          riskReturnMetrics,
          symbol,
          sectorEtf,
          broadSectorEtf,
          marketIndexEtf,
          peerIndexSet,
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
        const segmentChart = generateSegmentChart(rawData.revenue_by_segment, 'segment',   'セグメント収益');
        const geoChart     = generateSegmentChart(rawData.revenue_by_geography, 'geography', '地域別収益');

        const sector =
          metadata["GICS Sector"] || normalizeSector(rawData.info?.sector) || "Unknown";
        const subInd =
          metadata["GICS Sub-Industry"] || rawData.info?.industry || "Unknown";

        let summary_ja = translations[symbol]?.business_summary_ja || translations[symbol] || null;
        if (summary_ja) {
          summary_ja = formatSummary(summary_ja);
        }

        // 証券会社の取扱判定は表示用シンボル (BRK.B 等) で行う。
        const brokerCheckSymbol = metadata.Symbol || symbol;

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
          index: metadata["Index"] || null,
          exchange: toTradingViewExchange(rawData.info?.exchange),
          full_symbol: `${toTradingViewExchange(rawData.info?.exchange)}:${symbol.replace("-", ".")}`,
          sector_etf: sectorEtf,
          is_financial: ["Financials", "Real Estate"].includes(sector),
          
          benchmark_info: getBenchmarkInfo(metadata, rawData.info),

          // 全体 / セクター内ランキング (computeRanks の前計算結果)
          ranks: {
            sector: sectorBySymbol[symbol] || null,
            metrics: ranksBySymbol[symbol] || {},
          },

          // 日本の証券会社での購入可否。broker_availability.json の取扱銘柄
          // リストと照合する。表示シンボル優先、Symbol_YF もフォールバックとして確認
          // (BNY/BK 等のティッカー変更後も旧ティッカーで登録されている証券会社に対応)。
          is_available_monex: isAvailableAt(brokerAvailability.monex, brokerCheckSymbol, symbol),
          is_available_rakuten: isAvailableAt(brokerAvailability.rakuten, brokerCheckSymbol, symbol),
          is_available_sbi: isAvailableAt(brokerAvailability.sbi, brokerCheckSymbol, symbol),
          is_available_mufg: isAvailableAt(brokerAvailability.mufg, brokerCheckSymbol, symbol),
          is_available_matsui: isAvailableAt(brokerAvailability.matsui, brokerCheckSymbol, symbol),
          is_available_dmm: isAvailableAt(brokerAvailability.dmm, brokerCheckSymbol, symbol),
          is_available_paypay: isAvailableAt(brokerAvailability.paypay, brokerCheckSymbol, symbol),
          is_available_moomoo: isAvailableAt(brokerAvailability.moomoo, brokerCheckSymbol, symbol),
          is_available_iwaicosmo: isAvailableAt(brokerAvailability.iwaicosmo, brokerCheckSymbol, symbol),
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
          peers: (() => {
            // 同業種・競合 (sub_industry): S&P 1500 全銘柄から抽出 (指数フィルタなし)
            // 同セクター他社 (sector): 対象銘柄と同じ指数 (S&P 500/400/600) に限定。
            //   対象の指数が未取得の場合はフィルタを掛けない (全て表示)。
            const targetIndex = metadata["Index"] || null;
            const sectorPool = sectorMap[sector] || [];
            const sectorFiltered = targetIndex
              ? sectorPool.filter((s) => s.Index === targetIndex)
              : sectorPool;
            return {
              sub_industry: (subIndustryMap[subInd] || []).filter(
                (s) => s.Symbol_YF !== symbol,
              ),
              sector: sectorFiltered.filter(
                (s) =>
                  s.Symbol_YF !== symbol &&
                  !subIndustryMap[subInd]?.find((si) => si.Symbol_YF === s.Symbol_YF),
              ),
            };
          })(),
          dcf_valuation: rawData.dcf_valuation || null,
          fcf_metrics: fcfMetrics,
          dividend_streak: dividendStreak,
          short_interest: shortInterest,
          insider_transactions: insiderTransactions,
          institutional_ownership: institutionalOwnership,
          esg_scores: esgScores,
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
            // metadata が無い (= base stocks list に未登録) 個別銘柄でも
            // A-Z 一覧や検索が動くよう、最低限 Symbol を Symbol_YF から補完。
            Symbol: metadata.Symbol || symbol,
            Symbol_YF: symbol,
            Daily_Change: calculateDailyChange(rawData.history),
            Has_Movement_Reason: !!movementReasons[symbol],
          });
        }
        // メモリ解放: 1532銘柄分の raw JSON を保持し続けると 4GB を超えて
        // OOM になるため、処理済みの非 ETF を rawDataMap から外す。
        // ETF (SPY, XLK 等) は他銘柄の performance chart で参照されるため残す。
        if (!isETF) {
          delete rawDataMap[symbol];
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
    let finalList = updatedStocksList;
    // LOCAL_MODE では reports/stocks.json と src/data/stocks.json が同じファイル
    // を指す。一部の銘柄しか処理しない (TEST_SYMBOLS) ローカル運用で全エントリが
    // 失われないよう、既存のリストにマージする。
    if (LOCAL_MODE) {
      let existing = [];
      try {
        existing = await getJson("reports/stocks.json");
      } catch {
        existing = [];
      }
      const updatedKey = new Set(
        updatedStocksList.map((s) => s.Symbol_YF || s.Symbol),
      );
      const merged = existing.filter(
        (s) => !updatedKey.has(s.Symbol_YF || s.Symbol),
      );
      finalList = [...merged, ...updatedStocksList];
    } else {
      // R2 モード: stocks_list.json にいて raw データなし (fetch 失敗・新規追加) の
      // 銘柄も A-Z 一覧・カウントに含める。メタデータのみで Daily_Change は null。
      const processedSymbols = new Set(
        updatedStocksList.map((s) => s.Symbol_YF || s.Symbol),
      );
      const etfSet = new Set([
        ...Object.values(sectorEtfMap),
        ...Object.values(broadSectorEtfMap),
        ...Object.values(marketIndexMap),
      ]);
      const metadataOnly = baseStocksList.filter((s) => {
        const sym = s.Symbol_YF || s.Symbol;
        return sym && !processedSymbols.has(sym) && !etfSet.has(sym);
      });
      if (metadataOnly.length > 0) {
        console.log(
          `  Adding ${metadataOnly.length} metadata-only stocks (no raw data) to stocks.json`,
        );
        finalList = [...updatedStocksList, ...metadataOnly];
      }
    }
    await putJson("reports/stocks.json", finalList);
    console.log(
      `Saved reports/stocks.json with ${finalList.length} items.`,
    );
  }

  // ランキング一覧ページ用 rankings.json は重要度が低いので、失敗しても
  // パイプライン全体を落とさず警告だけ出して続行する。
  try {
    const rankings = buildRankings(ranksBySymbol, metadataBySymbol);
    await putJson("reports/rankings.json", rankings);
    console.log(`Saved reports/rankings.json (${Object.keys(rankings).length} metrics).`);
  } catch (e) {
    console.error(`rankings.json write failed (ignored): ${e?.message || e}`);
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`完了: reports=${symbols.length - putFails}, failed=${putFails}, elapsed=${elapsed}s`);
}

main().catch((e) => {
  console.error("FATAL:", e);
  process.exit(1);
});
