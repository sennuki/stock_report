#!/usr/bin/env node
/**
 * 株価が大きく変動した銘柄の理由を生成し reports/movement_reasons.json に保存する。
 *
 * 選定 (zスコア):
 *   S&P 500 / 400 / 600 全体で、当日の対数リターンを銘柄ごとの過去ボラティリティで
 *   標準化した z スコアを計算し、|z| が大きい順に上位 TARGET 銘柄 (既定 20) を選ぶ。
 *   生の変動率でランクすると「元々ボラの高い小型株」が常連化してしまうため、
 *   「その銘柄にとって異例の動き」を |z| で抽出する。過去ボラは当日リターンを母数
 *   から除いた直近 VOL_WINDOW 営業日の標準偏差。
 *
 * 理由生成:
 *   選定銘柄ごとに gemini-2.5-flash-lite を Google 検索グラウンディング付きで呼び、
 *   変動理由を日本語で生成する。
 *
 * API 予算:
 *   gemini-2.5-flash-lite の Google 検索グラウンディングは無料枠で 1 日あたりの
 *   利用回数が少ない (約 20 回)。API リクエスト総数を API_BUDGET で頭打ちにし、
 *   超過 (429) を防ぐ。TARGET = API_BUDGET = 20 を既定とし、1 銘柄 1 リクエストで
 *   ちょうど使い切る。リトライもこの予算を消費し、予算が尽きたら打ち切る。
 *
 * 出力 (reports/movement_reasons.json):
 *   { generated_date, generated_at, model,
 *     reasons: { SYMBOL: { date, change_pct, z, index, reason } } }
 *   generate-reports.mjs がこれを読み reports/{symbol}.json の movement_reason に流す。
 *
 * 同日スキップ:
 *   generated_date が当日のファイルが既にあれば何もしない。データ取得パイプラインは
 *   1 日に複数回 (master への push ごとに) 走るため、API 予算の二重消費を防ぐ。
 *   FORCE_MOVEMENT_REASONS=true で強制再生成。
 */

import "dotenv/config";
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  ListObjectsV2Command,
} from "@aws-sdk/client-s3";
import pMap from "p-map";

const BUCKET = process.env.R2_BUCKET_NAME || "stock-data-c1";
const OUTPUT_KEY =
  process.env.MOVEMENT_REASONS_KEY || "reports/movement_reasons.json";
const MODEL = (
  process.env.MOVEMENT_REASON_MODEL || "gemini-2.5-flash-lite"
).replace(/^models\//, "");
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
// Gemini 無料枠は 15 RPM。安全マージンを取って既定 13 RPM。
const GEMINI_RPM = Math.max(1, Number.parseInt(process.env.GEMINI_RPM || "13", 10));
// |z| 上位で理由を生成する銘柄数。
const TARGET = Math.max(1, Number.parseInt(process.env.MOVEMENT_TARGET || "20", 10));
// API リクエスト総数の上限。グラウンディング無料枠 (約 20 回/日) に合わせる。
// リトライもこの予算を消費し、尽きたら打ち切る。
const API_BUDGET = Math.max(
  1,
  Number.parseInt(process.env.MOVEMENT_API_BUDGET || "20", 10),
);
// 1 銘柄あたりの最大試行回数 (各試行が API_BUDGET を 1 消費する)。
const MAX_ATTEMPTS = Math.max(
  1,
  Number.parseInt(process.env.MOVEMENT_MAX_ATTEMPTS || "2", 10),
);
// z 計算用の過去ボラの窓 (営業日数)。当日リターンは母数から除外する。
const VOL_WINDOW = Math.max(
  20,
  Number.parseInt(process.env.MOVEMENT_VOL_WINDOW || "60", 10),
);
// z を計算するのに最低限必要な過去リターン本数。
const MIN_RETURNS = 20;
// ユニバースがこれ未満なら (テスト実行とみなし) 何もしない。
const MIN_UNIVERSE = Math.max(
  0,
  Number.parseInt(process.env.MOVEMENT_MIN_UNIVERSE || "300", 10),
);
const FORCE = process.env.FORCE_MOVEMENT_REASONS === "true";
const DOWNLOAD_CONCURRENCY = 20;

if (!GEMINI_API_KEY) {
  console.error("GEMINI_API_KEY is required.");
  process.exit(1);
}
for (const v of ["R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"]) {
  if (!process.env[v]) {
    console.error(`${v} is required.`);
    process.exit(1);
  }
}

const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  },
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function listAll(prefix) {
  const keys = [];
  let token;
  do {
    const res = await s3.send(
      new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: prefix,
        ContinuationToken: token,
      }),
    );
    if (res.Contents) keys.push(...res.Contents.map((c) => c.Key));
    token = res.NextContinuationToken;
  } while (token);
  return keys;
}

async function getJson(key) {
  const res = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
  const body = await res.Body.transformToString();
  return JSON.parse(
    body.replace(/\bNaN\b/g, "null").replace(/\b-?Infinity\b/g, "null"),
  );
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

function todayIso() {
  return new Date().toISOString().slice(0, 10);
}

// history ({Date|index, Close}[]) から当日リターン・過去ボラ・z スコアを計算する。
// データが不足している銘柄は null を返し、ランキング対象から外す。
function computeMove(history) {
  if (!Array.isArray(history) || history.length < MIN_RETURNS + 2) return null;
  const rows = history
    .map((h) => ({
      date: String(h.Date || h.index || "").split(" ")[0].split("T")[0],
      close: Number(h.Close),
    }))
    .filter((r) => r.date && Number.isFinite(r.close) && r.close > 0)
    .sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));
  if (rows.length < MIN_RETURNS + 2) return null;

  const logReturns = [];
  for (let i = 1; i < rows.length; i++) {
    logReturns.push(Math.log(rows[i].close / rows[i - 1].close));
  }
  const lastReturn = logReturns[logReturns.length - 1];
  // 当日リターンを母数から除外した過去ボラ (標本標準偏差)。
  const window = logReturns.slice(-1 - VOL_WINDOW, -1);
  if (window.length < MIN_RETURNS) return null;
  const mean = window.reduce((s, x) => s + x, 0) / window.length;
  const variance =
    window.reduce((s, x) => s + (x - mean) ** 2, 0) / (window.length - 1);
  const hv = Math.sqrt(variance);
  if (!Number.isFinite(hv) || hv <= 1e-6) return null;
  const z = lastReturn / hv;
  if (!Number.isFinite(z)) return null;

  const last = rows[rows.length - 1];
  const prev = rows[rows.length - 2];
  return { date: last.date, change_pct: (last.close - prev.close) / prev.close, z };
}

// Gemini リクエストを GEMINI_RPM 以内に平準化する。
let lastRequestAt = 0;
async function paceGemini() {
  const minIntervalMs = Math.ceil(60000 / GEMINI_RPM);
  const wait = lastRequestAt + minIntervalMs - Date.now();
  if (wait > 0) await sleep(wait);
  lastRequestAt = Date.now();
}

function buildPrompt({ symbol, name, date, changePct, indexLabel }) {
  const dir = changePct >= 0 ? "上昇" : "下落";
  const pct = Math.abs(changePct * 100).toFixed(2);
  return [
    "あなたは証券アナリストです。投資判断や売買の推奨は一切行いません。",
    "",
    `${name}（${symbol}、${indexLabel}構成銘柄）の株価は ${date} に前日比 約${pct}% ${dir}しました。`,
    "Google 検索で最新のニュース・開示情報を調べ、この株価変動の主な理由を日本語で説明してください。",
    "",
    "条件:",
    "- 2〜3文程度で簡潔に。",
    "- 決算・業績ガイダンス・M&A・規制・アナリスト格付け変更・マクロ要因など、具体的な事実に基づくこと。",
    "- 確証が得られない場合は「〜とみられる」など断定を避ける。",
    "- 個別の材料が見当たらない場合は、地合いやセクター全体の動きによる変動である旨を述べる。",
    "- 投資の推奨・助言はしない。",
    "- 回答は理由の説明文のみ。前置き・見出し・箇条書きは不要。",
  ].join("\n");
}

// API リクエストの累計。API_BUDGET を超えないよう全リクエストでカウントする。
let apiRequestsUsed = 0;

class RateLimitError extends Error {}

// gemini-2.5-flash-lite を Google 検索グラウンディング付きで呼び、理由テキストを返す。
// 各 fetch が API_BUDGET を 1 消費する。予算が尽きたら BudgetExhausted を投げる。
async function generateReason(meta) {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
    MODEL,
  )}:generateContent?key=${GEMINI_API_KEY}`;
  const payload = {
    contents: [{ role: "user", parts: [{ text: buildPrompt(meta) }] }],
    tools: [{ google_search: {} }],
    generationConfig: { temperature: 0.3 },
  };
  const opts = {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  };

  let lastErr = null;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    if (apiRequestsUsed >= API_BUDGET) {
      throw lastErr || new Error("API budget exhausted before request.");
    }
    await paceGemini();
    apiRequestsUsed += 1;

    let res;
    try {
      res = await fetch(url, opts);
    } catch (e) {
      lastErr = e;
      if (attempt < MAX_ATTEMPTS && apiRequestsUsed < API_BUDGET) {
        await sleep(2000 * attempt + Math.random() * 1000);
        continue;
      }
      throw e;
    }

    if (res.ok) {
      const data = await res.json();
      const text = (data.candidates?.[0]?.content?.parts || [])
        .map((p) => p.text || "")
        .join("")
        .trim();
      if (!text) throw new Error("Gemini returned empty text.");
      return text
        .replace(/^```(?:json|text)?/i, "")
        .replace(/```$/i, "")
        .trim();
    }

    const body = await res.text();
    // 429: 予算超過。これ以上叩いても無駄なので呼び出し側に伝えて全体を打ち切る。
    if (res.status === 429) {
      throw new RateLimitError(`Gemini API 429: ${body.slice(0, 300)}`);
    }
    // 5xx: 過渡的エラー。予算が残っていれば 1 回だけ間を置いて再試行する。
    lastErr = new Error(`Gemini API ${res.status}: ${body.slice(0, 300)}`);
    lastErr.status = res.status;
    if (res.status >= 500 && attempt < MAX_ATTEMPTS && apiRequestsUsed < API_BUDGET) {
      await sleep(3000 * attempt + Math.random() * 2000);
      continue;
    }
    throw lastErr;
  }
  throw lastErr || new Error("unreachable");
}

async function main() {
  console.log(
    `bucket=${BUCKET} model=${MODEL} target=${TARGET} apiBudget=${API_BUDGET} ` +
      `maxAttempts=${MAX_ATTEMPTS} volWindow=${VOL_WINDOW} rpm=${GEMINI_RPM}`,
  );

  // 既存ファイルを読む (同日スキップ判定 + 旧→新の差分で「今回外れた銘柄」を特定)。
  let existing = null;
  try {
    existing = await getJson(OUTPUT_KEY);
  } catch {
    // ファイルが無ければ生成する。
  }
  if (!FORCE && existing?.generated_date === todayIso()) {
    const n = Object.keys(existing.reasons || {}).length;
    console.log(
      `${OUTPUT_KEY} already generated today (${n} reasons). Skipping. ` +
        `Set FORCE_MOVEMENT_REASONS=true (workflow_dispatch の force) to regenerate.`,
    );
    return;
  }
  const prevSymbols = new Set(Object.keys(existing?.reasons || {}));

  // ユニバース (Index・社名付き) を取得。
  let stocksList;
  try {
    stocksList = await getJson("raw/stocks_list.json");
  } catch {
    console.error(
      "raw/stocks_list.json not found; cannot determine index membership. Aborting.",
    );
    return;
  }
  const indexBySymbol = {};
  const nameBySymbol = {};
  for (const s of stocksList) {
    const sym = s.Symbol_YF || s.Symbol;
    if (!sym || !s.Index) continue;
    indexBySymbol[sym] = s.Index;
    nameBySymbol[sym] = s.Security || sym;
  }

  const rawKeys = (await listAll("raw/")).filter(
    (k) =>
      k.endsWith(".json") &&
      k !== "raw/stocks_list.json" &&
      k !== "raw/broker_availability.json",
  );
  if (rawKeys.length < MIN_UNIVERSE) {
    console.log(
      `universe too small (${rawKeys.length} < ${MIN_UNIVERSE}); likely a test run. Skipping.`,
    );
    return;
  }
  console.log(`Computing z-scores for ${rawKeys.length} symbols...`);

  // 各銘柄の z スコアを計算 (Index 付きの銘柄のみ。ETF / 指数は除外)。
  const candidates = [];
  await pMap(
    rawKeys,
    async (key) => {
      const sym = key.slice("raw/".length).replace(/\.json$/, "");
      const indexLabel = indexBySymbol[sym];
      if (!indexLabel) return;
      let raw;
      try {
        raw = await getJson(key);
      } catch {
        return;
      }
      const move = computeMove(raw.history);
      if (!move) return;
      candidates.push({
        symbol: sym,
        name: nameBySymbol[sym] || raw.info?.longName || sym,
        index: indexLabel,
        ...move,
      });
    },
    { concurrency: DOWNLOAD_CONCURRENCY },
  );
  console.log(`${candidates.length} symbols have a valid z-score.`);

  // 全指数横断で |z| が大きい順に上位 TARGET 銘柄を選定。
  const targets = [...candidates]
    .sort((a, b) => Math.abs(b.z) - Math.abs(a.z))
    .slice(0, TARGET);
  for (const idx of ["S&P 500", "S&P 400", "S&P 600"]) {
    console.log(`  selected from ${idx}: ${targets.filter((t) => t.index === idx).length}`);
  }
  console.log(
    `Generating reasons for ${targets.length} symbols via ${MODEL} ` +
      `(API budget ${API_BUDGET})...`,
  );

  const reasons = {};
  let ok = 0;
  let failed = 0;
  let stopped = false;
  for (const t of targets) {
    if (apiRequestsUsed >= API_BUDGET) {
      console.log(
        `API budget (${API_BUDGET}) reached; ${targets.length - ok - failed} ` +
          `symbol(s) left unprocessed.`,
      );
      break;
    }
    try {
      const reason = await generateReason({
        symbol: t.symbol,
        name: t.name,
        date: t.date,
        changePct: t.change_pct,
        indexLabel: t.index,
      });
      reasons[t.symbol] = {
        date: t.date,
        change_pct: t.change_pct,
        z: Number(t.z.toFixed(3)),
        index: t.index,
        reason,
      };
      ok += 1;
      console.log(
        `[${t.symbol}] ${t.index} z=${t.z.toFixed(2)} ` +
          `change=${(t.change_pct * 100).toFixed(2)}% ok ` +
          `(${ok}/${targets.length}, api=${apiRequestsUsed}/${API_BUDGET})`,
      );
    } catch (e) {
      failed += 1;
      console.error(
        `[${t.symbol}] reason generation failed: ${String(e?.message || e)}`,
      );
      if (e instanceof RateLimitError) {
        console.log("Rate limited (429); stopping this run.");
        stopped = true;
        break;
      }
    }
  }

  if (ok === 0) {
    console.error(
      "No reasons were generated; leaving the existing file untouched so the next run can retry.",
    );
    process.exitCode = 1;
    return;
  }

  await putJson(OUTPUT_KEY, {
    generated_date: todayIso(),
    generated_at: new Date().toISOString(),
    model: MODEL,
    reasons,
  });
  console.log(
    `Saved ${OUTPUT_KEY}: ok=${ok} failed=${failed} ` +
      `api_requests=${apiRequestsUsed}${stopped ? " (stopped early)" : ""}`,
  );

  // 次の generate-reports.mjs 実行を待たずにサイトへ即時反映するため、
  // reports/{symbol}.json の movement_reason を直接書き換える。
  // 今回選定から外れた前回銘柄は null に戻す (古い理由を残さない)。
  await patchReports(reasons, prevSymbols);
}

// 選定銘柄の reports/{symbol}.json に movement_reason を直接反映する。
async function patchReports(reasons, prevSymbols) {
  const newSymbols = new Set(Object.keys(reasons));
  const ops = [
    ...Object.entries(reasons).map(([sym, value]) => ({ sym, value })),
    ...[...prevSymbols]
      .filter((sym) => !newSymbols.has(sym))
      .map((sym) => ({ sym, value: null })),
  ];
  if (ops.length === 0) return;

  let patched = 0;
  let cleared = 0;
  let patchFailed = 0;
  await pMap(
    ops,
    async ({ sym, value }) => {
      try {
        const report = await getJson(`reports/${sym}.json`);
        report.movement_reason = value;
        await putJson(`reports/${sym}.json`, report);
        if (value === null) cleared += 1;
        else patched += 1;
      } catch (e) {
        // レポートが未生成の銘柄などは generate-reports.mjs 側で
        // movement_reasons.json から反映されるためスキップでよい。
        patchFailed += 1;
        console.error(
          `[${sym}] report patch failed: ${String(e?.message || e)}`,
        );
      }
    },
    { concurrency: 10 },
  );
  console.log(
    `Patched reports: set=${patched} cleared=${cleared} failed=${patchFailed}`,
  );
}

main().catch((e) => {
  console.error("FATAL:", e);
  process.exit(1);
});
