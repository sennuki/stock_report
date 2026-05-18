#!/usr/bin/env node
/**
 * Translate company business summaries independently from report generation.
 *
 * 動作:
 *   - raw/{symbol}.json の英文 (info.longBusinessSummary) を翻訳し
 *     translations/business_summaries.json (R2) に保存する。
 *   - Pass 1: 未翻訳の銘柄を翻訳する (TRANSLATION_LIMIT 件まで)。
 *   - Pass 2: 翻訳済みでも translation_date が REFRESH_AFTER_DAYS より
 *     古いものを「再チェック対象」とし、古い順に REFRESH_LIMIT 件まで処理。
 *     英文ソースのハッシュ (source_hash) が前回と異なる場合のみ再翻訳し、
 *     同じ場合は translation_date だけ更新する (Gemini API を呼ばない)。
 *     REFRESH_AFTER_DAYS=365 なら全銘柄が約 1 年で 1 周する。
 *   - Gemini 無料枠の 15 RPM 制限に合わせ GEMINI_RPM でリクエストを平準化。
 *
 * 翻訳ストアの 1 エントリ:
 *   { symbol, business_summary_ja, translation_date, source_hash }
 */

import "dotenv/config";
import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";

const BUCKET = process.env.R2_BUCKET_NAME || "stock-data-c1";
const LIMIT = Number.parseInt(process.env.TRANSLATION_LIMIT || "50", 10);
const REFRESH_LIMIT = Number.parseInt(process.env.REFRESH_LIMIT || "50", 10);
const REFRESH_AFTER_DAYS = Number.parseInt(
  process.env.REFRESH_AFTER_DAYS || "365",
  10,
);
// Gemini 無料枠は 15 RPM。安全マージンを取って既定 13 RPM。
const GEMINI_RPM = Math.max(1, Number.parseInt(process.env.GEMINI_RPM || "13", 10));
const TRANSLATIONS_KEY =
  process.env.BUSINESS_SUMMARY_TRANSLATIONS_KEY ||
  "translations/business_summaries.json";
const SUMMARY_OUTPUT_PATH = process.env.TRANSLATION_SUMMARY_OUTPUT;
const GEMINI_MODEL = (
  process.env.GEMINI_TRANSLATION_MODEL || "gemini-3.1-flash-lite-preview"
).replace(/^models\//, "");
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

if (!GEMINI_API_KEY) {
  console.error("GEMINI_API_KEY is required.");
  process.exit(1);
}

const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  },
});

async function listAll(prefix) {
  const keys = [];
  let token = null;
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
  // Replace NaN/Infinity with null to ensure strict JSON parsing
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

function hasJapaneseText(value) {
  return typeof value === "string" && /[぀-ヿ㐀-鿿]/.test(value);
}

function getSavedTranslation(translations, symbol) {
  const value = translations?.[symbol];
  if (typeof value === "string") {
    return hasJapaneseText(value)
      ? { business_summary_ja: value, translation_date: null, source_hash: null }
      : null;
  }
  if (value && hasJapaneseText(value.business_summary_ja)) {
    return value;
  }
  return null;
}

function sourceHash(text) {
  return createHash("sha256").update(String(text ?? ""), "utf8").digest("hex");
}

function dateMs(value) {
  if (!value) return 0;
  const t = new Date(value).getTime();
  return Number.isNaN(t) ? 0 : t;
}

// translation_date が REFRESH_AFTER_DAYS より古い (または日付不明) なら true。
function isStale(entry) {
  const ms = dateMs(entry?.translation_date);
  if (ms === 0) return true;
  return Date.now() - ms >= REFRESH_AFTER_DAYS * 86400000;
}

function cleanTranslation(value) {
  return String(value || "")
    .replace(/^```(?:json|text)?/i, "")
    .replace(/```$/i, "")
    .trim();
}

// Gemini リクエストを GEMINI_RPM 以内に平準化する。
let lastGeminiRequestAt = 0;
async function paceGemini() {
  const minIntervalMs = Math.ceil(60000 / GEMINI_RPM);
  const wait = lastGeminiRequestAt + minIntervalMs - Date.now();
  if (wait > 0) await sleep(wait);
  lastGeminiRequestAt = Date.now();
}

async function translateSummary(symbol, summary) {
  const prompt = [
    "以下の英文の会社概要を日本語に翻訳してください。",
    "内容の省略、補足、要約、投資判断の追加はしないでください。",
    "会社名、製品名、地名などの固有名詞は自然な範囲で原文表記を残してください。",
    "回答は翻訳文のみを返してください。",
    "",
    `銘柄: ${symbol}`,
    "",
    summary,
  ].join("\n");

  const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(GEMINI_MODEL)}:generateContent?key=${GEMINI_API_KEY}`;
  const payload = {
    contents: [{ role: "user", parts: [{ text: prompt }] }],
    generationConfig: { temperature: 0.1 },
    systemInstruction: { parts: [{ text: "あなたはプロの翻訳者および証券アナリストです。英文の会社概要を正確で自然な日本語に翻訳します。" }] },
  };

  const opts = {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  };

  const maxAttempts = 3;
  let lastErr = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await paceGemini();
      const res = await fetch(url, opts);
      if (!res.ok) {
        const body = await res.text();
        const err = new Error(`Gemini API ${res.status}: ${body}`);
        err.status = res.status;
        err.body = body;
        // Retry on server errors (5xx)
        if (res.status >= 500 && attempt < maxAttempts) {
          const waitMs = 1000 * Math.pow(2, attempt - 1);
          console.log(`[${symbol}] Gemini API ${res.status}, retrying in ${waitMs}ms (attempt ${attempt}/${maxAttempts})`);
          await sleep(waitMs);
          continue;
        }
        throw err;
      }

      const data = await res.json();
      const text = data.candidates?.[0]?.content?.parts
        ?.map((part) => part.text || "")
        .join("")
        .trim();

      if (!text) throw new Error("Gemini returned an empty translation.");
      return cleanTranslation(text);
    } catch (e) {
      lastErr = e;
      // If rate-limited, bubble up immediately so caller can stop if desired
      if (String(e.message).includes("429")) throw e;
      if (attempt < maxAttempts) {
        const waitMs = 1000 * Math.pow(2, attempt - 1);
        console.log(`[${symbol}] fetch attempt ${attempt} failed: ${String(e.message)}. Retrying in ${waitMs}ms`);
        await sleep(waitMs);
        continue;
      }
      throw lastErr;
    }
  }
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function isRateLimited(error) {
  return String(error?.message || "").includes("429");
}

async function main() {
  console.log(
    `bucket=${BUCKET} limit=${LIMIT} refreshLimit=${REFRESH_LIMIT} ` +
      `refreshAfterDays=${REFRESH_AFTER_DAYS} rpm=${GEMINI_RPM} model=${GEMINI_MODEL}`,
  );

  let translations = {};
  try {
    translations = await getJson(TRANSLATIONS_KEY);
    console.log(
      `loaded ${Object.keys(translations).length} saved translations from ${TRANSLATIONS_KEY}`,
    );
  } catch {
    console.log(`${TRANSLATIONS_KEY} not found. It will be created.`);
  }

  const symbols = (await listAll("raw/"))
    .filter((key) => key.endsWith(".json") && key !== "raw/stocks_list.json")
    .map((key) => key.replace(/^raw\//, "").replace(/\.json$/, ""))
    .sort();

  async function loadSource(symbol) {
    const raw = await getJson(`raw/${symbol}.json`);
    return raw.info?.longBusinessSummary || null;
  }

  let translated = 0;
  let refreshed = 0;
  let touched = 0;
  const translatedSymbols = [];
  const refreshedSymbols = [];
  let skippedNoSource = 0;
  let failed = 0;
  let stop = false;

  // ---- Pass 1: 未翻訳の銘柄を翻訳 ----
  for (const symbol of symbols) {
    if (translated >= LIMIT) break;
    if (getSavedTranslation(translations, symbol)) continue; // 翻訳済みは Pass 2 で扱う

    let source = null;
    try {
      source = await loadSource(symbol);
    } catch (error) {
      failed += 1;
      console.log(`[${symbol}] raw download failed: ${error.message}`);
      continue;
    }
    if (!source) {
      skippedNoSource += 1;
      continue;
    }

    try {
      console.log(`[${symbol}] translating new (${translated + 1}/${LIMIT})`);
      const translation = await translateSummary(symbol, source);
      translations[symbol] = {
        symbol,
        business_summary_ja: translation,
        translation_date: new Date().toISOString(),
        source_hash: sourceHash(source),
      };
      await putJson(TRANSLATIONS_KEY, translations);
      translated += 1;
      translatedSymbols.push(symbol);
    } catch (error) {
      failed += 1;
      console.error(`[${symbol}] translation failed: ${String(error?.message || error)}`);
      if (error && error.status) console.error(`[${symbol}] response status: ${error.status}`);
      if (error && error.body) {
        console.error(`[${symbol}] response body (snippet): ${String(error.body).slice(0, 400)}`);
      }
      if (isRateLimited(error)) {
        console.log("Rate limited. Stopping this run so the next schedule can retry.");
        stop = true;
        break;
      }
    }
  }

  // ---- Pass 2: 古い翻訳を再チェック (約 1 年で 1 周) ----
  if (!stop) {
    const stale = symbols
      .map((symbol) => ({ symbol, entry: getSavedTranslation(translations, symbol) }))
      .filter((x) => x.entry && isStale(x.entry))
      .sort((a, b) => dateMs(a.entry.translation_date) - dateMs(b.entry.translation_date));

    if (stale.length > 0) {
      console.log(`refresh: ${stale.length} entries older than ${REFRESH_AFTER_DAYS}d`);
    }

    for (const { symbol, entry } of stale) {
      if (refreshed + touched >= REFRESH_LIMIT) break;

      let source = null;
      try {
        source = await loadSource(symbol);
      } catch (error) {
        failed += 1;
        console.log(`[${symbol}] raw download failed: ${error.message}`);
        continue;
      }

      // ソースが消えた場合は翻訳を残しつつ日付だけ更新し、再チェック対象から外す。
      if (!source) {
        translations[symbol] = {
          symbol,
          business_summary_ja: entry.business_summary_ja,
          translation_date: new Date().toISOString(),
          source_hash: translations[symbol]?.source_hash ?? null,
        };
        await putJson(TRANSLATIONS_KEY, translations);
        touched += 1;
        continue;
      }

      const currentHash = sourceHash(source);
      const stored = translations[symbol];
      const storedHash =
        stored && typeof stored === "object" ? stored.source_hash : undefined;

      // ソース未変更: 再翻訳せず翻訳日だけ更新 (次の周回まで対象外になる)。
      if (storedHash && storedHash === currentHash) {
        translations[symbol] = {
          symbol,
          business_summary_ja: entry.business_summary_ja,
          translation_date: new Date().toISOString(),
          source_hash: currentHash,
        };
        await putJson(TRANSLATIONS_KEY, translations);
        touched += 1;
        continue;
      }

      // ソース変更あり (または source_hash 未保存): 再翻訳する。
      try {
        console.log(`[${symbol}] re-translating (source changed) (${refreshed + 1})`);
        const translation = await translateSummary(symbol, source);
        translations[symbol] = {
          symbol,
          business_summary_ja: translation,
          translation_date: new Date().toISOString(),
          source_hash: currentHash,
        };
        await putJson(TRANSLATIONS_KEY, translations);
        refreshed += 1;
        refreshedSymbols.push(symbol);
      } catch (error) {
        failed += 1;
        console.error(`[${symbol}] re-translation failed: ${String(error?.message || error)}`);
        if (error && error.status) console.error(`[${symbol}] response status: ${error.status}`);
        if (error && error.body) {
          console.error(`[${symbol}] response body (snippet): ${String(error.body).slice(0, 400)}`);
        }
        if (isRateLimited(error)) {
          console.log("Rate limited. Stopping this run so the next schedule can retry.");
          stop = true;
          break;
        }
      }
    }
  }

  console.log(
    `done translated=${translated} refreshed=${refreshed} touched=${touched} ` +
      `skipped_no_source=${skippedNoSource} failed=${failed}`,
  );

  if (SUMMARY_OUTPUT_PATH) {
    await writeFile(
      SUMMARY_OUTPUT_PATH,
      JSON.stringify(
        {
          translated,
          translated_symbols: translatedSymbols,
          refreshed,
          refreshed_symbols: refreshedSymbols,
          touched,
          skipped_no_source: skippedNoSource,
          failed,
        },
        null,
        2,
      ),
    );
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
