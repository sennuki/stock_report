#!/usr/bin/env node
/**
 * Translate company business summaries independently from report generation.
 *
 * The script reads raw/{symbol}.json for the English source text and updates
 * reports/{symbol}.json in R2. Existing Japanese translations are skipped.
 */

import "dotenv/config";
import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { writeFile } from "node:fs/promises";

const BUCKET = process.env.R2_BUCKET_NAME || "stock-data-c1";
const LIMIT = Number.parseInt(process.env.TRANSLATION_LIMIT || "6", 10);
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
  return JSON.parse(body);
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
  return typeof value === "string" && /[\u3040-\u30ff\u3400-\u9fff]/.test(value);
}

function getSavedTranslation(translations, symbol) {
  const value = translations?.[symbol];
  if (typeof value === "string") {
    return hasJapaneseText(value)
      ? { business_summary_ja: value, translation_date: null }
      : null;
  }
  if (value && hasJapaneseText(value.business_summary_ja)) {
    return value;
  }
  return null;
}

function cleanTranslation(value) {
  return String(value || "")
    .replace(/^```(?:json|text)?/i, "")
    .replace(/```$/i, "")
    .trim();
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
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      contents: [{ role: "user", parts: [{ text: prompt }] }],
      generationConfig: {
        temperature: 0.1,
      },
      systemInstruction: {
        parts: [
          {
            text: "あなたはプロの翻訳者および証券アナリストです。英文の会社概要を正確で自然な日本語に翻訳します。",
          },
        ],
      },
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Gemini API ${res.status}: ${text}`);
  }

  const data = await res.json();
  const text = data.candidates?.[0]?.content?.parts
    ?.map((part) => part.text || "")
    .join("")
    .trim();

  if (!text) throw new Error("Gemini returned an empty translation.");
  return cleanTranslation(text);
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  console.log(`bucket=${BUCKET} limit=${LIMIT} model=${GEMINI_MODEL}`);

  let translations = {};
  try {
    translations = await getJson(TRANSLATIONS_KEY);
    console.log(
      `loaded ${Object.keys(translations).length} saved translations from ${TRANSLATIONS_KEY}`,
    );
  } catch {
    console.log(`${TRANSLATIONS_KEY} not found. It will be created.`);
  }

  const rawKeys = (await listAll("raw/"))
    .filter((key) => key.endsWith(".json") && key !== "raw/stocks_list.json")
    .sort();

  let translated = 0;
  const translatedSymbols = [];
  let skippedJapanese = 0;
  let skippedNoSource = 0;
  let failed = 0;

  for (const rawKey of rawKeys) {
    if (translated >= LIMIT) break;

    const symbol = rawKey.replace(/^raw\//, "").replace(/\.json$/, "");
    const reportKey = `reports/${symbol}.json`;

    if (getSavedTranslation(translations, symbol)) {
      skippedJapanese += 1;
      continue;
    }

    let report = null;
    try {
      report = await getJson(reportKey);
      if (hasJapaneseText(report.business_summary_ja)) {
        skippedJapanese += 1;
        continue;
      }
    } catch {
      report = null;
    }

    let rawData = null;
    try {
      rawData = await getJson(rawKey);
    } catch (error) {
      failed += 1;
      console.log(`[${symbol}] raw download failed: ${error.message}`);
      continue;
    }

    const source = rawData.info?.longBusinessSummary;
    if (!source) {
      skippedNoSource += 1;
      continue;
    }

    try {
      console.log(`[${symbol}] translating (${translated + 1}/${LIMIT})`);
      const translation = await translateSummary(symbol, source);
      const translationDate = new Date().toISOString();
      translations[symbol] = {
        symbol,
        business_summary_ja: translation,
        translation_date: translationDate,
      };
      await putJson(TRANSLATIONS_KEY, translations);
      if (report) {
        await putJson(reportKey, {
          ...report,
          symbol: report.symbol || symbol,
          symbol_yf: report.symbol_yf || symbol,
          business_summary_ja: translation,
          translation_date: translationDate,
        });
      }
      translated += 1;
      translatedSymbols.push(symbol);
      await sleep(1000);
    } catch (error) {
      failed += 1;
      console.log(`[${symbol}] translation failed: ${error.message}`);
      if (String(error.message).includes("429")) {
        console.log("Rate limited. Stopping this run so the next schedule can retry.");
        break;
      }
    }
  }

  console.log(
    `done translated=${translated} skipped_japanese=${skippedJapanese} skipped_no_source=${skippedNoSource} failed=${failed}`,
  );

  if (SUMMARY_OUTPUT_PATH) {
    await writeFile(
      SUMMARY_OUTPUT_PATH,
      JSON.stringify(
        {
          translated,
          translated_symbols: translatedSymbols,
          skipped_japanese: skippedJapanese,
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
