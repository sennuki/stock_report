// 決算説明会トランスクリプト索引の読み込みヘルパー。
// 索引・md 本体とも「正」は R2 にあり、generate_transcript_report.py が更新する。
// 銘柄ページ / トランスクリプトページは SSR (prerender=false) で実行時に読む。
import type { Sentiment } from "@/utils/sentiment";

export interface TranscriptEntry {
  fy: number;
  fq: number;
  /** "2026-Q3" 形式。URL のセグメントに使う。 */
  period: string;
  /** 生成日 (YYYY-MM-DD)。 */
  generated?: string;
  sentiment?: Sentiment;
}

export type TranscriptIndex = Record<string, TranscriptEntry[]>;

// R2 上の索引キー。generate_transcript_report.py の TRANSCRIPT_INDEX_KEY と一致させる。
const INDEX_KEY = "reports/transcripts/index.json";

/**
 * トランスクリプト索引を読み込む。
 *   1) 本番 (Cloudflare Worker): R2 バインディング STOCK_DATA から直接読む。
 *   2) ローカル開発: public/reports/transcripts/index.json へ自己 fetch。
 * いずれも失敗した場合は空の索引を返す（トランスクリプト未生成として扱う）。
 */
export async function loadTranscriptIndex(
  env: unknown,
  siteUrl: URL
): Promise<TranscriptIndex> {
  // 1) 本番: R2 バインディングから読む
  try {
    const bucket = (env as any)?.STOCK_DATA;
    if (bucket) {
      const object = await bucket.get(INDEX_KEY);
      if (object) return JSON.parse(await object.text()) as TranscriptIndex;
    }
  } catch (e) {
    if (import.meta.env.DEV)
      console.error("R2 transcript index fetch failed:", e);
  }
  // 2) ローカル開発フォールバック: public/ の静的ファイル
  try {
    const resp = await fetch(new URL(`/${INDEX_KEY}`, siteUrl));
    if (resp.ok) return (await resp.json()) as TranscriptIndex;
  } catch (e) {
    if (import.meta.env.DEV)
      console.error("Local transcript index fetch failed:", e);
  }
  return {};
}

/** 指定銘柄の登録トランスクリプトを新しい四半期順（降順）で返す。 */
export function transcriptsForSymbol(
  index: TranscriptIndex,
  symbol: string
): TranscriptEntry[] {
  return (index[symbol] || [])
    .slice()
    .sort((a, b) => b.fy - a.fy || b.fq - a.fq);
}
