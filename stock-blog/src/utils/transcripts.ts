// 決算説明会トランスクリプト索引の読み込みヘルパー。
// 索引・md 本体とも「正」は R2 にあり、generate_transcript_report.py が更新する。
// 銘柄ページ / トランスクリプトページは SSR (prerender=false) で実行時に読む。
import type { Sentiment } from "@/utils/sentiment";
import { Marked } from "marked";

// 決算トランスクリプト本文用の Markdown レンダラ。素の URL
// （www.example.com など）の自動リンクを無効化する。日本語本文には半角空白が
// 無いため、GFM の自動リンクが URL の後ろの本文まで貪欲に取り込んでリンク範囲
// が壊れる。明示リンク [text](url) や <url> は従来どおり有効。
const transcriptMarked = new Marked();
transcriptMarked.use({ tokenizer: { url: () => undefined } });

/** トランスクリプト本文の Markdown を HTML へ変換する。 */
export function renderTranscriptMarkdown(md: string): string {
  // CJK 本文では `**強調**` が、`**` の前後が日本語文字・括弧（「」等）の
  // ために CommonMark の delimiter run 規則を満たさず強調にならないことが
  // ある。marked に渡す前に **...** を <strong> へ変換しておく。
  const withStrong = md.replace(/\*\*([^\n]+?)\*\*/g, "<strong>$1</strong>");
  return transcriptMarked.parse(withStrong, { async: false }) as string;
}

/**
 * 逐次翻訳パートを発言（話者ターン）ごとに分割する。各ターンは行頭の
 * `**話者名**` 行で始まり、次の `**話者名**` 行の手前までを 1 発言とみなす。
 */
function splitSpeakerTurns(text: string): string[] {
  return text
    .split(/\n(?=\*\*[^\n]+\*\*[ \t]*\n)/)
    .map(turn => turn.trim())
    .filter(Boolean);
}

/**
 * トランスクリプト本文を HTML 化する。「## 逐次翻訳」見出し以降は発言ごとに
 * <div class="transcript-turn"> で囲み、灰色ボックスとして表示できるようにする。
 * 見出しが無い場合は全体をそのままレンダリングする。
 */
export function renderTranscriptBody(md: string): string {
  const heading = md.match(/^##[ \t]+逐次翻訳.*$/m);
  if (!heading || heading.index === undefined) {
    return renderTranscriptMarkdown(md);
  }
  const summaryPart = md.slice(0, heading.index);
  const translationPart = md.slice(heading.index + heading[0].length);
  const turns = splitSpeakerTurns(translationPart)
    .map(
      turn =>
        `<div class="transcript-turn">${renderTranscriptMarkdown(turn)}</div>`
    )
    .join("\n");
  return (
    renderTranscriptMarkdown(summaryPart) +
    renderTranscriptMarkdown(heading[0]) +
    turns
  );
}

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
