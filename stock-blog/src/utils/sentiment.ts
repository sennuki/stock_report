// 決算説明会トランスクリプトの感情分析（センチメント分析）の型とヘルパー。
// データは generate_transcript_report.py が生成し、R2 の索引 (reports/transcripts/index.json) に格納される。

export interface ToneScore {
  /** -100（非常に弱気）〜 +100（非常に強気）の整数 */
  score: number;
  /** 強気 / やや強気 / 中立 / やや慎重 / 慎重 など */
  label: string;
  /** overall のみ: 全体トーンの説明文 */
  summary?: string;
  /** analyst のみ: 市場の懸念度（低 / 中 / 高） */
  concern_level?: string;
}

export interface TopicSentiment {
  topic: string;
  score: number;
  label: string;
  note?: string;
}

export interface HedgingMetrics {
  word_count: number;
  hedge_count: number;
  /** 1000 語あたりのヘッジ語出現数 */
  hedge_density: number;
  /** Q&A パートが全体に占める割合（0〜1） */
  qa_ratio: number;
  analyst_count: number;
  management_count?: number;
}

export interface Sentiment {
  overall?: ToneScore;
  management?: ToneScore;
  analyst?: ToneScore;
  topics?: TopicSentiment[];
  hedging?: HedgingMetrics;
}

/** スコア (-100..100) に応じた文字色の Tailwind クラス */
export function scoreTextClass(score: number): string {
  if (score >= 40) return "text-green-600 dark:text-green-400";
  if (score >= 10) return "text-emerald-600 dark:text-emerald-400";
  if (score > -10) return "text-gray-500 dark:text-gray-400";
  if (score > -40) return "text-amber-600 dark:text-amber-400";
  return "text-red-600 dark:text-red-400";
}

/** スコア (-100..100) に応じたバー/バッジ背景色の Tailwind クラス */
export function scoreBarClass(score: number): string {
  if (score >= 40) return "bg-green-500";
  if (score >= 10) return "bg-emerald-500";
  if (score > -10) return "bg-gray-400";
  if (score > -40) return "bg-amber-500";
  return "bg-red-500";
}

/** スコア (-100..100) をバーの幅 (0..100%) に変換。50% が中立。 */
export function scoreToPct(score: number): number {
  return Math.round((Math.max(-100, Math.min(100, score)) + 100) / 2);
}

/** 符号付きでスコアを表示（+72 / -15 / 0） */
export function formatScore(score: number): string {
  return score > 0 ? `+${score}` : `${score}`;
}
