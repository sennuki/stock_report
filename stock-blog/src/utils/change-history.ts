// 変更履歴 (S&P 500/400/600 の構成銘柄入れ替え) の読み込みヘルパー。
// 「正」は R2 の reports/change_history.json で、generate-reports.mjs が更新する。
// 更新履歴ページ (/updates) は SSR (prerender=false) で実行時に読む。

export interface ChangeEvent {
  /** YYYY-MM-DD。 */
  date: string;
  type: "index_added" | "index_removed";
  symbol: string;
  /** 企業名 (英語)。 */
  security: string;
  /** "S&P 500" / "S&P 400" / "S&P 600"。 */
  index: string;
}

interface ChangeHistoryFile {
  universe?: unknown;
  events?: ChangeEvent[];
}

// R2 上のキー。generate-reports.mjs の recordIndexChanges と一致させる。
const CHANGE_HISTORY_KEY = "reports/change_history.json";

/**
 * 変更履歴イベントを読み込む。
 *   1) 本番 (Cloudflare Worker): R2 バインディング STOCK_DATA から直接読む。
 *   2) ローカル開発: public/reports/change_history.json へ自己 fetch。
 * いずれも失敗した場合は空配列を返す。
 */
export async function loadChangeHistory(
  env: unknown,
  siteUrl: URL
): Promise<ChangeEvent[]> {
  // 1) 本番: R2 バインディングから読む
  try {
    const bucket = (env as any)?.STOCK_DATA;
    if (bucket) {
      const object = await bucket.get(CHANGE_HISTORY_KEY);
      if (object) {
        const data = JSON.parse(await object.text()) as ChangeHistoryFile;
        return Array.isArray(data.events) ? data.events : [];
      }
    }
  } catch (e) {
    if (import.meta.env.DEV)
      console.error("R2 change history fetch failed:", e);
  }
  // 2) ローカル開発フォールバック: public/ の静的ファイル
  try {
    const resp = await fetch(new URL(`/${CHANGE_HISTORY_KEY}`, siteUrl));
    if (resp.ok) {
      const data = (await resp.json()) as ChangeHistoryFile;
      return Array.isArray(data.events) ? data.events : [];
    }
  } catch (e) {
    if (import.meta.env.DEV)
      console.error("Local change history fetch failed:", e);
  }
  return [];
}
