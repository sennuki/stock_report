/**
 * 会社概要を日本語に翻訳 (一時的に無効化)
 */
export async function translateSummary(summary: string): Promise<string | null> {
  console.log('Gemini translation is temporarily disabled.');
  return null;
}

/**
 * 株価変動理由を生成 (一時的に無効化)
 */
export async function generateMovementReason(symbol: string, stats: any, originalReason: string = ""): Promise<string | null> {
  console.log('Gemini reason generation is temporarily disabled.');
  return null;
}
