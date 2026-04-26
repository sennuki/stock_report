import { GoogleGenerativeAI } from '@google/generative-ai';

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY || '');

// モデルの定義
const TRANSLATION_MODEL = "gemma-4-26b-a4b-it";
const MOVEMENT_REASON_MODEL = "gemini-3.1-flash-lite-preview";

/**
 * 会社概要を日本語に翻訳
 */
export async function translateSummary(summary: string): Promise<string | null> {
  if (!summary) return null;

  try {
    const model = genAI.getGenerativeModel({ model: TRANSLATION_MODEL });
    const prompt = `以下の英文の会社概要を、内容を省略・補完することなく、原文に忠実かつ正確な日本語に翻訳してください。専門用語は日本の投資家が理解できる適切な用語を用い、自然な日本語の文章として整えてください。情報の追加や主観的な要約は行わないでください。\n\n${summary}`;
    
    const result = await model.generateContent(prompt);
    return result.response.text().trim();
  } catch (e) {
    console.error('Translation error:', e);
    return null;
  }
}

/**
 * 株価変動理由を生成
 */
export async function generateMovementReason(symbol: string, stats: any, originalReason: string = ""): Promise<string | null> {
  try {
    const model = genAI.getGenerativeModel({ 
      model: MOVEMENT_REASON_MODEL,
      systemInstruction: "あなたは日経新聞やロイター通信のシニア編集者です。正確で客観的、かつ洞察に富んだ金融ニュース記事を執筆します。"
    });

    const isUp = stats.diff_pct >= 0;
    const upDownWord = isUp ? "高" : "安";

    const prompt = `
以下の銘柄情報と背景理由を元に、プロの証券アナリストが執筆する金融ニュース記事のようなスタイルで文章を作成してください。
必要に応じて、最新の市場動向を検索して補完してください。

【銘柄】: ${symbol}
【日付】: ${stats.date}
【前日比】: ${stats.diff.toFixed(2)}ドル (${(stats.diff_pct * 100).toFixed(2)}%)
【終値】: ${stats.close.toFixed(2)}ドル
【年初来騰落率】: ${(stats.ytd_pct * 100).toFixed(2)}%
【主な背景理由】: ${originalReason}

【構成案】
1. 一行目に「年初来・株価騰落率：[+0.00]％」と記載。 (※${(stats.ytd_pct * 100).toFixed(2)}%)
2. 本文は「${stats.date_ja}の取引で、[会社概要や業界での立ち位置]の[社名]が大幅に[上昇/下落]。...」と開始し、背景理由を詳しく、プロフェッショナルな日本語で説明。
3. 最後に「株価は一時、前日比[0.00]ドル[高/安]([0.00]％)の[0.00]ドルまで[上昇/下落]し、[0.00]ドル[高/安]([0.00]％)の[0.00]ドルで終了。年初来では[0.00]％[高/安]となった。」という形式で締める。

※必ず指定のスタイルを守り、事実に基づいた格調高い文章にしてください。改行（\n）を適切に使用して読みやすくしてください。
`;

    const result = await model.generateContent(prompt);
    return result.response.text().trim();
  } catch (e) {
    console.error(`Error generating movement reason for ${symbol}:`, e);
    return null;
  }
}
