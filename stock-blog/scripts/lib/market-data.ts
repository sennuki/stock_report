export interface StockInfo {
  Symbol: string;
  Symbol_YF: string;
  Security: string;
  Security_JA?: string;
  'GICS Sector': string;
  'GICS Sub-Industry': string;
  Exchange: string;
  Daily_Change?: number;
}

/**
 * S&P 500 銘柄リストを Wikipedia から取得 (Python版の移植)
 */
export async function fetchSp500Companies(): Promise<StockInfo[]> {
  try {
    const response = await fetch('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies');
    const text = await response.text();
    
    // シンプルな正規表現によるパース (Wikiのテーブル構造に依存)
    const stocks: StockInfo[] = [];
    const rows = text.match(/<tr>\s*<td><a[^>]*>([^<]+)<\/a><\/td>\s*<td><a[^>]*>([^<]+)<\/a><\/td>[\s\S]*?<td>([^<]+)<\/td>\s*<td>([^<]+)<\/td>/g);

    if (rows) {
      for (const row of rows) {
        const cells = row.match(/<td>.*?<\/td>/g);
        if (cells && cells.length >= 4) {
          const symbol = cells[0].replace(/<[^>]*>/g, '').trim();
          const security = cells[1].replace(/<[^>]*>/g, '').trim();
          const sector = cells[2].replace(/<[^>]*>/g, '').trim();
          const subIndustry = cells[3].replace(/<[^>]*>/g, '').trim();
          
          stocks.push({
            Symbol: symbol,
            Symbol_YF: symbol.replace('.', '-'),
            Security: security,
            'GICS Sector': sector,
            'GICS Sub-Industry': subIndustry,
            Exchange: 'NYSE' // デフォルト、後で修正
          });
        }
      }
    }
    return stocks;
  } catch (e) {
    console.error('Failed to fetch S&P 500 list:', e);
    return [];
  }
}

/**
 * 各証券会社の取扱銘柄リストを取得
 */
export async function getBrokerageAvailability() {
  const brokers = [
    'monex', 'rakuten', 'sbi', 'mufg', 'matsui', 'dmm', 'paypay', 'moomoo'
  ];
  const result: Record<string, Set<string>> = {};
  
  for (const broker of brokers) {
    result[broker] = new Set<string>();
    // ローカルのCSVファイルなどから読み込むロジックをここに実装
    // 今回はプレースホルダとして空のSetを返します
  }
  
  return result;
}

export async function getMonexAvailableSymbols(): Promise<Record<string, string>> {
  return {}; // 実際にはCSVからパース
}

export async function getRakutenAvailableSymbols(): Promise<Set<string>> {
  return new Set();
}
