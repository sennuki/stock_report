import * as fs from 'fs';
import * as path from 'path';

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
    const response = await fetch('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
      }
    });
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
    'monex', 'rakuten', 'sbi', 'mufg', 'matsui', 'dmm', 'paypay', 'moomoo', 'iwaicosmo'
  ];
  const result: Record<string, Set<string>> = {};
  
  const baseDir = path.join(process.cwd(), 'scripts/data/broker_lists');

  // Monex
  try {
    const content = fs.readFileSync(path.join(baseDir, 'Monex_US_LIST.csv'), 'utf8');
    const symbols = new Set<string>();
    content.split('\n').forEach(line => {
      const parts = line.split(',');
      if (parts.length >= 3) {
        const symbol = parts[0].trim();
        if (symbol && /^[A-Z.-]+$/.test(symbol)) symbols.add(symbol);
      }
    });
    result['monex'] = symbols;
  } catch (e) {}

  // Rakuten
  try {
    const content = fs.readFileSync(path.join(baseDir, 'Rakuten_US_LIST.csv'), 'utf8');
    const symbols = new Set<string>();
    content.split('\n').forEach(line => {
      const parts = line.split(',');
      if (parts.length >= 6) {
        const symbol = parts[0].trim();
        const available = parts[5].trim();
        if (symbol && symbol !== "現地コード" && available.includes('○')) symbols.add(symbol);
      }
    });
    result['rakuten'] = symbols;
  } catch (e) {}

  // SBI (HTML regex)
  try {
    const content = fs.readFileSync(path.join(baseDir, 'SBI_US_LIST.html'), 'utf8');
    const symbols = new Set<string>();
    const matches = content.matchAll(/<th class="vaM alC">([A-Z.-]+)<\/th>/g);
    for (const match of matches) {
      if (match[1]) symbols.add(match[1]);
    }
    result['sbi'] = symbols;
  } catch (e) {}

  // MUFG (au Kabucom JS regex)
  try {
    const content = fs.readFileSync(path.join(baseDir, 'Mufg_US_LIST.js'), 'utf8');
    const symbols = new Set<string>();
    const matches = content.matchAll(/<td>([A-Z.-]+)<\/td>/g);
    for (const match of matches) {
      if (match[1]) symbols.add(match[1]);
    }
    result['mufg'] = symbols;
  } catch (e) {}

  // Matsui
  try {
    const content = fs.readFileSync(path.join(baseDir, 'Matsui_US_LIST.csv'), 'utf8');
    const symbols = new Set<string>();
    content.split('\n').forEach(line => {
      const parts = line.split(',');
      if (parts.length >= 1) {
        const symbol = parts[0].trim();
        if (symbol && symbol !== "コード" && /^[A-Z.-]+$/.test(symbol)) symbols.add(symbol);
      }
    });
    result['matsui'] = symbols;
  } catch (e) {}

  // DMM
  try {
    const content = fs.readFileSync(path.join(baseDir, 'Dmm_US_LIST.csv'), 'utf8');
    const symbols = new Set<string>();
    content.split('\n').forEach(line => {
      const parts = line.split(',');
      if (parts.length >= 5) {
        const symbol = parts[0].trim();
        const available = parts[4].trim();
        if (symbol && symbol !== "code" && available.includes('○')) symbols.add(symbol);
      }
    });
    result['dmm'] = symbols;
  } catch (e) {}

  // PayPay
  try {
    const content = fs.readFileSync(path.join(baseDir, 'Paypay_US_LIST.txt'), 'utf8');
    const symbols = new Set(content.split('\n').map(s => s.trim()).filter(s => s));
    result['paypay'] = symbols;
  } catch (e) {}

  // moomoo
  try {
    const content = fs.readFileSync(path.join(baseDir, 'moomoo_us_stocks.csv'), 'utf8');
    const symbols = new Set<string>();
    content.split('\n').forEach(line => {
      const parts = line.split(',');
      if (parts[0].startsWith('US.')) {
        symbols.add(parts[0].substring(3));
      }
    });
    result['moomoo'] = symbols;
  } catch (e) {}

  // Iwai Cosmo
  try {
    const content = fs.readFileSync(path.join(baseDir, 'IwaiCosmo_US_LIST.html'), 'utf8');
    const symbols = new Set<string>();
    // Simple <td>SYMBOL</td> match if it's the 3rd column
    // The format is <td>DATE</td><td>NAME</td><td>SYMBOL</td>...
    const matches = content.matchAll(/<td>\d{4}年\d{2}月\d{2}日<\/td>\s*<td>.*?<\/td>\s*<td>([A-Z.-]+)<\/td>/g);
    for (const match of matches) {
      if (match[1]) symbols.add(match[1]);
    }
    result['iwaicosmo'] = symbols;
  } catch (e) {}
  
  return result;
}

export async function getMonexAvailableSymbols(): Promise<Record<string, string>> {
  return {}; 
}

export async function getRakutenAvailableSymbols(): Promise<Set<string>> {
  return new Set();
}
