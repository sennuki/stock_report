import axios from 'axios';
import * as cheerio from 'cheerio';
// import { parse } from 'csv-parse/sync'; // 無効化
import * as fs from 'fs';
import * as path from 'path';

const BROKER_LISTS_DIR = path.join(process.cwd(), 'scripts/data/broker_lists');

export interface StockInfo {
  Symbol: string;
  Symbol_YF: string;
  Security: string;
  'GICS Sector': string;
  'GICS Sub-Industry': string;
  Exchange?: string;
  Daily_Change?: number;
  Security_JA?: string;
  Has_Movement_Reason?: boolean;
  Is_Recent_Actual?: boolean;
  Actual_Earnings_Date?: string | null;
}

/**
 * マネックス証券の取扱銘柄リストを取得
 */
export async function getMonexAvailableSymbols(): Promise<Record<string, string>> {
  const url = 'https://mst.monex.co.jp/pc/pdfroot/public/50/99/Monex_US_LIST.csv';
  const filePath = path.join(BROKER_LISTS_DIR, 'Monex_US_LIST.csv');
  
  try {
    let content: string;
    if (fs.existsSync(filePath)) {
      content = fs.readFileSync(filePath, 'utf8');
    } else {
      const resp = await axios.get(url, { responseType: 'arraybuffer' });
      // Monex uses Shift-JIS (cp932)
      const decoder = new TextDecoder('shift-jis');
      content = decoder.decode(resp.data);
      if (!fs.existsSync(BROKER_LISTS_DIR)) fs.mkdirSync(BROKER_LISTS_DIR, { recursive: true });
      fs.writeFileSync(filePath, content);
    }

    const mapping: Record<string, string> = {};
    const lines = content.split('\n');
    for (const line of lines) {
      const parts = line.split(',');
      if (parts.length >= 3) {
        const symbol = parts[0].trim();
        const jaName = parts[2].trim();
        if (symbol && /^[A-Z0-9.-]+$/.test(symbol)) {
          mapping[symbol] = jaName;
        }
      }
    }
    return mapping;
  } catch (e) {
    console.error('Error fetching Monex list:', e);
    return {};
  }
}

/**
 * 楽天証券の取扱銘柄リストを取得
 */
export async function getRakutenAvailableSymbols(): Promise<Set<string>> {
  const url = 'https://www.trkd-asia.com/rakutensec/exportcsvus?all=on&vall=on&forwarding=na&target=0&theme=na&returns=na&head_office=na&name=&code=&sector=na&pageNo=&c=us&p=result&r1=on';
  const filePath = path.join(BROKER_LISTS_DIR, 'Rakuten_US_LIST.csv');
  
  try {
    let content: string;
    if (fs.existsSync(filePath)) {
      content = fs.readFileSync(filePath, 'utf8');
    } else {
      const resp = await axios.get(url, { responseType: 'arraybuffer' });
      // Rakuten is UTF-8 with BOM
      const decoder = new TextDecoder('utf-8');
      content = decoder.decode(resp.data);
      if (!fs.existsSync(BROKER_LISTS_DIR)) fs.mkdirSync(BROKER_LISTS_DIR, { recursive: true });
      fs.writeFileSync(filePath, content);
    }

    const symbols = new Set<string>();
    const lines = content.split('\n');
    for (const line of lines) {
      const parts = line.split(',');
      if (parts.length >= 6) {
        const symbol = parts[0].trim();
        const available = parts[5].trim();
        if (symbol && symbol !== '現地コード' && available.includes('○')) {
          symbols.add(symbol);
        }
      }
    }
    return symbols;
  } catch (e) {
    console.error('Error fetching Rakuten list:', e);
    return new Set();
  }
}

/**
 * Wikipedia から S&P 500 銘柄リストを取得
 */
export async function fetchSp500Companies(): Promise<StockInfo[]> {
  console.log('S&P 500リストを取得中...');
  const url = 'https://en.wikipedia.org/wiki/List_of_S&P_500_companies';
  
  try {
    const { data } = await axios.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
      }
    });
    const $ = cheerio.load(data);
    const table = $('#constituents');
    const stocks: StockInfo[] = [];

    table.find('tbody tr').each((i, el) => {
      if (i === 0) return; // Skip header
      const tds = $(el).find('td');
      const symbol = $(tds[0]).text().trim();
      const security = $(tds[1]).text().trim();
      const sector = $(tds[2]).text().trim();
      const subIndustry = $(tds[3]).text().trim();

      if (symbol) {
        stocks.push({
          Symbol: symbol.replace('-', '.'),
          Symbol_YF: symbol.replace('.', '-'),
          Security: security,
          'GICS Sector': sector,
          'GICS Sub-Industry': subIndustry
        });
      }
    });

    return stocks;
  } catch (e) {
    console.error('Failed to fetch S&P 500 list:', e);
    return [];
  }
}

/**
 * 手動メンテンナンス用の日本語名マップ
 */
export function getManualJaNameMap(): Record<string, string> {
  return {
    "AMT": "アメリカン・タワー",
    "ARE": "アレクサンドリア・リアル・エステート・エクイティーズ",
    "AVB": "アバロンベイ・コミュニティーズ",
    "BXP": "BXP",
    "CCI": "クラウン・キャッスル",
    "CPT": "カムデン・プロプロティ・トラスト",
    "DLR": "デジタル・リアルティ",
    "DOC": "ヘルスピーク・プロパティーズ",
    "EQIX": "エクイニクス",
    "EQR": "エクイティ・レジデンシャル",
    "ESS": "エセックス・プロパティ・トラスト",
    "EXR": "エクストラ・スペース・ストレージ",
    "FRT": "フェデラル・リアルティー・インベストメント・トラスト",
    "HST": "ホスト・ホテルズ＆リゾーツ",
    "INVH": "インビテーション・ホームズ",
    "IRM": "アイアン・マウンテン",
    "KIM": "キムコ・リアルティ",
    "MAA": "ミッド・アメリカ・アパートメント・コミュニティーズ",
    "O": "リアルティー・インカム",
    "PLD": "プロロジス",
    "PSA": "パブリック・ストレージ",
    "REG": "リージェンシー・センターズ",
    "SBAC": "SBAコミュニケーションズ",
    "SPG": "サイモン・プロパティ・グループ",
    "UDR": "UDR",
    "VICI": "VICIプロパティーズ",
    "VTR": "ベンタス",
    "WELL": "ウェルタワー",
    "WY": "ウェアーハウザー"
  };
}

/**
 * 全ての証券会社の情報を統合してシンボルごとの可用性マップを作成する
 */
export async function getBrokerAvailability(): Promise<Record<string, Record<string, boolean>>> {
  // 他の証券会社の実装もここに追加予定だが、まずは Monex と Rakuten
  const monexMap = await getMonexAvailableSymbols();
  const rakutenSet = await getRakutenAvailableSymbols();
  // TODO: SBI, MUFG, Matsui, DMM, PayPay, moomoo, IwaiCosmo の実装
  
  const availability: Record<string, Record<string, boolean>> = {};
  
  // 統合されたシンボルリストを作成するためのプレースホルダ
  // 実際には全銘柄に対してループを回す
  return availability;
}
