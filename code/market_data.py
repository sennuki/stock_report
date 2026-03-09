# -*- coding: utf-8 -*-
import os
import yfinance as yf
import polars as pl
import pandas as pd
import requests
import utils
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    curl_requests = None

# ==========================================
#  Part C (前半): データ取得
# ==========================================

def get_monex_available_symbols():
    """
    マネックス証券の米国株取扱銘柄リストを取得し、シンボルのセットを返します。
    """
    url = "https://mst.monex.co.jp/pc/pdfroot/public/50/99/Monex_US_LIST.csv"
    csv_path = "Monex_US_LIST.csv"
    
    # 既にファイルがある場合はそれを使う（デバッグ・キャッシュ用）
    if os.path.exists(csv_path):
        try:
            with open(csv_path, "rb") as f:
                content = f.read().decode("cp932", errors="replace")
        except Exception:
            content = None
    else:
        content = None

    if not content:
        if curl_requests:
            try:
                # TLSフィンガープリントをChromeに偽装して取得
                resp = curl_requests.get(url, impersonate="chrome110")
                resp.raise_for_status()
                content = resp.content.decode("cp932", errors="replace")
                # キャッシュ保存
                with open(csv_path, "wb") as f:
                    f.write(resp.content)
            except Exception as e:
                print(f"Error fetching Monex list: {e}")
                return set()
        else:
            print("curl-cffi is not installed. Skipping Monex list fetch.")
            return set()

    symbols = set()
    lines = content.splitlines()
    for line in lines:
        parts = line.split(",")
        if len(parts) >= 1:
            symbol = parts[0].strip()
            # シンボルが英数字（一部記号含む）であることを確認
            if symbol and any(c.isalnum() for c in symbol):
                # ハイフンやドットの表記揺れを考慮して、比較用には正規化が必要かもしれないが
                # マネックスのリストは A, AAPL のような形式
                symbols.add(symbol)
    
    return symbols

def get_rakuten_available_symbols():
    """
    楽天証券の米国株取扱銘柄リストを取得し、シンボルのセットを返します。
    """
    url = "https://www.trkd-asia.com/rakutensec/exportcsvus?all=on&vall=on&forwarding=na&target=0&theme=na&returns=na&head_office=na&name=&code=&sector=na&pageNo=&c=us&p=result&r1=on"
    csv_path = "Rakuten_US_LIST.csv"
    
    if os.path.exists(csv_path):
        try:
            # 楽天は UTF-8 with BOM (utf-8-sig)
            with open(csv_path, "rb") as f:
                content = f.read().decode("utf-8-sig", errors="replace")
        except Exception:
            content = None
    else:
        content = None

    if not content:
        if curl_requests:
            try:
                resp = curl_requests.get(url, impersonate="chrome110")
                resp.raise_for_status()
                content = resp.content.decode("utf-8-sig", errors="replace")
                with open(csv_path, "wb") as f:
                    f.write(resp.content)
            except Exception as e:
                print(f"Error fetching Rakuten list: {e}")
                return set()
        else:
            return set()

    symbols = set()
    lines = content.splitlines()
    for line in lines:
        parts = line.split(",")
        if len(parts) >= 6:
            symbol = parts[0].strip()
            available = parts[5].strip()
            # 「○」または「現地コード」以外の行を処理
            if symbol and symbol != "現地コード" and "○" in available:
                symbols.add(symbol)
    
    return symbols

def get_sbi_available_symbols():
    """
    SBI証券の米国株取扱銘柄リストをHTMLスクレイピングで取得し、シンボルのセットを返します。
    """
    url = "https://search.sbisec.co.jp/v2/popwin/info/stock/pop6040_usequity_list.html"
    cache_path = "SBI_US_LIST.html"
    
    # BeautifulSoup をインポート
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("BeautifulSoup4 is not installed. Skipping SBI list fetch.")
        return set()

    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="cp932", errors="replace") as f:
                html = f.read()
        except Exception:
            html = None
    else:
        html = None

    if not html:
        if curl_requests:
            try:
                resp = curl_requests.get(url, impersonate="chrome110")
                resp.raise_for_status()
                # SBIは Shift-JIS (cp932)
                resp.encoding = "cp932"
                html = resp.text
                with open(cache_path, "w", encoding="cp932", errors="replace") as f:
                    f.write(html)
            except Exception as e:
                print(f"Error fetching SBI list: {e}")
                return set()
        else:
            return set()

    symbols = set()
    soup = BeautifulSoup(html, "html.parser")
    
    # 構造: <tr><th class="vaM alC">SYMBOL</th>...</tr>
    for th in soup.find_all("th", class_=lambda x: x and "vaM" in x and "alC" in x):
        symbol = th.get_text(strip=True)
        if symbol and symbol.isupper() and len(symbol) <= 5:
            symbols.add(symbol)
            
    return symbols

def get_mufg_available_symbols():
    """
    三菱UFJ eスマート証券（auカブコム証券）の米国株取扱銘柄リストを取得し、シンボルのセットを返します。
    """
    url = "https://kabu.com/process/beikabu.js"
    csv_path = "Mufg_US_LIST.js"
    
    if os.path.exists(csv_path):
        try:
            with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
        except Exception:
            content = None
    else:
        content = None

    if not content:
        if curl_requests:
            try:
                resp = curl_requests.get(url, impersonate="chrome110")
                resp.raise_for_status()
                # Content is usually UTF-8
                content = resp.content.decode("utf-8", errors="replace")
                with open(csv_path, "w", encoding="utf-8", errors="replace") as f:
                    f.write(content)
            except Exception as e:
                print(f"Error fetching MUFG list: {e}")
                return set()
        else:
            return set()

    import re
    symbols = set()
    # <td>A</td> のような形式を抽出
    # beikabu.js の構造: <td>SYMBOL</td>
    matches = re.findall(r"<td>([A-Z\.]+?)</td>", content)
    for symbol in matches:
        if symbol and any(c.isalnum() for c in symbol):
            symbols.add(symbol)
    
    return symbols

def get_matsui_available_symbols():
    """
    松井証券の米国株取扱銘柄リストを取得し、シンボルのセットを返します。
    """
    url = "https://www.matsui.co.jp/us-stock/domestic/list/symbollist/symbollist.csv"
    csv_path = "Matsui_US_LIST.csv"
    
    if os.path.exists(csv_path):
        try:
            with open(csv_path, "rb") as f:
                content = f.read().decode("cp932", errors="replace")
        except Exception:
            content = None
    else:
        content = None

    if not content:
        if curl_requests:
            try:
                resp = curl_requests.get(url, impersonate="chrome110")
                resp.raise_for_status()
                content = resp.content.decode("cp932", errors="replace")
                with open(csv_path, "wb") as f:
                    f.write(resp.content)
            except Exception as e:
                print(f"Error fetching Matsui list: {e}")
                return set()
        else:
            return set()

    symbols = set()
    lines = content.splitlines()
    for line in lines:
        parts = line.split(",")
        if len(parts) >= 1:
            symbol = parts[0].strip()
            # ヘッダー「コード」を除外
            if symbol and symbol != "コード" and any(c.isalnum() for c in symbol):
                symbols.add(symbol)
    
    return symbols

def get_dmm_available_symbols():
    """
    DMM株の米国株取扱銘柄リストを取得し、シンボルのセットを返します。
    """
    url = "https://kabu.dmm.com/_data/us-stock.csv"
    csv_path = "Dmm_US_LIST.csv"
    
    if os.path.exists(csv_path):
        try:
            with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
        except Exception:
            content = None
    else:
        content = None

    if not content:
        if curl_requests:
            try:
                resp = curl_requests.get(url, impersonate="chrome110")
                resp.raise_for_status()
                content = resp.text
                with open(csv_path, "w", encoding="utf-8", errors="replace") as f:
                    f.write(content)
            except Exception as e:
                print(f"Error fetching DMM list: {e}")
                return set()
        else:
            return set()

    symbols = set()
    lines = content.splitlines()
    for line in lines:
        parts = line.split(",")
        if len(parts) >= 1:
            symbol = parts[0].strip()
            # ヘッダー「code」を除外
            if symbol and symbol != "code" and any(c.isalnum() for c in symbol):
                symbols.add(symbol)
    
    return symbols

def get_paypay_available_symbols():
    """
    PayPay証券の米国株取扱銘柄リストを取得し、シンボルのセットを返します。
    """
    urls = [
        "https://www.paypay-sec.co.jp/us-stock/list/data-us_stock.json",
        "https://www.paypay-sec.co.jp/us-stock/list/data-us_etf.json"
    ]
    cache_path = "Paypay_US_LIST.txt"
    
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                return set(line.strip() for line in f if line.strip())
        except Exception:
            pass

    if not curl_requests:
        return set()

    import json
    symbols = set()
    
    for url in urls:
        try:
            resp = curl_requests.get(url, impersonate="chrome110", timeout=15)
            resp.raise_for_status()
            data = resp.json()
            for item in data:
                symbol = item.get("codenumber", "").strip()
                if symbol and any(c.isalnum() for c in symbol):
                    symbols.add(symbol)
        except Exception as e:
            print(f"Error fetching PayPay list from {url}: {e}")

    if symbols:
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                for s in sorted(list(symbols)):
                    f.write(f"{s}\n")
        except Exception:
            pass

    return symbols

def get_market_info(symbol):
    try:
        t = utils.get_ticker(symbol)
        info = t.info
        ex = info.get('exchange', 'Unknown')
        m_map = {'NMS':'NASDAQ', 'NGM':'NASDAQ', 'NCM':'NASDAQ', 'NYQ':'NYSE', 'ASE':'AMEX', 'PCX':'NYSE', 'PNK':'OTC'}
        
        # 株価変化率の取得
        prev_close = info.get('previousClose')
        curr_price = info.get('currentPrice') or info.get('regularMarketPrice')
        daily_change = None
        if prev_close and curr_price:
            daily_change = (curr_price - prev_close) / prev_close
            
        return symbol, m_map.get(ex, ex), daily_change
    except Exception as e:
        # print(f"Error fetching info for {symbol}: {e}") # Debug output
        return symbol, "NYSE", None

def fetch_sp500_companies_optimized():
    print("S&P 500リストを取得中...")
    url = "https://en.wikipedia.org/wiki/List_of_S&P_500_companies"
    try:
        # Wikipediaのテーブルを取得
        wiki_df = pd.read_html(StringIO(requests.get(url, headers={"User-Agent": "Mozilla/5.0"}).text))[0]
        df = pl.from_pandas(wiki_df).select(['Symbol', 'Security', 'GICS Sector', 'GICS Sub-Industry'])
        
        # Symbol_YF: Yahoo Finance用 (ドットをハイフンに変換: BRK.B -> BRK-B)
        # Symbol: 表示用 (ドットに統一: BRK-B -> BRK.B)
        df = df.with_columns([
            pl.col('Symbol').str.replace(r"\.", "-", literal=False).alias('Symbol_YF'),
            pl.col('Symbol').str.replace(r"-", ".", literal=False).alias('Symbol')
        ])

        symbols = df['Symbol_YF'].to_list()
        ex_map = {}
        change_map = {}
        print(f"{len(symbols)} 銘柄の市場情報を取得中... (並列処理)")
        # Rate limit回避のため並列数を抑える
        # GitHub Actions では 1、ローカルでも 1 をデフォルトにする
        default_max_workers = 1 if os.getenv("GITHUB_ACTIONS") == "true" else 1
        current_max_workers = int(os.getenv("MAX_WORKERS", default_max_workers))

        with ThreadPoolExecutor(max_workers=current_max_workers) as ex:
            f_map = {ex.submit(get_market_info, s): s for s in symbols}
            for f in tqdm(as_completed(f_map), total=len(symbols)):
                s, e, c = f.result()
                ex_map[s] = e
                change_map[s] = c

        return df.with_columns([
            pl.col('Symbol_YF').map_elements(lambda s: ex_map.get(s, "NYSE"), return_dtype=pl.Utf8).alias('Exchange'),
            pl.col('Symbol_YF').map_elements(lambda s: change_map.get(s), return_dtype=pl.Float64).alias('Daily_Change')
        ])
    except Exception as e:
        print(f"Failed to fetch S&P 500 list: {e}")
        return pl.DataFrame()

if __name__ == "__main__":
    print("S&P 500データの取得テストを実行します...")
    df = fetch_sp500_companies_optimized()
    print("\n--- S&P 500 List (First 5 rows) ---")
    print(df.head())
    print(f"Total records: {len(df)}")
