# -*- coding: utf-8 -*-
import os
import yfinance as yf
import polars as pl
import pandas as pd
import requests
import utils
import json
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    curl_requests = None

# ==========================================
#  Broker Lists Management
# ==========================================

# Use a central directory for all broker list caches
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BROKER_LISTS_DIR = os.path.join(BASE_DIR, "data", "broker_lists")
if not os.path.exists(BROKER_LISTS_DIR):
    os.makedirs(BROKER_LISTS_DIR)

# ==========================================
#  Part C (前半): データ取得
# ==========================================

def get_monex_available_symbols():
    """
    マネックス証券の米国株取扱銘柄リストを取得し、{シンボル: 日本語名} の辞書を返します。
    """
    url = "https://mst.monex.co.jp/pc/pdfroot/public/50/99/Monex_US_LIST.csv"
    csv_path = os.path.join(BROKER_LISTS_DIR, "Monex_US_LIST.csv")
    # リポジトリにコミット済みのCSVをフォールバックとして使う
    repo_root = os.path.dirname(BASE_DIR)
    fallback_csv_path = os.path.join(repo_root, "stock-blog", "scripts", "data", "broker_lists", "Monex_US_LIST.csv")

    def _read_csv(path):
        try:
            with open(path, "rb") as f:
                return f.read().decode("cp932", errors="replace")
        except Exception:
            return None

    # 1. キャッシュ済みファイルを優先
    content = _read_csv(csv_path) if os.path.exists(csv_path) else None
    # 2. リポジトリ内のコミット済みファイルにフォールバック
    if not content:
        content = _read_csv(fallback_csv_path)
        if content:
            print(f"Monex CSV: using committed fallback at {fallback_csv_path}")
    # 3. ネットワークから取得
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
                return {}
        else:
            print("curl-cffi is not installed. Skipping Monex list fetch.")
            return {}

    mapping = {}
    lines = content.splitlines()
    for line in lines:
        parts = line.split(",")
        if len(parts) >= 3:
            symbol = parts[0].strip()
            ja_name = parts[2].strip()
            # シンボルが英数字（一部記号含む）であることを確認
            if symbol and any(c.isalnum() for c in symbol):
                mapping[symbol] = ja_name
    
    return mapping

def get_manual_ja_name_map():
    return {
        # === S&P 500 REITs (マネックスでカバーされない大型 REIT) ===
        "AMT": "アメリカン・タワー",
        "ARE": "アレクサンドリア・リアル・エステート・エクイティーズ",
        "AVB": "アバロンベイ・コミュニティーズ",
        "BXP": "BXP",
        "CCI": "クラウン・キャッスル",
        "CPT": "カムデン・プロパティ・トラスト",
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
        "WY": "ウェアーハウザー",

        # === S&P 400/600 REITs (中小型 REIT) ===
        "AAT": "アメリカン・アセット・トラスト",
        "ACC": "アメリカン・キャンパス・コミュニティーズ",
        "ADC": "アグリー・リアルティ",
        "AHH": "アームダ・ホフラー・プロパティーズ",
        "AHT": "アシュフォード・ホスピタリティ・トラスト",
        "AIRC": "アパートメント・インカム・REIT",
        "AKR": "アケイディア・リアルティ・トラスト",
        "APLE": "アップル・ホスピタリティ REIT",
        "BNL": "ブロードストーン・ネット・リース",
        "BRT": "BRT アパートメンツ",
        "BRX": "ブリックスモア・プロパティ・グループ",
        "CDP": "コーポレート・オフィス・プロパティーズ",
        "CTRE": "ケアトラスト REIT",
        "CUBE": "キューブスマート",
        "CUZ": "カズンズ・プロパティーズ",
        "DEA": "イースタリー・ガバメント・プロパティーズ",
        "DEI": "ダグラス・エメット",
        "DRH": "ダイヤモンドロック・ホスピタリティ",
        "EGP": "イーストグループ・プロパティーズ",
        "ELME": "エルメ・コミュニティーズ",
        "ELS": "エクイティ・ライフスタイル・プロパティーズ",
        "EPR": "EPR プロパティーズ",
        "EPRT": "エッセンシャル・プロパティーズ・リアルティ",
        "FCPT": "フォー・コーナーズ・プロパティ・トラスト",
        "GLPI": "ゲーミング・アンド・レジャー・プロパティーズ",
        "GTY": "ゲッティ・リアルティ",
        "HASI": "ハノン・アームストロング・サステイナブル・インフラストラクチャー",
        "HIW": "ハイウッズ・プロパティーズ",
        "HPP": "ハドソン・パシフィック・プロパティーズ",
        "HR": "ヘルスケア・リアルティ・トラスト",
        "IIPR": "イノベイティブ・インダストリアル・プロパティーズ",
        "INN": "サミット・ホテル・プロパティーズ",
        "IRT": "インディペンデンス・リアルティ・トラスト",
        "JBGS": "JBG スミス・プロパティーズ",
        "KRC": "カイルロイ・リアルティ",
        "KRG": "カイト・リアルティ・グループ",
        "LAMR": "ラマー・アドバタイジング",
        "LSI": "ライフ・ストレージ",
        "LTC": "LTC プロパティーズ",
        "LXP": "LXP インダストリアル・トラスト",
        "MAC": "メイサリック",
        "NHI": "ナショナル・ヘルス・インベスターズ",
        "NLY": "アナリー・キャピタル・マネジメント",
        "NNN": "ナショナル・リテール・プロパティーズ",
        "NSA": "ナショナル・ストレージ・アフィリエイツ・トラスト",
        "OHI": "オメガ・ヘルスケア・インベスターズ",
        "OUT": "アウトフロント・メディア",
        "PEB": "ペブルブルック・ホテル・トラスト",
        "PK": "パーク・ホテルズ＆リゾーツ",
        "PSB": "PS ビジネス・パークス",
        "RHP": "ライマン・ホスピタリティ・プロパティーズ",
        "RPT": "RPT リアルティ",
        "RYN": "レイオニア",
        "SAFE": "セーフホールド",
        "SBRA": "サブラ・ヘルスケア REIT",
        "SHO": "サンストーン・ホテル・インベスターズ",
        "SLG": "SL グリーン・リアルティ",
        "SRC": "スピリット・リアルティ・キャピタル",
        "STAG": "スタッグ・インダストリアル",
        "STWD": "スターウッド・プロパティ・トラスト",
        "TRNO": "テレーノ・リアルティ",
        "UE": "アーバン・エッジ・プロパティーズ",
        "UNIT": "ユニティ・グループ",
        "VNO": "ボルナド・リアルティ・トラスト",
        "WPC": "W.P. ケアリー",

        # === 持株会社・MLP・複雑な企業構造 (マネックス未カバーが多い) ===
        "AGNC": "AGNC インベストメント",
        "ARES": "エイリーズ・マネジメント",
        "EFC": "エリントン・ファイナンシャル",
        "MFA": "MFA フィナンシャル",
        "PMT": "PennyMac モーゲージ・インベストメント・トラスト",
        "RC": "レディ・キャピタル",
        "REXR": "レックスフォード・インダストリアル・リアルティ",
        "RWT": "レッドウッド・トラスト",
        "TWO": "Two ハーバーズ・インベストメント",

        # === Liberty Media 系・その他デュアルクラス ===
        "LBRDA": "リバティ・ブロードバンド (Class A)",
        "LSXMA": "リバティ・メディア・シリウス XM (Class A)",
        "BATRA": "リバティ・メディア・アトランタ・ブレーブス (Class A)",
        "FWONA": "リバティ・メディア・フォーミュラ・ワン (Class A)",
        "NWSA": "ニューズ・コーポレーション (Class A)",
        "FOXA": "フォックス・コーポレーション (Class A)",

        # === マネックス未カバー: 一般消費財 ===
        "LESL": "レスリーズ",
        "MDC": "MDCホールディングス",
        "MHO": "M/Iホームズ",
        "MODG": "トップゴルフ・キャロウェイ・ブランズ",
        "SKX": "スケッチャーズ",
        "THS": "ツリーハウス・フーズ",
        "TPH": "トライポイント・ホームズ",

        # === マネックス未カバー: 生活必需品 ===
        "SPTN": "スパルタン・ナッシュ",

        # === マネックス未カバー: エネルギー ===
        "DRQ": "ドリル・クイップ",
        "ETRN": "エクイトランス・ミッドストリーム",
        "HFC": "HFシンクレア",
        "PDCE": "PDCエナジー",

        # === マネックス未カバー: 金融 ===
        "ARI": "アポロ・コマーシャル REIT",
        "ATH": "アシーン・ホールディング",
        "CCCS": "CCC インテリジェント・ソリューションズ",
        "CMA": "コメリカ",
        "COOP": "ミスター・クーパー・グループ",
        "DNB": "ダン＆ブラッドストリート",
        "SNV": "シノバス・ファイナンシャル",
        "SPKE": "スパーク・エナジー",

        # === マネックス未カバー: ヘルスケア ===
        "AMED": "アメディシス",

        # === マネックス未カバー: 資本財 ===
        "MOG": "ムーグ",
        "MTOR": "メリター",
        "TGI": "トライアンフ・グループ",
        "TRQ": "ターコイズ・ヒル・リソーシーズ",
        "UFP": "UFPテクノロジーズ",

        # === マネックス未カバー: 情報技術 ===
        "HBI": "ヘインズブランズ",
        "MXIM": "マキシム・インテグレーテッド",
        "PDCO": "パターソン・カンパニーズ",
        "PSTG": "ピュア・ストレージ",
        "SGEN": "シアトル・ジェネティクス",
        "SWI": "ソーラーウィンズ",
        "ZI": "ズームインフォ・テクノロジーズ",

        # === マネックス未カバー: 素材 ===
        "HHC": "ハワード・ヒューズ・ホールディングス",
        "TMST": "ティムケン・スチール",
        "X": "USスチール",

        # === マネックス未カバー: 公益事業 ===
        "SJW": "SJWグループ",

        # === ホテル REIT (全ブローカー未カバー) ===
        "XHR": "ゼニア・ホテルズ＆リゾーツ",
        "RLJ": "RLJロッジング・トラスト",
        "CLDT": "チャタム・ロッジング・トラスト",
        "HT": "ハーシャ・ホスピタリティ・トラスト",
        "SVC": "サービス・プロパティーズ・トラスト",

        # === オフィス・多角型 REIT (全ブローカー未カバー) ===
        "OFC": "コーポレート・オフィス・プロパティーズ・トラスト",
        "PDM": "ピードモント・オフィス・リアルティ・トラスト",
        "ROIC": "リテール・オポチュニティ・インベストメンツ",
        "ABR": "アーバー・リアルティ・トラスト",
        "MPW": "メディカル・プロパティーズ・トラスト",
        "PCH": "ポトラッチデルティック",
        "NRZ": "ニュー・レジデンシャル・インベストメント",
        "RITM": "リズム・キャピタル",
        "NXRT": "ネクスポイント・レジデンシャル・トラスト",
        "GOOD": "グラッドストーン・コマーシャル",
        "LAND": "グラッドストーン・ランド",
        "GNL": "グローバル・ネット・リース",
        "BRSP": "ブライトスピア・キャピタル",
        "DBRG": "デジタルブリッジ・グループ",

        # === エネルギー (全ブローカー未カバー) ===
        "CIVI": "シビタス・リソーシーズ",
        "CPE": "キャロン・ペトロリアム",
        "LPI": "ラレド・ペトロリアム",
        "SM": "SMエナジー",

        # === 資本財・素材 (全ブローカー未カバー) ===
        "SPR": "スピリット・エアロシステムズ",
        "SUM": "サミット・マテリアルズ",
        "BECN": "ビーコン・ルーフィング・サプライ",
        "HI": "ヒレンブランド",
        "ALEX": "アレクサンダー＆ボールドウィン",
        "GOGL": "ゴールデン・オーシャン・グループ",
        "UNVR": "ユニバー・ソリューションズ",
        "PTVE": "パクティブ・エバーグリーン",

        # === 情報技術 (全ブローカー未カバー) ===
        "JNPR": "ジュニパーネットワークス",
        "LNW": "ライト＆ワンダー",
        "PWSC": "パワースクール・ホールディングス",

        # === 金融 (全ブローカー未カバー) ===
        "VGR": "ベクター・グループ",
        "IHC": "インディペンデンス・ホールディング・グループ",
        "SASR": "サンディ・スプリング・バンコープ",
        "HMST": "ホームストリート",
        "LBC": "ルーサー・バーバンク",
        "TPVG": "トリプルポイント・ベンチャー・グロース",

        # === 一般消費財 (全ブローカー未カバー) ===
        "VVI": "バイアド・コープ",
        "FBHS": "フォーチュン・ブランズ・ホーム＆セキュリティ",
        "FBIN": "フォーチュン・ブランズ・イノベーションズ",
    }

def get_ja_translations_csv_map():
    """code/data/ja_translations.csv から {Symbol: Security_JA} を読み込む。

    このCSVは S&P 500 + マネックス・楽天等の主要ブローカー + 手動翻訳を
    マージした単一の翻訳ソースで、ブローカー CSV の取得失敗や Wikipedia の
    新規 S&P 400/600 追加で日本語名が空になるのを防ぐ目的でリポジトリに
    コミットされている。新たに翻訳を追加するときはこの CSV に行を追加するだけで
    良い（Python 側の get_manual_ja_name_map に追記する必要はない）。
    """
    csv_path = os.path.join(BASE_DIR, "data", "ja_translations.csv")
    if not os.path.exists(csv_path):
        return {}
    mapping = {}
    try:
        import csv as _csv
        with open(csv_path, encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            for row in reader:
                sym = (row.get("Symbol") or "").strip()
                ja = (row.get("Security_JA") or "").strip()
                if sym and ja:
                    mapping[sym] = ja
                # Symbol_YF も別キーとして登録（クラス株などのため）
                sym_yf = (row.get("Symbol_YF") or "").strip()
                if sym_yf and ja and sym_yf != sym:
                    mapping.setdefault(sym_yf, ja)
    except Exception as e:
        print(f"Failed to load ja_translations.csv: {e}")
    return mapping


def get_combined_ja_name_map():
    """日本語名マッピングを 3 ソースの優先順位でマージして返す。

    優先順位（後勝ち）:
      1. マネックス CSV (ブローカー実取扱、自動更新)
      2. ja_translations.csv (コミット済みの網羅的マスタ)
      3. get_manual_ja_name_map() (Python ハードコード、最優先)

    通常運用では 2 の CSV に追記して翻訳を増やす。Python 側の手動 dict は
    緊急対応・コードレビューで明示したい銘柄のために残してある。
    """
    monex_mapping = get_monex_available_symbols()
    csv_mapping = get_ja_translations_csv_map()
    manual_mapping = get_manual_ja_name_map()
    return {**monex_mapping, **csv_mapping, **manual_mapping}

def get_rakuten_available_symbols():
    """
    楽天証券の米国株取扱銘柄リストを取得し、シンボルのセットを返します。
    """
    url = "https://www.trkd-asia.com/rakutensec/exportcsvus?all=on&vall=on&forwarding=na&target=0&theme=na&returns=na&head_office=na&name=&code=&sector=na&pageNo=&c=us&p=result&r1=on"
    csv_path = os.path.join(BROKER_LISTS_DIR, "Rakuten_US_LIST.csv")
    
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
    cache_path = os.path.join(BROKER_LISTS_DIR, "SBI_US_LIST.html")
    
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
    csv_path = os.path.join(BROKER_LISTS_DIR, "Mufg_US_LIST.js")
    
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
    csv_path = os.path.join(BROKER_LISTS_DIR, "Matsui_US_LIST.csv")
    
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
    csv_path = os.path.join(BROKER_LISTS_DIR, "Dmm_US_LIST.csv")
    
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
    cache_path = os.path.join(BROKER_LISTS_DIR, "Paypay_US_LIST.txt")
    
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                return set(line.strip() for line in f if line.strip())
        except Exception:
            pass

    if not curl_requests:
        return set()

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

def get_moomoo_available_symbols():
    """
    moomoo証券の米国株取扱銘柄リストをローカルCSVから取得し、シンボルのセットを返します。
    """
    csv_path = os.path.join(BROKER_LISTS_DIR, "moomoo_us_stocks.csv")
    symbols = set()
    
    if not os.path.exists(csv_path):
        print(f"Warning: moomoo stock list not found at {csv_path}")
        return symbols

    try:
        df = pl.read_csv(csv_path)
        if "code" in df.columns:
            for code in df["code"]:
                if code and code.startswith("US."):
                    symbol = code[3:]  # "US.AAPL" -> "AAPL"
                    if symbol:
                        symbols.add(symbol)
    except Exception as e:
        print(f"Error reading moomoo list with Polars: {e}, falling back to csv module")
        try:
            import csv as _csv
            with open(csv_path, newline="", encoding="utf-8") as f:
                reader = _csv.reader(f)
                header = next(reader, [])
                if "code" in header:
                    code_idx = header.index("code")
                    for row in reader:
                        if len(row) > code_idx:
                            code = row[code_idx].strip()
                            if code.startswith("US."):
                                symbol = code[3:]
                                if symbol:
                                    symbols.add(symbol)
        except Exception as e2:
            print(f"Error reading moomoo list with csv module: {e2}")

    return symbols

def get_iwaicosmo_available_symbols():
    """
    岩井コスモ証券の米国株取扱銘柄リストをHTMLスクレイピングで取得し、シンボルのセットを返します。
    """
    url = "https://www.iwaicosmo.co.jp/investment/list/"
    cache_path = os.path.join(BROKER_LISTS_DIR, "IwaiCosmo_US_LIST.html")
    
    # BeautifulSoup をインポート
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("BeautifulSoup4 is not installed. Skipping IwaiCosmo list fetch.")
        return set()

    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8", errors="replace") as f:
                html = f.read()
        except Exception:
            html = None
    else:
        html = None

    if not html:
        if curl_requests:
            try:
                # TLSフィンガープリントをChromeに偽装して取得
                resp = curl_requests.get(url, impersonate="chrome110")
                resp.raise_for_status()
                html = resp.text
                with open(cache_path, "w", encoding="utf-8", errors="replace") as f:
                    f.write(html)
            except Exception as e:
                print(f"Error fetching IwaiCosmo list: {e}")
                return set()
        else:
            return set()

    symbols = set()
    soup = BeautifulSoup(html, "html.parser")
    
    # id="myTable" の tbody 内の 各 tr の 3番目の td がシンボル
    table = soup.find("table", id="myTable")
    if table:
        tbody = table.find("tbody")
        if tbody:
            for tr in tbody.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 3:
                    symbol = tds[2].get_text(strip=True)
                    if symbol and any(c.isalnum() for c in symbol):
                        symbols.add(symbol)
            
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

def _fetch_index_constituents(index_label: str, url: str):
    """Wikipedia から指定された指数 (S&P 500/400/600) の銘柄リストを取得し、
    polars DataFrame を返す（市場情報なしの純粋なリスト）。

    返す DataFrame は以下のカラムを持つ:
      - Symbol (表示用、ドット区切り)
      - Symbol_YF (Yahoo Finance 用、ハイフン区切り)
      - Security
      - GICS Sector
      - GICS Sub-Industry
      - Index (引数の index_label がそのまま入る)
    """
    print(f"{index_label} リストを取得中... URL={url}")
    # Wikipedia の User-Agent ポリシー (https://meta.wikimedia.org/wiki/User-Agent_policy)
    # に従い、ボット名と連絡先 (GitHub repo) を含む説明的な UA を送る。
    # 「Mozilla/5.0」など汎用 UA は CI / クラウド IP からだとブロックされやすい。
    wiki_headers = {
        "User-Agent": (
            "amerikabu-stockfetcher/1.0 "
            "(+https://github.com/sennuki/stock_report) python-requests"
        ),
        "Accept": "text/html,application/xhtml+xml",
    }
    try:
        # 一時的な 429/5xx に備えて指数バックオフでリトライする (合計 3 回)。
        html = None
        for attempt in range(3):
            try:
                resp = requests.get(url, headers=wiki_headers, timeout=30)
                if resp.status_code == 200:
                    html = resp.text
                    break
                print(f"  → HTTP {resp.status_code} (attempt {attempt + 1}/3)")
                if resp.status_code in (403, 451):
                    # 恒久ブロックの可能性が高いのでリトライしない
                    break
            except requests.RequestException as req_err:
                print(f"  → request error (attempt {attempt + 1}/3): {req_err}")
            if attempt < 2:
                import time as _time
                _time.sleep(2 ** attempt)
        if html is None:
            print(f"  ✗ {index_label}: Wikipedia へのリクエストが全試行失敗")
            return pl.DataFrame()

        # Wikipedia の銘柄リストページは通常 id="constituents" の table を持つ。
        # それを優先して取得し、見つからない場合は最初のテーブルにフォールバック。
        wiki_df = None
        try:
            tables = pd.read_html(StringIO(html), attrs={"id": "constituents"})
            if tables:
                wiki_df = tables[0]
                print(f"  → constituents テーブルを検出 ({len(wiki_df)} 行)")
        except Exception as e_inner:
            print(f"  → id=constituents で取得不可 ({e_inner})、最初のテーブルにフォールバック")

        if wiki_df is None:
            tables = pd.read_html(StringIO(html))
            print(f"  → ページ内テーブル数: {len(tables)}")
            # Symbol カラムを持つ最初のテーブルを採用する
            for i, t in enumerate(tables):
                cols = [str(c).strip() for c in t.columns]
                if any(c.lower() in ("symbol", "ticker", "ticker symbol") for c in cols):
                    wiki_df = t
                    print(f"  → table[{i}] を採用 ({len(wiki_df)} 行) cols={cols[:6]}")
                    break

        if wiki_df is None or wiki_df.empty:
            print(f"  ✗ {index_label}: 構成銘柄テーブルが見つかりませんでした")
            return pl.DataFrame()

        # カラム名のゆらぎ吸収（'Ticker symbol' → 'Symbol' など）
        rename_map = {}
        for c in wiki_df.columns:
            cs = str(c).strip()
            if cs.lower() in ("ticker symbol", "ticker"):
                rename_map[c] = "Symbol"
            elif cs.lower() in ("company", "security name"):
                rename_map[c] = "Security"
        if rename_map:
            wiki_df = wiki_df.rename(columns=rename_map)

        required_cols = ["Symbol", "Security", "GICS Sector", "GICS Sub-Industry"]
        missing = [c for c in required_cols if c not in wiki_df.columns]
        if missing:
            print(f"  ✗ {index_label}: 必須カラムが欠けています: {missing} (実際のカラム: {list(wiki_df.columns)[:8]})")
            return pl.DataFrame()

        df = pl.from_pandas(wiki_df).select(required_cols)

        # Symbol_YF: Yahoo Finance用 (ドットをハイフンに変換: BRK.B -> BRK-B)
        # Symbol: 表示用 (ドットに統一: BRK-B -> BRK.B)
        df = df.with_columns([
            pl.col('Symbol').str.replace(r"\.", "-", literal=False).alias('Symbol_YF'),
            pl.col('Symbol').str.replace(r"-", ".", literal=False).alias('Symbol'),
            pl.lit(index_label).alias('Index'),
        ])
        print(f"  ✓ {index_label}: {len(df)} 銘柄を取得")
        return df
    except Exception as e:
        print(f"  ✗ Failed to fetch {index_label} list: {e}")
        return pl.DataFrame()

def _enrich_with_market_info(df):
    """与えられた銘柄リストに Yahoo Finance の市場情報を付与する。

    Exchange, Daily_Change, Security_JA を追加した DataFrame を返す。
    元のカラムはすべて保持される。
    """
    if df.is_empty():
        return df

    # マネックスの日本語名マッピングを取得 (手動補完分を含む)
    ja_name_combined_mapping = get_combined_ja_name_map()

    symbols = df['Symbol_YF'].to_list()
    ex_map = {}
    change_map = {}
    ja_name_map = {}

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

            # 日本語名の紐付け (Yahoo Finance 用シンボル A -> A, BRK-B -> BRK.B など考慮)
            display_symbol = s.replace("-", ".")
            ja_name = ja_name_combined_mapping.get(display_symbol)
            if not ja_name:
                ja_name = ja_name_combined_mapping.get(s)
            ja_name_map[s] = ja_name

    # 未翻訳銘柄の警告ログ（CI 出力で網羅性を確認できるようにする）
    untranslated = [s for s, ja in ja_name_map.items() if not ja]
    if untranslated:
        # 英語名と Index も合わせて出して CSV 追加作業を楽にする
        idx_map = dict(zip(df['Symbol_YF'].to_list(), df['Index'].to_list() if 'Index' in df.columns else [''] * len(df)))
        sec_map = dict(zip(df['Symbol_YF'].to_list(), df['Security'].to_list() if 'Security' in df.columns else [''] * len(df)))
        print(f"\n[WARN] {len(untranslated)} 銘柄に Security_JA が設定されていません。")
        print("  これらは code/data/ja_translations.csv に追記してください:")
        for s in sorted(untranslated):
            print(f"    {s},{s},{sec_map.get(s, '')},,{idx_map.get(s, '')},todo")

    return df.with_columns([
        pl.col('Symbol_YF').map_elements(lambda s: ex_map.get(s, "NYSE"), return_dtype=pl.Utf8).alias('Exchange'),
        pl.col('Symbol_YF').map_elements(lambda s: change_map.get(s), return_dtype=pl.Float64).alias('Daily_Change'),
        pl.col('Symbol_YF').map_elements(lambda s: ja_name_map.get(s), return_dtype=pl.Utf8).alias('Security_JA'),
    ])


# Wikipedia URL 定義 (S&P 500 / 400 / 600)
SP_INDEX_URLS = {
    "S&P 500": "https://en.wikipedia.org/wiki/List_of_S&P_500_companies",
    "S&P 400": "https://en.wikipedia.org/wiki/List_of_S&P_400_companies",
    "S&P 600": "https://en.wikipedia.org/wiki/List_of_S&P_600_companies",
}

# Wikipedia の S&P 構成銘柄表はティッカー変更の反映が遅れることがあるため、
# 既知の変更を取得直後に上書きする。{ 旧ティッカー: 新ティッカー }
# 注意: ドット/ハイフンを含まないティッカーのみ対応 (Symbol と Symbol_YF を
#       同一視して置換する)。
TICKER_OVERRIDES = {
    # Fiserv: 2025-11-11 に NYSE → NASDAQ へ上場替えし、ティッカーが FI → FISV に
    # 戻った (https://mondovisione.com/.../fiserv-...20251029)。Wikipedia 反映が遅れて
    # FI を返してきた場合に備えて FISV に上書きする。
    "FI": "FISV",
}


def fetch_sp_indices_companies(indices=None):
    """S&P 500 / 400 / 600 の銘柄リストを Wikipedia から取得して結合し、
    Yahoo Finance の市場情報を付与した DataFrame を返す。

    引数 indices で対象指数を絞り込める（既定: 3 指数すべて）。
    重複する銘柄（複数指数に跨る場合）は最初に出現する指数のみ採用する。
    """
    if indices is None:
        indices = list(SP_INDEX_URLS.keys())

    frames = []
    for label in indices:
        url = SP_INDEX_URLS.get(label)
        if not url:
            continue
        df_part = _fetch_index_constituents(label, url)
        if not df_part.is_empty():
            frames.append(df_part)

    if not frames:
        return pl.DataFrame()

    # 縦結合 + 重複除去（先勝ち：S&P 500 に含まれていれば 400/600 側は捨てる）
    combined = pl.concat(frames, how='vertical_relaxed').unique(subset=['Symbol_YF'], keep='first')

    # 既知のティッカー変更を上書き（Wikipedia の反映遅延対策）。
    # Exchange は後段の _enrich_with_market_info が Yahoo から再取得するため不要。
    if TICKER_OVERRIDES:
        combined = combined.with_columns([
            pl.col('Symbol_YF').map_elements(
                lambda s: TICKER_OVERRIDES.get(s, s), return_dtype=pl.Utf8),
            pl.col('Symbol').map_elements(
                lambda s: TICKER_OVERRIDES.get(s, s), return_dtype=pl.Utf8),
        ])
        combined = combined.unique(subset=['Symbol_YF'], keep='first')

    return _enrich_with_market_info(combined)


def fetch_sp500_companies_optimized():
    """既存呼び出しとの互換のために残す。S&P 500 のみを返す。"""
    df = _fetch_index_constituents("S&P 500", SP_INDEX_URLS["S&P 500"])
    if df.is_empty():
        return df
    return _enrich_with_market_info(df)

if __name__ == "__main__":
    print("S&P 500/400/600 データの取得テストを実行します...")
    df = fetch_sp_indices_companies()
    print("\n--- S&P 500/400/600 List (First 5 rows) ---")
    print(df.head())
    print("\n--- Index breakdown ---")
    print(df.group_by('Index').len())
    print(f"Total records: {len(df)}")
