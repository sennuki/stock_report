# -*- coding: utf-8 -*-
import yfinance as yf
try:
    from curl_cffi import requests as curl_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False
    
import requests as std_requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import os
import time
import random
import pandas as pd
import datetime
from dotenv import load_dotenv
from google import genai
from google.genai import types
from defeatbeta_api.data.ticker import Ticker as DBTicker
from yfinance.exceptions import YFRateLimitError

# .envファイルを読み込む
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"), override=True)

LOG_FILE = "run_log.txt"

def get_gemini_client():
    """
    最新の google-genai SDK クライアントを初期化して返します。
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        log_event("ERROR", "SYSTEM", "GEMINI_API_KEY not found in environment variables.")
        return None
    
    try:
        # SDK クライアントの初期化
        return genai.Client(api_key=api_key)
    except Exception as e:
        log_event("ERROR", "SYSTEM", f"Failed to initialize Gemini client: {e}")
        return None

# 2026年時点の推奨モデル
DEFAULT_MODEL = "gemini-3.1-flash-lite-preview"

def get_generate_config(thinking_level="MINIMAL"):
    """
    SDK 形式の GenerateContentConfig (ThinkingConfig を含む) を返します。
    """
    return types.GenerateContentConfig(
        thinking_config=types.ThinkingConfig(
            thinking_level=thinking_level,
        ),
    )

# 2026年時点の推奨モデル
# DEFAULT_TRANSLATION_MODEL = "models/gemini-3.1-flash-lite-preview" # 一時的に制限中のため以下を使用
DEFAULT_TRANSLATION_MODEL = "models/gemma-4-26b-a4b-it"

# 互換性のために get_gemini_model も残し、クライアントを返す
def get_gemini_model(model_name=DEFAULT_TRANSLATION_MODEL):
    """
    (旧SDK互換用) クライアントを初期化します。
    """
    return get_gemini_client()

def log_event(category, symbol, message):
    """
    category: "INFO", "SUCCESS", "WARN", "ERROR"
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] [{category:7}] [{symbol:6}] {message}\n"
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_line)

def get_session():
    # 常に新しいセッションを作成するのではなく、可能な限り同じセッションを維持しつつ
    # 最初に Yahoo Finance を訪れてクッキーを確立する
    if HAS_CURL_CFFI:
        session = curl_requests.Session(impersonate="chrome")
    else:
        session = std_requests.Session()
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    
    if not HAS_CURL_CFFI:
        session.headers.update(headers)
        # リトライ設定
        retry = Retry(
            total=5,
            backoff_factor=3,
            status_forcelist=[403, 429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
    
    # プライミング: クッキーとCrumb取得のために一度 Yahoo のトップを訪れる
    try:
        session.get("https://fc.yahoo.com", timeout=10)
        session.get("https://finance.yahoo.com", timeout=10)
    except:
        pass
        
    return session

import re

def format_summary(text):
    """
    翻訳された会社概要に適宜改行・空行を入れて読みやすく整形する。
    """
    if not text:
        return text
    
    # 既存の改行を一旦削除して正規化
    text = text.replace('\n\n', '\n').replace('\n', '')
    
    # 句点で分割（句点を保持）
    sentences = re.split('(?<=。)', text)
    
    formatted_sentences = []
    chunk_size = 0
    for i, s in enumerate(sentences):
        if not s.strip():
            continue
            
        formatted_sentences.append(s)
        chunk_size += len(s)
        
        # 次の文がある場合、改行を入れるか判定
        if i < len(sentences) - 1:
            next_sentence = sentences[i+1].strip()
            should_break = False
            
            # 1. チャンクがある程度の長さ（150文字以上）になった場合、次の文の区切りで改行
            if chunk_size > 150:
                should_break = True
            
            # 2. 明確な接続詞や段落の開始を示すキーワードで始まる場合
            # 文頭にあることを重視
            start_keywords = ["また、", "また ", "さらに", "加えて", "同社は", "同社の", "主要な", "事業は"]
            if any(next_sentence.startswith(word) for word in start_keywords):
                # ただし、直前のチャンクが極端に短い（40文字以下）場合は、細切れ感を防ぐため改行しない
                if chunk_size > 40:
                    should_break = True
                
            if should_break:
                formatted_sentences.append('\n\n')
                chunk_size = 0
            
    return "".join(formatted_sentences).strip()

def calculate_dcf(symbol, ticker=None, yf_info=None, yf_growth_estimates=None):
    """
    詳細なDCF理論株価を計算する。

    将来成長率 (1-5年) は単一指標のハードクリップではなく、複数の成長シグナル
    (EPS 9Y CAGR, Rev/FCF/EBITDA 3Y CAGR, アナリスト LT 予想, Sustainable
    Growth = ROE × (1 - 配当性向)) を R_f〜30% で winsorize した上で median を取る。

    Args:
      symbol: ティッカー。
      ticker: defeatbeta-api の Ticker (任意)。
      yf_info: yfinance.Ticker.info 相当の dict (任意)。
        analyst LT (earningsGrowth) と Sustainable Growth (returnOnEquity,
        payoutRatio) の取得に使う。
      yf_growth_estimates: yfinance.Ticker.growth_estimates 相当の rows list/dict
        (任意)。+5y 期間の stockTrend をアナリスト LT として優先的に使う。
    """
    if ticker is None:
        db_ticker = DBTicker(symbol)
    elif hasattr(ticker, '_db_ticker'):
        db_ticker = ticker._db_ticker
    else:
        db_ticker = ticker
    
    try:
        # 1. WACCと基本データの取得
        df_wacc = db_ticker.wacc()
        if df_wacc.empty:
            return None
        last_wacc_data = df_wacc.iloc[-1]
        
        wacc_details = {
            "wacc": float(last_wacc_data['wacc']),
            "beta": float(last_wacc_data.get('beta_5y', 0)),
            "risk_free_rate": float(last_wacc_data.get('treasure_10y_yield', 0.04)),
            "market_return": float(last_wacc_data.get('sp500_10y_cagr', 0.10)),
            "tax_rate": float(last_wacc_data.get('tax_rate_for_calcs', 0.21)),
            "cost_of_equity": float(last_wacc_data.get('cost_of_equity', 0)),
            "cost_of_debt": float(last_wacc_data.get('cost_of_debt', 0)),
            "weight_of_equity": float(last_wacc_data.get('weight_of_equity', 0)),
            "weight_of_debt": float(last_wacc_data.get('weight_of_debt', 0))
        }
        
        wacc = wacc_details["wacc"]

        # リスクフリーレート: 直近5年の10年国債利回り平均 (Excelの L9 相当)
        try:
            treasure_df = db_ticker.treasure.daily_treasure_yield()
            treasure_df['report_date'] = pd.to_datetime(treasure_df['report_date'])
            five_years_ago = pd.Timestamp.now() - pd.DateOffset(years=5)
            recent_treasure = treasure_df[treasure_df['report_date'] >= five_years_ago]
            risk_free_rate = float(recent_treasure['bc10_year'].mean()) if not recent_treasure.empty else float(last_wacc_data.get('treasure_10y_yield', 0.04))
        except Exception:
            risk_free_rate = float(last_wacc_data.get('treasure_10y_yield', 0.04))
        wacc_details["risk_free_rate"] = risk_free_rate

        # 2. 成長率の取得
        def get_3y_cagr(growth_df):
            """DataFrame の実値列から 3Y CAGR を計算。負値・データ不足は None を返す。"""
            if growth_df is None or growth_df.empty:
                return None
            recent = growth_df.tail(3)
            if len(recent) < 2:
                return None
            metric_col = [c for c in recent.columns
                          if c not in ('symbol', 'report_date', 'yoy_growth')
                          and not c.startswith('prev_year_')]
            if not metric_col:
                return None
            col = metric_col[0]
            v_start = float(recent.iloc[0][col])
            v_end   = float(recent.iloc[-1][col])
            n = len(recent) - 1
            if v_start <= 0 or v_end <= 0:
                return None
            return (v_end / v_start) ** (1.0 / n) - 1

        rev_cagr    = get_3y_cagr(db_ticker.annual_revenue_yoy_growth())
        fcf_cagr    = get_3y_cagr(db_ticker.annual_fcf_yoy_growth())
        ebitda_cagr = get_3y_cagr(db_ticker.annual_ebitda_yoy_growth())
        ni_cagr     = get_3y_cagr(db_ticker.annual_net_income_yoy_growth())

        # EPS 9Y CAGR (defeatbeta Excel 新方式: MIN(MAX(EPS 9Y CAGR, 5%), 20%))
        eps_9y_cagr = None
        try:
            ttm_eps_df = db_ticker.ttm_eps()
            if not ttm_eps_df.empty:
                ttm_eps_df['report_date'] = pd.to_datetime(ttm_eps_df['report_date'])
                ttm_eps_df = ttm_eps_df.sort_values('report_date').reset_index(drop=True)
                eps_col = [c for c in ttm_eps_df.columns
                           if c not in ('symbol', 'report_date')][0]
                valid_eps = ttm_eps_df.dropna(subset=[eps_col])
                valid_eps = valid_eps[valid_eps[eps_col] > 0].reset_index(drop=True)
                if len(valid_eps) >= 2:
                    latest_eps  = float(valid_eps.iloc[-1][eps_col])
                    latest_date = valid_eps.iloc[-1]['report_date']
                    nine_years_ago = latest_date - pd.DateOffset(years=9)
                    historical = valid_eps[valid_eps['report_date'] >= nine_years_ago]
                    if not historical.empty:
                        oldest_eps  = float(historical.iloc[0][eps_col])
                        oldest_date = historical.iloc[0]['report_date']
                        years_diff  = round((latest_date - oldest_date).days / 365.25)
                        if years_diff > 0 and oldest_eps > 0 and latest_eps > 0:
                            eps_9y_cagr = (latest_eps / oldest_eps) ** (1.0 / years_diff) - 1
        except Exception:
            pass

        cagr_details = {
            "revenue":      rev_cagr,
            "fcf":          fcf_cagr,
            "ebitda":       ebitda_cagr,
            "net_income":   ni_cagr,
            "eps_9y_cagr":  eps_9y_cagr,
        }

        # アナリスト LT 予想 (forward-looking)
        # 優先順位: growth_estimates の +5y stockTrend > info.earningsGrowth
        # 前者は 5 年 EPS 成長予想 (LT)、後者は直近 YoY なので近似に過ぎないが
        # 両方欠ける可能性に備えてフォールバックとして使う。
        analyst_lt = None
        if yf_growth_estimates is not None:
            rows = yf_growth_estimates if isinstance(yf_growth_estimates, list) \
                else yf_growth_estimates.get("data") if isinstance(yf_growth_estimates, dict) else None
            if isinstance(rows, list):
                for r in rows:
                    period = str(r.get("period") or r.get("Period") or r.get("index") or "").lower().strip()
                    if period == "+5y":
                        v = r.get("stockTrend") or r.get("StockTrend") or r.get("stocktrend")
                        if isinstance(v, (int, float)) and not pd.isna(v):
                            analyst_lt = float(v)
                        break
        if analyst_lt is None and yf_info is not None:
            v = yf_info.get("earningsGrowth")
            if isinstance(v, (int, float)) and not pd.isna(v):
                analyst_lt = float(v)

        # Sustainable Growth (内部成長率) = ROE × (1 - 配当性向)
        # ファンダ理論的な成長持続可能性指標。balance sheet ベースで forward-looking。
        sustainable = None
        if yf_info is not None:
            roe = yf_info.get("returnOnEquity")
            payout = yf_info.get("payoutRatio")
            if (isinstance(roe, (int, float)) and not pd.isna(roe)
                and isinstance(payout, (int, float)) and not pd.isna(payout)
                and 0 <= payout <= 1):
                sustainable = float(roe) * (1.0 - float(payout))

        # 6 シグナルを集約
        signals_raw = {
            "eps_9y":      eps_9y_cagr,
            "revenue_3y":  rev_cagr,
            "fcf_3y":      fcf_cagr,
            "ebitda_3y":   ebitda_cagr,
            "analyst_5y":  analyst_lt,
            "sustainable": sustainable,
        }

        # 将来成長率 (1-5年): 各シグナルを (R_f, 30%) で winsorize し median を取る。
        # ハードクリップ (5-20%) と異なり 1 つの外れ値で結果が引っ張られないため、
        # EPS 9Y CAGR が一時的に異常値でも他 5 つで補正できる。
        # フロアを R_f に揃えることで 5→6 年目の成長率ジャンプが消えグライドパスが
        # 滑らかになる (永続成長率 = R_f のため)。
        WINSORIZE_CAP = 0.30
        signals_clipped = {
            k: (min(max(v, risk_free_rate), WINSORIZE_CAP) if v is not None else None)
            for k, v in signals_raw.items()
        }
        valid_clipped = [v for v in signals_clipped.values() if v is not None]
        if valid_clipped:
            sorted_v = sorted(valid_clipped)
            n = len(sorted_v)
            growth_1_5y = sorted_v[n // 2] if n % 2 == 1 \
                else (sorted_v[n // 2 - 1] + sorted_v[n // 2]) / 2.0
        else:
            # 何も取れなければ最保守 = リスクフリーレートで成長する想定
            growth_1_5y = risk_free_rate

        # 永続成長率 = 5年平均リスクフリーレート
        # ただし Gordon Growth モデルは WACC > terminal_growth が前提のため、
        # ディフェンシブ銘柄 (低 β + 高負債) で WACC が risk-free rate を
        # 下回るケースでは TV が負になり破綻する。WACC より十分小さく
        # キャップする (最低 1pt のスプレッドを確保)。
        terminal_growth = min(risk_free_rate, max(0.0, wacc - 0.01))
        
        # 3. キャッシュフロー予測
        # 基準 FCF: 直近 1 四半期末の TTM 単一値は運転資本変動などのノイズが
        # 大きいため、直近 3 会計年度の年次 FCF の「中央値」を基準とする
        # (成長率推定が median_of_winsorized なのと手法を揃える)。
        # 年次データが 2 期未満しか取れない場合のみ TTM 値にフォールバック。
        annual_fcfs = []
        try:
            cf = db_ticker.annual_cash_flow()
            cf_df = cf.df() if cf is not None else None
            if cf_df is not None and not cf_df.empty and 'Breakdown' in cf_df.columns:
                fcf_rows = cf_df[cf_df['Breakdown'].astype(str)
                                 .str.contains('Free Cash Flow', case=False, na=False)]
                if not fcf_rows.empty:
                    # 列は年度。新しい順に直近 3 年を採用。
                    date_cols = sorted(
                        (c for c in cf_df.columns if c != 'Breakdown'), reverse=True)
                    for c in date_cols[:3]:
                        v = fcf_rows.iloc[0][c]
                        try:
                            fv = float(str(v).replace(',', '')) if isinstance(v, str) else float(v)
                            if not pd.isna(fv):
                                annual_fcfs.append(fv)
                        except (ValueError, TypeError):
                            pass
        except Exception:
            annual_fcfs = []

        df_ttm_fcf = db_ticker.ttm_fcf()
        ttm_fcf = None
        if df_ttm_fcf is not None and not df_ttm_fcf.empty:
            ttm_fcf = float(df_ttm_fcf.iloc[-1]['ttm_free_cash_flow_usd'])

        if len(annual_fcfs) >= 2:
            base_fcf = float(pd.Series(annual_fcfs).median())
            base_fcf_method = f"median_of_{len(annual_fcfs)}y_annual_fcf"
        elif ttm_fcf is not None:
            base_fcf = ttm_fcf
            base_fcf_method = "ttm_fcf_fallback"
        else:
            return None

        # FCF が継続的にマイナスの企業は DCF / リバース DCF の前提が成立しない
        # (どんな成長率を掛けても将来 FCF は負のまま) ため評価対象外とする。
        if base_fcf <= 0:
            return {
                "dcf_applicable": False,
                "dcf_not_applicable_reason": "negative_normalized_fcf",
                "base_fcf": float(base_fcf),
                "base_fcf_method": base_fcf_method,
                "annual_fcfs": [float(x) for x in annual_fcfs],
                "ttm_fcf": float(ttm_fcf) if ttm_fcf is not None else None,
            }

        projections = []
        current_fcf = base_fcf
        
        # 1-10年目の予測
        for i in range(1, 11):
            if i <= 5 or growth_1_5y <= terminal_growth:
                # 1-5年目は初期成長率。6年目以降も初期成長率 <= 永続成長率なら
                # 漸減ロジックが「加速」になってしまうため、初期成長率を維持する。
                rate = growth_1_5y
            else:
                # 6年目以降はTerminal Growthに向けて線形に漸減させる
                rate = growth_1_5y - (i - 5) * (growth_1_5y - terminal_growth) / 5
            
            current_fcf *= (1 + rate)
            discounted_fcf = current_fcf / ((1 + wacc) ** i)
            projections.append({
                "year": i,
                "fcf": float(current_fcf),
                "discounted_fcf": float(discounted_fcf),
                "growth_rate": float(rate)
            })
        
        # 6年目の成長率を growth_6_10y として保持 (Excel の C13 = C12-(C12-C14)/5 に相当)
        if growth_1_5y <= terminal_growth:
            growth_6_10y = growth_1_5y
        else:
            growth_6_10y = growth_1_5y - (growth_1_5y - terminal_growth) / 5
            
        # 継続価値 (Terminal Value)
        tv = (current_fcf * (1 + terminal_growth)) / (wacc - terminal_growth)
        npv_tv = tv / ((1 + wacc) ** 10)
        
        npv_fcf_sum = sum(p["discounted_fcf"] for p in projections)
        enterprise_value = npv_fcf_sum + npv_tv
        
        # 4. 理論株価の算出
        # 現金及び短期投資
        bs_df = db_ticker.quarterly_balance_sheet().df()
        cash_value = 0
        if not bs_df.empty:
            cash_rows = bs_df[bs_df['Breakdown'].str.contains('Cash, Cash Equivalents & Short Term Investments', na=False)]
            if not cash_rows.empty:
                date_cols = [c for c in bs_df.columns if c != 'Breakdown']
                if date_cols:
                    val = cash_rows.iloc[0][date_cols[0]]
                    try:
                        if isinstance(val, str):
                            cash_value = float(val.replace(',', '')) if val != '*' else 0
                        else:
                            cash_value = float(val) if not pd.isna(val) else 0
                    except: cash_value = 0
        
        # 負債
        total_debt = float(last_wacc_data.get('total_debt_usd', 0))
        
        # 発行済株式数
        mc_df = db_ticker.market_capitalization()
        if mc_df.empty:
            return None
        shares = float(mc_df.iloc[-1]['shares_outstanding'])
        
        equity_value = enterprise_value + cash_value - total_debt
        fair_price = equity_value / shares

        # 現在価格
        price_df = db_ticker.price()
        current_price = float(price_df['close'].iloc[-1]) if not price_df.empty else 0

        # DCF サニティチェック: 計算ロジックが通っても、入力データの不整合や
        # 極端な成長率 / 低 WACC が組み合わさると無意味な fair_price になり得る。
        #   - 負の理論株価: 企業価値 < (負債 - 現金) または TV が負
        #   - 現在価格との極端な乖離: 入力データ品質に問題がある可能性が高い
        # フロントに「割安/割高」を出す材料にならない値はここで dcf_applicable=False
        # に倒して "DCF 評価対象外" の説明文を表示させる。
        FAIR_PRICE_MAX_MULTIPLE = 5.0  # 現在価格の 5 倍を超える理論株価は要警戒
        implausible_reason = None
        if fair_price <= 0:
            implausible_reason = "negative_fair_price"
        elif current_price > 0 and fair_price > current_price * FAIR_PRICE_MAX_MULTIPLE:
            implausible_reason = "implausible_fair_price"
        if implausible_reason is not None:
            return {
                "dcf_applicable": False,
                "dcf_not_applicable_reason": implausible_reason,
                "fair_price_raw": float(fair_price),
                "current_price": float(current_price),
                "wacc_details": wacc_details,
                "cagr_details": {k: (float(v) if v is not None else None) for k, v in cagr_details.items()},
                "growth_1_5y": float(growth_1_5y),
                "terminal_growth": float(terminal_growth),
                "base_fcf": float(base_fcf),
                "base_fcf_method": base_fcf_method,
            }

        # リバースDCF：現在株価から逆算して必要な成長率を計算
        reverse_growth = None
        try:
            current_equity_value = current_price * shares
            current_ev = current_equity_value + total_debt - cash_value

            if current_ev > 0 and base_fcf > 0:
                def calculate_ev_for_growth(g):
                    """指定の成長率で企業価値を計算"""
                    fcf = base_fcf
                    pv_fcf_sum = 0
                    for i in range(1, 11):
                        if i <= 5 or g <= terminal_growth:
                            # g <= 永続成長率なら漸減ロジックが「加速」になるため
                            # 全年で g を維持する (リバース DCF の対称化)。
                            rate = g
                        else:
                            rate = g - (i - 5) * (g - terminal_growth) / 5
                        fcf *= (1 + rate)
                        pv_fcf_sum += fcf / ((1 + wacc) ** i)
                    tv = (fcf * (1 + terminal_growth)) / (wacc - terminal_growth)
                    npv_tv = tv / ((1 + wacc) ** 10)
                    return pv_fcf_sum + npv_tv

                # 二分探索で必要な成長率を求める
                low, high = 0.001, 0.50  # 0.1% ～ 50%
                tolerance = 0.0001
                for _ in range(100):
                    mid = (low + high) / 2
                    ev_mid = calculate_ev_for_growth(mid)
                    if abs(ev_mid - current_ev) < tolerance * current_ev:
                        reverse_growth = mid
                        break
                    elif ev_mid < current_ev:
                        low = mid
                    else:
                        high = mid
                else:
                    reverse_growth = (low + high) / 2
        except Exception as e:
            pass

        return {
            "fair_price": float(fair_price),
            "current_price": float(current_price),
            "enterprise_value": float(enterprise_value),
            "equity_value": float(equity_value),
            "shares": float(shares),
            "cash_value": float(cash_value),
            "total_debt": float(total_debt),
            "wacc_details": wacc_details,
            "cagr_details": {k: (float(v) if v is not None else None) for k, v in cagr_details.items()},
            "projections": projections,
            "terminal_value": float(tv),
            "npv_tv": float(npv_tv),
            "growth_1_5y": float(growth_1_5y),
            "growth_6_10y": float(growth_6_10y),
            "terminal_growth": float(terminal_growth),
            "growth_signals_raw": {k: (float(v) if v is not None else None) for k, v in signals_raw.items()},
            "growth_signals_clipped": {k: (float(v) if v is not None else None) for k, v in signals_clipped.items()},
            "growth_winsorize_bounds": {"floor": float(risk_free_rate), "cap": float(WINSORIZE_CAP)},
            "growth_aggregation": "median_of_winsorized",
            "base_fcf": float(base_fcf),
            "base_fcf_method": base_fcf_method,
            "annual_fcfs": [float(x) for x in annual_fcfs],
            "ttm_fcf": float(ttm_fcf) if ttm_fcf is not None else None,
            "dcf_applicable": True,
            "reverse_growth": float(reverse_growth) if reverse_growth is not None else None
        }
    except Exception as e:
        print(f"Error calculating detailed DCF for {symbol}: {e}")
        return None

_shared_session = None


def _normalize_revenue_df(df: "pd.DataFrame") -> "pd.DataFrame":
    """Strip whitespace from column names and merge any duplicate columns (sum)."""
    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
    if df.columns.duplicated().any():
        id_cols = [c for c in ('symbol', 'report_date') if c in df.columns]
        df = df.T.groupby(level=0).max().T
        others = [c for c in df.columns if c not in id_cols]
        df = df[id_cols + others]
    return df


class YFinanceAdapterTicker:
    def __init__(self, symbol):
        self.ticker = symbol
        self._db_ticker = DBTicker(symbol)
        self._yf_ticker_cached = None

    @property
    def _yf_ticker(self):
        if self._yf_ticker_cached is None:
            global _shared_session
            if _shared_session is None:
                _shared_session = get_session()
            self._yf_ticker_cached = yf.Ticker(self.ticker, session=_shared_session)
        return self._yf_ticker_cached

    def history(self, period="10y", start=None, end=None, **kwargs):
        # 1. Try DB first (defeatbeta_api)
        df = self._db_ticker.price()
        if df is not None and not df.empty:
            df = df.rename(columns={
                'report_date': 'Date',
                'open': 'Open',
                'close': 'Close',
                'high': 'High',
                'low': 'Low',
                'volume': 'Volume'
            })
            df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize('UTC')
            if 'symbol' in df.columns:
                df = df.drop(columns=['symbol'])
            df = df.set_index('Date')
            df = df.sort_index()

            # Trim to requested period if needed
            now = datetime.datetime.now(datetime.timezone.utc)
            if period == '10y':
                start_date = now - datetime.timedelta(days=365 * 10)
                df = df[df.index >= start_date]
            elif period == '1mo':
                start_date = now - datetime.timedelta(days=30)
                df = df[df.index >= start_date]
            return df

        # 2. If DB is empty, use local persistent cache and yfinance (Incremental)
        cache_dir = os.path.join(os.path.dirname(__file__), "data", "price_cache")
        os.makedirs(cache_dir, exist_ok=True)
        # Using .csv for simpler inspection, but .parquet is also an option
        cache_path = os.path.join(cache_dir, f"{self.ticker.replace('^', '_')}.csv")
        # print(f"DEBUG: Using cache path: {cache_path}") # Debug line
        
        cached_df = pd.DataFrame()
        if os.path.exists(cache_path):
            try:
                cached_df = pd.read_csv(cache_path)
                if not cached_df.empty and 'Date' in cached_df.columns:
                    cached_df['Date'] = pd.to_datetime(cached_df['Date'], utc=True)
                    cached_df = cached_df.set_index('Date').sort_index()
            except Exception as e:
                log_event("WARN", self.ticker, f"Failed to load cache: {e}")

        # Determine start date for yfinance fetch
        fetch_start = None
        needs_full_fetch = False
        
        if not cached_df.empty:
            # Check if cache covers the requested period
            cache_start = cached_df.index.min()
            now = datetime.datetime.now(datetime.timezone.utc)
            
            # Map period strings to days for checking
            period_to_days = {
                '1mo': 30, '3mo': 91, '6mo': 182, 'ytd': 365,
                '1y': 365, '2y': 365*2, '5y': 365*5, '10y': 365*10, 'max': 365*50
            }
            required_days = period_to_days.get(period.lower(), 30)
            expected_start = now - datetime.timedelta(days=required_days)
            
            if cache_start > expected_start:
                # Cache doesn't have enough history, need to fetch the whole period
                needs_full_fetch = True
                fetch_start = None
            else:
                # Fetch from last date + 1 day
                last_date = cached_df.index.max()
                if isinstance(last_date, pd.Timestamp):
                    # If last update was today or later, no need to fetch more
                    if last_date.date() >= now.date():
                        return self._trim_period(cached_df, period)
                    fetch_start = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                else:
                    fetch_start = None
        else:
            # No cache, full fetch for the period
            fetch_start = None

        try:
            # If start/end provided as args, override our incremental logic
            s_arg = start if start else fetch_start
            e_arg = end
            
            # yfinance call
            if s_arg and not needs_full_fetch:
                yf_hist = self._yf_ticker.history(start=s_arg, end=e_arg, **kwargs)
            else:
                yf_hist = self._yf_ticker.history(period=period, **kwargs)

            if not yf_hist.empty:
                # Ensure yf_hist index is UTC-aware and has no timezone shift issues during merge
                if yf_hist.index.tz is None:
                    yf_hist.index = yf_hist.index.tz_localize('UTC')
                else:
                    yf_hist.index = yf_hist.index.tz_convert('UTC')

                # Merge with cache
                if not cached_df.empty:
                    # Drop overlapping dates from cache just in case
                    cached_df = cached_df[~cached_df.index.isin(yf_hist.index)]
                    merged_df = pd.concat([cached_df, yf_hist]).sort_index()
                else:
                    merged_df = yf_hist.sort_index()
                
                # Save back to cache (keep full 10y+ in cache)
                merged_df.to_csv(cache_path)
                return self._trim_period(merged_df, period)
            
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error in yfinance history incremental: {e}")

        return self._trim_period(cached_df, period) if not cached_df.empty else pd.DataFrame()

    def _trim_period(self, df, period):
        if df.empty: return df
        now = datetime.datetime.now(datetime.timezone.utc)
        
        # Mapping yfinance period to days
        period_to_days = {
            '1d': 1, '5d': 5, '1mo': 30, '3mo': 91, '6mo': 182, 
            '1y': 365, '2y': 365*2, '5y': 365*5, '10y': 365*10, 'ytd': None
        }
        
        days = period_to_days.get(period.lower())
        if period.lower() == 'ytd':
            # Set to Jan 1st of current year
            start_date = datetime.datetime(now.year, 1, 1, tzinfo=datetime.timezone.utc)
        elif days:
            start_date = now - datetime.timedelta(days=days)
        else:
            # Fallback or 'max'
            return df
            
        return df[df.index >= start_date]

    @property
    def info(self):
        info_dict = {}
        
        # 1. Fetch live info from yfinance first (most up-to-date for price and dynamic metrics)
        try:
            yf_info = self._yf_ticker.info
            if isinstance(yf_info, dict):
                info_dict.update(yf_info)
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error fetching yf_ticker.info: {e}")

        # 2. Merge/Fallback with DB cache for basic profile info if needed
        try:
            info_df = self._db_ticker.info()
            if info_df is not None and not info_df.empty:
                base_info = info_df.iloc[0].to_dict()
                mapping = {
                    'market_capitalization': 'marketCap',
                    'full_time_employees': 'fullTimeEmployees',
                    'web_site': 'website',
                    'company_name': 'shortName',
                    'description': 'longBusinessSummary'  # Defeatbeta uses 'description'
                }
                for k, v in base_info.items():
                    key = mapping.get(k, k)
                    # For business summary, prioritize defeatbeta if available and not empty
                    if key == 'longBusinessSummary' and v:
                        info_dict[key] = v
                    # For others, only overwrite if yfinance didn't provide it or provided None
                    elif info_dict.get(key) is None:
                        info_dict[key] = v
        except: pass
                
        # 3. Dynamic metrics from DB if still missing
        try:
            # If currentPrice is still missing, try DB price cache
            if info_dict.get('currentPrice') is None:
                price_df = self._db_ticker.price()
                if not price_df.empty:
                    last_row = price_df.iloc[-1]
                    info_dict['currentPrice'] = last_row['close']
                    if len(price_df) > 1:
                        info_dict['previousClose'] = price_df.iloc[-2]['close']
            
            # Additional Metrics from DB cache if missing in info_dict
            metrics_map = {
                'trailingPE': self._db_ticker.ttm_pe,
                'trailingEps': self._db_ticker.ttm_eps,
                'returnOnEquity': self._db_ticker.roe,
                'returnOnAssets': self._db_ticker.roa,
                'profitMargins': self._db_ticker.quarterly_net_margin,
                'operatingMargins': self._db_ticker.quarterly_operating_margin
            }
            
            for key, method in metrics_map.items():
                if info_dict.get(key) is None:
                    try:
                        res_df = method()
                        if not res_df.empty:
                            col = res_df.columns[-1]
                            info_dict[key] = res_df.iloc[-1][col]
                    except: pass
            
            # Dividend info
            if info_dict.get('dividendYield') is None:
                divs = self._db_ticker.dividends()
                if not divs.empty and info_dict.get('currentPrice'):
                    last_year_divs = divs.tail(4)['amount'].sum()
                    info_dict['dividendYield'] = last_year_divs / info_dict['currentPrice']
                
            # Balance Sheet metrics
            if info_dict.get('currentRatio') is None or info_dict.get('debtToEquity') is None:
                qbs = self._db_ticker.quarterly_balance_sheet().df()
                if not qbs.empty and qbs.columns[1] != 'Breakdown':
                    latest_col = qbs.columns[1]
                    qbs_dict = qbs[['Breakdown', latest_col]].set_index('Breakdown').to_dict()[latest_col]
                    if info_dict.get('currentRatio') is None:
                        ca = qbs_dict.get('Total Current Assets')
                        cl = qbs_dict.get('Total Current Liabilities')
                        if ca and cl: info_dict['currentRatio'] = ca / cl
                    if info_dict.get('debtToEquity') is None:
                        debt = qbs_dict.get('Total Debt')
                        equity = qbs_dict.get('Total Equity') or qbs_dict.get("Stockholders' Equity")
                        if debt and equity: info_dict['debtToEquity'] = (debt / equity) * 100
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error merging DB metrics: {e}")
            
        return info_dict

    @property
    def earnings_estimate(self):
        try:
            return self._yf_ticker.earnings_estimate
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error in earnings_estimate: {e}")
            return None

    @property
    def revenue_estimate(self):
        try:
            return self._yf_ticker.revenue_estimate
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error in revenue_estimate: {e}")
            return None

    @property
    def eps_trend(self):
        try:
            return self._yf_ticker.eps_trend
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error in eps_trend: {e}")
            return None

    @property
    def eps_revisions(self):
        try:
            return self._yf_ticker.eps_revisions
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error in eps_revisions: {e}")
            return None

    @property
    def recommendations_summary(self):
        try:
            return self._yf_ticker.recommendations_summary
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error in recommendations_summary: {e}")
            return None

    @property
    def upgrades_downgrades(self):
        try:
            return self._yf_ticker.upgrades_downgrades
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error in upgrades_downgrades: {e}")
            return None

    @property
    def earnings_dates(self):
        try:
            # First try the real yfinance data as it contains surprises and estimates
            yf_ed = self._yf_ticker.earnings_dates
            if yf_ed is not None and not yf_ed.empty:
                return yf_ed
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error fetching yfinance earnings_dates: {e}")
            
        # Fallback to DB cache
        try:
            cal_df = self._db_ticker.calendar()
            if cal_df is None or cal_df.empty:
                return None
            cal_df['Earnings Date'] = pd.to_datetime(cal_df['report_date']).dt.tz_localize('UTC')
            cal_df = cal_df.set_index('Earnings Date')
            cal_df = cal_df.sort_index(ascending=False)
            if 'EPS Estimate' not in cal_df.columns: cal_df['EPS Estimate'] = float('nan')
            if 'Reported EPS' not in cal_df.columns: cal_df['Reported EPS'] = float('nan')
            if 'Surprise(%)' not in cal_df.columns: cal_df['Surprise(%)'] = float('nan')
            return cal_df
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error in earnings_dates (DB fallback): {e}")
            return None

    @property
    def calendar(self):
        try:
            return self._yf_ticker.calendar
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error in calendar: {e}")
            return None

    @property
    def dividends(self):
        div_df = self._db_ticker.dividends()
        if div_df is None or div_df.empty:
            return pd.Series(dtype='float64')
            
        div_df['Date'] = pd.to_datetime(div_df['report_date']).dt.tz_localize('UTC')
        div_df = div_df.set_index('Date')
        series = div_df['amount']
        series.name = 'Dividends'
        return series

    @property
    def balancesheet(self):
        try:
            bs = self._db_ticker.annual_balance_sheet()
            if bs and not bs.df().empty:
                df = bs.df()
                if 'Breakdown' in df.columns:
                    return df.set_index('Breakdown')
                return df
        except: pass
        try:
            return self._yf_ticker.balancesheet
        except: return pd.DataFrame()

    @property
    def quarterly_balancesheet(self):
        try:
            bs = self._db_ticker.quarterly_balance_sheet()
            if bs and not bs.df().empty:
                df = bs.df()
                if 'Breakdown' in df.columns:
                    return df.set_index('Breakdown')
                return df
        except: pass
        try:
            return self._yf_ticker.quarterly_balancesheet
        except: return pd.DataFrame()

    @property
    def income_stmt(self):
        try:
            is_stmt = self._db_ticker.annual_income_statement()
            if is_stmt and not is_stmt.df().empty:
                df = is_stmt.df()
                if 'Breakdown' in df.columns:
                    return df.set_index('Breakdown')
                return df
        except: pass
        try:
            return self._yf_ticker.income_stmt
        except: return pd.DataFrame()

    @property
    def quarterly_income_stmt(self):
        try:
            is_stmt = self._db_ticker.quarterly_income_statement()
            if is_stmt and not is_stmt.df().empty:
                df = is_stmt.df()
                if 'Breakdown' in df.columns:
                    return df.set_index('Breakdown')
                return df
        except: pass
        try:
            return self._yf_ticker.quarterly_income_stmt
        except: return pd.DataFrame()

    @property
    def cashflow(self):
        try:
            cf = self._db_ticker.annual_cash_flow()
            if cf and not cf.df().empty:
                df = cf.df()
                if 'Breakdown' in df.columns:
                    return df.set_index('Breakdown')
                return df
        except: pass
        try:
            return self._yf_ticker.cashflow
        except: return pd.DataFrame()
        
    @property
    def quarterly_cashflow(self):
        try:
            cf = self._db_ticker.quarterly_cash_flow()
            if cf and not cf.df().empty:
                df = cf.df()
                if 'Breakdown' in df.columns:
                    return df.set_index('Breakdown')
                return df
        except: pass
        try:
            return self._yf_ticker.quarterly_cashflow
        except: return pd.DataFrame()

    def revenue_by_segment(self):
        try:
            df = self._db_ticker.revenue_by_segment()
            if df is not None and not df.empty:
                df = _normalize_revenue_df(df)
            return df
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error in revenue_by_segment: {e}")
            return pd.DataFrame()

    _GEO_NORM = {
        'United States': 'US', 'USA': 'US', 'U.S.A.': 'US', 'U.S.': 'US',
        'Total US': 'US', 'Total United States': 'US', 'Domestic': 'US',
        'Outside United States': 'International', 'Foreign': 'International',
    }

    def revenue_by_geography(self):
        try:
            df = self._db_ticker.revenue_by_geography()
            if df is not None and not df.empty:
                df.columns = [self._GEO_NORM.get(c.strip(), c.strip()) if isinstance(c, str) else c
                              for c in df.columns]
                df = _normalize_revenue_df(df)
            return df
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error in revenue_by_geography: {e}")
            return pd.DataFrame()

def get_ticker(symbol):
    """
    Returns a ticker object. Currently switched to use Defeat Beta API adapter.
    """
    return YFinanceAdapterTicker(symbol)

def safe_get(ticker_obj, attr_name, default=None, max_retries=3):
    """
    Safely access yfinance Ticker properties with retries and throttling.
    """
    symbol = getattr(ticker_obj, 'ticker', 'Unknown')
    
    # 連続リクエストを避けるための微小なスロットリング
    time.sleep(random.uniform(0.1, 0.3))
    
    for attempt in range(max_retries):
        try:
            val = getattr(ticker_obj, attr_name, None)
            if val is not None:
                # If it's a dataframe, check if it's empty
                if hasattr(val, 'empty') and val.empty:
                    return default
                return val
            return default
        except YFRateLimitError:
            wait_time = (attempt + 1) * 15 + random.uniform(0, 10)
            log_event("WARN", symbol, f"Rate limited on {attr_name}. Waiting {wait_time:.1f}s (Attempt {attempt+1}/{max_retries})")
            time.sleep(wait_time)
        except Exception as e:
            err_str = str(e)
            if "Too Many Requests" in err_str or "429" in err_str or "Rate limited" in err_str:
                wait_time = (attempt + 1) * 15 + random.uniform(0, 10)
                log_event("WARN", symbol, f"429 error on {attr_name}. Waiting {wait_time:.1f}s (Attempt {attempt+1}/{max_retries})")
                time.sleep(wait_time)
                continue
            
            # 404などはリトライせずスキップ
            # yfinance internally might print "404 Not Found" but not raise Exception for some properties
            log_event("DEBUG", symbol, f"Failed to get {attr_name}: {e}")
            break
            
    return default

def safe_call(ticker_obj, method_name, *args, **kwargs):
    """
    Safely call yfinance Ticker methods with retries and throttling.
    """
    symbol = getattr(ticker_obj, 'ticker', 'Unknown')
    # Extract max_retries if present, default to 3
    # Use a copy to avoid modifying kwargs if it's reused
    retries = kwargs.pop('max_retries', 3)
    
    # 連続リクエストを避けるための微小なスロットリング
    time.sleep(random.uniform(0.1, 0.3))
    
    for attempt in range(retries):
        try:
            method = getattr(ticker_obj, method_name)
            return method(*args, **kwargs)
        except YFRateLimitError:
            wait_time = (attempt + 1) * 20 + random.uniform(0, 10)
            log_event("WARN", symbol, f"Rate limited on {method_name}. Waiting {wait_time:.1f}s (Attempt {attempt+1}/{retries})")
            time.sleep(wait_time)
        except Exception as e:
            err_str = str(e)
            if "Too Many Requests" in err_str or "429" in err_str or "Rate limited" in err_str:
                wait_time = (attempt + 1) * 20 + random.uniform(0, 10)
                log_event("WARN", symbol, f"429 error on {method_name}. Waiting {wait_time:.1f}s (Attempt {attempt+1}/{retries})")
                time.sleep(wait_time)
                continue
            
            # その他のエラーはログに記録して再スロー
            log_event("ERROR", symbol, f"Error calling {method_name}: {e}")
            raise e
            
    return None
