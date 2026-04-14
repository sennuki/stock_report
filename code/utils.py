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
from defeatbeta_api.data.ticker import Ticker as DBTicker
from yfinance.exceptions import YFRateLimitError

# .envファイルを読み込む
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

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
        client = genai.Client(api_key=api_key)
        return client
    except Exception as e:
        log_event("ERROR", "SYSTEM", f"Failed to initialize Gemini client: {e}")
        return None

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

def calculate_dcf(symbol, ticker=None):
    """
    defeatbeta-apiのロジックに基づいて詳細なDCF理論株価を計算する。
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
        risk_free_rate = wacc_details["risk_free_rate"]
        
        # 2. 成長率の取得 (3Y CAGR)
        def get_cagr(growth_df):
            if growth_df is None or growth_df.empty: return 0
            return float(growth_df['yoy_growth'].tail(3).mean())

        rev_cagr = get_cagr(db_ticker.annual_revenue_yoy_growth())
        fcf_cagr = get_cagr(db_ticker.annual_fcf_yoy_growth())
        ebitda_cagr = get_cagr(db_ticker.annual_ebitda_yoy_growth())
        ni_cagr = get_cagr(db_ticker.annual_net_income_yoy_growth())
        
        cagr_details = {
            "revenue": rev_cagr,
            "fcf": fcf_cagr,
            "ebitda": ebitda_cagr,
            "net_income": ni_cagr
        }
        
        # 将来成長率 (1-5年) - defeatbetaの重み付け
        growth_1_5y = (rev_cagr * 0.4 + fcf_cagr * 0.3 + ebitda_cagr * 0.2 + ni_cagr * 0.1)
        
        # 将来成長率 (6-10年) - Decay Factor 0.9
        decay_factor = 0.9
        growth_6_10y = max(growth_1_5y * (decay_factor ** 5), risk_free_rate)
        
        # 永続成長率
        terminal_growth = risk_free_rate
        
        # 3. キャッシュフロー予測
        df_ttm_fcf = db_ticker.ttm_fcf()
        if df_ttm_fcf.empty:
            return None
        base_fcf = float(df_ttm_fcf.iloc[-1]['ttm_free_cash_flow_usd'])
        
        projections = []
        current_fcf = base_fcf
        
        # 1-10年目の予測
        for i in range(1, 11):
            rate = growth_1_5y if i <= 5 else growth_6_10y
            current_fcf *= (1 + rate)
            discounted_fcf = current_fcf / ((1 + wacc) ** i)
            projections.append({
                "year": i,
                "fcf": float(current_fcf),
                "discounted_fcf": float(discounted_fcf),
                "growth_rate": float(rate)
            })
            
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
        
        return {
            "fair_price": float(fair_price),
            "current_price": float(current_price),
            "enterprise_value": float(enterprise_value),
            "equity_value": float(equity_value),
            "shares": float(shares),
            "cash_value": float(cash_value),
            "total_debt": float(total_debt),
            "wacc_details": wacc_details,
            "cagr_details": cagr_details,
            "projections": projections,
            "terminal_value": float(tv),
            "npv_tv": float(npv_tv),
            "growth_1_5y": float(growth_1_5y),
            "growth_6_10y": float(growth_6_10y),
            "terminal_growth": float(terminal_growth),
            "base_fcf": float(base_fcf)
        }
    except Exception as e:
        print(f"Error calculating detailed DCF for {symbol}: {e}")
        return None

_shared_session = None


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
                # Strip whitespace from column names
                df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
                # Merge duplicate columns by taking the maximum value (to avoid double-counting)
                if df.columns.duplicated().any():
                    # Preserve 'symbol' and 'report_date' which shouldn't be duplicated in a way that needs max
                    # but if they are, groupby will handle them.
                    # We want to group by columns and take the max for each row.
                    df = df.T.groupby(level=0).max().T
                    # Reorder columns to put symbol and report_date first if they exist
                    cols = df.columns.tolist()
                    if 'symbol' in cols and 'report_date' in cols:
                        cols.remove('symbol')
                        cols.remove('report_date')
                        df = df[['symbol', 'report_date'] + cols]
            return df
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error in revenue_by_segment: {e}")
            return pd.DataFrame()

    def revenue_by_product(self):
        try:
            df = self._db_ticker.revenue_by_product()
            if df is not None and not df.empty:
                # Strip whitespace from column names
                df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
                # Merge duplicate columns by taking the maximum value (to avoid double-counting)
                if df.columns.duplicated().any():
                    df = df.T.groupby(level=0).max().T
                    # Reorder columns to put symbol and report_date first if they exist
                    cols = df.columns.tolist()
                    if 'symbol' in cols and 'report_date' in cols:
                        cols.remove('symbol')
                        cols.remove('report_date')
                        df = df[['symbol', 'report_date'] + cols]
            return df
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error in revenue_by_product: {e}")
            return pd.DataFrame()

    def revenue_by_geography(self):
        try:
            df = self._db_ticker.revenue_by_geography()
            if df is not None and not df.empty:
                # Normalization mapping
                norm_map = {
                    'United States': 'US',
                    'USA': 'US',
                    'U.S.A.': 'US',
                    'U.S.': 'US',
                    'Total US': 'US',
                    'Total United States': 'US',
                    'International': 'International',
                    'Outside United States': 'International',
                    'Foreign': 'International',
                    'Domestic': 'US',
                }
                # Normalize column names
                df.columns = [norm_map.get(c.strip(), c.strip()) if isinstance(c, str) else c for c in df.columns]
                # Merge duplicate columns by taking the maximum value (to avoid double-counting)
                if df.columns.duplicated().any():
                    # Preserve 'symbol' and 'report_date' which shouldn't be duplicated in a way that needs max
                    # but if they are, groupby will handle them.
                    # We want to group by columns and take the max for each row.
                    df = df.T.groupby(level=0).max().T
                    # Reorder columns to put symbol and report_date first if they exist
                    cols = df.columns.tolist()
                    if 'symbol' in cols and 'report_date' in cols:
                        cols.remove('symbol')
                        cols.remove('report_date')
                        df = df[['symbol', 'report_date'] + cols]
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
