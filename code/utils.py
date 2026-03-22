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
from defeatbeta_api.data.ticker import Ticker as DBTicker
from yfinance.exceptions import YFRateLimitError

LOG_FILE = "run_log.txt"

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
        df = self._db_ticker.price()
        if df is None or df.empty:
            return pd.DataFrame()
        
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

        now = datetime.datetime.now(datetime.timezone.utc)
        if period == '10y':
            start_date = now - datetime.timedelta(days=365 * 10)
            df = df[df.index >= start_date]
        elif period == '1mo':
            start_date = now - datetime.timedelta(days=30)
            df = df[df.index >= start_date]

        return df

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
                    'company_name': 'shortName'
                }
                for k, v in base_info.items():
                    key = mapping.get(k, k)
                    # Only overwrite if yfinance didn't provide it or provided None
                    if info_dict.get(key) is None:
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
        bs = self._db_ticker.annual_balance_sheet()
        return bs.df().set_index('Breakdown') if bs else pd.DataFrame()

    @property
    def quarterly_balancesheet(self):
        bs = self._db_ticker.quarterly_balance_sheet()
        return bs.df().set_index('Breakdown') if bs else pd.DataFrame()

    @property
    def income_stmt(self):
        is_stmt = self._db_ticker.annual_income_statement()
        return is_stmt.df().set_index('Breakdown') if is_stmt else pd.DataFrame()

    @property
    def quarterly_income_stmt(self):
        is_stmt = self._db_ticker.quarterly_income_statement()
        return is_stmt.df().set_index('Breakdown') if is_stmt else pd.DataFrame()

    @property
    def cashflow(self):
        cf = self._db_ticker.annual_cash_flow()
        return cf.df().set_index('Breakdown') if cf else pd.DataFrame()
        
    @property
    def quarterly_cashflow(self):
        cf = self._db_ticker.quarterly_cash_flow()
        return cf.df().set_index('Breakdown') if cf else pd.DataFrame()

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
