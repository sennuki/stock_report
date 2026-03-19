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
        
        # Basic Info
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
                info_dict[mapping.get(k, k)] = v
                
        # Fundamentals (Current Price & Market Cap are often dynamic, using last known)
        price_df = self._db_ticker.price()
        if not price_df.empty:
            last_row = price_df.iloc[-1]
            info_dict['currentPrice'] = last_row['close']
            if len(price_df) > 1:
                info_dict['previousClose'] = price_df.iloc[-2]['close']
                
        # Additional Metrics via specific methods
        try:
            pe_df = self._db_ticker.ttm_pe()
            if not pe_df.empty: info_dict['trailingPE'] = pe_df.iloc[-1]['ttm_pe']
            
            eps_df = self._db_ticker.ttm_eps()
            if not eps_df.empty: info_dict['trailingEps'] = eps_df.iloc[-1]['tailing_eps']
            
            roe_df = self._db_ticker.roe()
            if not roe_df.empty: info_dict['returnOnEquity'] = roe_df.iloc[-1]['roe']
            
            roa_df = self._db_ticker.roa()
            if not roa_df.empty: info_dict['returnOnAssets'] = roa_df.iloc[-1]['roa']
            
            nm_df = self._db_ticker.quarterly_net_margin()
            if not nm_df.empty: info_dict['profitMargins'] = nm_df.iloc[-1]['net_margin']
            
            om_df = self._db_ticker.quarterly_operating_margin()
            if not om_df.empty: info_dict['operatingMargins'] = om_df.iloc[-1]['operating_margin']
            
            divs = self._db_ticker.dividends()
            if not divs.empty and 'currentPrice' in info_dict:
                last_year_divs = divs.tail(4)['amount'].sum()
                info_dict['dividendYield'] = last_year_divs / info_dict['currentPrice']
                
            # For Current Ratio & Debt to Equity, we can use quarterly_balance_sheet
            qbs = self._db_ticker.quarterly_balance_sheet().df()
            if not qbs.empty and qbs.columns[1] != 'Breakdown':
                latest_col = qbs.columns[1]
                qbs_dict = qbs[['Breakdown', latest_col]].set_index('Breakdown').to_dict()[latest_col]
                
                ca = qbs_dict.get('Total Current Assets')
                cl = qbs_dict.get('Total Current Liabilities')
                if ca and cl: info_dict['currentRatio'] = ca / cl
                
                debt = qbs_dict.get('Total Debt')
                equity = qbs_dict.get('Total Equity') or qbs_dict.get("Stockholders' Equity")
                if debt and equity: info_dict['debtToEquity'] = (debt / equity) * 100
                
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error fetching extra info: {e}")
            
        return info_dict

    @property
    def earnings_dates(self):
        try:
            cal_df = self._db_ticker.calendar()
            if cal_df is None or cal_df.empty:
                return None
            
            cal_df['Earnings Date'] = pd.to_datetime(cal_df['report_date']).dt.tz_localize('UTC')
            cal_df = cal_df.set_index('Earnings Date')
            cal_df = cal_df.sort_index(ascending=False)
            
            cal_df['EPS Estimate'] = float('nan')
            cal_df['Reported EPS'] = float('nan')
            cal_df['Surprise(%)'] = float('nan')
            
            return cal_df
        except Exception as e:
            log_event("DEBUG", self.ticker, f"Error in earnings_dates: {e}")
            return None

    @property
    def dividends(self):
        div_df = self._db_ticker.dividends()
        if div_df is None or div_df.empty:
            return pd.Series(dtype='float64')
            
        div_df['Date'] = pd.to_datetime(div_df['report_date']).dt.tz_localize('UTC')
        div_df = div_df.set_index('Date')
        return div_df['amount']

    @property
    def cashflow(self):
        cf = self._db_ticker.annual_cash_flow()
        if cf:
             df = cf.df()
             if not df.empty:
                 df = df.set_index('Breakdown')
                 return df
        return pd.DataFrame()
        
    @property
    def quarterly_cashflow(self):
        cf = self._db_ticker.quarterly_cash_flow()
        if cf:
             df = cf.df()
             if not df.empty:
                 df = df.set_index('Breakdown')
                 return df
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
