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
from datetime import datetime
from yfinance.exceptions import YFRateLimitError

LOG_FILE = "run_log.txt"

def log_event(category, symbol, message):
    """
    category: "INFO", "SUCCESS", "WARN", "ERROR"
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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

def get_ticker(symbol):
    global _shared_session
    if _shared_session is None:
        _shared_session = get_session()
    return yf.Ticker(symbol, session=_shared_session)

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
