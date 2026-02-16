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
from datetime import datetime

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
            backoff_factor=2,
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