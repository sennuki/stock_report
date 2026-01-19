# -*- coding: utf-8 -*-
import yfinance as yf
try:
    from curl_cffi import requests as curl_requests
    # HAS_CURL_CFFI = True
    # Temporary disable curl_cffi to test if standard requests works better with current yfinance
    HAS_CURL_CFFI = False
except ImportError:
    HAS_CURL_CFFI = False
    
import requests as std_requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_session():
    if HAS_CURL_CFFI:
        # curl_cffiが使える場合はそれを使う (yfinanceの推奨)
        # impersonate="chrome" でブラウザ偽装
        # curl_cffiのSessionは標準requestsと完全互換ではないためmountなどは避ける
        session = curl_requests.Session(impersonate="chrome")
        return session
    else:
        # フォールバック (従来のrequests)
        session = std_requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        
        # リトライ設定
        retry = Retry(
            total=10,
            backoff_factor=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

def get_ticker(symbol):
    # session = get_session()
    # Let yfinance handle session management automatically
    return yf.Ticker(symbol)