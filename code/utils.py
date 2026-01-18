# -*- coding: utf-8 -*-
import yfinance as yf
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_session():
    session = requests.Session()
    # User-Agent偽装 (GitHub Actionsからのアクセスとバレないように)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })
    
    # リトライ設定 (Rate Limit対策)
    retry = Retry(
        total=10,
        backoff_factor=3, # さらに待機時間を増やす (3, 6, 12, 24...)
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def get_ticker(symbol):
    """カスタムセッションを使用してyf.Tickerオブジェクトを生成する"""
    session = get_session()
    return yf.Ticker(symbol, session=session)
