# -*- coding: utf-8 -*-
import os
import json
import time
# from google import genai  # 無効化
from dotenv import load_dotenv
import yfinance as yf
import pandas as pd
import polars as pl

# from google.genai import types  # 無効化
import utils

# .envの読み込み
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

# 2026年時点のモデル設定
GEMINI_MODEL = "gemini-3.1-flash-lite-preview"

# APIクライアントの初期化 (無効化)
def get_gemini_client():
    return None

def generate_stock_movement_reason(symbol, stats, original_reason=""):
    """
    Gemini呼び出しを一時的に無効化しています。
    """
    return None

def fetch_recent_movers(symbols, threshold=0.03):
    """
    リスト内の銘柄から、前日比が閾値以上のものを抽出する。
    """
    results = []
    print(f"Checking {len(symbols)} symbols for significant movements...")
    
    for symbol in symbols:
        try:
            tk = yf.Ticker(symbol)
            hist = tk.history(period="5d")
            if len(hist) < 2:
                continue
                
            last_close = hist['Close'].iloc[-1]
            prev_close = hist['Close'].iloc[-2]
            diff = last_close - prev_close
            diff_pct = diff / prev_close
            
            if abs(diff_pct) >= threshold:
                # 年初来計算
                ytd_start = f"{pd.Timestamp.now().year}-01-01"
                ytd_hist = tk.history(start=ytd_start)
                ytd_pct = (last_close / ytd_hist['Close'].iloc[0]) - 1 if not ytd_hist.empty else 0
                
                results.append({
                    'symbol': symbol,
                    'close': last_close,
                    'diff': diff,
                    'diff_pct': diff_pct,
                    'date': hist.index[-1].strftime('%Y-%m-%d'),
                    'date_ja': hist.index[-1].strftime('%-m月%-d日'),
                    'ytd_pct': ytd_pct
                })
                print(f"Found mover: {symbol} ({diff_pct*100:+.2f}%)")
            
            # 待機時間を短縮（テスト用）
            time.sleep(1)
        except Exception as e:
            print(f"Failed to fetch detailed data for {symbol}: {e}")
            continue
            
    return results
