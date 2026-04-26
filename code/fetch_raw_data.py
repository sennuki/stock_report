# -*- coding: utf-8 -*-
import os
import json
import time
import pandas as pd
import yfinance as yf
import polars as pl
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import utils
import market_data
from defeatbeta_api.data.ticker import Ticker as DBTicker

# 保存先ディレクトリ
RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), "raw_data")
if not os.path.exists(RAW_DATA_DIR):
    os.makedirs(RAW_DATA_DIR)

def clean_value(v):
    """NaN や Inf を None (null) に変換し、Timestampなどを文字列に変換する"""
    if isinstance(v, (float, np.float64, np.float32)):
        if np.isnan(v) or np.isinf(v):
            return None
    if isinstance(v, (pd.Timestamp, pd.DatetimeIndex)):
        return str(v)
    return v

def stringify_keys_and_clean(d):
    """辞書のキーを文字列にし、値をJSONセーフにする"""
    if isinstance(d, dict):
        return {str(k): stringify_keys_and_clean(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [stringify_keys_and_clean(i) for i in d]
    else:
        return clean_value(d)

def df_to_dict_safe(df):
    """Pandas/Polars DataFrameをJSONシリアライズ可能な辞書に変換"""
    if df is None or (hasattr(df, 'empty') and df.empty):
        return None
    try:
        if isinstance(df, pd.DataFrame):
            df_copy = df.copy()
            if isinstance(df_copy.columns, pd.DatetimeIndex):
                df_copy.columns = df_copy.columns.strftime('%Y-%m-%d')
            data = df_copy.reset_index().to_dict(orient='records')
            return stringify_keys_and_clean(data)
        if isinstance(df, pl.DataFrame):
            return stringify_keys_and_clean(df.to_dicts())
    except Exception as e:
        print(f"Conversion error: {e}")
    return None

def fetch_raw_data_for_ticker(symbol):
    """
    1銘柄の生データを yfinance と defeatbeta-api から取得
    """
    try:
        ticker = yf.Ticker(symbol)
        
        raw_payload = {
            "symbol": symbol,
            "info": stringify_keys_and_clean(ticker.info),
            "history": df_to_dict_safe(ticker.history(period="10y")),
            "income_stmt": df_to_dict_safe(ticker.income_stmt),
            "balancesheet": df_to_dict_safe(ticker.balance_sheet),
            "cashflow": df_to_dict_safe(ticker.cashflow),
            "quarterly_income_stmt": df_to_dict_safe(ticker.quarterly_income_stmt),
            "quarterly_balancesheet": df_to_dict_safe(ticker.quarterly_balance_sheet),
            "quarterly_cashflow": df_to_dict_safe(ticker.quarterly_cashflow),
            "earnings_dates": df_to_dict_safe(ticker.earnings_dates),
            "calendar": stringify_keys_and_clean(ticker.calendar) if ticker.calendar is not None else None,
            "analyst_ratings": df_to_dict_safe(ticker.recommendations_summary),
            "upgrades_downgrades": df_to_dict_safe(ticker.upgrades_downgrades)
        }

        try:
            db_ticker = DBTicker(symbol)
            raw_payload["dcf_valuation"] = utils.calculate_dcf(symbol, ticker=db_ticker)
            try:
                raw_payload["db_metrics"] = {
                    "wacc": df_to_dict_safe(db_ticker.wacc()),
                    "revenue_growth": df_to_dict_safe(db_ticker.annual_revenue_yoy_growth()),
                    "fcf_growth": df_to_dict_safe(db_ticker.annual_fcf_yoy_growth())
                }
            except:
                raw_payload["db_metrics"] = None
        except Exception as e:
            raw_payload["dcf_valuation"] = None

        file_path = os.path.join(RAW_DATA_DIR, f"{symbol}_raw.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(raw_payload, f, ensure_ascii=False, default=str)
        
        return True
    except Exception as e:
        print(f"Failed to fetch {symbol}: {e}")
        return False

def main():
    import sys
    # 引数から銘柄を取得
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    
    if args:
        print(f"Fetching specific symbols: {args}")
        symbols = args
    elif "--test-msft-only" in sys.argv or os.getenv("TEST_MODE") == "true":
        print("Test mode active: Fetching MSFT only.")
        symbols = ["MSFT"]
    else:
        print("Fetching S&P 500 list...")
        df_sp500 = market_data.fetch_sp500_companies_optimized()
        if df_sp500.is_empty():
            symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
        else:
            symbols = df_sp500['Symbol_YF'].to_list()
    
    max_workers = 1 if len(symbols) <= 3 else int(os.getenv("MAX_WORKERS", 2))
    
    if max_workers == 1:
        for s in symbols:
            print(f"Processing {s}...")
            fetch_raw_data_for_ticker(s)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_raw_data_for_ticker, s): s for s in symbols}
            for future in tqdm(as_completed(futures), total=len(symbols)):
                try:
                    future.result()
                except: pass

if __name__ == "__main__":
    main()
