import sys
import os
# codeディレクトリをパスに追加
sys.path.append(os.path.join(os.getcwd(), "code"))

import pandas as pd
from utils import YFinanceAdapterTicker

def test_switch():
    symbol = "MSFT"
    print(f"--- Testing Defeat Beta API Adapter for {symbol} ---")
    
    try:
        ticker = YFinanceAdapterTicker(symbol)
        
        # 1. Test history()
        print("\n[1/3] Testing history()...")
        hist = ticker.history(period="1mo")
        if not hist.empty:
            print(f"Successfully fetched {len(hist)} rows of history data.")
            print(hist.tail(3))
        else:
            print("Failed: history data is empty.")
            
        # 2. Test info property
        print("\n[2/3] Testing info property...")
        info = ticker.info
        if info:
            print(f"Successfully fetched info. Keys found: {list(info.keys())[:5]}...")
            print(f"Current Price: {info.get('currentPrice')}")
            print(f"Short Name: {info.get('shortName')}")
        else:
            print("Failed: info is empty.")
            
        # 3. Test dividends
        print("\n[3/3] Testing dividends...")
        divs = ticker.dividends
        if not divs.empty:
            print(f"Successfully fetched {len(divs)} dividend records.")
            print(divs.tail(3))
        else:
            print("No dividend data found or failed to fetch.")
            
    except Exception as e:
        print(f"An error occurred during testing: {e}")

if __name__ == "__main__":
    test_switch()
