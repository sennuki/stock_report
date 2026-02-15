import yfinance as yf
import polars as pl
import sys
import os

# codeディレクトリをパスに追加
sys.path.append(os.path.join(os.getcwd(), 'code'))

import fundamentals

def debug_dis():
    ticker = "DIS"
    print(f"--- Debugging {ticker} ---")
    stock = yf.Ticker(ticker)
    
    print("\n[Income Statement (First 5 rows)]")
    try:
        print(stock.income_stmt.head())
    except Exception as e:
        print(f"Failed to get income statement: {e}")
    
    print("\n[Calculated Metrics]")
    try:
        df_metrics = fundamentals.get_financial_metrics(ticker)
        if df_metrics is None or len(df_metrics) == 0:
            print("No metrics calculated (Empty DataFrame)")
        else:
            print(df_metrics)
    except Exception as e:
        print(f"Error in get_financial_metrics: {e}")

if __name__ == "__main__":
    debug_dis()
