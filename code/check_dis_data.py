import yfinance as yf
import polars as pl
import pandas as pd
from io import StringIO
import sys
import os

sys.path.append(os.path.join(os.getcwd(), 'code'))

def check_dis_details():
    ticker = "DIS"
    stock = yf.Ticker(ticker)
    
    # 1. IS check
    target_is = ['Total Revenue', 'Gross Profit', 'Operating Income', 'Net Income']
    is_df = stock.income_stmt
    print("\n--- Income Statement ---")
    if is_df is not None and not is_df.empty:
        available_is = [t for t in target_is if t in is_df.index]
        print(f"Available targets: {available_is}")
        print(is_df.loc[available_is])
    else:
        print("Income Statement is Empty")

    # 2. CF check
    target_cf = ['Operating Cash Flow', 'Investing Cash Flow', 'Financing Cash Flow', 'Free Cash Flow']
    cf_df = stock.cashflow
    print("\n--- Cash Flow ---")
    if cf_df is not None and not cf_df.empty:
        available_cf = [t for t in target_cf if t in cf_df.index]
        print(f"Available targets: {available_cf}")
        print(cf_df.loc[available_cf])
    else:
        print("Cash Flow is Empty")

    # 3. TP check (Payout Ratio)
    target_tp = ['Net Income From Continuing Operations', 'Repurchase Of Capital Stock', 'Cash Dividends Paid']
    print("\n--- Payout Ratio Items ---")
    if cf_df is not None and not cf_df.empty:
        available_tp = [t for t in target_tp if t in cf_df.index]
        print(f"Available targets in CF: {available_tp}")
        print(cf_df.loc[available_tp])

if __name__ == "__main__":
    check_dis_details()
