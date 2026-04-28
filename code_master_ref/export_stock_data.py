# -*- coding: utf-8 -*-
import sys
import json
import os
import pandas as pd
import numpy as np

# 出力を抑制してASCIIアートなどを消す
import io
original_stdout = sys.stdout
sys.stdout = io.StringIO()

import utils
from defeatbeta_api.data.ticker import Ticker as DBTicker

def format_df_to_dict(df):
    if df is None or df.empty:
        return None
    return json.loads(df.to_json(orient='split', date_format='iso'))

def main(symbol):
    try:
        db_ticker = DBTicker(symbol)
        
        # 1. 基本的な財務データ
        is_annual = db_ticker.annual_income_statement().df()
        bs_annual = db_ticker.annual_balance_sheet().df()
        cf_annual = db_ticker.annual_cash_flow().df()
        
        # 2. セグメント・地域別収益
        rev_segment = db_ticker.revenue_by_segment()
        rev_geo = db_ticker.revenue_by_geography()
        
        # 3. DCF計算 (utils.py のロジックを使用)
        dcf_data = utils.calculate_dcf(symbol, ticker=db_ticker)
        
        # 結果の集約
        data = {
            "symbol": symbol,
            "financials": {
                "income_statement": format_df_to_dict(is_annual),
                "balance_sheet": format_df_to_dict(bs_annual),
                "cash_flow": format_df_to_dict(cf_annual)
            },
            "segments": format_df_to_dict(rev_segment),
            "geography": format_df_to_dict(rev_geo),
            "dcf": dcf_data
        }
        
        # 抑制していた出力を元に戻し、JSONのみを出力
        sys.stdout = original_stdout
        print(json.dumps(data, ensure_ascii=False))

    except Exception as e:
        sys.stdout = original_stdout
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    main(sys.argv[1])
