# -*- coding: utf-8 -*-
import sys
import json
import os
import pandas as pd
import numpy as np
from decimal import Decimal

# JSON encoder for Decimal
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

# 出力を抑制してASCIIアートなどを消す
import io
original_stdout = sys.stdout
sys.stdout = io.StringIO()

import utils
from defeatbeta_api.data.ticker import Ticker as DBTicker
from risk_return import process_single_stock

def format_df_to_dict(df):
    if df is None or df.empty:
        return None
    return json.loads(df.to_json(orient='split', date_format='iso'))

def get_performance_data(symbol, target_etf):
    """累積リターン比較用データの生成"""
    symbols = [symbol, target_etf, 'SPY']
    dfs = {}
    for s in symbols:
        try:
            ticker = utils.get_ticker(s)
            df = utils.safe_call(ticker, "history", period="10y")
            if df is not None and not df.empty:
                # Close価格のみを抽出し、インデックス(Date)を文字列に変換
                dfs[s] = {k.isoformat(): v for k, v in df['Close'].to_dict().items()}
        except:
            pass
    
    return dfs

def main(symbol):
    try:
        db_ticker = DBTicker(symbol)
        yf_ticker = utils.get_ticker(symbol)
        
        # 1. 基本的な財務データ (DefeatBeta)
        is_annual = db_ticker.annual_income_statement().df()
        bs_annual = db_ticker.annual_balance_sheet().df()
        cf_annual = db_ticker.annual_cash_flow().df()

        is_quarterly = db_ticker.quarterly_income_statement().df()
        bs_quarterly = db_ticker.quarterly_balance_sheet().df()
        cf_quarterly = db_ticker.quarterly_cash_flow().df()
        
        # 2. セグメント・地域別収益
        rev_segment = db_ticker.revenue_by_segment()
        rev_geo = db_ticker.revenue_by_geography()
        
        # 3. DCF計算
        dcf_data = utils.calculate_dcf(symbol, ticker=db_ticker)

        # 4. YFinanceデータの統合 (TypeScript側の負担を減らす)
        info = utils.safe_get(yf_ticker, 'info') or {}
        
        # セクターに応じた比較対象ETFの選択 (TypeScript側と同期)
        sector_etf_map = {
            'Information Technology': 'XLK',
            'Consumer Discretionary': 'XLY',
            'Financials': 'XLF',
            'Health Care': 'XLV',
            'Communication Services': 'XLC',
            'Industrials': 'XLI',
            'Consumer Staples': 'XLP',
            'Energy': 'XLE',
            'Utilities': 'XLU',
            'Real Estate': 'XLRE',
            'Materials': 'XLB',
            'Homebuilding': 'XHB'
        }
        
        # セクター情報を取得 (DefeatBeta または yfinance)
        gics_sector = info.get('sector', '')
        # Sub-Industryなどの詳細が必要な場合は info から探す
        target_etf = sector_etf_map.get(gics_sector, 'SPY')
        
        # リスク・リターン指標
        risk_metrics = {}
        risk_metrics[symbol] = process_single_stock(symbol)
        risk_metrics['SPY'] = process_single_stock('SPY')
        if target_etf != 'SPY':
            risk_metrics[target_etf] = process_single_stock(target_etf)
        
        # パフォーマンス比較データ (簡易版)
        history_data = get_performance_data(symbol, target_etf)

        # 結果の集約
        data = {
            "symbol": symbol,
            "financials": {
                "income_statement": format_df_to_dict(is_annual),
                "balance_sheet": format_df_to_dict(bs_annual),
                "cash_flow": format_df_to_dict(cf_annual),
                "income_statement_quarterly": format_df_to_dict(is_quarterly),
                "balance_sheet_quarterly": format_df_to_dict(bs_quarterly),
                "cash_flow_quarterly": format_df_to_dict(cf_quarterly)
            },
            "segments": format_df_to_dict(rev_segment),
            "geography": format_df_to_dict(rev_geo),
            "dcf": dcf_data,
            "yf_data": {
                "info": info,
                "risk_metrics": risk_metrics,
                "history": history_data
            }
        }
        
        # 抑制していた出力を元に戻し、JSONのみを出力
        sys.stdout = original_stdout
        print(json.dumps(data, ensure_ascii=False, cls=DecimalEncoder))

    except Exception as e:
        sys.stdout = original_stdout
        print(json.dumps({"error": str(e)}, cls=DecimalEncoder), file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    main(sys.argv[1])
