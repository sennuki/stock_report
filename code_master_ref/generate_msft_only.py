
import os
import polars as pl
import yfinance as yf
import market_data
import risk_return
import generate_json_reports
import utils

def generate_msft_report():
    symbol = "MSFT"
    print(f"--- Generating Report for {symbol} ---")
    
    # 1. 銘柄情報の準備
    # MSFTの基本情報を取得（本来はS&P500リストから取るが、テスト用に手動作成）
    df_sp500 = pl.DataFrame([{
        "Symbol": "MSFT",
        "Security": "Microsoft Corp",
        "GICS Sector": "Information Technology",
        "GICS Sub-Industry": "Systems Software",
        "Symbol_YF": "MSFT",
        "Exchange": "NASDAQ"
    }])
    
    # 2. リスク指標の計算
    df_metrics = risk_return.calculate_market_metrics_parallel(["MSFT"])
    
    # 3. JSONレポートの生成 (修正した fundamentals.py が使用される)
    # 出力先は自動的に ../stock-blog/public/reports/ になる
    generate_json_reports.export_json_reports(df_sp500, df_metrics)
    
    print(f"Report generation for {symbol} completed.")

if __name__ == "__main__":
    generate_msft_report()
