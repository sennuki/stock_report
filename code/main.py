# -*- coding: utf-8 -*-
import warnings
import logging
# yfinance 1.1.0+ outputs noisy 404 errors for some symbols
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# Plotly 6.0.0+ deprecation warnings (scattermapbox -> scattermap)
warnings.filterwarnings("ignore", category=FutureWarning, message=".*scattermapbox.*")
warnings.filterwarnings("ignore", message=".*Timestamp.utcnow is deprecated.*")

import market_data
import risk_return
import generate_json_reports
import utils
import os
import shutil
import json
import plotly.io as pio
import polars as pl
from tqdm import tqdm
import concurrent.futures

# Google Drive check (optional, kept from original)
if os.path.exists('/content/drive'):
    os.chdir('/content/drive/MyDrive/python')
    print(f"Google Driveに接続しました: {os.getcwd()}")

def export_stocks_json(df):
    """銘柄リストをAstro用のJSONデータとして保存する"""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    dest_path = os.path.join(base_dir, "../stock-blog/src/data/stocks.json")
    try:
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        data = df.to_dicts()
        with open(dest_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"銘柄リストJSONを保存しました: {dest_path}")
    except Exception as e:
        print(f"JSON保存エラー: {e}")

def export_raw_data(df_info, df_metrics):
    """銘柄ごとの生データを取得して保存する (Cloudflare R2用)"""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "data/raw_data")
    os.makedirs(output_dir, exist_ok=True)

    # 1. マスター銘柄リストの作成 (D1インサート/一覧表示用)
    # df_metrics にある Daily_Change や Movement_Reason を結合
    try:
        df_master = df_info.join(
            df_metrics.select([pl.col("Symbol"), pl.col("Daily_Change")]), 
            left_on="Symbol_YF", right_on="Symbol", how="left"
        )
        export_stocks_json(df_master)
    except Exception as e:
        print(f"マスター銘柄リスト作成エラー: {e}")

    # 2. 各銘柄の詳細生データの取得 (並列処理)
    rows = df_info.to_dicts()
    # Rate limitを考慮し、GitHub Actions環境では並列数を抑える
    max_workers = int(os.environ.get("PYTHON_MAX_WORKERS", 1))
    
    print(f"全 {len(rows)} 銘柄の生データを取得中 (Workers: {max_workers})...")
    
    def fetch_and_save_single(row):
        symbol = row['Symbol_YF']
        try:
            ticker = utils.get_ticker(symbol)
            
            # DCF評価の計算 (defeatbeta-apiを使用)
            dcf_valuation = utils.calculate_dcf(symbol, ticker=ticker)
            
            # 会社概要の翻訳 (Gemini API)
            business_summary_ja = None
            if ticker.info.get("longBusinessSummary"):
                # レート制限を考慮した待機 (15 RPM想定)
                import time
                import random
                time.sleep(random.uniform(2.0, 4.0))
                
                try:
                    from movement_reasons import get_gemini_client
                    client = get_gemini_client()
                    if client:
                        prompt = f"以下の英文の会社概要を、正確な日本語に翻訳してください。専門用語は適切に扱い、自然な文章にしてください。追加情報は不要です。\n\n{ticker.info.get('longBusinessSummary')}"
                        # GEMINI.mdの指示に従い gemma-4-26b-a4b-it を使用
                        response = client.models.generate_content(
                            model="models/gemma-4-26b-a4b-it",
                            contents=prompt
                        )
                        business_summary_ja = response.text
                except Exception as e:
                    utils.log_event("WARNING", symbol, f"Translation failed: {e}")

            # 生データの抽出 (Workers側で処理しやすい形にする)
            raw_data = {
                "symbol": symbol,
                "info": ticker.info,
                "metadata": row,
                "dcf_valuation": dcf_valuation,
                "business_summary_ja": business_summary_ja,
                # 財務諸表 (DataFrame -> Dict)
                "income_stmt": ticker.income_stmt.to_dict() if not ticker.income_stmt.empty else {},
                "balancesheet": ticker.balancesheet.to_dict() if not ticker.balancesheet.empty else {},
                "cashflow": ticker.cashflow.to_dict() if not ticker.cashflow.empty else {},
                "quarterly_income_stmt": ticker.quarterly_income_stmt.to_dict() if not ticker.quarterly_income_stmt.empty else {},
                "quarterly_balancesheet": ticker.quarterly_balancesheet.to_dict() if not ticker.quarterly_balancesheet.empty else {},
                "quarterly_cashflow": ticker.quarterly_cashflow.to_dict() if not ticker.quarterly_cashflow.empty else {},
                "history": ticker.history(period="10y").reset_index().to_dict(orient='records'),
                "earnings_dates": ticker.earnings_dates.reset_index().to_dict(orient='records') if ticker.earnings_dates is not None and not ticker.earnings_dates.empty else [],
                "calendar": ticker.calendar if hasattr(ticker, 'calendar') else None,
                "recommendations": ticker.recommendations.to_dict() if hasattr(ticker, 'recommendations') and ticker.recommendations is not None and not ticker.recommendations.empty else None
            }
            
            # JSON保存 (日付型などは文字列に変換)
            path = os.path.join(output_dir, f"{symbol}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(raw_data, f, ensure_ascii=False, default=str)
            return True
        except Exception as e:
            # 個別のエラーはログに記録して続行
            utils.log_event("ERROR", symbol, f"Raw fetch failed: {e}")
            return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(tqdm(executor.map(fetch_and_save_single, rows), total=len(rows)))
        success_count = sum(1 for r in results if r)
        print(f"生データ取得完了: {success_count}/{len(rows)} 銘柄成功")

if __name__ == "__main__":
    utils.log_event("INFO", "SYSTEM", "--- Execution started (Raw Mode) ---")
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # 1. 銘柄リスト取得
    df_sp500 = market_data.fetch_sp500_companies_optimized()
    
    if not df_sp500.is_empty():
        # 2. 基本指標・前日比などの一括取得
        symbols = df_sp500['Symbol_YF'].to_list()
        print(f"{len(symbols)} 銘柄のマーケットメトリクスを計算中...")
        df_metrics = risk_return.calculate_market_metrics_parallel(symbols)

        # 3. 生データの取得とエクスポート
        export_raw_data(df_sp500, df_metrics)
        
        utils.log_event("SUCCESS", "SYSTEM", "--- Execution completed (Raw Mode) ---")
    else:
        print("S&P 500リストの取得に失敗しました。")
        utils.log_event("ERROR", "SYSTEM", "Failed to fetch S&P 500 list")
