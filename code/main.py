# -*- coding: utf-8 -*-
import warnings
# Suppress Pandas4Warning: Timestamp.utcnow is deprecated (mostly from yfinance)
warnings.filterwarnings("ignore", message=".*Timestamp.utcnow is deprecated.*")

import market_data
import risk_return
import report_generator
import os
import shutil
import json
import plotly.io as pio
import polars as pl

# Plotly 6.0.0+ deprecation fix: 
# Default templates still contain 'scattermapbox', which triggers a warning.
# We migrate them to 'scattermap' to follow the recommendation.
def fix_plotly_templates():
    for name in pio.templates:
        template = pio.templates[name]
        try:
            if hasattr(template.layout.template.data, 'scattermapbox'):
                # Accessing it might trigger the warning, but we do it once to fix it
                smb = template.layout.template.data.scattermapbox
                if smb:
                    template.layout.template.data.scattermap = smb
                template.layout.template.data.scattermapbox = None
        except:
            pass

fix_plotly_templates()

# Google Drive check (optional, kept from original)
if os.path.exists('/content/drive'):
    os.chdir('/content/drive/MyDrive/python')
    print(f"Google Driveに接続しました: {os.getcwd()}")

def copy_reports_to_astro():
    """生成されたレポートをAstroプロジェクトのpublicフォルダにコピーする"""
    # スクリプトのディレクトリを基準にする
    base_dir = os.path.dirname(os.path.abspath(__file__))
    source_dir = os.path.join(base_dir, "output_reports_full")
    dest_dir = os.path.join(base_dir, "../stock-blog/public/output_reports_full")
    
    # 宛先ディレクトリが存在しない場合は作成
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
        print(f"ディレクトリを作成しました: {dest_dir}")

    print(f"レポートをコピー中... {source_dir} -> {dest_dir}")
    
    # ファイルをコピー
    if os.path.exists(source_dir):
        file_count = 0
        for filename in os.listdir(source_dir):
            if filename.endswith(".html"):
                src_file = os.path.join(source_dir, filename)
                dest_file = os.path.join(dest_dir, filename)
                shutil.copy2(src_file, dest_file)
                file_count += 1
        print(f"コピー完了: {file_count} ファイルをAstroプロジェクトに同期しました。")
    else:
        print(f"警告: ソースディレクトリが見つかりません: {source_dir}")

def export_stocks_json(df):
    """S&P 500の銘柄リストをAstro用のJSONデータとして保存する"""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    dest_path = os.path.join(base_dir, "../stock-blog/src/data/stocks.json")
    try:
        # ディレクトリ作成
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        
        # Polars DataFrame -> List of Dicts -> JSON
        data = df.to_dicts()
        
        with open(dest_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"銘柄リストJSONを保存しました: {dest_path}")
    except Exception as e:
        print(f"JSON保存エラー: {e}")

if __name__ == "__main__":
    # スクリプトのディレクトリを基準にする
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_reports_dir = os.path.join(base_dir, "output_reports_full")

    # 1. データ準備
    df_sp500 = market_data.fetch_sp500_companies_optimized()

    # 必須銘柄の確認と追加 (GOOGL, METAなど)
    required_tickers = {
        "GOOGL": {"Security": "Alphabet Inc (Class A)", "Sector": "Communication Services", "Sub": "Interactive Media & Services"},
        "META": {"Security": "Meta Platforms Inc", "Sector": "Communication Services", "Sub": "Interactive Media & Services"}
    }

    if not df_sp500.is_empty():
        current_symbols = set(df_sp500['Symbol_YF'].to_list())
        missing_rows = []
        
        for ticker, info in required_tickers.items():
            if ticker not in current_symbols:
                print(f"Adding missing ticker: {ticker}")
                missing_rows.append({
                    "Symbol": ticker,
                    "Security": info["Security"],
                    "GICS Sector": info["Sector"],
                    "GICS Sub-Industry": info["Sub"],
                    "Symbol_YF": ticker,
                    "Exchange": "NASDAQ"
                })
        
        if missing_rows:
            df_missing = pl.DataFrame(missing_rows)
            # カラムの型合わせや並び順を調整
            df_missing = df_missing.select(df_sp500.columns)
            df_sp500 = pl.concat([df_sp500, df_missing])
        # JSONリストのエクスポート (レポート生成前でもOK)
        export_stocks_json(df_sp500)

        # 2. リスク指標計算 (全銘柄)
        df_metrics = risk_return.calculate_market_metrics_parallel(df_sp500['Symbol_YF'].to_list())

        # 3. レポート作成
        report_generator.export_full_analysis_reports(df_sp500, df_metrics, output_dir=output_reports_dir)
        
        # 4. Astroへ反映
        copy_reports_to_astro()
    else:
        print("S&P 500リストの取得に失敗したため、処理を中断します。")
