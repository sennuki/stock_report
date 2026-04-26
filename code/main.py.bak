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

# Plotly 6.0.0+ template migration:
# Default templates still contain 'scattermapbox' references.
# We migrate them to 'scattermap' to align with Plotly 6.0 recommendations.
def fix_plotly_templates():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for name in pio.templates:
            template = pio.templates[name]
            try:
                data = template.layout.template.data
                if hasattr(data, 'scattermapbox'):
                    smb = data.scattermapbox
                    if smb:
                        data.scattermap = smb
                    data.scattermapbox = None
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
            if filename.endswith(".html") or filename.endswith(".json"):
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
    # ログの記録開始
    utils.log_event("INFO", "SYSTEM", "--- Execution started ---")

    # スクリプトのディレクトリを基準にする
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_reports_dir = os.path.join(base_dir, "output_reports_full")

    # 1. データ準備
    try:
        df_sp500 = market_data.fetch_sp500_companies_optimized()
        
        # TEST_MODEなら銘柄数を制限する
        if os.environ.get("TEST_MODE") == "true":
            print("TEST_MODE is active: limiting to 10 stocks.")
            df_sp500 = df_sp500.head(10)
            
        if not df_sp500.is_empty():
            utils.log_event("SUCCESS", "SYSTEM", f"Fetched {len(df_sp500)} companies")
    except Exception as e:
        utils.log_event("ERROR", "SYSTEM", f"Failed to fetch S&P 500 list: {e}")
        df_sp500 = pl.DataFrame()

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
                # Symbol_YFはハイフン形式、Symbolはドット形式に統一
                sym_yf = ticker.replace(".", "-")
                sym_display = ticker.replace("-", ".")
                missing_rows.append({
                    "Symbol": sym_display,
                    "Security": info["Security"],
                    "GICS Sector": info["Sector"],
                    "GICS Sub-Industry": info["Sub"],
                    "Symbol_YF": sym_yf,
                    "Exchange": "NASDAQ"
                })
        
        if missing_rows:
            df_missing = pl.DataFrame(missing_rows)
            # Ensure all columns from df_sp500 exist in df_missing
            for col in df_sp500.columns:
                if col not in df_missing.columns:
                    df_missing = df_missing.with_columns(pl.lit(None).alias(col))
            
            # Match column order and types
            df_missing = df_missing.select(df_sp500.columns)
            df_sp500 = pl.concat([df_sp500, df_missing])
        # JSONリストのエクスポート (レポート生成前でもOK)
        export_stocks_json(df_sp500)

        # 2. リスク指標計算 (全銘柄)
        try:
            # 並列度を抑えてレート制限を回避 (特に Earnings_Date の取得用)
            import os
            os.environ["MAX_WORKERS"] = "3"
            df_metrics = risk_return.calculate_market_metrics_parallel(df_sp500['Symbol_YF'].to_list())
            utils.log_event("SUCCESS", "SYSTEM", "Calculated risk metrics")
        except Exception as e:
            utils.log_event("ERROR", "SYSTEM", f"Failed to calculate risk metrics: {e}")
            df_metrics = pl.DataFrame()

        # 3. レポート作成 (JSON)
        try:
            # generate_json_reports.py はデフォルトで ../stock-blog/public/reports に出力する
            # レート制限を考慮し、内部で max_workers=1 を使用中
            generate_json_reports.export_json_reports(df_sp500, df_metrics)
            utils.log_event("SUCCESS", "SYSTEM", "Generated all JSON reports")
            
            # --- ここから実績データの存在を stocks.json に反映 ---
            print("実績データの更新情報を stocks.json に反映しています...")
            reports_dir = os.path.join(base_dir, "../stock-blog/public/reports")
            
            from datetime import datetime, timedelta
            now = datetime.now()
            limit_date = now - timedelta(days=7)
            
            updated_rows = []
            for row in df_sp500.to_dicts():
                ticker = row['Symbol_YF']
                report_path = os.path.join(reports_dir, f"{ticker}.json")
                is_recent_actual = False
                actual_date = None
                
                if os.path.exists(report_path):
                    try:
                        with open(report_path, 'r', encoding='utf-8') as rf:
                            report_data = json.load(rf)
                            # 実績データがあるか確認
                            surprise = report_data.get("earnings_surprise")
                            if surprise and surprise.get("date"):
                                s_date = datetime.strptime(surprise["date"], "%Y-%m-%d")
                                actual_date = surprise["date"]
                                if s_date >= limit_date:
                                    is_recent_actual = True
                    except:
                        pass
                
                row["Is_Recent_Actual"] = is_recent_actual
                row["Actual_Earnings_Date"] = actual_date
                updated_rows.append(row)
            
            df_sp500_updated = pl.DataFrame(updated_rows)
            export_stocks_json(df_sp500_updated)
            # --- 反映完了 ---

        except Exception as e:
            utils.log_event("ERROR", "SYSTEM", f"JSON Report generation failed: {e}")
        
        # 4. Astroへ反映 (JSON生成で直接出力しているため不要)
        # copy_reports_to_astro()

        # 最後にエラー要約を表示
        if os.path.exists(utils.LOG_FILE):
            print("\n" + "="*50)
            print(" 実行ログ要約 (エラー・警告) ")
            print("="*50)
            with open(utils.LOG_FILE, "r", encoding="utf-8") as f:
                logs = f.readlines()
                # 重複を避けてエラー・警告を抽出
                errors = sorted(list(set([line.strip() for line in logs if "ERROR" in line or "WARN" in line])))
                if errors:
                    for err in errors:
                        print(err)
                else:
                    print("重大なエラーは見つかりませんでした。")
            print("="*50)
    else:
        print("S&P 500リストの取得に失敗したため、処理を中断します。")
