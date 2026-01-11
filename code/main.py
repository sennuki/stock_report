# -*- coding: utf-8 -*-
import market_data
import risk_return
import report_generator
import os
import shutil
import json

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
    # 1. データ準備
    df_sp500 = market_data.fetch_sp500_companies_optimized()

    if not df_sp500.is_empty():
        # JSONリストのエクスポート (レポート生成前でもOK)
        export_stocks_json(df_sp500)

        # 2. リスク指標計算 (全銘柄)
        df_metrics = risk_return.calculate_market_metrics_parallel(df_sp500['Symbol_YF'].to_list())

        # 3. レポート作成 (※テスト用に先頭5銘柄のみ実行)
        # 全銘柄実行したい場合は .head(5) を削除してください
        # print("【テスト実行】最初の5銘柄のみ生成します...")
        report_generator.export_full_analysis_reports(df_sp500, df_metrics)
        
        # 4. Astroへ反映
        copy_reports_to_astro()
    else:
        print("S&P 500リストの取得に失敗したため、処理を中断します。")
