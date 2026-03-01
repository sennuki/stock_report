# -*- coding: utf-8 -*-
import os
import polars as pl
import market_data
import risk_return
import report_generator
import shutil

def run_quick_test(target_symbols=["MSFT", "MO", "GOOGL"]):
    print(f"🚀 テスト実行開始 (対象: {', '.join(target_symbols)})")
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    # テスト専用の出力ディレクトリ
    output_reports_dir = os.path.join(base_dir, "test_reports")
    if os.path.exists(output_reports_dir):
        shutil.rmtree(output_reports_dir)
    os.makedirs(output_reports_dir)
    
    # 1. 銘柄情報の取得とフィルタリング
    df_all = market_data.fetch_sp500_companies_optimized()
    df_test = df_all.filter(pl.col("Symbol_YF").is_in(target_symbols))
    
    if df_test.is_empty():
        print("指定された銘柄が見つかりませんでした。")
        # 取得できなかった場合、ダミーで作成して続行
        df_test = pl.DataFrame({
            "Symbol": ["MSFT"],
            "Symbol_YF": ["MSFT"],
            "Security": ["Microsoft Corp"],
            "GICS Sector": ["Information Technology"],
            "GICS Sub-Industry": ["Systems Software"],
            "Exchange": ["NASDAQ"]
        })

    # 2. リスク指標計算
    if 'Symbol_YF' in df_all.columns and not df_all.is_empty():
        symbols_to_calc = df_all['Symbol_YF'].to_list()
    else:
        symbols_to_calc = df_test['Symbol_YF'].to_list()
        
    df_metrics = risk_return.calculate_market_metrics_parallel(symbols_to_calc)

    # 3. レポート作成
    report_generator.export_full_analysis_reports(df_test, df_metrics, output_dir=output_reports_dir)
    
    # 4. Astroプロジェクトへ同期
    dest_dir = os.path.join(base_dir, "../stock-blog/public/output_reports_full")
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    
    print(f"Astroへ同期中... {output_reports_dir} -> {dest_dir}")
    for filename in os.listdir(output_reports_dir):
        if filename.endswith(".html"):
            shutil.copy2(os.path.join(output_reports_dir, filename), os.path.join(dest_dir, filename))
            
    print(f"\n✅ テスト完了！")
    print(f"以下のURLで確認できます:")
    for sym in target_symbols:
        print(f" - http://localhost:4321/output_reports_full/{sym}.html")

if __name__ == "__main__":
    # 環境変数などから銘柄を指定できるようにしても良い
    run_quick_test()
