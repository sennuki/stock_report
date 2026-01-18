# -*- coding: utf-8 -*-
import polars as pl
import market_data
import risk_return
import report_generator
import os
import shutil

def generate_specific_reports(symbols):
    print(f"ターゲット銘柄: {symbols}")
    
    # 1. 銘柄情報の取得
    df_sp500 = market_data.fetch_sp500_companies_optimized()
    df_info = df_sp500.filter(pl.col("Symbol_YF").is_in(symbols))
    
    if df_info.is_empty():
        print("指定された銘柄がS&P 500リストに見つかりませんでした。")
        # リストにない場合でも直接作成を試みる（予備）
        df_info = pl.DataFrame({
            "Symbol": [s.replace(".", "-") for s in symbols],
            "Symbol_YF": symbols,
            "Security": ["Alphabet Inc (Class A)" if s == "GOOGL" else "Meta Platforms Inc" for s in symbols],
            "GICS Sector": ["Communication Services" for _ in symbols],
            "GICS Sub-Industry": ["Interactive Media & Services" for _ in symbols],
            "Exchange": ["NASDAQ" for _ in symbols]
        })

    # 2. リスク指標の計算 (セクターETFとS&P500も含める)
    target_symbols = symbols + ["VOX", "^GSPC"]
    df_metrics = risk_return.calculate_market_metrics_parallel(target_symbols)

    # 3. レポート生成
    output_dir = "code/output_reports_full"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    report_generator.export_full_analysis_reports(df_info, df_metrics, output_dir=output_dir)
    
    # 4. Astroプロジェクトのpublicフォルダへコピー
    dest_dir = "stock-blog/public/output_reports_full"
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
        
    for symbol in symbols:
        filename = f"{symbol.replace('-', '.')}.html"
        src = os.path.join(output_dir, filename)
        dst = os.path.join(dest_dir, filename)
        if os.path.exists(src):
            shutil.copy2(src, dst)
            print(f"生成完了: {dst}")

if __name__ == "__main__":
    generate_specific_reports(["GOOGL", "META"])
