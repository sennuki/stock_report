# -*- coding: utf-8 -*-
import os
import polars as pl
import market_data

def generate_az_sheet():
    print("S&P 500銘柄リストを取得中...")
    # market_dataからS&P 500のリストを取得
    df = market_data.fetch_sp500_companies_optimized()
    
    if df.is_empty():
        print("エラー: 銘柄リストを取得できませんでした。")
        return

    # 各証券会社の取扱い銘柄を取得
    print("各証券会社の取扱い情報を取得中...")
    monex = market_data.get_monex_available_symbols()
    rakuten = market_data.get_rakuten_available_symbols()
    sbi = market_data.get_sbi_available_symbols()
    mufg = market_data.get_mufg_available_symbols()
    matsui = market_data.get_matsui_available_symbols()
    dmm = market_data.get_dmm_available_symbols()
    paypay = market_data.get_paypay_available_symbols()
    iwaicosmo = market_data.get_iwaicosmo_available_symbols()

    # 取扱いフラグを追加 (Symbolカラムで判定)
    df = df.with_columns([
        pl.col('Symbol').is_in(list(monex)).alias('Monex'),
        pl.col('Symbol').is_in(list(rakuten)).alias('Rakuten'),
        pl.col('Symbol').is_in(list(sbi)).alias('SBI'),
        pl.col('Symbol').is_in(list(mufg)).alias('MUFG'),
        pl.col('Symbol').is_in(list(matsui)).alias('Matsui'),
        pl.col('Symbol').is_in(list(dmm)).alias('DMM'),
        pl.col('Symbol').is_in(list(paypay)).alias('PayPay'),
        pl.col('Symbol').is_in(list(iwaicosmo)).alias('IwaiCosmo'),
    ])

    # A-Z順にソート (Symbol基準)
    df_sorted = df.sort('Symbol')

    # CSVファイルとして保存
    output_path = "stock_list_az.csv"
    df_sorted.write_csv(output_path)
    print(f"成功: {output_path} を作成しました。({len(df_sorted)} 銘柄)")

    # プレビュー表示
    print("\n--- プレビュー (最初の10銘柄) ---")
    print(df_sorted.head(10).select(['Symbol', 'Security', 'GICS Sector']))

if __name__ == "__main__":
    generate_az_sheet()
