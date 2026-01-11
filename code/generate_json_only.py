# -*- coding: utf-8 -*-
import market_data
import os
import json

def export_stocks_json(df):
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

if __name__ == "__main__":
    print("Generating stocks.json only...")
    df_sp500 = market_data.fetch_sp500_companies_optimized()
    if not df_sp500.is_empty():
        export_stocks_json(df_sp500)
    else:
        print("Failed to fetch data.")
