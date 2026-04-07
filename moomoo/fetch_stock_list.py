import moomoo as ft
import pandas as pd
import os

def fetch_moomoo_stocks():
    # Moomoo OpenD のデフォルト設定
    host = '127.0.0.1'
    port = 11111
    
    # 接続
    quote_ctx = ft.OpenQuoteContext(host=host, port=port)
    
    try:
        print("Connecting to Moomoo OpenD...")
        # 米国株一覧の取得
        print("Fetching US stocks...")
        ret, us_data = quote_ctx.get_stock_basicinfo(ft.Market.US, ft.SecurityType.STOCK)
        if ret == ft.RET_OK:
            us_data.to_csv('moomoo_us_stocks.csv', index=False)
            print(f"Saved {len(us_data)} US stocks to moomoo_us_stocks.csv")
        else:
            print(f"Error fetching US stocks: {us_data}")

        # 日本株一覧の取得
        print("Fetching JP stocks...")
        ret, jp_data = quote_ctx.get_stock_basicinfo(ft.Market.JP, ft.SecurityType.STOCK)
        if ret == ft.RET_OK:
            jp_data.to_csv('moomoo_jp_stocks.csv', index=False)
            print(f"Saved {len(jp_data)} JP stocks to moomoo_jp_stocks.csv")
        else:
            print(f"Error fetching JP stocks: {jp_data}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        quote_ctx.close()

if __name__ == "__main__":
    fetch_moomoo_stocks()
