import os
import sys
import pandas as pd
import yfinance as yf
from datetime import datetime

# codeディレクトリをパスに追加
sys.path.append(os.path.join(os.getcwd(), 'code'))
import utils

def debug_rating_prices(symbol):
    print(f"--- Debugging {symbol} ---")
    ticker = utils.get_ticker(symbol)
    
    # 格付けデータの取得
    ud = utils.safe_get(ticker, 'upgrades_downgrades')
    if ud is None or ud.empty:
        print("No upgrades/downgrades found.")
        return

    recent_ud = ud.sort_index(ascending=False).head(10).reset_index()
    
    # 株付け期間をカバーするために長めに取得 (10年)
    hist = ticker.history(period="10y")
    
    # ここを修正版と同じロジックにする
    if hist.index.tz is None:
        hist.index = hist.index.tz_localize('UTC').normalize()
    else:
        hist.index = hist.index.tz_convert('UTC').normalize()
    
    def get_price_at_date(date_ts):
        try:
            # タイムゾーンを考慮して正規化
            if date_ts.tzinfo is None:
                date_only = pd.Timestamp(date_ts).tz_localize('UTC').normalize()
            else:
                date_only = pd.Timestamp(date_ts).tz_convert('UTC').normalize()
            
            if date_only in hist.index:
                return float(hist.loc[date_only]['Close'])
            
            # 見つからない場合は直前の営業日を探す
            prev_dates = hist.index[hist.index <= date_only]
            if not prev_dates.empty:
                return float(hist.loc[prev_dates[-1]]['Close'])
        except Exception as e:
            print(f"Error for date {date_ts}: {e}")
        return None

    recent_ud['PriceAtRating'] = recent_ud['GradeDate'].apply(get_price_at_date)
    print(recent_ud[['GradeDate', 'Firm', 'FromGrade', 'ToGrade', 'PriceAtRating']])

if __name__ == "__main__":
    debug_rating_prices("MSFT")
    debug_rating_prices("AAPL")
