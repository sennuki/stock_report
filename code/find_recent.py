import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import pytz
import polars as pl
import market_data
import utils

def find_recent_earnings():
    print("Fetching S&P 500 list...")
    df = market_data.fetch_sp500_companies_optimized()
    if df.is_empty():
        print("Empty SP500 list.")
        return
        
    symbols = df['Symbol_YF'].to_list()
    
    now = datetime.now(pytz.utc)
    one_week_ago = now - timedelta(days=7)
    
    print(f"Checking {len(symbols)} stocks for earnings between {one_week_ago.date()} and {now.date()}...")
    
    recent_earnings_stocks = []
    
    # Check first 50 as a sample to be fast
    for symbol in symbols[:50]:
        try:
            ticker = utils.get_ticker(symbol)
            # Use safe_get for earnings_dates
            ed = utils.safe_get(ticker, 'earnings_dates')
            if ed is not None and not ed.empty:
                if ed.index.tzinfo is not None:
                    ed.index = ed.index.tz_convert('UTC')
                
                past_ed = ed[ed.index <= now]
                if not past_ed.empty:
                    latest = past_ed.index.max()
                    if latest >= one_week_ago:
                        print(f"FOUND: {symbol} on {latest.date()}")
                        recent_earnings_stocks.append((symbol, latest.date()))
        except Exception as e:
            # print(f"Error for {symbol}: {e}")
            continue
            
    print(f"Found {len(recent_earnings_stocks)} recent earnings in first 50 stocks.")

if __name__ == "__main__":
    find_recent_earnings()
