
import yfinance as yf
from datetime import datetime
import pandas as pd

def test_earnings():
    symbol = "AAPL"
    ticker = yf.Ticker(symbol)
    print(f"Testing {symbol}...")
    
    try:
        earnings_df = ticker.earnings_dates
        if earnings_df is not None and not earnings_df.empty:
            print("Earnings dates found:")
            print(earnings_df.head())
            
            # Use naive datetime for comparison if earnings_df is naive
            now = datetime.now()
            if earnings_df.index.tzinfo is not None:
                # If index is aware, make now aware
                import pytz
                now = datetime.now(pytz.utc)
            
            past_earnings = earnings_df[earnings_df.index <= now]
            if not past_earnings.empty:
                recent_earnings_date = past_earnings.index.max()
                print(f"Most recent past earnings date: {recent_earnings_date}")
            else:
                print("No past earnings found in the table.")
        else:
            print("Earnings dates table is empty or None.")
            
            # Try alternate way
            print("Trying ticker.calendar...")
            calendar = ticker.calendar
            print(calendar)
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_earnings()
