
import yfinance as yf
from datetime import datetime

def test_original_logic():
    symbol = "AAPL"
    ticker = yf.Ticker(symbol)
    print(f"Testing {symbol} with original logic...")
    
    try:
        earnings_df = ticker.earnings_dates
        if earnings_df is not None and not earnings_df.empty:
            # This is what's in risk_return.py
            print("Comparing aware index with naive datetime.now()...")
            past_earnings = earnings_df[earnings_df.index <= datetime.now()]
            print("Comparison succeeded.")
        else:
            print("No earnings dates.")
    except Exception as e:
        print(f"Comparison failed as expected: {e}")

if __name__ == "__main__":
    test_original_logic()
