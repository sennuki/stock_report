
import yfinance as yf

def test_mmm():
    symbol = "MMM"
    ticker = yf.Ticker(symbol)
    print(f"Testing {symbol} earnings_dates access...")
    try:
        # This property access triggers yfinance internal data fetching
        ed = ticker.earnings_dates
        print("Success!")
        print(ed.head())
    except Exception as e:
        print(f"Failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_mmm()
