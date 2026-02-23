import yfinance as yf
symbol = "AAPL"
ticker_obj = yf.Ticker(symbol)
try:
    divs = ticker_obj.dividends
    print(f"AAPL dividends: {not divs.empty}")
except Exception as e:
    print(f"AAPL error: {e}")

symbol = "AFL"
ticker_obj = yf.Ticker(symbol)
try:
    divs = ticker_obj.dividends
    print(f"AFL dividends: {not divs.empty}")
except Exception as e:
    print(f"AFL error: {e}")
