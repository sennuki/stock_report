import yfinance as yf
import time

symbol = "AFL"
for i in range(10):
    try:
        ticker_obj = yf.Ticker(symbol)
        divs = ticker_obj.dividends
        print(f"Attempt {i+1}: Success, divs empty: {divs.empty}")
    except Exception as e:
        print(f"Attempt {i+1}: Failed with {e}")
    time.sleep(1)
