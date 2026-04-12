
import yfinance as yf
import pandas as pd

def check_tsla_nominal_prices():
    symbol = "TSLA"
    ticker = yf.Ticker(symbol)
    
    # TSLA 3-for-1 split was on 2022-08-25
    split_date = "2022-08-25"
    
    print(f"Checking {symbol} prices around {split_date}...")
    
    # Try with auto_adjust=False
    hist = ticker.history(start="2022-08-20", end="2022-08-30", auto_adjust=False)
    print("\n--- auto_adjust=False ---")
    print(hist[['Open', 'Close', 'Stock Splits']])
    
    # Try with direct download (sometimes behaves differently)
    print("\n--- yf.download(threads=False) ---")
    df = yf.download(symbol, start="2022-08-20", end="2022-08-30", progress=False)
    print(df[['Open', 'Close']])

if __name__ == "__main__":
    check_tsla_nominal_prices()
