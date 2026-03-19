import sys
import os
sys.path.append(os.path.join(os.getcwd(), "code"))

from utils import YFinanceAdapterTicker
import traceback

def test_mmm_error():
    symbol = "MMM"
    print(f"--- Testing {symbol} for reproduction of 'int or Decimal expected' error ---")
    try:
        ticker = YFinanceAdapterTicker(symbol)
        
        print("\nChecking .history()...")
        h = ticker.history(period="1mo")
        print("OK")
        
        print("\nChecking .info property (This often triggers underlying data fetches)...")
        info = ticker.info
        print(f"OK, Price: {info.get('currentPrice')}")
        
        print("\nChecking .dividends property...")
        divs = ticker.dividends
        print(f"OK, {len(divs)} dividends found.")

        print("\nChecking other properties used in info...")
        # これらのメソッドが内部で呼ばれているはず
        from defeatbeta_api.data.ticker import Ticker as DBTicker
        db = DBTicker(symbol)
        print("Calling db.info()...")
        db.info()
        print("Calling db.price()...")
        db.price()
        print("Calling db.dividends()...")
        db.dividends()
        
    except Exception:
        print("\n!!! Caught Exception !!!")
        traceback.print_exc()

if __name__ == "__main__":
    test_mmm_error()
