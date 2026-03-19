import sys
import os
sys.path.append(os.path.join(os.getcwd(), "code"))
from defeatbeta_api.data.ticker import Ticker as DBTicker

def test_broader():
    for symbol in ["MMM", "ABBV"]:
        print(f"--- Testing {symbol} ---")
        try:
            db = DBTicker(symbol)
            # aliasesに関係しそうなメソッドを網羅
            print("Calling .info()...")
            db.info()
            print("Calling .price()...")
            db.price()
            print("Calling .dividends()...")
            db.dividends()
            print("Calling .splits()...")
            db.splits()
            print("Calling .calendar()...")
            db.calendar()
            print("Calling .ttm_pe()...")
            db.ttm_pe()
            print("Calling .ttm_eps()...")
            db.ttm_eps()
            print("Calling .quarterly_balance_sheet()...")
            db.quarterly_balance_sheet()
            print("Calling .annual_balance_sheet()...")
            db.annual_balance_sheet()
            
            print(f"Finished {symbol} without crashing.\n")
        except Exception as e:
            print(f"Error for {symbol}: {e}")

if __name__ == "__main__":
    test_broader()
