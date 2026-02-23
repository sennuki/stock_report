import yfinance as yf
import datetime
import pandas as pd

symbol = "AFL"
ticker_obj = yf.Ticker(symbol)

try:
    divs = ticker_obj.dividends
    print(f"Dividends empty: {divs.empty}")
    if not divs.empty:
        df_divs = divs.to_frame().reset_index()
        print("df_divs head:")
        print(df_divs.head())
        
        start_date = df_divs['Date'].min()
        end_date = df_divs['Date'].max() + datetime.timedelta(days=5)
        print(f"Fetching history from {start_date} to {end_date}")
        history = ticker_obj.history(start=start_date, end=end_date)
        print(f"History empty: {history.empty}")
        
        def get_price(date):
            try:
                # target_date = date.replace(hour=0, minute=0, second=0) # This might fail if date is already tz-aware
                # yfinance dates are usually tz-aware (UTC)
                target_date = date
                if target_date in history.index:
                    return history.loc[target_date]['Close']
                else:
                    return history.asof(target_date)['Close']
            except Exception as e:
                print(f"Error in get_price for {date}: {e}")
                return None

        df_divs['Price'] = df_divs['Date'].apply(get_price)
        print("df_divs with Price head:")
        print(df_divs.head())

except Exception as e:
    print(f"Caught exception: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
