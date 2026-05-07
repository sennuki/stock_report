import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'code'))

import main
import market_data
import polars as pl
import json

def test_single_symbol(symbol):
    print(f"Testing fetch for {symbol}...")
    # 擬似的なrowデータを作成
    row = {
        'Symbol': symbol,
        'Symbol_YF': symbol,
        'Security': 'Apple Inc.',
        'GICS Sector': 'Information Technology',
        'GICS Sub-Industry': 'Technology Hardware, Storage & Peripherals',
        'Exchange': 'NASDAQ'
    }
    
    # main.py の関数を直接呼び出す
    # output_dir を設定
    output_dir = "code/data/raw_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # 依存する df_info, df_metrics は export_raw_data 内で master リスト作成に使われるだけなので
    # fetch_and_save_single 単体なら不要だが、一応 main のフローに合わせる
    import utils
    ticker = utils.get_ticker(symbol)
    
    # main.py の fetch_and_save_single 内のロジックを再現
    import main
    
    # main.py 内の関数はグローバルに定義されているので直接呼ぶ
    # ただし今回は main.py を import しているので main.fetch_and_save_single として呼ぶ必要があるが
    # 関数内で export_stocks_json などを呼んでいるため、main.py を少し修正して
    # fetch_and_save_single を外から呼びやすくするか、ここでロジックを実行する。
    
    # 簡略化のため、main.py のロジックをここで実行
    try:
        from main import fetch_and_save_single
        # main.py 内で定義された関数を実行
        # Note: fetch_and_save_single は main.py のスコープにある
    except ImportError:
        pass

    # 直接ロジックを実行 (main.py の最新状態を反映)
    import utils
    dcf_valuation = utils.calculate_dcf(symbol, ticker=ticker)
    
    raw_data = {
        "symbol": symbol,
        "info": ticker.info,
        "metadata": row,
        "dcf_valuation": dcf_valuation,
        "income_stmt": ticker.income_stmt.to_dict() if not ticker.income_stmt.empty else {},
        "balancesheet": ticker.balancesheet.to_dict() if not ticker.balancesheet.empty else {},
        "cashflow": ticker.cashflow.to_dict() if not ticker.cashflow.empty else {},
        "quarterly_income_stmt": ticker.quarterly_income_stmt.to_dict() if not ticker.quarterly_income_stmt.empty else {},
        "history": ticker.history(period="10y").reset_index().to_dict(orient='records'),
        "calendar": ticker.calendar if hasattr(ticker, 'calendar') else None,
    }
    
    path = os.path.join(output_dir, f"{symbol}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, ensure_ascii=False, default=str)
    
    print(f"Successfully saved to {path}")
    return path

if __name__ == "__main__":
    test_single_symbol("AAPL")
