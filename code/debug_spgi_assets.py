import yfinance as yf
ticker = yf.Ticker("SPGI")
bs = ticker.balancesheet
latest_2021 = [col for col in bs.columns if '2021' in str(col)][0]
data_2021 = bs[latest_2021]

print("--- SPGI 2021 All Assets ---")
for item, val in data_2021.items():
    # 資産に関連しそうなキーワードを網羅的にチェック
    keywords = ['Asset', 'Inventory', 'Receivables', 'Cash', 'Property', 'Goodwill', 'Intangible', 'Investment']
    if any(k in str(item) for k in keywords):
        print(f"{item}: {val}")

print(f"\nTotal Assets (API): {data_2021.get('Total Assets')}")
print(f"Current Assets (API): {data_2021.get('Current Assets')}")
print(f"Total Non Current Assets (API): {data_2021.get('Total Non Current Assets')}")

# 計算チェック
ca = data_2021.get('Current Assets', 0)
nca = data_2021.get('Total Non Current Assets', 0)
ta = data_2021.get('Total Assets', 0)
print(f"\nCA ({ca}) + NCA ({nca}) = {ca + nca}")
print(f"Diff TA - (CA+NCA) = {ta - (ca + nca)}")