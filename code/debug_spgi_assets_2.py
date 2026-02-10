import yfinance as yf
ticker = yf.Ticker("SPGI")
bs = ticker.balancesheet
latest_2021 = [col for col in bs.columns if '2021' in str(col)][0]
data_2021 = bs[latest_2021]

print("--- SPGI 2021 Specific Check ---")
items_to_check = [
    'Total Assets', 'Current Assets', 'Total Non Current Assets',
    'Property Plant Equipment Net', 'Other Non Current Assets'
]

for item in items_to_check:
    print(f"{item}: {data_2021.get(item)}")

# 全ての項目を表示
print("\n--- All items ---")
for item, val in data_2021.items():
    print(f"{item}: {val}")