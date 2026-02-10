import yfinance as yf
import pandas as pd
import polars as pl

ticker_symbol = "SPGI"
print(f"Fetching {ticker_symbol} data...")
ticker = yf.Ticker(ticker_symbol)

def get_melt(df):
    date_cols = [col for col in df.columns if col != 'Item']
    df_melt = df.unpivot(index='Item', on=date_cols, variable_name='Date', value_name='Value')
    df_melt = df_melt.with_columns(pl.col('Value').cast(pl.Float64, strict=False))
    df_melt = df_melt.drop_nulls()
    df_melt = df_melt.with_columns(pl.col('Date').cast(pl.String).str.slice(0, 10)).sort(['Item', 'Date'])
    return df_melt

def extract_and_melt(pandas_df, targets):
    if pandas_df.empty:
        return pl.DataFrame()
    try:
        df = pl.from_pandas(pandas_df, include_index=True)
        df = df.rename({df.columns[0]: 'Item'})
        return get_melt(df.filter(pl.col('Item').is_in(targets)))
    except Exception as e:
        print(f"Error processing data: {e}")
        return pl.DataFrame()

target_bs = ['Total Non Current Assets', 'Current Liabilities', 'Total Equity Gross Minority Interest',
                'Current Assets', 'Total Non Current Liabilities Net Minority Interest',
                'Total Assets', 'Total Liabilities Net Minority Interest', 'Total Liabilities',
                'Long Term Debt And Capital Lease Obligation','Employee Benefits', 'Non Current Deferred Liabilities',
                'Other Non Current Liabilities']

def check_consistency(df, label):
    print(f"\n=== Checking {label} Balance Sheet ===")
    if df.is_empty():
        print("DataFrame is empty")
        return

    all_dates = sorted(df['Date'].unique().to_list())
    df_plot = pl.DataFrame({'Date': all_dates})

    def join_item(target_df, item_name, col_name):
        item_data = df.filter(pl.col('Item') == item_name).select(['Date', 'Value']).rename({'Value': col_name})
        return target_df.join(item_data, on='Date', how='left').fill_null(0.0)

    df_plot = join_item(df_plot, 'Current Assets', 'CurrAssets')
    df_plot = join_item(df_plot, 'Total Non Current Assets', 'NonCurrAssets')
    df_plot = join_item(df_plot, 'Current Liabilities', 'CurrLiab')
    df_plot = join_item(df_plot, 'Total Equity Gross Minority Interest', 'Equity')
    df_plot = join_item(df_plot, 'Total Assets', 'TotalAssets')
    df_plot = join_item(df_plot, 'Total Liabilities Net Minority Interest', 'TotalLiab')
    if 'TotalLiab' not in df_plot.columns or df_plot['TotalLiab'].sum() == 0:
         df_plot = join_item(df_plot, 'Total Liabilities', 'TotalLiab')

    fixed_liab_items = ['Long Term Debt And Capital Lease Obligation', 'Employee Benefits', 'Non Current Deferred Liabilities', 'Other Non Current Liabilities']
    liab_parts = df.filter(pl.col('Item').is_in(fixed_liab_items))

    if not liab_parts.is_empty():
        non_curr_liab_pivoted = liab_parts.pivot(on='Item', index='Date', values='Value').fill_null(0.0)
        sum_cols = [c for c in non_curr_liab_pivoted.columns if c != 'Date']
        fixed_liab_data = non_curr_liab_pivoted.with_columns(
            pl.sum_horizontal(sum_cols).alias('FixedLiab')
        ).select(['Date', 'FixedLiab'])
        df_plot = df_plot.join(fixed_liab_data, on='Date', how='left').fill_null(0.0)
    else:
        fixed_data = df.filter(pl.col('Item') == 'Total Non Current Liabilities Net Minority Interest').select(['Date', 'Value']).rename({'Value': 'FixedLiab'})
        df_plot = df_plot.join(fixed_data, on='Date', how='left').fill_null(0.0)

    df_plot = df_plot.with_columns([
        (pl.col('CurrAssets') + pl.col('NonCurrAssets')).alias('TotalAssetsCalc'),
        (pl.col('CurrLiab') + pl.col('FixedLiab') + pl.col('Equity')).alias('TotalLiabEquityCalc'),
    ])
    
    df_plot = df_plot.with_columns([
        (pl.col('TotalAssets') - pl.col('TotalLiabEquityCalc')).alias('Diff')
    ])

    print(df_plot.select(['Date', 'TotalAssets', 'TotalLiabEquityCalc', 'Diff']))
    
    target_2021 = df_plot.filter(pl.col('Date').str.starts_with('2021'))
    if not target_2021.is_empty():
        print("\n--- 2021 Detail ---")
        row = target_2021.to_dicts()[0]
        print(f"Date: {row['Date']}")
        print(f"Total Assets: {row['TotalAssets']}")
        print(f"  Curr Assets: {row['CurrAssets']}")
        print(f"  Non-Curr Assets: {row['NonCurrAssets']}")
        print(f"Total L+E (Calc): {row['TotalLiabEquityCalc']}")
        print(f"  Curr Liab: {row['CurrLiab']}")
        print(f"  Fixed Liab (Calc): {row['FixedLiab']}")
        print(f"  Equity: {row['Equity']}")
        print(f"Diff: {row['Diff']}")
        
        date_val = row['Date']
        parts = df.filter((pl.col('Date') == date_val) & (pl.col('Item').is_in(fixed_liab_items)))
        print("\nFixed Liab Parts:")
        print(parts)

print("Processing Annual Balance Sheet for SPGI...")
check_consistency(extract_and_melt(ticker.balancesheet, target_bs), "Annual")