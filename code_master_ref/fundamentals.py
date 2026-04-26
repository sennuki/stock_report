# -*- coding: utf-8 -*-
import yfinance as yf
import polars as pl
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
import numpy as np
import warnings
# Plotly 6.0.0+ deprecation warnings (scattermapbox -> scattermap)
warnings.filterwarnings("ignore", category=FutureWarning, message=".*scattermapbox.*")

import yfinance as yf
import polars as pl
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
import numpy as np
import utils
import datetime

# Plotly 6.0.0+ template migration:
# Default templates still contain 'scattermapbox' references.
# We migrate them to 'scattermap' to align with Plotly 6.0 recommendations.
def fix_plotly_templates():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for name in pio.templates:
            template = pio.templates[name]
            try:
                data = template.layout.template.data
                if hasattr(data, 'scattermapbox'):
                    smb = data.scattermapbox
                    if smb:
                        data.scattermap = smb
                    data.scattermapbox = None
            except:
                pass

fix_plotly_templates()

# ==========================================
#  Part A: ファンダメンタルズ分析 (グラフ生成)
# ==========================================

def get_melt(df):
    """Polars DataFrameをUnpivot（縦持ち変換）する共通関数"""
    date_cols = [col for col in df.columns if col != 'Item']
    df_melt = df.unpivot(index='Item', on=date_cols, variable_name='Date', value_name='Value')
    
    # Valueを数値にキャスト (失敗した場合はnullになる)
    df_melt = df_melt.with_columns(pl.col('Value').cast(pl.Float64, strict=False))

    df_melt = df_melt.drop_nulls()
    # 日付文字列 (YYYY-MM-DD) に変換してソート
    df_melt = df_melt.with_columns(pl.col('Date').cast(pl.String).str.slice(0, 10)).sort(['Item', 'Date'])
    return df_melt

def get_financial_data(ticker_obj):
    """1つのTickerオブジェクトから4種類の財務データ（Annual & Quarterly）を取得・整形"""
    data = {}
    symbol = getattr(ticker_obj, 'ticker', 'Unknown')

    def get_attr(obj, names):
        for name in names:
            val = utils.safe_get(obj, name)
            if val is not None and not (isinstance(val, pd.DataFrame) and val.empty):
                return val
        return None

    def extract_with_aliases(pandas_df, alias_dict):
        if pandas_df is None or (isinstance(pandas_df, pd.DataFrame) and pandas_df.empty):
            return pl.DataFrame()
        try:
            # Cast all columns to numeric (float) to avoid Decimal/object issues with Polars
            pandas_df = pandas_df.copy()
            # Rename index to 'Item' for Polars if not already
            if pandas_df.index.name != 'Item':
                pandas_df.index.name = 'Item'
                
            for col in pandas_df.columns:
                # 確実に数値に変換し、'*'などの文字列をNaNにする
                # Decimalオブジェクト対策として明示的にfloatに変換
                if pandas_df[col].dtype == object:
                    pandas_df[col] = pandas_df[col].apply(lambda x: float(x) if x is not None and str(x).replace('.','',1).replace('-','',1).isdigit() else np.nan if x == '*' else x)
                pandas_df[col] = pd.to_numeric(pandas_df[col], errors='coerce').astype(float)
            
            # Polarsに変換する前にインデックスを列に戻す
            df = pl.from_pandas(pandas_df.reset_index())
            
            # アイテム名リスト（小文字）を準備
            item_list = df['Item'].to_list()
            item_list_lower = [str(x).lower() for x in item_list]
            
            for standard_name, aliases in alias_dict.items():
                # 標準名が既に存在し、かつ全期間でNaNでないか確認
                standard_name_lower = standard_name.lower()
                has_valid_standard = False
                if standard_name_lower in item_list_lower:
                    idx = item_list_lower.index(standard_name_lower)
                    actual_standard_name = item_list[idx]
                    # 標準名列の全データの合計をチェック（より堅牢な判定）
                    row_df = df.filter(pl.col('Item') == actual_standard_name).select(pl.all().exclude('Item'))
                    if not row_df.is_empty():
                        # いずれかの列に null 以外、かつ NaN 以外の有効な値があるか確認
                        is_valid = row_df.select(
                            pl.any_horizontal(
                                pl.all().is_not_null() & pl.all().is_not_nan()
                            )
                        ).to_series()[0]
                        if is_valid:
                            has_valid_standard = True
                            # 大文字小文字が異なる場合は標準名にリネームしておく（後でフィルタリングできるように）
                            if actual_standard_name != standard_name:
                                df = df.with_columns(
                                    pl.when(pl.col('Item') == actual_standard_name)
                                    .then(pl.lit(standard_name))
                                    .otherwise(pl.col('Item'))
                                    .alias('Item')
                                )
                
                # 有効な標準名がない場合、または標準名が全データNaNの場合、エイリアスから検索してリネームする
                if not has_valid_standard:
                    for alias in aliases:
                        alias_lower = alias.lower()
                        if alias_lower in item_list_lower:
                            idx = item_list_lower.index(alias_lower)
                            actual_name = item_list[idx]
                            
                            # そのエイリアスが有効な値（非NaN）を持っているか確認
                            row_df = df.filter(pl.col('Item') == actual_name).select(pl.all().exclude('Item'))
                            if not row_df.is_empty():
                                is_valid = row_df.select(
                                    pl.any_horizontal(
                                        pl.all().is_not_null() & pl.all().is_not_nan()
                                    )
                                ).to_series()[0]
                                if is_valid:
                                    # リネーム処理
                                    if actual_name != standard_name:
                                        # 標準名がNaNで存在する場合も、その名前をこの有効なエイリアスに譲るために一度別名にするなどの処理はPolarsのwhen().then()で対応可能
                                        df = df.with_columns(
                                            pl.when(pl.col('Item') == actual_name)
                                            .then(pl.lit(standard_name))
                                            .when(pl.col('Item') == standard_name)
                                            .then(pl.lit(f"OLD_{standard_name}")) # 既存の無効な標準名を退避
                                            .otherwise(pl.col('Item'))
                                            .alias('Item')
                                        )
                                    break # 有効なエイリアスを採用したので終了
            
            target_list = list(alias_dict.keys())
            df_filtered = df.filter(pl.col('Item').is_in(target_list))
            
            if df_filtered.is_empty():
                return pl.DataFrame()
            return get_melt(df_filtered)
        except Exception as e:
            # 不要なデバッグ出力を抑制
            # print(f"Error processing aliases for {symbol}: {e}")
            return pl.DataFrame()
            return pl.DataFrame()

    # 1. 貸借対照表
    bs_annual = get_attr(ticker_obj, ['balance_sheet', 'balancesheet'])
    bs_quarterly = get_attr(ticker_obj, ['quarterly_balance_sheet', 'quarterly_balancesheet'])
    
    if (bs_annual is None or bs_annual.empty) and (bs_quarterly is None or bs_quarterly.empty):
        utils.log_event("WARN", symbol, "Financial data (BS) is empty. GitHub Actions IP might be blocked or data is unavailable.")
    
    # 2. 損益計算書
    is_annual = get_attr(ticker_obj, ['income_stmt', 'incomestmt', 'financials'])
    is_quarterly = get_attr(ticker_obj, ['quarterly_income_stmt', 'quarterly_incomestmt', 'quarterly_financials'])

    # 3. キャッシュフロー
    cf_annual = get_attr(ticker_obj, ['cashflow', 'cash_flow'])
    cf_quarterly = get_attr(ticker_obj, ['quarterly_cashflow', 'quarterly_cash_flow'])

    # 貸借対照表エイリアス
    bs_aliases = {
        'Total Assets': ['Total Assets', 'TotalAssets', 'Total Liabilities And Equity'],
        'Total Equity Gross Minority Interest': ['Total Equity Gross Minority Interest', 'Total Equity', 'TotalEquity'],
        'Stockholders Equity': ['Stockholders Equity', 'StockholdersEquity', 'Common Stock Equity', "Stockholders' Equity"],
        'Total Liabilities Net Minority Interest': ['Total Liabilities Net Minority Interest', 'Total Liabilities', 'TotalLiabilities'],
        'Current Assets': ['Current Assets', 'CurrentAssets', 'Total Current Assets', 'TotalCurrentAssets'],
        'Total Non Current Assets': ['Total Non Current Assets', 'TotalNonCurrentAssets', 'Total Non-Current Assets', 'TotalNonCurrentAssets'],
        'Current Liabilities': ['Current Liabilities', 'CurrentLiabilities', 'Total Current Liabilities', 'TotalCurrentLiabilities'],
        'Total Non Current Liabilities Net Minority Interest': ['Total Non Current Liabilities Net Minority Interest', 'Total Non Current Liabilities', 'NonCurrentLiabilities', 'Total Non-Current Liabilities Net Minority Interest', 'Total Non-Current Liabilities', 'TotalNonCurrentLiabilities'],
        'Long Term Debt And Capital Lease Obligation': ['Long Term Debt And Capital Lease Obligation', 'LongTermDebt', 'Long Term Debt'],
        'Other Non Current Liabilities': ['Other Non Current Liabilities', 'OtherNonCurrentLiabilities']
    }

    data['bs'] = {
        'annual': extract_with_aliases(bs_annual, bs_aliases),
        'quarterly': extract_with_aliases(bs_quarterly, bs_aliases)
    }

    # 損益計算書
    is_aliases = {
        'Total Revenue': ['Total Revenue', 'Revenue', 'Operating Revenue'],
        'Gross Profit': ['Gross Profit', 'GrossProfit'],
        'Operating Income': ['Operating Income', 'OperatingIncome', 'Operating Profit'],
        'Net Income': ['Net Income', 'NetIncome', 'Net Income Common Stockholders'],
        'Basic EPS': ['Basic EPS', 'BasicEPS', 'Earnings Per Share Basic']
    }

    df_is_annual = extract_with_aliases(is_annual, is_aliases)
    df_is_quarterly = extract_with_aliases(is_quarterly, is_aliases)
    data['is'] = {
        'annual': df_is_annual,
        'quarterly': df_is_quarterly
    }

    # キャッシュフロー
    cf_aliases = {
        'Operating Cash Flow': ['Operating Cash Flow', 'Cash Flow From Operating Activities', 'Net Cash From Operating Activities', 'Cash Flow from Continuing Operating Activities'],
        'Investing Cash Flow': ['Investing Cash Flow', 'Cash Flow From Investing Activities', 'Net Cash From Investing Activities', 'Cash Flow from Continuing Investing Activities'],
        'Financing Cash Flow': ['Financing Cash Flow', 'Cash Flow From Financing Activities', 'Net Cash From Financing Activities', 'Cash Flow from Continuing Financing Activities'],
        'Free Cash Flow': ['Free Cash Flow', 'FreeCashFlow']
    }
    df_cf_annual = extract_with_aliases(cf_annual, cf_aliases)
    df_cf_quarterly = extract_with_aliases(cf_quarterly, cf_aliases)


    # Net IncomeをISから取得してCFに結合 (CF側にNet Incomeがない場合が多いため)
    def merge_ni_to_cf(df_cf, df_is):
        if df_is.is_empty(): return df_cf
        ni_data = df_is.filter(pl.col('Item') == 'Net Income')
        if ni_data.is_empty(): return df_cf
        if df_cf.is_empty(): return ni_data
        return pl.concat([df_cf, ni_data]).sort(['Item', 'Date'])

    data['cf'] = {
        'annual': merge_ni_to_cf(df_cf_annual, df_is_annual),
        'quarterly': merge_ni_to_cf(df_cf_quarterly, df_is_quarterly)
    }

    # 4. 総還元性向
    def process_tp(df_cf, df_is=None):
        try:
            if df_cf is None or df_cf.empty:
                return pl.DataFrame()
            
            # Use pandas level merge to ensure Net Income is available
            df_source = df_cf.copy()
            if df_is is not None and not df_is.empty:
                # Find any net income related rows in IS
                ni_keys = ['Net Income', 'NetIncome', 'Net Income Common Stockholders', 'Net Income From Continuing Operations']
                ni_rows = df_is.loc[df_is.index.isin(ni_keys)]
                if not ni_rows.empty:
                    # Rename them to 'Net Income' for simplicity during merge
                    ni_rows.index = ['Net Income'] * len(ni_rows)
                    # If df_source already has these keys, they will be handled by extract_with_aliases
                    df_source = pd.concat([df_source, ni_rows])

            # Use extract_with_aliases logic locally for TP
            tp_aliases = {
                'Net Income From Continuing Operations': ['Net Income From Continuing Operations', 'Net Income from Continuing Operations', 'Net Income', 'NetIncome'],
                'Repurchase Of Capital Stock': ['Repurchase Of Capital Stock', 'Repurchase of Capital Stock', 'Repurchase Of Common Stock', 'Repurchase of Common Stock', 'Common Stock Repurchased', 'RepurchaseOfCapitalStock'],
                'Cash Dividends Paid': ['Cash Dividends Paid', 'Cash dividends paid', 'Common Stock Dividend Paid', 'CashDividendsPaid', 'DividendsPaid']
            }
            
            # Use existing extract_with_aliases if possible, or just mimic its logic
            # Since extract_with_aliases already exists in this scope, we can use it.
            df_tp_processed = extract_with_aliases(df_source, tp_aliases)
            
            if not df_tp_processed.is_empty():
                # Item, Date, Value の縦持ちデータを一度横持ち(Pivot)にして計算
                df_pivot = df_tp_processed.pivot(on='Item', index='Date', values='Value')
                
                # 必要な列が不足している場合の補完 (0で埋める)
                target_tp = list(tp_aliases.keys())
                for col in target_tp:
                    if col not in df_pivot.columns:
                        df_pivot = df_pivot.with_columns(pl.lit(0.0).alias(col))

                # データ欠損(null)を0で埋める
                df_pivot = df_pivot.fill_null(0.0)

                # 純利益が0以下の場合は、還元性向を0にする
                df_ratio_calc = df_pivot.with_columns([
                    pl.when(pl.col('Net Income From Continuing Operations') > 0)
                    .then(pl.col('Cash Dividends Paid').abs() / pl.col('Net Income From Continuing Operations'))
                    .otherwise(0.0)
                    .alias('Dividends Ratio / Net Income'),

                    pl.when(pl.col('Net Income From Continuing Operations') > 0)
                    .then((pl.col('Repurchase Of Capital Stock').abs() + pl.col('Cash Dividends Paid').abs()) / pl.col('Net Income From Continuing Operations'))
                    .otherwise(0.0)
                    .alias('Total Payout Ratio / Net Income')
                ])

                cols_to_melt = [c for c in target_tp if c in df_pivot.columns]
                df_amount_melt = df_pivot.unpivot(index='Date', on=cols_to_melt, variable_name='Item', value_name='Value')
                df_amount_melt = df_amount_melt.select(['Item', 'Date', 'Value'])
                df_amount_melt = df_amount_melt.with_columns(pl.col('Value').cast(pl.Float64, strict=False))

                df_ratios = df_ratio_calc.select(['Date', 'Dividends Ratio / Net Income', 'Total Payout Ratio / Net Income'])
                df_ratios_melt = df_ratios.unpivot(index='Date', variable_name='Item', value_name='Value').select(['Item', 'Date', 'Value'])
                df_ratios_melt = df_ratios_melt.with_columns(pl.col('Value').cast(pl.Float64, strict=False))
                
                return pl.concat([df_amount_melt, df_ratios_melt]).sort(['Item', 'Date'])
            else:
                return pl.DataFrame()
        except Exception as e:
            # print(f"Error in tp for {symbol}: {e}")
            return pl.DataFrame()

    data['tp'] = {
        'annual': process_tp(utils.safe_get(ticker_obj, 'cashflow'), is_annual),
        'quarterly': process_tp(utils.safe_get(ticker_obj, 'quarterly_cashflow'), is_quarterly)
    }

    # 7. Segment Revenue Data
    try:
        df_seg = utils.safe_call(ticker_obj, 'revenue_by_segment')
        if df_seg is not None and not df_seg.empty:
            # We want to filter out rows where all segment values are 0
            seg_cols = [c for c in df_seg.columns if c not in ['symbol', 'report_date']]
            if seg_cols:
                # Remove rows where all segments are 0 or NaN
                df_seg = df_seg[df_seg[seg_cols].sum(axis=1) > 0]
                if not df_seg.empty:
                    # Rename 'report_date' to 'Date' for consistency
                    df_seg = df_seg.rename(columns={'report_date': 'Date'})
                    # Convert to Polars
                    data['segment'] = pl.from_pandas(df_seg)
                else:
                    data['segment'] = pl.DataFrame()
            else:
                data['segment'] = pl.DataFrame()
        else:
            data['segment'] = pl.DataFrame()
    except Exception as e:
        print(f"Error fetching segment data for {symbol}: {e}")
        data['segment'] = pl.DataFrame()

    # 5. PER Valuation Data
    # try:
    #     # Get historical EPS
    #     is_q = get_attr(ticker_obj, ['quarterly_income_stmt', 'quarterly_incomestmt', 'quarterly_financials'])
    #     if is_q is not None and not is_q.empty:
    #         # Look for EPS items
    #         eps_keys = ['Basic EPS', 'BasicEPS', 'DilEarningsPerShare', 'Diluted EPS']
    #         eps_row = None
    #         for k in eps_keys:
    #             if k in is_q.index:
    #                 eps_row = is_q.loc[k]
    #                 break
    #         
    #         if eps_row is not None:
    #             # Convert to series and sort by date
    #             eps_series = eps_row.iloc[::-1] # Oldest to newest
    #             # Calculate TTM EPS (Rolling sum of 4 quarters)
    #             ttm_eps = eps_series.rolling(window=4).sum().dropna()
    #             
    #             if not ttm_eps.empty:
    #                 # Get historical prices for those dates
    #                 dates = ttm_eps.index
    #                 start_date = dates.min() - datetime.timedelta(days=5)
    #                 end_date = dates.max() + datetime.timedelta(days=5)
    #                 hist = ticker_obj.history(start=start_date, end=end_date)
    #                 
    #                 pe_list = []
    #                 for date in dates:
    #                     try:
    #                         # Use price close at the financial report date
    #                         target_date = date.replace(hour=0, minute=0, second=0, tzinfo=None)
    #                         if target_date in hist.index:
    #                             price = hist.loc[target_date]['Close']
    #                         else:
    #                             price = hist.asof(target_date)['Close']
    #                         
    #                         eps = ttm_eps[date]
    #                         if eps > 0: # Avoid negative PER for valuation
    #                             pe_list.append(price / eps)
    #                     except:
    #                         continue
    #                 
    #                 if pe_list:
    #                     pe_array = np.array(pe_list)
    #                     # Current PE
    #                     current_price = ticker_obj.fast_info['lastPrice']
    #                     current_ttm_eps = ttm_eps.iloc[-1]
    #                     current_pe = current_price / current_ttm_eps if current_ttm_eps > 0 else None
    #                     
    #                     data['valuation'] = {
    #                         'min': float(np.percentile(pe_array, 10)), # 10th percentile for stability
    #                         'median': float(np.median(pe_array)),
    #                         'max': float(np.percentile(pe_array, 90)), # 90th percentile for stability
    #                         'current': float(current_pe) if current_pe else None
    #                     }
    # except Exception as ve:
    #     print(f"Error calculating valuation for {symbol}: {ve}")

    # 6. 1株あたり配当金 (DPS)
    try:
        divs = utils.safe_get(ticker_obj, 'dividends')
        if divs is not None and not divs.empty:
            df_divs = divs.to_frame().reset_index()
            # 配当利回り計算のために、当時の株価を取得
            history = utils.safe_call(ticker_obj, 'history', start=df_divs['Date'].min(), end=df_divs['Date'].max() + datetime.timedelta(days=5))
            
            def get_price(date):
                if history.empty:
                    return None
                try:
                    # 時間を00:00:00に正規化し、マイクロ秒を除去
                    target_date = date.replace(hour=0, minute=0, second=0, microsecond=0)
                    
                    # タイムゾーンの整合性を確認（両方がaware、または両方がnaiveである必要がある）
                    if hasattr(target_date, 'tzinfo') and hasattr(history.index, 'tzinfo'):
                        if (target_date.tzinfo is None) != (history.index.tzinfo is None):
                            # 片方だけがnaiveな場合は、もう片方に合わせる
                            if target_date.tzinfo is None:
                                target_date = target_date.replace(tzinfo=history.index.tzinfo)
                            else:
                                target_date = target_date.replace(tzinfo=None)
                        elif target_date.tzinfo != history.index.tzinfo:
                            # 両方awareだがタイムゾーンが異なる場合は変換
                            target_date = target_date.astimezone(history.index.tzinfo)
                    
                    if target_date in history.index:
                        val = history.loc[target_date]['Close']
                    else:
                        # 休日などの場合は直近の営業日を探す
                        val = history.asof(target_date)
                        if isinstance(val, pd.Series):
                            val = val['Close']
                    
                    return float(val) if pd.notnull(val) else None
                except Exception:
                    return None

            df_divs['Price'] = df_divs['Date'].apply(get_price)
            df_divs['Date'] = df_divs['Date'].dt.strftime('%Y-%m-%d')
            
            # 年次集計
            df_divs['Year'] = df_divs['Date'].str.slice(0, 4)
            df_annual = df_divs.groupby('Year')['Dividends'].sum().reset_index()
            df_annual = df_annual.rename(columns={'Year': 'Date', 'Dividends': 'Value'})
            
            # 進行中の年度について、推定年間配当に置き換える (直近の配当 * 年間配当回数)
            current_year_str = str(datetime.datetime.now().year)
            df_annual['ActualValue'] = df_annual['Value']
            df_annual['EstimatedPart'] = 0.0
            df_annual['IsEstimate'] = False

            if current_year_str in df_annual['Date'].values:
                # 配当頻度の推定 (直近1〜2年のデータから年間回数を割り出す)
                # 昨年の配当回数、または直近12ヶ月の回数を確認
                last_year_str = str(datetime.datetime.now().year - 1)
                div_freq = 4 # デフォルトは四半期
                
                if last_year_str in df_annual['Date'].values:
                    # 昨年の実績回数をカウント
                    div_freq = len(df_divs[df_divs['Year'] == last_year_str])
                else:
                    # 昨年データがない場合は直近12ヶ月
                    one_year_ago = datetime.datetime.now() - datetime.timedelta(days=365)
                    # 文字列比較のために変換
                    one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')
                    div_freq = len(df_divs[df_divs['Date'] >= one_year_ago_str])
                
                # 異常値（0回や極端に多い場合）のガード
                if div_freq <= 0: div_freq = 4
                if div_freq > 12: div_freq = 12

                latest_q_div = df_divs.sort_values('Date')['Dividends'].iloc[-1]
                # 既に支払われた合計よりも推定値の方が大きい場合のみ更新
                est_total = latest_q_div * div_freq
                idx = df_annual[df_annual['Date'] == current_year_str].index[0]
                actual_paid = df_annual.loc[idx, 'Value']
                
                if est_total > actual_paid:
                    df_annual.loc[idx, 'Value'] = est_total
                    df_annual.loc[idx, 'ActualValue'] = actual_paid
                    df_annual.loc[idx, 'EstimatedPart'] = est_total - actual_paid
                    df_annual.loc[idx, 'IsEstimate'] = True
            
            # 各年の年初の株価を取得して配当利回りを計算
            def get_year_start_price(year):
                target_date = datetime.datetime(int(year), 1, 1)
                # タイムゾーンを履歴データに合わせる
                if hasattr(history.index, 'tzinfo') and history.index.tzinfo is not None:
                    target_date = target_date.replace(tzinfo=history.index.tzinfo)
                
                try:
                    # historyからその年以降の最初のデータを探す
                    year_data = history[history.index >= target_date]
                    if not year_data.empty:
                        return float(year_data.iloc[0]['Close'])
                    else:
                        return None
                except Exception:
                    return None

            df_annual['YearStartPrice'] = df_annual['Date'].apply(get_year_start_price)
            df_annual['Yield'] = df_annual.apply(
                lambda row: (row['Value'] / row['YearStartPrice']) if row['YearStartPrice'] and row['YearStartPrice'] > 0 else 0, 
                axis=1
            )
            df_annual['Item'] = 'DPS'
            
            # 四半期データ
            df_q = df_divs.copy().rename(columns={'Dividends': 'Value'})
            df_q['Item'] = 'DPS'

            data['dps'] = {
                'annual': pl.from_pandas(df_annual),
                'quarterly': pl.from_pandas(df_q[['Date', 'Value', 'Item', 'Price']])
            }
        else:
            data['dps'] = {'annual': pl.DataFrame(), 'quarterly': pl.DataFrame()}
    except Exception as e:
        print(f"Error fetching dividends for {symbol}: {e}")
        data['dps'] = {'annual': pl.DataFrame(), 'quarterly': pl.DataFrame()}

    # 8. Geographic Revenue Data
    try:
        df_geo = utils.safe_call(ticker_obj, 'revenue_by_geography')
        if df_geo is not None and not df_geo.empty:
            geo_cols = [c for c in df_geo.columns if c not in ['symbol', 'report_date']]
            if geo_cols:
                df_geo = df_geo[df_geo[geo_cols].sum(axis=1) > 0]
                if not df_geo.empty:
                    df_geo = df_geo.rename(columns={'report_date': 'Date'})
                    data['geography'] = pl.from_pandas(df_geo)
                else:
                    data['geography'] = pl.DataFrame()
            else:
                data['geography'] = pl.DataFrame()
        else:
            data['geography'] = pl.DataFrame()
    except Exception as e:
        print(f"Error fetching geographic data for {symbol}: {e}")
        data['geography'] = pl.DataFrame()

    return data

def get_geo_chart_html(data_dict):
    fig = get_geo_chart_data(data_dict)
    if isinstance(fig, str): return f'<h3 id="revenue-by-geography">地域別収益</h3><p>{fig}</p>'
    return '<h3 id="revenue-by-geography">地域別収益</h3>' + create_chart_html(fig)

def get_geo_chart_data(df_geo):
    if df_geo is None or df_geo.is_empty():
        return '詳細な地域別収益のデータが取得できませんでした（企業による未開示、またはデータソースの制約によるもの）。'
    
    # report_date (Date) と symbol を除く列が項目
    cols_all = [c for c in df_geo.columns if c not in ['symbol', 'Date']]
    if not cols_all:
        return '詳細な地域別収益のデータが取得できませんでした（企業による未開示、またはデータソースの制約によるもの）。'

    # 1. 四半期データ (直近12四半期)
    df_q = df_geo.sort('Date').tail(12)
    active_cols_q = [col for col in cols_all if df_q[col].sum() != 0]

    # 2. 通年データ (年次集計)
    try:
        df_a = df_geo.with_columns(
            pl.col("Date").str.slice(0, 4).alias("Year")
        ).group_by("Year").agg([pl.col(c).sum() for c in cols_all]).sort("Year").tail(5)
        active_cols_a = [col for col in cols_all if df_a[col].sum() != 0]
    except Exception as e:
        # print(f"Error in geo aggregation: {e}")
        df_a = pl.DataFrame()
        active_cols_a = []

    if not active_cols_q and not active_cols_a:
        return '詳細な地域別収益のデータが取得できませんでした（企業による未開示、またはデータソースの制約によるもの）。'

    fig = go.Figure()
    # カラーパレット
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
              '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']

    # 四半期トレース (Default visible)
    for i, col in enumerate(active_cols_q):
        fig.add_trace(go.Bar(
            name=f"{col} (四半期)",
            x=df_q['Date'],
            y=df_q[col],
            marker_color=colors[i % len(colors)],
            visible=True,
            hovertemplate='日付: %{x}<br>地域: ' + col + '<br>収益: $%{y:,.0f}<extra></extra>'
        ))

    # 通年トレース (Hidden by default)
    num_q = len(active_cols_q)
    for i, col in enumerate(active_cols_a):
        fig.add_trace(go.Bar(
            name=f"{col} (通年)",
            x=df_a['Year'],
            y=df_a[col],
            marker_color=colors[i % len(colors)],
            visible=False,
            hovertemplate='年度: %{x}<br>地域: ' + col + '<br>収益: $%{y:,.0f}<extra></extra>'
        ))

    fig.update_layout(
        barmode='stack',
        height=500,
        margin=dict(t=50, b=80, l=60, r=40),
        template='plotly_white',
        showlegend=True,
        xaxis=dict(type='category', tickangle=0),
        yaxis=dict(title='収益 ($)', type='linear', automargin=True, gridcolor='#F3F4F6'),
        legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5)
    )

    return fig

def get_valuation_chart_data(valuation_data):
    """
    valuation_data: {'min': 10, 'median': 20, 'max': 30, 'current': 25}
    1D Visualization of P/E valuation relative to history.
    """
    if not valuation_data or not valuation_data.get('current'):
        return "評価データ不足"
    
    v = valuation_data
    # Determine Color
    # Under median = green (undervalued), Over median = yellow/orange (overvalued)
    color = "#2ca02c" if v['current'] <= v['median'] else "#ff7f0e"
    if v['current'] > v['max']: color = "#d62728" # Deep red if over max

    fig = go.Figure()

    # Base track (range min to max)
    fig.add_trace(go.Scatter(
        x=[v['min'], v['max']], y=[0, 0],
        mode='lines+markers',
        line=dict(color='#E5E7EB', width=12),
        marker=dict(size=14, symbol='line-ns', color='#9CA3AF'),
        hoverinfo='skip',
        showlegend=False
    ))

    # Median Tick
    fig.add_trace(go.Scatter(
        x=[v['median']], y=[0],
        mode='markers+text',
        marker=dict(size=20, symbol='line-ns', color='#4B5563', line=dict(width=2)),
        text=["中央値"], textposition="bottom center",
        hovertemplate=f"過去5年中央値: {v['median']:.1f}x<extra></extra>",
        showlegend=False
    ))

    # Current Point
    fig.add_trace(go.Scatter(
        x=[v['current']], y=[0],
        mode='markers+text',
        marker=dict(size=18, color=color, line=dict(color='white', width=2)),
        text=[f"{v['current']:.1f}x"], textposition="top center",
        textfont=dict(size=16, color=color),
        hovertemplate=f"現在PER: {v['current']:.1f}x<extra></extra>",
        showlegend=False
    ))

    fig.update_layout(
        height=140, margin=dict(t=40, b=40, l=40, r=40),
        template='plotly_white',
        xaxis=dict(
            showgrid=False, zeroline=False, showticklabels=True,
            range=[min(v['min'], v['current']) * 0.8, max(v['max'], v['current']) * 1.2],
            dtick=5, ticks="outside", ticksuffix="x"
        ),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-1, 1]),
        hovermode='closest',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def get_dps_eps_chart_html(data_dict, is_data_dict):
    fig = get_dps_eps_chart_data(data_dict, is_data_dict)
    if isinstance(fig, str): return f'<h3 id="dividend-history">1株あたり配当金</h3><p>{fig}</p>'
    return '<h3 id="dividend-history">1株あたり配当金</h3>' + create_chart_html(fig)

def get_dps_eps_chart_data(data_dict, is_data_dict):
    df_annual = data_dict.get('annual', pl.DataFrame())
    df_q = data_dict.get('quarterly', pl.DataFrame())
    
    if df_annual.is_empty() and df_q.is_empty(): return '配当実績なし'
    
    fig = go.Figure()
    
    # --- 1. 年間配当 & 利回りトレース (棒グラフ & 折れ線) ---
    if not df_annual.is_empty():
        df_ann_plot = df_annual.sort('Date').tail(10)
        
        # 各年度のラベル（最上部に表示するため、推定があるかないかで出し分ける）
        def get_label(row):
            # 推定がない年度は実績バーにラベルを出し、推定がある年度はここでは出さない
            if not row['IsEstimate']:
                return f"${row['Value']:.2f}"
            return ""
        
        labels_base = [get_label(r) for r in df_ann_plot.to_dicts()]

        # 実績分の配当 (棒グラフ - 土台)
        fig.add_trace(go.Bar(
            name='年間配当 (年間推移)', x=df_ann_plot['Date'], y=df_ann_plot['ActualValue'],
            marker_color='#1f77b4', # 濃い青
            text=labels_base,
            textposition='auto',
            hovertemplate='年度: %{x}<br>配当額: $%{text}<extra></extra>',
            visible=True,
            legendgroup='annual_div',
            showlegend=True
        ))

        # 推定分の配当 (積み上げ用棒グラフ - 上乗せ分)
        if 'EstimatedPart' in df_ann_plot.columns and df_ann_plot['EstimatedPart'].sum() > 0:
            # 推定がある年度のみラベルを表示
            labels_est = [f"${r['Value']:.2f}" if r['IsEstimate'] else "" for r in df_ann_plot.to_dicts()]
            
            fig.add_trace(go.Bar(
                name='年間配当 (年間推移)', x=df_ann_plot['Date'], y=df_ann_plot['EstimatedPart'],
                marker_color='#aec7e8', # 薄い青
                text=labels_est,
                textposition='auto',
                hovertemplate='年度: %{x}<br>配当額: $%{text}<extra></extra>',
                visible=True,
                legendgroup='annual_div',
                showlegend=False
            ))
        
        # 利回りトレース (年初株価ベース - 折れ線)
        if 'Yield' in df_ann_plot.columns:
            fig.add_trace(go.Scatter(
                name='配当利回り (年間推移)', x=df_ann_plot['Date'], y=df_ann_plot['Yield'],
                mode='lines+markers',
                line=dict(color='#ff7f0e', width=3, dash='dot'),
                marker=dict(size=8),
                yaxis='y2',
                hovertemplate='年度: %{x}<br>配当利回り: %{y:.2%}<extra></extra>',
                visible=True
            ))

    # --- 2. 権利落日ごとの配当トレース ---
    if not df_q.is_empty():
        df_q_plot = df_q.sort('Date').tail(20)
        
        # 配当額 (棒グラフ)
        fig.add_trace(go.Bar(
            name='配当額 (権利落日別)', x=df_q_plot['Date'], y=df_q_plot['Value'],
            marker_color='#1f77b4',
            hovertemplate='権利落日: %{x}<br>配当額: $%{y:.4f}<extra></extra>',
            visible=False
        ))
        
        # 利回り (折れ線)
        if 'Price' in df_q_plot.columns and not df_q_plot['Price'].is_null().all():
            df_q_plot = df_q_plot.with_columns(
                (pl.col('Value') * 4 / pl.col('Price')).alias('Yield_Q')
            )
            fig.add_trace(go.Scatter(
                name='配当利回り (権利落日別)', x=df_q_plot['Date'], y=df_q_plot['Yield_Q'],
                mode='lines+markers',
                line=dict(color='#d62728', width=2, dash='dot'),
                yaxis='y2',
                hovertemplate='権利落日: %{x}<br>予想利回り: %{y:.2%}<extra></extra>',
                visible=False
            ))

    fig.update_layout(
        height=450, margin=dict(t=50, b=50, l=60, r=60),
        template='plotly_white', showlegend=True,
        barmode='stack',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(type='category', tickangle=0, gridcolor='#F3F4F6'),
        yaxis=dict(title="年間配当額 ($)", side="left", gridcolor="#F3F4F6", rangemode="tozero"),
        yaxis2=dict(title="配当利回り", side="right", overlaying="y", showgrid=False, tickformat=".2%", rangemode="tozero"),
        hovermode='x unified'
    )
    
    return fig
    
    return fig

def get_dps_history_chart_data(data_dict):
    # 下位互換性のために残すが、統合されたので実際には使用しない
    return '統合されました'

def create_chart_html(fig):
    """HTML化ヘルパー (ファイルサイズ削減のためライブラリ重複ロード回避)"""
    fig.update_xaxes(fixedrange=True)
    fig.update_yaxes(fixedrange=True)
    return fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False, 'scrollZoom': False, 'responsive': True})

def _add_traces(fig, df, func, visible=True):
    """トレース追加の共通ヘルパー"""
    if df.is_empty(): return
    func(fig, df, visible)

def get_bs_chart_html(data_dict):
    fig = get_bs_chart_data(data_dict)
    if isinstance(fig, str): return f'<h3 id="balance-sheet">貸借対照表</h3><p>{fig}</p>'
    return '<h3 id="balance-sheet">貸借対照表</h3>' + create_chart_html(fig)

def get_bs_chart_data(data_dict):
    df_a = data_dict.get('annual', pl.DataFrame())
    df_q = data_dict.get('quarterly', pl.DataFrame())
    if df_a.is_empty() and df_q.is_empty(): return 'データなし'
    
    fig = go.Figure()

    def add_bs_traces(fig, df, suffix="", visible=True):
        if df.is_empty(): return 0
        # 全ての日付を網羅したベースのDataFrameを作成
        all_dates = sorted(df['Date'].unique().to_list())
        df_base = pl.DataFrame({'Date': all_dates})
        
        def join_item(target_df, item_name, col_name):
            item_data = df.filter(pl.col('Item') == item_name).select(['Date', 'Value']).rename({'Value': col_name})
            return target_df.join(item_data, on='Date', how='left').fill_null(0.0)

        # 必要項目の結合
        df_plot = join_item(df_base, 'Current Assets', 'CurrAssets')
        df_plot = join_item(df_plot, 'Total Non Current Assets', 'NonCurrAssets')
        df_plot = join_item(df_plot, 'Current Liabilities', 'CurrLiab')
        df_plot = join_item(df_plot, 'Total Equity Gross Minority Interest', 'Equity')
        df_plot = join_item(df_plot, 'Total Assets', 'TotalAssets')
        df_plot = join_item(df_plot, 'Total Liabilities Net Minority Interest', 'TotalLiab')

        # 総資産が0のデータを除外
        df_plot = df_plot.filter(pl.col('TotalAssets') > 0)
        if df_plot.is_empty(): return 0

        # 固定負債
        total_non_curr_liab = df.filter(pl.col('Item') == 'Total Non Current Liabilities Net Minority Interest').select(['Date', 'Value']).rename({'Value': 'FixedLiab'})
        if not total_non_curr_liab.is_empty() and total_non_curr_liab['FixedLiab'].sum() != 0:
            df_plot = df_plot.join(total_non_curr_liab, on='Date', how='left').fill_null(0.0)
        else:
            df_plot = df_plot.with_columns(pl.lit(0.0).alias('FixedLiab'))

        # 表示制限
        limit = 6 if suffix == "" else 8
        df_plot = df_plot.tail(limit)

        # 数値を省略形式でフォーマットする関数
        def format_bs_val(series):
            return [
                f"${v/1e9:.1f}B" if abs(v) >= 1e9 else (f"${v/1e6:.0f}M" if abs(v) >= 1e6 else f"${v:.0f}")
                for v in series
            ]

        trace_count = 0
        has_breakdown = (df_plot['CurrAssets'].sum() != 0)
        if has_breakdown:
            # 資産側 - 下から順に追加 (固定->流動)
            fig.add_trace(go.Bar(
                name='固定資産' + suffix, x=df_plot['Date'], y=df_plot['NonCurrAssets'], 
                marker_color='#1f77b4', offsetgroup=0, visible=visible,
                text=format_bs_val(df_plot['NonCurrAssets']), textposition='auto'
            )) 
            fig.add_trace(go.Bar(
                name='流動資産' + suffix, x=df_plot['Date'], y=df_plot['CurrAssets'], 
                marker_color='#aec7e8', offsetgroup=0, visible=visible,
                text=format_bs_val(df_plot['CurrAssets']), textposition='auto'
            ))
            # 負債・純資産側 - 下から順に追加 (純資産->固定負債->流動負債)
            fig.add_trace(go.Bar(
                name='純資産' + suffix, x=df_plot['Date'], y=df_plot['Equity'], 
                marker_color='#2ca02c', offsetgroup=1, visible=visible,
                text=format_bs_val(df_plot['Equity']), textposition='auto'
            ))
            fig.add_trace(go.Bar(
                name='固定負債' + suffix, x=df_plot['Date'], y=df_plot['FixedLiab'], 
                marker_color='#ff7f0e', offsetgroup=1, visible=visible,
                text=format_bs_val(df_plot['FixedLiab']), textposition='auto'
            ))
            fig.add_trace(go.Bar(
                name='流動負債' + suffix, x=df_plot['Date'], y=df_plot['CurrLiab'], 
                marker_color='#ffbb78', offsetgroup=1, visible=visible,
                text=format_bs_val(df_plot['CurrLiab']), textposition='auto'
            ))
            trace_count = 5
        else:
            fig.add_trace(go.Bar(
                name='総資産' + suffix, x=df_plot['Date'], y=df_plot['TotalAssets'], 
                marker_color='#1f77b4', offsetgroup=0, visible=visible,
                text=format_bs_val(df_plot['TotalAssets']), textposition='auto'
            ))
            fig.add_trace(go.Bar(
                name='純資産' + suffix, x=df_plot['Date'], y=df_plot['Equity'], 
                marker_color='#2ca02c', offsetgroup=1, visible=visible,
                text=format_bs_val(df_plot['Equity']), textposition='auto'
            ))
            fig.add_trace(go.Bar(
                name='総負債' + suffix, x=df_plot['Date'], y=df_plot['TotalLiab'], 
                marker_color='#ff7f0e', offsetgroup=1, visible=visible,
                text=format_bs_val(df_plot['TotalLiab']), textposition='auto'
            ))
        return trace_count

    add_bs_traces(fig, df_a, suffix=" (通年)", visible=True)
    add_bs_traces(fig, df_q, suffix=" (四半期)", visible=False)

    fig.update_layout(barmode='relative', height=500, margin=dict(t=50, b=80, l=60, r=40),
                      template='plotly_white', showlegend=True,
                      xaxis=dict(type='category', tickangle=0),
                      yaxis=dict(type='linear', rangemode='tozero', automargin=True, gridcolor='#F3F4F6'),
                      legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5),
                      uniformtext=dict(minsize=8, mode='hide'))
    fig.update_traces(texttemplate='%{text}', textposition='inside')
    return fig

def get_is_chart_html(data_dict):
    fig = get_is_chart_data(data_dict)
    if isinstance(fig, str): return f'<h3 id="income-statement">損益計算書</h3><p>{fig}</p>'
    return '<h3 id="income-statement">損益計算書</h3>' + create_chart_html(fig)

def get_is_chart_data(data_dict):
    df_a = data_dict.get('annual', pl.DataFrame())
    df_q = data_dict.get('quarterly', pl.DataFrame())
    if df_a.is_empty() and df_q.is_empty(): return 'データなし'

    fig = go.Figure()

    def add_is_traces(fig, df, suffix="", visible=True):
        if df.is_empty(): return 0
        df = df.with_columns(pl.col('Date').cast(pl.Utf8))
        df = df.filter(pl.col('Date').str.contains('TTM').not_())
        
        limit = 6 if suffix == "" else 8
        valid_dates = df.filter((pl.col('Item') == 'Total Revenue') & (pl.col('Value') > 0)) \
                          .select('Date').unique().sort('Date')['Date'].to_list()[-limit:]
        if not valid_dates: return 0
        df_plot = df.filter(pl.col('Date').is_in(valid_dates)).unique(subset=['Item', 'Date']).sort('Date')

        trace_count = 0
        items = [('Total Revenue', '売上高', '#aec7e8'), ('Gross Profit', '売上総利益', '#1f77b4'),
                 ('Operating Income', '営業利益', '#ffbb78'), ('Net Income', '純利益', '#2ca02c')]
        for item_key, name, color in items:
            sub = df_plot.filter(pl.col('Item') == item_key)
            if not sub.is_empty():
                fig.add_trace(go.Bar(name=name + suffix, x=sub['Date'], y=sub['Value'], marker_color=color, visible=visible))
                trace_count += 1
        
        # 利益率
        try:
            df_pivot = df_plot.pivot(on='Item', index='Date', values='Value').sort('Date')
            ratio_configs = [('Gross Profit', 'Total Revenue', '売上総利益率', '#1f77b4'),
                             ('Operating Income', 'Total Revenue', '営業利益率', '#ffbb78'),
                             ('Net Income', 'Total Revenue', '純利益率', '#2ca02c')]
            for num, den, name, color in ratio_configs:
                if num in df_pivot.columns and den in df_pivot.columns:
                    calc = df_pivot.with_columns((pl.col(num) / pl.col(den)).alias('Ratio'))
                    fig.add_trace(go.Scatter(name=name + suffix, x=calc['Date'], y=calc['Ratio'], line=dict(color=color, width=2), mode='lines+markers', yaxis='y2', hovertemplate='%{y:.1%}', visible=visible))
                    trace_count += 1
        except: pass
        return trace_count

    # トレース追加
    add_is_traces(fig, df_a, suffix=" (通年)", visible=True)
    add_is_traces(fig, df_q, suffix=" (四半期)", visible=False)

    # 利益率の軸(yaxis2)の範囲調整
    ratios = [v for t in fig.data if t.yaxis == 'y2' and t.y is not None for v in (t.y if isinstance(t.y, (list, tuple)) else (t.y.tolist() if hasattr(t.y, 'tolist') else t.y.to_list())) if v is not None]
    y2_range = None
    if ratios:
        max_r = max(ratios)
        min_r = min(ratios)
        y2_range = [min(0, min_r - 0.05), max_r + 0.05]

    fig.update_layout(
        barmode='group', height=500, margin=dict(t=50, b=80, l=60, r=60), 
        template='plotly_white', showlegend=True,
        xaxis=dict(type='category', tickangle=0),
        yaxis=dict(title='金額', showgrid=True, type='linear', automargin=True, gridcolor='#F3F4F6', zeroline=True, zerolinecolor='#444', zerolinewidth=2, rangemode='tozero'),
        yaxis2=dict(title='利益率', overlaying='y', side='right', tickformat='.0%', showgrid=False, type='linear', automargin=True, zeroline=True, zerolinecolor='#444', zerolinewidth=2, range=y2_range),
        legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5))
    return fig

def get_cf_chart_html(data_dict):
    fig = get_cf_chart_data(data_dict)
    if isinstance(fig, str): return f'<h3 id="cash-flow">キャッシュフロー</h3><p>{fig}</p>'
    return '<h3 id="cash-flow">キャッシュフロー</h3>' + create_chart_html(fig)

def get_cf_chart_data(data_dict):
    df_a = data_dict.get('annual', pl.DataFrame())
    df_q = data_dict.get('quarterly', pl.DataFrame())
    if df_a.is_empty() and df_q.is_empty(): return 'データなし'
    
    fig = go.Figure()

    def add_cf_traces(fig, df, suffix="", visible=True):
        if df.is_empty(): return 0
        df = df.with_columns(pl.col('Date').cast(pl.Utf8))
        df = df.filter(pl.col('Date').str.contains('TTM').not_())
        limit = 6 if suffix == "" else 8
        valid_dates = df.filter(pl.col('Item') == 'Operating Cash Flow').select('Date').unique().sort('Date')['Date'].to_list()[-limit:]
        if not valid_dates: return 0
        df_plot = df.filter(pl.col('Date').is_in(valid_dates)).unique(subset=['Item', 'Date']).sort('Date')

        trace_count = 0
        items = [('Net Income', '純利益', '#2ca02c'), ('Operating Cash Flow', '営業CF', '#aec7e8'), 
                 ('Investing Cash Flow', '投資CF', '#1f77b4'), ('Financing Cash Flow', '財務CF', '#ffbb78'), 
                 ('Free Cash Flow', 'フリーCF', '#9467bd')]

        for item_key, name, color in items:
            sub = df_plot.filter(pl.col('Item') == item_key)
            if not sub.is_empty():
                fig.add_trace(go.Bar(name=name + suffix, x=sub['Date'], y=sub['Value'], marker_color=color, visible=visible))
                trace_count += 1
        return trace_count

    add_cf_traces(fig, df_a, suffix=" (通年)", visible=True)
    add_cf_traces(fig, df_q, suffix=" (四半期)", visible=False)

    fig.update_layout(barmode='group', height=500, margin=dict(t=50, b=80, l=60, r=40),
                      template='plotly_white', showlegend=True,
                      xaxis=dict(type='category', tickangle=0),
                      yaxis=dict(title='金額', type='linear', automargin=True),
                      legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5))
    return fig

def get_tp_chart_html(data_dict):
    fig = get_tp_chart_data(data_dict)
    if isinstance(fig, str): return f'<h3 id="shareholder-return">株主還元</h3><p>{fig}</p>'
    return '<h3 id="shareholder-return">株主還元</h3>' + create_chart_html(fig)

def get_tp_chart_data(data_dict):
    df_a = data_dict.get('annual', pl.DataFrame())
    df_q = data_dict.get('quarterly', pl.DataFrame())
    if df_a.is_empty() and df_q.is_empty(): return 'データなし'

    fig = go.Figure()
    
    def add_tp_traces(fig, df, suffix="", visible=True):
        if df.is_empty(): return 0
        df = df.with_columns(pl.col('Date').cast(pl.Utf8))
        df = df.filter(pl.col('Date').str.contains('TTM').not_())
        
        # 日付の選定
        limit = 6 if suffix == "" else 8
        valid_dates = df.filter(pl.col('Item') == 'Net Income From Continuing Operations').select('Date').unique().sort('Date')['Date'].to_list()[-limit:]
        if not valid_dates: return 0
        
        df_plot = df.filter(pl.col('Date').is_in(valid_dates)).unique(subset=['Item', 'Date']).sort('Date')
        
        ni = df_plot.filter(pl.col('Item') == 'Net Income From Continuing Operations')
        div = df_plot.filter(pl.col('Item') == 'Cash Dividends Paid')
        repo = df_plot.filter(pl.col('Item') == 'Repurchase Of Capital Stock')
        div_r = df_plot.filter(pl.col('Item') == 'Dividends Ratio / Net Income')
        total_r = df_plot.filter(pl.col('Item') == 'Total Payout Ratio / Net Income')

        trace_count = 0
        # 純利益
        if not ni.is_empty():
            fig.add_trace(go.Bar(name='純利益' + suffix, x=ni['Date'], y=ni['Value'], marker_color='#2ca02c', offsetgroup=0, visible=visible))
            trace_count += 1
        # 還元
        if not div.is_empty():
            dv = div['Value'].abs()
            fig.add_trace(go.Bar(name='配当金' + suffix, x=div['Date'], y=dv, marker_color='#aec7e8', offsetgroup=1, visible=visible))
            trace_count += 1
        if not repo.is_empty():
            rv = repo['Value'].abs()
            fig.add_trace(go.Bar(name='自社株買い' + suffix, x=repo['Date'], y=rv, marker_color='#1f77b4', offsetgroup=1, visible=visible))
            trace_count += 1
        
        # 性向
        if not div_r.is_empty():
            fig.add_trace(go.Scatter(name='配当性向' + suffix, x=div_r['Date'], y=div_r['Value'], marker_color='#ffbb78', mode='lines+markers', yaxis='y2', hovertemplate='%{y:.1%}', visible=visible))
            trace_count += 1
        if not total_r.is_empty():
            fig.add_trace(go.Scatter(name='総還元性向' + suffix, x=total_r['Date'], y=total_r['Value'], marker_color='#ff7f0e', mode='lines+markers', yaxis='y2', hovertemplate='%{y:.1%}', visible=visible))
            trace_count += 1
        return trace_count

    # トレースの追加
    add_tp_traces(fig, df_a, suffix=" (通年)", visible=True)
    add_tp_traces(fig, df_q, suffix=" (四半期)", visible=False)

    # 利益率の軸(yaxis2)の範囲調整
    ratios = [v for t in fig.data if t.yaxis == 'y2' and t.y is not None for v in (t.y if isinstance(t.y, (list, tuple)) else (t.y.tolist() if hasattr(t.y, 'tolist') else t.y.to_list())) if v is not None]
    y2_range = None
    if ratios:
        max_r = max(ratios)
        min_r = min(ratios)
        y2_range = [min(0, min_r - 0.05), max_r + 0.05]

    fig.update_layout(
        barmode='group', height=500, margin=dict(t=50, b=80, l=60, r=60), template='plotly_white',
        xaxis=dict(type='category', tickangle=0),
        yaxis=dict(title='金額', showgrid=True, type='linear', automargin=True),
        yaxis2=dict(title='還元性向', overlaying='y', side='right', tickformat='.0%', showgrid=False, type='linear', automargin=True, range=y2_range),
        legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5)
    )
    return fig

def get_segment_chart_html(data_dict):
    fig = get_segment_chart_data(data_dict)
    if isinstance(fig, str): return f'<h3 id="revenue-by-segment">セグメント収益</h3><p>{fig}</p>'
    return '<h3 id="revenue-by-segment">セグメント収益</h3>' + create_chart_html(fig)

def get_segment_chart_data(df_segment):
    if df_segment is None or df_segment.is_empty():
        return '詳細なセグメント収益のデータが取得できませんでした（企業による未開示、またはデータソースの制約によるもの）。'

    # report_date (Date) と symbol を除く列がセグメント
    seg_cols_all = [c for c in df_segment.columns if c not in ['symbol', 'Date']]
    if not seg_cols_all:
        return '詳細なセグメント収益のデータが取得できませんでした（企業による未開示、またはデータソースの制約によるもの）。'

    # 1. 四半期データ (直近12四半期)
    df_q = df_segment.sort('Date').tail(12)
    active_seg_cols_q = [col for col in seg_cols_all if df_q[col].sum() != 0]

    # 2. 通年データ (年次集計)
    try:
        df_a = df_segment.with_columns(
            pl.col("Date").str.slice(0, 4).alias("Year")
        ).group_by("Year").agg([pl.col(c).sum() for c in seg_cols_all]).sort("Year").tail(5)
        active_seg_cols_a = [col for col in seg_cols_all if df_a[col].sum() != 0]
    except Exception as e:
        # print(f"Error in segment aggregation: {e}")
        df_a = pl.DataFrame()
        active_seg_cols_a = []

    if not active_seg_cols_q and not active_seg_cols_a:
        return '詳細なセグメント収益のデータが取得できませんでした（企業による未開示、またはデータソースの制約によるもの）。'
    fig = go.Figure()
    # 視認性の高いカラーパレット
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
              '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']

    # 四半期トレース (Default visible)
    for i, col in enumerate(active_seg_cols_q):
        fig.add_trace(go.Bar(
            name=f"{col} (四半期)",
            x=df_q['Date'],
            y=df_q[col],
            marker_color=colors[i % len(colors)],
            visible=True,
            hovertemplate='日付: %{x}<br>セグメント: ' + col + '<br>収益: $%{y:,.0f}<extra></extra>'
        ))

    # 通年トレース (Hidden by default)
    num_q = len(active_seg_cols_q)
    for i, col in enumerate(active_seg_cols_a):
        fig.add_trace(go.Bar(
            name=f"{col} (通年)",
            x=df_a['Year'],
            y=df_a[col],
            marker_color=colors[i % len(colors)],
            visible=False,
            hovertemplate='年度: %{x}<br>セグメント: ' + col + '<br>収益: $%{y:,.0f}<extra></extra>'
        ))

    fig.update_layout(
        barmode='stack',
        height=500,
        margin=dict(t=50, b=80, l=60, r=40),
        template='plotly_white',
        showlegend=True,
        xaxis=dict(type='category', tickangle=0),
        yaxis=dict(title='収益 ($)', type='linear', automargin=True, gridcolor='#F3F4F6'),
        legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5)
    )

    return fig
