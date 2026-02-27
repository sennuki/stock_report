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
            val = getattr(obj, name, None)
            if val is not None and not (isinstance(val, pd.DataFrame) and val.empty):
                return val
        return None

    def extract_and_melt(pandas_df, targets):
        if pandas_df is None or (isinstance(pandas_df, pd.DataFrame) and pandas_df.empty):
            return pl.DataFrame()
        try:
            # Convert to Polars, keeping index
            df = pl.from_pandas(pandas_df, include_index=True)
            # Rename first column to 'Item'
            df = df.rename({df.columns[0]: 'Item'})
            # Filter
            df_filtered = df.filter(pl.col('Item').is_in(targets))
            if df_filtered.is_empty():
                return pl.DataFrame()
            return get_melt(df_filtered)
        except Exception as e:
            print(f"Error processing data for {symbol}: {e}")
            return pl.DataFrame()

    # 1. 貸借対照表
    bs_annual = get_attr(ticker_obj, ['balancesheet', 'balance_sheet'])
    bs_quarterly = get_attr(ticker_obj, ['quarterly_balancesheet', 'quarterly_balance_sheet'])
    
    if (bs_annual is None or bs_annual.empty) and (bs_quarterly is None or bs_quarterly.empty):
        utils.log_event("WARN", symbol, "Financial data (BS) is empty. GitHub Actions IP might be blocked or data is unavailable.")
    
    # 2. 損益計算書
    is_annual = get_attr(ticker_obj, ['income_stmt', 'incomestmt', 'financials'])
    is_quarterly = get_attr(ticker_obj, ['quarterly_income_stmt', 'quarterly_incomestmt', 'quarterly_financials'])

    # 3. キャッシュフロー
    cf_annual = get_attr(ticker_obj, ['cashflow', 'cash_flow'])
    cf_quarterly = get_attr(ticker_obj, ['quarterly_cashflow', 'quarterly_cash_flow'])

    # Total Liabilities がない場合の補完
    def ensure_total_liabilities(df_pd):
        if df_pd is None or df_pd.empty: return df_pd
        if 'Total Liabilities' not in df_pd.index:
            for alt in ['Total Liabilities Net Minority Interest', 'Total Liabilities And Equity']:
                if alt in df_pd.index:
                    df_pd.loc['Total Liabilities'] = df_pd.loc[alt]
                    break
        return df_pd

    target_bs = ['Total Non Current Assets', 'Current Liabilities', 'Total Equity Gross Minority Interest',
                 'Current Assets', 'Total Non Current Liabilities Net Minority Interest',
                 'Total Assets', 'Total Liabilities Net Minority Interest', 'Total Liabilities',
                 'Long Term Debt And Capital Lease Obligation','Employee Benefits', 'Non Current Deferred Liabilities',
                 'Other Non Current Liabilities']

    data['bs'] = {
        'annual': extract_and_melt(ensure_total_liabilities(bs_annual), target_bs),
        'quarterly': extract_and_melt(ensure_total_liabilities(bs_quarterly), target_bs)
    }

    # 損益計算書
    is_aliases = {
        'Total Revenue': ['Total Revenue', 'Revenue', 'Operating Revenue'],
        'Gross Profit': ['Gross Profit', 'GrossProfit'],
        'Operating Income': ['Operating Income', 'OperatingIncome', 'Operating Profit'],
        'Net Income': ['Net Income', 'NetIncome', 'Net Income Common Stockholders'],
        'Basic EPS': ['Basic EPS', 'BasicEPS', 'Earnings Per Share Basic']
    }

    def extract_with_aliases(pandas_df, alias_dict):
        if pandas_df is None or (isinstance(pandas_df, pd.DataFrame) and pandas_df.empty):
            return pl.DataFrame()
        try:
            df = pl.from_pandas(pandas_df, include_index=True)
            df = df.rename({df.columns[0]: 'Item'})
            
            # 各標準名に対して、最初に見つかったエイリアスの名前を標準名に書き換える
            for standard_name, aliases in alias_dict.items():
                for alias in aliases:
                    if alias in df['Item'].to_list() and alias != standard_name:
                        # 標準名が既に存在する場合は、それを優先する（何もしない）
                        if standard_name not in df['Item'].to_list():
                             df = df.with_columns(
                                 pl.when(pl.col('Item') == alias)
                                 .then(pl.lit(standard_name))
                                 .otherwise(pl.col('Item'))
                                 .alias('Item')
                             )
                        break
            
            target_list = list(alias_dict.keys())
            df_filtered = df.filter(pl.col('Item').is_in(target_list))
            if df_filtered.is_empty():
                return pl.DataFrame()
            return get_melt(df_filtered)
        except Exception as e:
            print(f"Error processing aliases for {symbol}: {e}")
            return pl.DataFrame()

    df_is_annual = extract_with_aliases(is_annual, is_aliases)
    df_is_quarterly = extract_with_aliases(is_quarterly, is_aliases)
    data['is'] = {
        'annual': df_is_annual,
        'quarterly': df_is_quarterly
    }

    # キャッシュフロー
    cf_aliases = {
        'Operating Cash Flow': ['Operating Cash Flow', 'Cash Flow From Operating Activities', 'Net Cash From Operating Activities'],
        'Investing Cash Flow': ['Investing Cash Flow', 'Cash Flow From Investing Activities', 'Net Cash From Investing Activities'],
        'Financing Cash Flow': ['Financing Cash Flow', 'Cash Flow From Financing Activities', 'Net Cash From Financing Activities'],
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
    def process_tp(df_source):
        try:
            if df_source is None or df_source.empty:
                return pl.DataFrame()
            
            df_tp = pl.from_pandas(df_source, include_index=True)
            df_tp = df_tp.rename({df_tp.columns[0]: 'Item'})
            
            # 項目名の補完: Net Income From Continuing Operations がない場合は Net Income を使う
            if 'Net Income From Continuing Operations' not in df_tp['Item'].to_list():
                if 'Net Income' in df_tp['Item'].to_list():
                    # Net Income の行をコピーして Item 名を変更
                    ni_row = df_tp.filter(pl.col('Item') == 'Net Income').with_columns(pl.lit('Net Income From Continuing Operations').alias('Item'))
                    df_tp = pl.concat([df_tp, ni_row])

            target_tp = ['Net Income From Continuing Operations', 'Repurchase Of Capital Stock', 'Cash Dividends Paid']
            df_tp = df_tp.filter(pl.col('Item').is_in(target_tp))
            if not df_tp.is_empty():
                df_tp_melt = get_melt(df_tp)
                df_pivot = df_tp_melt.pivot(on='Item', index='Date', values='Value')
                
                # 必要な列が不足している場合の補完
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
            print(f"Error in tp for {symbol}: {e}")
            return pl.DataFrame()

    data['tp'] = {
        'annual': process_tp(ticker_obj.cashflow),
        'quarterly': process_tp(ticker_obj.quarterly_cashflow)
    }

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
        divs = ticker_obj.dividends
        if not divs.empty:
            df_divs = divs.to_frame().reset_index()
            # 配当利回り計算のために、当時の株価を取得
            history = ticker_obj.history(start=df_divs['Date'].min(), end=df_divs['Date'].max() + datetime.timedelta(days=5))
            
            def get_price(date):
                try:
                    # 配当落ち日前後の終値を取得 (権利落ち日は株価が下がるため、前日の価格が理想的だが、
                    # ここではシンプルに当日のCloseを取得)
                    target_date = date.replace(hour=0, minute=0, second=0)
                    if target_date in history.index:
                        return history.loc[target_date]['Close']
                    else:
                        # 休日などの場合は直近の営業日を探す
                        return history.asof(target_date)['Close']
                except:
                    return None

            df_divs['Price'] = df_divs['Date'].apply(get_price)
            df_divs['Date'] = df_divs['Date'].dt.strftime('%Y-%m-%d')
            
            # 年次集計
            df_divs['Year'] = df_divs['Date'].str.slice(0, 4)
            df_annual = df_divs.groupby('Year')['Dividends'].sum().reset_index()
            df_annual = df_annual.rename(columns={'Year': 'Date', 'Dividends': 'Value'})
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

    return data

def get_valuation_plotly_fig(valuation_data):
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

def get_dps_eps_plotly_html(data_dict, is_data_dict):
    fig = get_dps_eps_plotly_fig(data_dict, is_data_dict)
    if isinstance(fig, str): return f'<h3 id="dividend-history">1株あたり配当金</h3><p>{fig}</p>'
    return '<h3 id="dividend-history">1株あたり配当金</h3>' + create_chart_html(fig)

def get_dps_eps_plotly_fig(data_dict, is_data_dict):
    df_annual = data_dict.get('annual', pl.DataFrame())
    
    if df_annual.is_empty(): return '配当実績なし'
    
    # 直近5年分程度を表示
    df_plot = df_annual.sort('Date').tail(10)

    fig = go.Figure()

    # 配当額
    fig.add_trace(go.Bar(
        name='年間配当', x=df_plot['Date'], y=df_plot['Value'],
        marker_color='#1f77b4',
        text=df_plot['Value'].map_elements(lambda x: f"${x:.2f}", return_dtype=pl.Utf8),
        textposition='auto',
        hovertemplate='年度: %{x}<br>合計配当額: $%{y:.2f}<extra></extra>'
    ))

    fig.update_layout(
        height=450, margin=dict(t=50, b=80, l=60, r=40),
        template='plotly_white',
        xaxis=dict(title='年度', type='category'),
        yaxis=dict(title='年間配当額 ($)', side='left', gridcolor='#F3F4F6', range=[0, df_plot['Value'].max() * 1.2]),
        legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5),
        hovermode='x unified'
    )
    
    return fig

def create_chart_html(fig):
    """HTML化ヘルパー (ファイルサイズ削減のためライブラリ重複ロード回避)"""
    fig.update_xaxes(fixedrange=True)
    fig.update_yaxes(fixedrange=True)
    return fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False, 'scrollZoom': False, 'responsive': True})

def _add_traces(fig, df, func, visible=True):
    """トレース追加の共通ヘルパー"""
    if df.is_empty(): return
    func(fig, df, visible)

def get_bs_plotly_html(data_dict):
    fig = get_bs_plotly_fig(data_dict)
    if isinstance(fig, str): return f'<h3 id="balance-sheet">貸借対照表</h3><p>{fig}</p>'
    return '<h3 id="balance-sheet">貸借対照表</h3>' + create_chart_html(fig)

def get_bs_plotly_fig(data_dict):
    df_a = data_dict.get('annual', pl.DataFrame())
    
    if df_a.is_empty(): return 'データなし'
    
    fig = go.Figure()

    def add_bs_traces(fig, df):
        # 全ての日付を網羅したベースのDataFrameを作成
        all_dates = sorted(df['Date'].unique().to_list())
        df_plot = pl.DataFrame({'Date': all_dates})
        
        def join_item(target_df, item_name, col_name):
            item_data = df.filter(pl.col('Item') == item_name).select(['Date', 'Value']).rename({'Value': col_name})
            return target_df.join(item_data, on='Date', how='left').fill_null(0.0)

        # 必要項目の結合
        df_plot = join_item(df_plot, 'Current Assets', 'CurrAssets')
        df_plot = join_item(df_plot, 'Total Non Current Assets', 'NonCurrAssets')
        df_plot = join_item(df_plot, 'Current Liabilities', 'CurrLiab')
        df_plot = join_item(df_plot, 'Total Equity Gross Minority Interest', 'Equity')
        df_plot = join_item(df_plot, 'Total Assets', 'TotalAssets')
        df_plot = join_item(df_plot, 'Total Liabilities Net Minority Interest', 'TotalLiab')
        if 'TotalLiab' not in df_plot.columns or df_plot['TotalLiab'].sum() == 0:
             df_plot = join_item(df_plot, 'Total Liabilities', 'TotalLiab')

        # 総資産が0のデータ（実質的な欠損データ）を除外
        df_plot = df_plot.filter(pl.col('TotalAssets') > 0)

        # 固定負債の計算
        total_non_curr_liab = df.filter(pl.col('Item') == 'Total Non Current Liabilities Net Minority Interest').select(['Date', 'Value']).rename({'Value': 'FixedLiab'})
        
        if not total_non_curr_liab.is_empty() and total_non_curr_liab['FixedLiab'].sum() != 0:
            df_plot = df_plot.join(total_non_curr_liab, on='Date', how='left').fill_null(0.0)
        else:
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
                df_plot = df_plot.with_columns(pl.lit(0.0).alias('FixedLiab'))

        has_breakdown = (df_plot['CurrAssets'].sum() != 0)

        if has_breakdown:
            df_plot = df_plot.with_columns([
                pl.when(pl.col('Equity') > 0).then(pl.col('Equity')).otherwise(0.0).alias('BaseFixed'),
            ])
            df_plot = df_plot.with_columns([
                (pl.col('BaseFixed') + pl.col('FixedLiab')).alias('BaseCurr')
            ])

            fig.add_trace(go.Bar(name='流動資産', x=df_plot['Date'], y=df_plot['CurrAssets'], marker_color='#aec7e8',
                                base=df_plot['NonCurrAssets'], offsetgroup=0))
            fig.add_trace(go.Bar(name='固定資産', x=df_plot['Date'], y=df_plot['NonCurrAssets'], marker_color='#1f77b4',
                                 base=0, offsetgroup=0)) 
            
            fig.add_trace(go.Bar(name='流動負債', x=df_plot['Date'], y=df_plot['CurrLiab'], marker_color='#ffbb78',
                                 base=df_plot['BaseCurr'], offsetgroup=1))
            fig.add_trace(go.Bar(name='固定負債', x=df_plot['Date'], y=df_plot['FixedLiab'], marker_color='#ff7f0e',
                                 base=df_plot['BaseFixed'], offsetgroup=1))
            fig.add_trace(go.Bar(name='純資産', x=df_plot['Date'], y=df_plot['Equity'], marker_color='#2ca02c',
                                 base=0, offsetgroup=1))
            return 5
        
        elif df_plot['TotalAssets'].sum() != 0:
            df_plot = df_plot.with_columns([
                pl.when(pl.col('Equity') > 0).then(pl.col('Equity')).otherwise(0.0).alias('BaseLiab'),
            ])
            
            fig.add_trace(go.Bar(name='総資産', x=df_plot['Date'], y=df_plot['TotalAssets'], marker_color='#1f77b4',
                                 offsetgroup=0))
            fig.add_trace(go.Bar(name='総負債', x=df_plot['Date'], y=df_plot['TotalLiab'], marker_color='#ff7f0e',
                                 base=df_plot['BaseLiab'], offsetgroup=1))
            fig.add_trace(go.Bar(name='純資産', x=df_plot['Date'], y=df_plot['Equity'], marker_color='#2ca02c',
                                 base=0, offsetgroup=1))
            return 3
        return 0

    add_bs_traces(fig, df_a)

    fig.update_layout(barmode='relative', height=500, margin=dict(t=50, b=80, l=60, r=40),
                      template='plotly_white', showlegend=True,
                      xaxis=dict(type='category', tickangle=0),
                      yaxis=dict(type='linear', rangemode='tozero', automargin=True, gridcolor='#F3F4F6'),
                      legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5))
    return fig

def get_is_plotly_html(data_dict):
    fig = get_is_plotly_fig(data_dict)
    if isinstance(fig, str): return f'<h3 id="income-statement">損益計算書</h3><p>{fig}</p>'
    return '<h3 id="income-statement">損益計算書</h3>' + create_chart_html(fig)

def get_is_plotly_fig(data_dict):
    df_a = data_dict.get('annual', pl.DataFrame())
    if df_a.is_empty(): return 'データなし'

    # 日付を文字列として正規化し、TTMを除外
    df_a = df_a.with_columns(pl.col('Date').cast(pl.Utf8))
    df_a = df_a.filter(pl.col('Date').str.contains('TTM').not_())
    
    # 基準となる日付
    valid_dates = df_a.filter((pl.col('Item') == 'Total Revenue') & (pl.col('Value') > 0)) \
                      .select('Date').unique().sort('Date')['Date'].to_list()[-6:]
    
    if not valid_dates: return 'データなし'
    
    # 全ての項目について、基準日に最も近いデータを使用するようにアラインメント（簡易化のため完全一致のみ）
    df_plot = df_a.filter(pl.col('Date').is_in(valid_dates)).unique(subset=['Item', 'Date']).sort('Date')

    fig = go.Figure()
    items = [('Total Revenue', '売上高', '#aec7e8'), ('Gross Profit', '売上総利益', '#1f77b4'),
             ('Operating Income', '営業利益', '#ffbb78'), ('Net Income', '純利益', '#2ca02c')]
    
    for item_key, name, color in items:
        sub = df_plot.filter(pl.col('Item') == item_key)
        if not sub.is_empty():
            fig.add_trace(go.Bar(name=name, x=sub['Date'], y=sub['Value'], marker_color=color))
    
    # 利益率（散布図/折れ線）
    try:
        df_pivot = df_plot.pivot(on='Item', index='Date', values='Value').sort('Date')
        ratio_configs = [('Gross Profit', 'Total Revenue', '売上総利益率', '#1f77b4'),
                         ('Operating Income', 'Total Revenue', '営業利益率', '#ffbb78'),
                         ('Net Income', 'Total Revenue', '純利益率', '#2ca02c')]
        for num, den, name, color in ratio_configs:
            if num in df_pivot.columns and den in df_pivot.columns:
                calc = df_pivot.with_columns((pl.col(num) / pl.col(den)).alias('Ratio'))
                fig.add_trace(go.Scatter(name=name, x=calc['Date'], y=calc['Ratio'], line=dict(color=color, width=2), mode='lines+markers', yaxis='y2', hovertemplate='%{y:.1%}'))
    except Exception as e: print(f"IS Ratio Error: {e}")

    fig.update_layout(
        barmode='group', height=500, margin=dict(t=50, b=80, l=60, r=60), 
        template='plotly_white', showlegend=True,
        xaxis=dict(type='category', tickangle=0),
        yaxis=dict(title='金額', showgrid=True, type='linear', automargin=True, gridcolor='#F3F4F6', zeroline=True, zerolinecolor='#444', zerolinewidth=2, rangemode='tozero'),
        yaxis2=dict(title='利益率', overlaying='y', side='right', tickformat='.0%', showgrid=False, type='linear', automargin=True, zeroline=True, zerolinecolor='#444', zerolinewidth=2, rangemode='tozero'),
        legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5))
    return fig

def get_cf_plotly_html(data_dict):
    fig = get_cf_plotly_fig(data_dict)
    if isinstance(fig, str): return f'<h3 id="cash-flow">キャッシュフロー</h3><p>{fig}</p>'
    return '<h3 id="cash-flow">キャッシュフロー</h3>' + create_chart_html(fig)

def get_cf_plotly_fig(data_dict):
    df_a = data_dict.get('annual', pl.DataFrame())
    if df_a.is_empty(): return 'データなし'
    
    df_a = df_a.with_columns(pl.col('Date').cast(pl.Utf8))
    df_a = df_a.filter(pl.col('Date').str.contains('TTM').not_())
    valid_dates = df_a.filter(pl.col('Item') == 'Operating Cash Flow').select('Date').unique().sort('Date')['Date'].to_list()[-6:]
    if not valid_dates: return 'データなし'
    
    df_plot = df_a.filter(pl.col('Date').is_in(valid_dates)).unique(subset=['Item', 'Date']).sort('Date')

    fig = go.Figure()
    items = [('Net Income', '純利益', '#2ca02c'), ('Operating Cash Flow', '営業CF', '#aec7e8'), 
             ('Investing Cash Flow', '投資CF', '#1f77b4'), ('Financing Cash Flow', '財務CF', '#ffbb78'), 
             ('Free Cash Flow', 'フリーCF', '#9467bd')]

    for i, (item_key, name, color) in enumerate(items):
        sub = df_plot.filter(pl.col('Item') == item_key)
        if not sub.is_empty():
            fig.add_trace(go.Bar(name=name, x=sub['Date'], y=sub['Value'], marker_color=color))

    fig.update_layout(barmode='group', height=500, margin=dict(t=50, b=80, l=60, r=40),
                      template='plotly_white', showlegend=True,
                      xaxis=dict(type='category', tickangle=0),
                      yaxis=dict(title='金額', type='linear', automargin=True),
                      legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5))
    return fig

def get_tp_plotly_html(data_dict):
    fig = get_tp_plotly_fig(data_dict)
    if isinstance(fig, str): return f'<h3 id="shareholder-return">株主還元</h3><p>{fig}</p>'
    return '<h3 id="shareholder-return">株主還元</h3>' + create_chart_html(fig)

def get_tp_plotly_fig(data_dict):
    df_a = data_dict.get('annual', pl.DataFrame())
    if df_a.is_empty(): return 'データなし'

    df_a = df_a.with_columns(pl.col('Date').cast(pl.Utf8))
    df_a = df_a.filter(pl.col('Date').str.contains('TTM').not_())
    valid_dates = df_a.filter(pl.col('Item') == 'Net Income From Continuing Operations').select('Date').unique().sort('Date')['Date'].to_list()[-6:]
    if not valid_dates: return 'データなし'
    
    df_plot = df_a.filter(pl.col('Date').is_in(valid_dates)).unique(subset=['Item', 'Date']).sort('Date')

    fig = go.Figure()
    ni = df_plot.filter(pl.col('Item') == 'Net Income From Continuing Operations')
    div = df_plot.filter(pl.col('Item') == 'Cash Dividends Paid')
    repo = df_plot.filter(pl.col('Item') == 'Repurchase Of Capital Stock')
    div_r = df_plot.filter(pl.col('Item') == 'Dividends Ratio / Net Income')
    total_r = df_plot.filter(pl.col('Item') == 'Total Payout Ratio / Net Income')

    # 純利益
    fig.add_trace(go.Bar(name='純利益', x=ni['Date'], y=ni['Value'], marker_color='#2ca02c', offsetgroup=0))
    
    # 還元
    dv = div['Value'].abs()
    rv = repo['Value'].abs()
    fig.add_trace(go.Bar(name='配当金', x=div['Date'], y=dv, marker_color='#aec7e8', offsetgroup=1))
    fig.add_trace(go.Bar(name='自社株買い', x=repo['Date'], y=rv, marker_color='#1f77b4', base=dv, offsetgroup=1))
    
    fig.add_trace(go.Scatter(name='配当性向', x=div_r['Date'], y=div_r['Value'], marker_color='#ffbb78', mode='lines+markers', yaxis='y2', hovertemplate='%{y:.1%}'))
    fig.add_trace(go.Scatter(name='総還元性向', x=total_r['Date'], y=total_r['Value'], marker_color='#ff7f0e', mode='lines+markers', yaxis='y2', hovertemplate='%{y:.1%}'))

    fig.update_layout(
        barmode='group', height=500, margin=dict(t=50, b=80, l=60, r=60), template='plotly_white',
        xaxis=dict(type='category', tickangle=0),
        yaxis=dict(title='金額', showgrid=True, type='linear', automargin=True),
        yaxis2=dict(title='還元性向', overlaying='y', side='right', tickformat='.0%', showgrid=False, type='linear', automargin=True),
        legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5)
    )
    return fig

