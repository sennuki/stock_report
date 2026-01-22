# -*- coding: utf-8 -*-
import yfinance as yf
import polars as pl
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
import numpy as np

# Plotly 6.0.0+ deprecation fix: 
# Default templates still contain 'scattermapbox', which triggers a warning.
# We migrate them to 'scattermap' to follow the recommendation.
def fix_plotly_templates():
    for name in pio.templates:
        template = pio.templates[name]
        try:
            if hasattr(template.layout.template.data, 'scattermapbox'):
                smb = template.layout.template.data.scattermapbox
                if smb:
                    template.layout.template.data.scattermap = smb
                template.layout.template.data.scattermapbox = None
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
    """1つのTickerオブジェクトから4種類の財務データ（Annual + Quarterly）を取得・整形"""
    data = {}
    symbol = getattr(ticker_obj, 'ticker', 'Unknown')

    def extract_and_melt(pandas_df, targets):
        if pandas_df.empty:
            return pl.DataFrame()
        try:
            # Convert to Polars, keeping index
            df = pl.from_pandas(pandas_df, include_index=True)
            # Rename first column to 'Item'
            df = df.rename({df.columns[0]: 'Item'})
            # Filter and Melt
            return get_melt(df.filter(pl.col('Item').is_in(targets)))
        except Exception as e:
            print(f"Error processing data for {symbol}: {e}")
            return pl.DataFrame()

    # 共通ヘルパー: AnnualとQuarterlyの両方を取得
    def process_category(attr_annual, attr_quarterly, targets):
        return {
            'annual': extract_and_melt(getattr(ticker_obj, attr_annual, pd.DataFrame()), targets),
            'quarterly': extract_and_melt(getattr(ticker_obj, attr_quarterly, pd.DataFrame()), targets)
        }

    # 1. 貸借対照表
    target_bs = ['Total Non Current Assets', 'Current Liabilities', 'Total Equity Gross Minority Interest',
                 'Current Assets', 'Total Non Current Liabilities Net Minority Interest',
                 'Total Assets', 'Total Liabilities Net Minority Interest', 'Total Liabilities']
    data['bs'] = process_category('balancesheet', 'quarterly_balancesheet', target_bs)

    # 2. 損益計算書
    target_is = ['Total Revenue', 'Gross Profit', 'Operating Income', 'Net Income']
    data['is'] = process_category('income_stmt', 'quarterly_income_stmt', target_is)

    # 3. キャッシュフロー
    target_cf = ['Operating Cash Flow', 'Investing Cash Flow', 'Financing Cash Flow', 'Free Cash Flow']
    data['cf'] = process_category('cashflow', 'quarterly_cashflow', target_cf)

    # 4. 総還元性向
    def process_tp(df_source):
        try:
            if df_source.empty:
                return pl.DataFrame()
            
            df_tp = pl.from_pandas(df_source, include_index=True)
            df_tp = df_tp.rename({df_tp.columns[0]: 'Item'})
            
            target_tp = ['Net Income From Continuing Operations', 'Repurchase Of Capital Stock', 'Cash Dividends Paid']
            df_tp = df_tp.filter(pl.col('Item').is_in(target_tp))
            if not df_tp.is_empty():
                df_tp_melt = get_melt(df_tp)
                df_pivot = df_tp_melt.pivot(on='Item', index='Date', values='Value')
                
                # 必要な列が不足している場合の補完
                for col in target_tp:
                    if col not in df_pivot.columns:
                        df_pivot = df_pivot.with_columns(pl.lit(0.0).alias(col))

                # データ欠損(null)を0で埋める (ここが重要: 配当がない年などを0にする)
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

                # 実額データもpivot済み(0埋め済み)のものから再生成する
                # これにより、全日付×全項目のデータが揃う
                cols_to_melt = [c for c in target_tp if c in df_pivot.columns]
                df_amount_melt = df_pivot.unpivot(index='Date', on=cols_to_melt, variable_name='Item', value_name='Value')
                df_amount_melt = df_amount_melt.select(['Item', 'Date', 'Value']) # 列順を統一
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

    return data

def create_chart_html(fig):
    """HTML化ヘルパー (ファイルサイズ削減のためライブラリ重複ロード回避)"""
    fig.update_xaxes(fixedrange=True)
    fig.update_yaxes(fixedrange=True)
    return fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False, 'scrollZoom': False})

def _add_traces(fig, df, func, visible=True):
    """トレース追加の共通ヘルパー"""
    if df.is_empty(): return
    func(fig, df, visible)

def get_bs_plotly_html(data_dict):
    df_a = data_dict.get('annual', pl.DataFrame())
    df_q = data_dict.get('quarterly', pl.DataFrame())
    
    if df_a.is_empty() and df_q.is_empty(): return "<p>データなし</p>"
    
    fig = go.Figure()

    def add_bs_traces(fig, df, visible):
        def get_val(item): return df.filter(pl.col('Item') == item)

        curr_assets = get_val('Current Assets')
        non_curr_assets = get_val('Total Non Current Assets')
        curr_liab = get_val('Current Liabilities')
        non_curr_liab = get_val('Total Non Current Liabilities Net Minority Interest')
        equity = get_val('Total Equity Gross Minority Interest')
        
        # Fallback items
        total_assets = get_val('Total Assets')
        total_liab = get_val('Total Liabilities Net Minority Interest')

        # Check if we have detailed breakdown data
        has_breakdown = not curr_assets.is_empty()

        if has_breakdown:
            if non_curr_liab.is_empty() or equity.is_empty(): base_liab = 0
            else: base_liab = non_curr_liab['Value'] + equity['Value']

            # 資産
            fig.add_trace(go.Bar(name='流動資産', x=curr_assets['Date'], y=curr_assets['Value'], marker_color='#aec7e8',
                                base=non_curr_assets['Value'] if not non_curr_assets.is_empty() else 0,
                                offsetgroup=0, visible=visible))
            fig.add_trace(go.Bar(name='固定資産', x=curr_assets['Date'], y=non_curr_assets['Value'], marker_color='#1f77b4',
                                 base=0, offsetgroup=0, visible=visible)) 
            
            # 負債・純資産
            fig.add_trace(go.Bar(name='流動負債', x=curr_liab['Date'], y=curr_liab['Value'], marker_color='#ffbb78',
                                 base=base_liab, offsetgroup=1, visible=visible))
            fig.add_trace(go.Bar(name='固定負債', x=non_curr_liab['Date'], y=non_curr_liab['Value'], marker_color='#ff7f0e',
                                 base=equity['Value'] if not equity.is_empty() else 0,
                                 offsetgroup=1, visible=visible))
            fig.add_trace(go.Bar(name='純資産', x=equity['Date'], y=equity['Value'], marker_color='#2ca02c',
                                 base=0, offsetgroup=1, visible=visible))
        
        elif not total_assets.is_empty():
            # Fallback for financial institutions (no current/non-current distinction)
            # Use Total Assets date as x-axis
            
            # Check for fallback liability item
            if total_liab.is_empty():
                total_liab = get_val('Total Liabilities')

            # Merge data on Date to ensure alignment
            dates = set()
            if not total_assets.is_empty(): dates.update(total_assets['Date'].to_list())
            if not total_liab.is_empty(): dates.update(total_liab['Date'].to_list())
            if not equity.is_empty(): dates.update(equity['Date'].to_list())
            
            df_plot = pl.DataFrame({'Date': sorted(list(dates))})
            
            def join_val(df_main, df_sub, col_name):
                if df_sub.is_empty():
                     return df_main.with_columns(pl.lit(0.0).alias(col_name))
                temp = df_sub.select(['Date', 'Value']).rename({'Value': col_name})
                return df_main.join(temp, on='Date', how='left').fill_null(0.0)

            df_plot = join_val(df_plot, total_assets, 'Total Assets')
            df_plot = join_val(df_plot, total_liab, 'Total Liabilities')
            df_plot = join_val(df_plot, equity, 'Total Equity')
            
            # 資産 (Total Assets only)
            fig.add_trace(go.Bar(name='総資産', x=df_plot['Date'], y=df_plot['Total Assets'], marker_color='#1f77b4',
                                 offsetgroup=0, visible=visible))
            
            # 負債・純資産
            # Stack: Equity on bottom, Liabilities on top
            
            fig.add_trace(go.Bar(name='総負債', x=df_plot['Date'], y=df_plot['Total Liabilities'], marker_color='#ff7f0e',
                                     base=df_plot['Total Equity'], offsetgroup=1, visible=visible))
            
            fig.add_trace(go.Bar(name='純資産', x=df_plot['Date'], y=df_plot['Total Equity'], marker_color='#2ca02c',
                                     base=0, offsetgroup=1, visible=visible))

    start_traces = len(fig.data)
    _add_traces(fig, df_a, add_bs_traces, visible=True)
    n_traces_a = len(fig.data) - start_traces

    start_traces = len(fig.data)
    _add_traces(fig, df_q, add_bs_traces, visible=False)
    n_traces_q = len(fig.data) - start_traces

    updatemenus = [dict(
        type="buttons",
        direction="right",
        x=0.5, y=1.15, xanchor='center',
        buttons=list([
            dict(label="年間",
                 method="update",
                 args=[{"visible": [True]*n_traces_a + [False]*n_traces_q}]),
            dict(label="四半期",
                 method="update",
                 args=[{"visible": [False]*n_traces_a + [True]*n_traces_q}]),
        ]),
    )]

    fig.update_layout(barmode='group', height=450, margin=dict(t=60,b=50),
                      template='plotly_white', showlegend=True, updatemenus=updatemenus,
                      xaxis=dict(type='category'),
                      yaxis=dict(type='linear', rangemode='tozero'),
                      legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5))
    return '<h3 id="balance-sheet">貸借対照表</h3>' + create_chart_html(fig)

def get_is_plotly_html(data_dict):
    df_a = data_dict.get('annual', pl.DataFrame())
    df_q = data_dict.get('quarterly', pl.DataFrame())
    
    if df_a.is_empty() and df_q.is_empty(): return "<p>データなし</p>"

    fig = go.Figure()

    # 実額の項目
    items = [('Total Revenue', '売上高', '#aec7e8'), ('Gross Profit', '売上総利益', '#1f77b4'),
             ('Operating Income', '営業利益', '#ffbb78'), ('Net Income', '純利益', '#ff7f0e')]

    # 利益率の項目
    ratio_items = [
        ('Gross Profit', 'Total Revenue', '売上総利益率', '#1f77b4'),
        ('Operating Income', 'Total Revenue', '営業利益率', '#ffbb78'),
        ('Net Income', 'Total Revenue', '純利益率', '#ff7f0e')
    ]

    def add_is_traces(fig, df, visible):
        # 1. 実額 (Bar: 左軸)
        for item_key, name, color in items:
            sub = df.filter(pl.col('Item') == item_key)
            if not sub.is_empty():
                fig.add_trace(go.Bar(name=name, x=sub['Date'], y=sub['Value'], marker_color=color, visible=visible))

        # 2. 利益率 (Line: 右軸)
        try:
            df_pivot = df.pivot(on='Item', index='Date', values='Value')
            
            for num_key, den_key, name, color in ratio_items:
                if num_key in df_pivot.columns and den_key in df_pivot.columns:
                    calc = df_pivot.with_columns(
                        (pl.col(num_key) / pl.col(den_key)).alias('Ratio')
                    ).select(['Date', 'Ratio']).filter(pl.col('Ratio').is_not_nan() & pl.col('Ratio').is_infinite().not_())
                    
                    fig.add_trace(go.Scatter(name=name, x=calc['Date'], y=calc['Ratio'], 
                                         line=dict(color=color, width=2), mode='lines+markers', 
                                         yaxis='y2', visible=visible,
                                         hovertemplate='%{y:.1%}'))
        except Exception as e:
            print(f"Error calculating ratios: {e}")

    start_traces = len(fig.data)
    _add_traces(fig, df_a, add_is_traces, visible=True)
    n_traces_a = len(fig.data) - start_traces

    start_traces = len(fig.data)
    _add_traces(fig, df_q, add_is_traces, visible=False)
    n_traces_q = len(fig.data) - start_traces
    
    # def count_traces(df): ... (Removed or Ignored)

    updatemenus = [dict(
        type="buttons", direction="right", x=0.5, y=1.2, xanchor='center',
        buttons=[
            dict(label="年間", method="update", args=[{"visible": [True]*n_traces_a + [False]*n_traces_q}]),
            dict(label="四半期", method="update", args=[{"visible": [False]*n_traces_a + [True]*n_traces_q}]),
        ]
    )]
    
    fig.update_layout(
        barmode='group', height=500, margin=dict(t=60,b=50), 
        template='plotly_white', showlegend=True, updatemenus=updatemenus,
        xaxis=dict(type='category'),
        yaxis=dict(title='金額', showgrid=True, type='linear'),
        yaxis2=dict(title='利益率', overlaying='y', side='right', tickformat='.0%', showgrid=False, type='linear'),
        legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5)
    )
    
    return '<h3 id="income-statement">損益計算書</h3>' + create_chart_html(fig)

def get_cf_plotly_html(data_dict):
    df_a = data_dict.get('annual', pl.DataFrame())
    df_q = data_dict.get('quarterly', pl.DataFrame())
    
    if df_a.is_empty() and df_q.is_empty(): return "<p>データなし</p>"

    fig = go.Figure()
    items = [('Operating Cash Flow', '営業CF', '#aec7e8'), ('Investing Cash Flow', '投資CF', '#1f77b4'),
             ('Financing Cash Flow', '財務CF', '#ffbb78'), ('Free Cash Flow', 'フリーCF', '#ff7f0e')]

    def add_cf_traces(fig, df, visible):
        for item_key, name, color in items:
            sub = df.filter(pl.col('Item') == item_key)
            fig.add_trace(go.Bar(name=name, x=sub['Date'], y=sub['Value'], marker_color=color, visible=visible))

    start_traces = len(fig.data)
    _add_traces(fig, df_a, add_cf_traces, visible=True)
    n_traces_a = len(fig.data) - start_traces

    start_traces = len(fig.data)
    _add_traces(fig, df_q, add_cf_traces, visible=False)
    n_traces_q = len(fig.data) - start_traces

    updatemenus = [dict(
        type="buttons", direction="right", x=0.5, y=1.15, xanchor='center',
        buttons=[
            dict(label="年間", method="update", args=[{"visible": [True]*n_traces_a + [False]*n_traces_q}]),
            dict(label="四半期", method="update", args=[{"visible": [False]*n_traces_a + [True]*n_traces_q}]),
        ]
    )]
    fig.update_layout(barmode='group', height=450, margin=dict(t=60,b=50),
                      template='plotly_white', showlegend=True, updatemenus=updatemenus,
                      xaxis=dict(type='category'),
                      yaxis=dict(type='linear'),
                      legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5))
    return '<h3 id="cash-flow">キャッシュフロー</h3>' + create_chart_html(fig)

def get_tp_plotly_html(data_dict):
    df_a = data_dict.get('annual', pl.DataFrame())
    df_q = data_dict.get('quarterly', pl.DataFrame())
    
    if df_a.is_empty() and df_q.is_empty(): return "<p>データなし</p>"

    fig = go.Figure()

    def add_tp_traces(fig, df, visible):
        div = df.filter(pl.col('Item') == 'Cash Dividends Paid')
        repo = df.filter(pl.col('Item') == 'Repurchase Of Capital Stock')
        div_r = df.filter(pl.col('Item') == 'Dividends Ratio / Net Income')
        total_r = df.filter(pl.col('Item') == 'Total Payout Ratio / Net Income')

        fig.add_trace(go.Bar(name='配当金', x=div['Date'], y=div['Value'].abs(), marker_color='#aec7e8', yaxis='y', visible=visible))
        fig.add_trace(go.Bar(name='自社株買い', x=repo['Date'], y=repo['Value'].abs(), marker_color='#1f77b4', base=div['Value'].abs(), yaxis='y', visible=visible))
        fig.add_trace(go.Scatter(name='配当性向', x=div_r['Date'], y=div_r['Value'], marker_color='#ffbb78', mode='lines+markers', yaxis='y2', hovertemplate='%{y:.1%}', visible=visible))
        fig.add_trace(go.Scatter(name='総還元性向', x=total_r['Date'], y=total_r['Value'], marker_color='#ff7f0e', mode='lines+markers', yaxis='y2', hovertemplate='%{y:.1%}', visible=visible))

    start_traces = len(fig.data)
    _add_traces(fig, df_a, add_tp_traces, visible=True)
    n_traces_a = len(fig.data) - start_traces

    start_traces = len(fig.data)
    _add_traces(fig, df_q, add_tp_traces, visible=False)
    n_traces_q = len(fig.data) - start_traces

    updatemenus = [dict(
        type="buttons", direction="right", x=0.5, y=1.2, xanchor='center',
        buttons=[
            dict(label="年間", method="update", args=[{"visible": [True]*n_traces_a + [False]*n_traces_q}]),
            dict(label="四半期", method="update", args=[{"visible": [False]*n_traces_a + [True]*n_traces_q}]),
        ]
    )]

    fig.update_layout(
        barmode='stack', height=450, margin=dict(t=60,b=50), template='plotly_white',
        xaxis=dict(type='category'),
        yaxis=dict(title='', showgrid=True, type='linear', rangemode='tozero'),
        yaxis2=dict(title='', overlaying='y', side='right', tickformat='.0%', showgrid=False, type='linear', rangemode='tozero'),
        legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
        updatemenus=updatemenus
    )
    return '<h3 id="shareholder-return">株主還元</h3>' + create_chart_html(fig)

if __name__ == "__main__":
    print("MSFTのファンダメンタルズデータを取得中 (Annual & Quarterly)...")
    ticker = yf.Ticker("MSFT")
    
    # データの取得
    data = get_financial_data(ticker)
    
    # 結果の確認
    for key, data_dict in data.items():
        print(f"\n--- {key.upper()} Data ---")
        df_a = data_dict['annual']
        df_q = data_dict['quarterly']
        print(f"Annual: {df_a.shape}, Quarterly: {df_q.shape}")
        
        if not df_a.is_empty() or not df_q.is_empty():
            if key == 'bs':
                print("BS Graph generated (len):", len(get_bs_plotly_html(data_dict)))
            elif key == 'is':
                print("IS Graph generated (len):", len(get_is_plotly_html(data_dict)))
            elif key == 'cf':
                print("CF Graph generated (len):", len(get_cf_plotly_html(data_dict)))
            elif key == 'tp':
                print("TP Graph generated (len):", len(get_tp_plotly_html(data_dict)))
