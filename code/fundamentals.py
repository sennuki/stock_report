# -*- coding: utf-8 -*-
import yfinance as yf
import polars as pl
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
import numpy as np
import warnings

# Suppress plotly deprecation warnings
warnings.filterwarnings("ignore", message=".*scattermapbox.*")

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
    """1つのTickerオブジェクトから4種類の財務データ（Annual & Quarterly）を取得・整形"""
    data = {}
    symbol = getattr(ticker_obj, 'ticker', 'Unknown')

    def extract_and_melt(pandas_df, targets):
        if pandas_df is None or pandas_df.empty:
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

    # 1. 貸借対照表
    target_bs = ['Total Non Current Assets', 'Current Liabilities', 'Total Equity Gross Minority Interest',
                 'Current Assets', 'Total Non Current Liabilities Net Minority Interest',
                 'Total Assets', 'Total Liabilities Net Minority Interest', 'Total Liabilities',
                 'Long Term Debt And Capital Lease Obligation','Employee Benefits', 'Non Current Deferred Liabilities',
                 'Other Non Current Liabilities']
    data['bs'] = {
        'annual': extract_and_melt(ticker_obj.balancesheet, target_bs),
        'quarterly': extract_and_melt(ticker_obj.quarterly_balancesheet, target_bs)
    }

    # 2. 損益計算書
    target_is = ['Total Revenue', 'Gross Profit', 'Operating Income', 'Net Income']
    data['is'] = {
        'annual': extract_and_melt(ticker_obj.income_stmt, target_is),
        'quarterly': extract_and_melt(ticker_obj.quarterly_income_stmt, target_is)
    }

    # 3. キャッシュフロー
    target_cf = ['Operating Cash Flow', 'Investing Cash Flow', 'Financing Cash Flow', 'Free Cash Flow', 'Net Income']
    data['cf'] = {
        'annual': extract_and_melt(ticker_obj.cashflow, target_cf),
        'quarterly': extract_and_melt(ticker_obj.quarterly_cashflow, target_cf)
    }

    # 4. 総還元性向
    def process_tp(df_source):
        try:
            if df_source is None or df_source.empty:
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
                                base=df_plot['NonCurrAssets'], offsetgroup=0, visible=visible))
            fig.add_trace(go.Bar(name='固定資産', x=df_plot['Date'], y=df_plot['NonCurrAssets'], marker_color='#1f77b4',
                                 base=0, offsetgroup=0, visible=visible)) 
            
            fig.add_trace(go.Bar(name='流動負債', x=df_plot['Date'], y=df_plot['CurrLiab'], marker_color='#ffbb78',
                                 base=df_plot['BaseCurr'], offsetgroup=1, visible=visible))
            fig.add_trace(go.Bar(name='固定負債', x=df_plot['Date'], y=df_plot['FixedLiab'], marker_color='#ff7f0e',
                                 base=df_plot['BaseFixed'], offsetgroup=1, visible=visible))
            fig.add_trace(go.Bar(name='純資産', x=df_plot['Date'], y=df_plot['Equity'], marker_color='#2ca02c',
                                 base=0, offsetgroup=1, visible=visible))
            return 5
        
        elif df_plot['TotalAssets'].sum() != 0:
            df_plot = df_plot.with_columns([
                pl.when(pl.col('Equity') > 0).then(pl.col('Equity')).otherwise(0.0).alias('BaseLiab'),
            ])
            
            fig.add_trace(go.Bar(name='総資産', x=df_plot['Date'], y=df_plot['TotalAssets'], marker_color='#1f77b4',
                                 offsetgroup=0, visible=visible))
            fig.add_trace(go.Bar(name='総負債', x=df_plot['Date'], y=df_plot['TotalLiab'], marker_color='#ff7f0e',
                                 base=df_plot['BaseLiab'], offsetgroup=1, visible=visible))
            fig.add_trace(go.Bar(name='純資産', x=df_plot['Date'], y=df_plot['Equity'], marker_color='#2ca02c',
                                 base=0, offsetgroup=1, visible=visible))
            return 3
        return 0

    num_a = add_bs_traces(fig, df_a, visible=True) if not df_a.is_empty() else 0
    num_q = add_bs_traces(fig, df_q, visible=False) if not df_q.is_empty() else 0

    buttons = []
    if num_a > 0:
        buttons.append(dict(label="通期", method="update", args=[{"visible": [True]*num_a + [False]*num_q}]))
    if num_q > 0:
        buttons.append(dict(label="四半期", method="update", args=[{"visible": [False]*num_a + [True]*num_q}]))

    updatemenus = [dict(type="buttons", direction="right", x=0.0, y=1.15, showactive=True, buttons=buttons)] if len(buttons) > 1 else None

    fig.update_layout(barmode='group', height=450, margin=dict(t=80,b=50),
                      template='plotly_white', showlegend=True,
                      xaxis=dict(type='category'),
                      yaxis=dict(type='linear', rangemode='tozero'),
                      legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
                      updatemenus=updatemenus)
    return '<h3 id="balance-sheet">貸借対照表</h3>' + create_chart_html(fig)

def get_is_plotly_html(data_dict):
    df_a = data_dict.get('annual', pl.DataFrame())
    df_q = data_dict.get('quarterly', pl.DataFrame())
    
    if df_a.is_empty() and df_q.is_empty(): return "<p>データなし</p>"

    fig = go.Figure()
    items = [('Total Revenue', '売上高', '#aec7e8'), ('Gross Profit', '売上総利益', '#1f77b4'),
             ('Operating Income', '営業利益', '#ffbb78'), ('Net Income', '純利益', '#2ca02c')]
    ratio_items = [
        ('Gross Profit', 'Total Revenue', '売上総利益率', '#1f77b4'),
        ('Operating Income', 'Total Revenue', '営業利益率', '#ffbb78'),
        ('Net Income', 'Total Revenue', '純利益率', '#2ca02c')
    ]

    def add_is_traces(fig, df, visible):
        valid_dates = df.filter((pl.col('Item') == 'Total Revenue') & (pl.col('Value') > 0)).select('Date').unique()['Date'].to_list()
        df = df.filter(pl.col('Date').is_in(valid_dates))
        count = 0
        for item_key, name, color in items:
            sub = df.filter(pl.col('Item') == item_key)
            if not sub.is_empty():
                fig.add_trace(go.Bar(name=name, x=sub['Date'], y=sub['Value'], marker_color=color, visible=visible))
                count += 1
        try:
            df_pivot = df.pivot(on='Item', index='Date', values='Value')
            for num_key, den_key, name, color in ratio_items:
                if num_key in df_pivot.columns and den_key in df_pivot.columns:
                    calc = df_pivot.with_columns((pl.col(num_key) / pl.col(den_key)).alias('Ratio')).select(['Date', 'Ratio']).filter(pl.col('Ratio').is_not_nan() & pl.col('Ratio').is_infinite().not_())
                    fig.add_trace(go.Scatter(name=name, x=calc['Date'], y=calc['Ratio'], line=dict(color=color, width=2), mode='lines+markers', yaxis='y2', visible=visible, hovertemplate='%{y:.1%}'))
                    count += 1
        except Exception as e:
            print(f"Error calculating ratios: {e}")
        return count

    num_a = add_is_traces(fig, df_a, visible=True) if not df_a.is_empty() else 0
    num_q = add_is_traces(fig, df_q, visible=False) if not df_q.is_empty() else 0

    buttons = []
    if num_a > 0:
        buttons.append(dict(label="通期", method="update", args=[{"visible": [True]*num_a + [False]*num_q}]))
    if num_q > 0:
        buttons.append(dict(label="四半期", method="update", args=[{"visible": [False]*num_a + [True]*num_q}]))

    updatemenus = [dict(type="buttons", direction="right", x=0.0, y=1.15, showactive=True, buttons=buttons)] if len(buttons) > 1 else None

    fig.update_layout(
        barmode='group', height=500, margin=dict(t=80,b=50), 
        template='plotly_white', showlegend=True,
        xaxis=dict(type='category'),
        yaxis=dict(title='金額', showgrid=True, type='linear'),
        yaxis2=dict(title='利益率', overlaying='y', side='right', tickformat='.0%', showgrid=False, type='linear'),
        legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
        updatemenus=updatemenus
    )
    return '<h3 id="income-statement">損益計算書</h3>' + create_chart_html(fig)

def get_cf_plotly_html(data_dict):
    df_a = data_dict.get('annual', pl.DataFrame())
    df_q = data_dict.get('quarterly', pl.DataFrame())
    
    if df_a.is_empty() and df_q.is_empty(): return "<p>データなし</p>"

    fig = go.Figure()
    items = [('Net Income', '純利益', '#2ca02c'),
             ('Operating Cash Flow', '営業CF', '#aec7e8'), ('Investing Cash Flow', '投資CF', '#1f77b4'),
             ('Financing Cash Flow', '財務CF', '#ffbb78'), ('Free Cash Flow', 'フリーCF', '#9467bd')]

    def add_cf_traces(fig, df, visible):
        valid_dates = df.filter((pl.col('Item') == 'Operating Cash Flow')).select('Date').unique()['Date'].to_list()
        df = df.filter(pl.col('Date').is_in(valid_dates))
        for item_key, name, color in items:
            sub = df.filter(pl.col('Item') == item_key)
            if not sub.is_empty():
                fig.add_trace(go.Bar(name=name, x=sub['Date'], y=sub['Value'], marker_color=color, visible=visible))
        return len(items)

    num_a = add_cf_traces(fig, df_a, visible=True) if not df_a.is_empty() else 0
    num_q = add_cf_traces(fig, df_q, visible=False) if not df_q.is_empty() else 0

    buttons = []
    if num_a > 0:
        buttons.append(dict(label="通期", method="update", args=[{"visible": [True]*num_a + [False]*num_q}]))
    if num_q > 0:
        buttons.append(dict(label="四半期", method="update", args=[{"visible": [False]*num_a + [True]*num_q}]))

    updatemenus = [dict(type="buttons", direction="right", x=0.0, y=1.15, showactive=True, buttons=buttons)] if len(buttons) > 1 else None

    fig.update_layout(barmode='group', height=450, margin=dict(t=80,b=50),
                      template='plotly_white', showlegend=True,
                      xaxis=dict(type='category'),
                      yaxis=dict(title='金額', type='linear'),
                      legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
                      updatemenus=updatemenus)
    return '<h3 id="cash-flow">キャッシュフロー</h3>' + create_chart_html(fig)

def get_tp_plotly_html(data_dict):
    df_a = data_dict.get('annual', pl.DataFrame())
    df_q = data_dict.get('quarterly', pl.DataFrame())
    
    if df_a.is_empty() and df_q.is_empty(): return "<p>データなし</p>"

    fig = go.Figure()

    def add_tp_traces(fig, df, visible):
        valid_dates = df.filter(pl.col('Item') == 'Net Income From Continuing Operations').select('Date').unique()['Date'].to_list()
        df = df.filter(pl.col('Date').is_in(valid_dates))
        ni = df.filter(pl.col('Item') == 'Net Income From Continuing Operations')
        div = df.filter(pl.col('Item') == 'Cash Dividends Paid')
        repo = df.filter(pl.col('Item') == 'Repurchase Of Capital Stock')
        div_r = df.filter(pl.col('Item') == 'Dividends Ratio / Net Income')
        total_r = df.filter(pl.col('Item') == 'Total Payout Ratio / Net Income')

        # 純利益 (左側)
        fig.add_trace(go.Bar(name='純利益', x=ni['Date'], y=ni['Value'], marker_color='#2ca02c', offsetgroup=0, visible=visible))
        # 配当金 (右側の下)
        fig.add_trace(go.Bar(name='配当金', x=div['Date'], y=div['Value'].abs(), marker_color='#aec7e8', offsetgroup=1, visible=visible))
        # 自社株買い (右側の配当の上)
        fig.add_trace(go.Bar(name='自社株買い', x=repo['Date'], y=repo['Value'].abs(), marker_color='#1f77b4', base=div['Value'].abs(), offsetgroup=1, visible=visible))
        
        # 性向 (折れ線)
        fig.add_trace(go.Scatter(name='配当性向', x=div_r['Date'], y=div_r['Value'], marker_color='#ffbb78', mode='lines+markers', yaxis='y2', hovertemplate='%{y:.1%}', visible=visible))
        fig.add_trace(go.Scatter(name='総還元性向', x=total_r['Date'], y=total_r['Value'], marker_color='#ff7f0e', mode='lines+markers', yaxis='y2', hovertemplate='%{y:.1%}', visible=visible))

    _add_traces(fig, df_a, add_tp_traces, visible=True)
    _add_traces(fig, df_q, add_tp_traces, visible=False)

    # タブ切り替えボタンの作成
    num_a = 5 if not df_a.is_empty() else 0
    num_q = 5 if not df_q.is_empty() else 0
    
    buttons = []
    if num_a > 0:
        buttons.append(dict(label="通期", method="update", args=[{"visible": [True]*num_a + [False]*num_q}]))
    if num_q > 0:
        buttons.append(dict(label="四半期", method="update", args=[{"visible": [False]*num_a + [True]*num_q}]))

    updatemenus = [dict(type="buttons", direction="right", x=0.0, y=1.15, showactive=True, buttons=buttons)] if len(buttons) > 1 else None

    fig.update_layout(
        barmode='group', height=450, margin=dict(t=80,b=50), template='plotly_white',
        xaxis=dict(type='category'),
        yaxis=dict(title='金額', showgrid=True, type='linear', rangemode='tozero'),
        yaxis2=dict(title='還元性向', overlaying='y', side='right', tickformat='.0%', showgrid=False, type='linear', rangemode='tozero'),
        legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
        updatemenus=updatemenus
    )
    return '<h3 id="shareholder-return">株主還元</h3>' + create_chart_html(fig)

if __name__ == "__main__":
    print("Updated upstream")
