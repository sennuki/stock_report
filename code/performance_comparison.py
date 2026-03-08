# -*- coding: utf-8 -*-
import yfinance as yf
import polars as pl
import numpy as np
import plotly.graph_objects as go
from datetime import datetime

PERIOD_CONFIGS = [
    {"key": "1M", "label": "1ヶ月", "days": 30},
    {"key": "3M", "label": "3ヶ月", "days": 91},
    {"key": "6M", "label": "6ヶ月", "days": 182},
    {"key": "YTD", "label": "年初来", "days": "YTD"},
    {"key": "1Y", "label": "1年", "days": 365},
    {"key": "3Y", "label": "3年", "days": 365 * 3},
    {"key": "5Y", "label": "5年", "days": 365 * 5},
    {"key": "ALL", "label": "全期間", "days": None},
]

def generate_performance_chart_html(target_symbol, sector_etf_symbol):
    """
    対象銘柄、セクターETF、S&P 500の累積リターン比較チャートを生成。
    """
    fig = generate_performance_chart_fig(target_symbol, sector_etf_symbol)
    if fig is None:
        return "<p>データ取得に失敗しました。</p>"
    return fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False, 'responsive': True})

def generate_performance_chart_fig(target_symbol, sector_etf_symbol):
    """
    対象銘柄、セクターETF、S&P 500の累積リターン比較チャートの Plotly Figure を生成。
    """
    symbols = [target_symbol, sector_etf_symbol, "^GSPC"]
    labels = {target_symbol: target_symbol, sector_etf_symbol: sector_etf_symbol, "^GSPC": "S&P 500"}
    colors = {target_symbol: "#ff6b01", sector_etf_symbol: "#006cac", "^GSPC": "#22c55e"}

    # 最大10年分のデータを取得
    all_data = {}
    last_date = None
    for sym in symbols:
        try:
            ticker = yf.Ticker(sym)
            hist = ticker.history(period="10y")
            if hist.empty: continue
            df = pl.from_pandas(hist.reset_index()).select(['Date', 'Close'])
            df = df.with_columns(pl.col("Date").dt.replace_time_zone(None))
            all_data[sym] = df
            if last_date is None or df['Date'][-1] > last_date:
                last_date = df['Date'][-1]
        except Exception as e:
            print(f"Error fetching data for {sym}: {e}")

    if not all_data:
        return None

    fig = go.Figure()
    buttons = []
    total_trace_count = 0
    period_traces = []

    for p_idx, p in enumerate(PERIOD_CONFIGS):
        key = p['key']
        start_date = None
        if p['days'] == "YTD":
            start_date = datetime(last_date.year, 1, 1)
        elif p['days'] is not None:
            start_date = last_date - np.timedelta64(p['days'], 'D')
        
        current_period_traces = []
        for sym in symbols:
            if sym not in all_data: continue
            df = all_data[sym]
            if start_date:
                df_p = df.filter(pl.col("Date") >= start_date)
            else:
                df_p = df

            if df_p.is_empty(): continue

            # 累積リターン計算 (最初の値を 0% とする)
            base_price = df_p['Close'][0]
            df_p = df_p.with_columns([
                ((pl.col("Close") / base_price) - 1).alias("Cumulative_Return")
            ])

            visible = (key == "1Y") # 1年をデフォルト
            trace = go.Scatter(
                x=df_p['Date'].to_list(),
                y=df_p['Cumulative_Return'].to_list(),
                name=f"{labels[sym]} ({p['label']})",
                line=dict(color=colors[sym], width=2),
                visible=visible,
                hovertemplate="<b>" + labels[sym] + "</b><br>日付: %{x|%Y-%m-%d}<br>リターン: %{y:.2%}<extra></extra>"
            )
            fig.add_trace(trace)
            current_period_traces.append(total_trace_count)
            total_trace_count += 1
        period_traces.append(current_period_traces)

    fig.update_layout(
        xaxis=dict(title="日付", gridcolor="#E5E7EB", nticks=8),
        yaxis=dict(title="累積リターン", tickformat=".0%", gridcolor="#E5E7EB"),
        margin=dict(l=60, r=30, t=80, b=40),
        height=500,
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1.0)
    )

    return fig
