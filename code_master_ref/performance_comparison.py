# -*- coding: utf-8 -*-
import yfinance as yf
import polars as pl
import numpy as np
import plotly.graph_objects as go
from datetime import datetime
import utils

PERIOD_CONFIGS = [
    {"key": "1M", "label": "1ヶ月", "days": 30},
    {"key": "3M", "label": "3ヶ月", "days": 91},
    {"key": "6M", "label": "6ヶ月", "days": 182},
    {"key": "YTD", "label": "年初来", "days": "YTD"},
    {"key": "1Y", "label": "1年", "days": 365},
    {"key": "3Y", "label": "3年", "days": 365 * 3},
    {"key": "5Y", "label": "5年", "days": 365 * 5},
    {"key": "10Y", "label": "10年", "days": 365 * 10},
]

def generate_performance_chart_html(target_symbol, sector_etf_symbol):
    """
    対象銘柄、セクターETF、S&P 500の累積リターン比較チャートを生成。
    """
    fig = generate_performance_chart_fig(target_symbol, sector_etf_symbol)
    if fig is None:
        return "<p>データ取得に失敗しました。</p>"
    return fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False, 'responsive': True})
import threading

_history_cache = {}
_cache_lock = threading.Lock()

def get_cached_history(symbol):
    with _cache_lock:
        if symbol in _history_cache:
            return _history_cache[symbol]
    
    # ロックの外で取得を試みる (重い処理)
    try:
        ticker = utils.get_ticker(symbol)
        # 取得に失敗した場合に備えてリトライ回数を増やす
        hist = utils.safe_call(ticker, "history", period="10y", max_retries=5)
        if hist is not None and not hist.empty:
            df = pl.from_pandas(hist.reset_index()).select(['Date', 'Close'])
            # 時刻を切り捨てて日付のみにする
            df = df.with_columns(pl.col("Date").dt.replace_time_zone(None).dt.date())
            with _cache_lock:
                _history_cache[symbol] = df
            return df
        else:
            return None
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

def prefetch_common_data(symbols):
    """
    主要なETFや指数のデータを事前に一括取得してキャッシュする。
    """
    print(f"\n共通データ ({len(symbols)} 銘柄) を事前取得中...")
    for sym in symbols:
        get_cached_history(sym)

def generate_performance_chart_fig(target_symbol, sector_etf_symbol):
    """
    対象銘柄、セクターETF、S&P 500の累積リターン比較チャートの Plotly Figure を生成。
    """
    symbols = [target_symbol, sector_etf_symbol, "^GSPC"]
    labels = {target_symbol: target_symbol, sector_etf_symbol: sector_etf_symbol, "^GSPC": "S&P 500"}
    colors = {target_symbol: "#ff6b01", sector_etf_symbol: "#006cac", "^GSPC": "#22c55e"}

    # 最大10年分のデータを取得 (キャッシュ利用)
    all_data = {}
    last_date = None

    # ターゲット銘柄はキャッシュせず毎回取得 (個別銘柄は多いため)
    try:
        ticker = utils.get_ticker(target_symbol)
        hist = utils.safe_call(ticker, "history", period="10y", max_retries=5)
        if hist is not None and not hist.empty:
            df = pl.from_pandas(hist.reset_index()).select(['Date', 'Close'])
            df = df.with_columns(pl.col("Date").dt.replace_time_zone(None).dt.date())
            all_data[target_symbol] = df
            last_date = df['Date'][-1]
    except Exception as e:
        print(f"Error fetching data for {target_symbol}: {e}")

    # セクターETFとS&P500はキャッシュを利用
    for sym in [sector_etf_symbol, "^GSPC"]:
        df = get_cached_history(sym)
        if df is not None:
            all_data[sym] = df
            if last_date is None or df['Date'][-1] > last_date:
                last_date = df['Date'][-1]

    if not all_data or last_date is None:
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
        valid_symbols_for_period = []
        
        # Determine the latest common start date for this period across all symbols
        period_start_dates = []
        for sym in symbols:
            if sym not in all_data: continue
            df = all_data[sym]
            if start_date:
                df_p = df.filter(pl.col("Date") >= start_date)
            else:
                df_p = df
            if not df_p.is_empty():
                period_start_dates.append(df_p['Date'][0])
                valid_symbols_for_period.append((sym, df_p))
        
        if not period_start_dates: continue
        common_start_date = max(period_start_dates)
        
        for sym, df_p in valid_symbols_for_period:
            # Sync all to the common start date
            df_p = df_p.filter(pl.col("Date") >= common_start_date)
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
