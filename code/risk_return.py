# -*- coding: utf-8 -*-
import warnings
# Plotly 6.0.0+ deprecation warnings (scattermapbox -> scattermap)
warnings.filterwarnings("ignore", category=FutureWarning, message=".*scattermapbox.*")

import os
import yfinance as yf
import polars as pl
import numpy as np
import plotly.graph_objects as go
import plotly.io as pio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

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

PERIODS = [
    {"label": "1ヶ月", "days": 21},
    {"label": "6ヶ月", "days": 126},
    {"label": "年初来", "days": "ytd"},
    {"label": "1年", "days": 252},
    {"label": "2年", "days": 504},
    {"label": "5年", "days": 1260},
    {"label": "10年", "days": 2520},
]

def process_single_stock(symbol):
    """1銘柄の各期間のHVとリターンを計算"""
    try:
        ticker = yf.Ticker(symbol)
        # 最大10年分取得
        hist_pd = ticker.history(period="10y")
        if hist_pd.empty: return None

        hist = pl.from_pandas(hist_pd.reset_index()).select(['Date', 'Close'])
        # タイムゾーンを除去して日付操作を容易にする
        hist = hist.with_columns(pl.col("Date").dt.replace_time_zone(None))
        hist = hist.with_columns([(pl.col("Close") / pl.col("Close").shift(1)).log().alias("Log_Return")])
        
        last_date = hist['Date'].max()
        results = {'Symbol': symbol}

        # 前日比の計算
        if len(hist) >= 2:
            last_close = hist['Close'][-1]
            prev_close = hist['Close'][-2]
            results['Daily_Change'] = (last_close - prev_close) / prev_close
        else:
            results['Daily_Change'] = None

        for p in PERIODS:
            label = p['label']
            if p['days'] == "ytd":
                start_of_year = datetime(last_date.year, 1, 1)
                sub = hist.filter(pl.col('Date') >= start_of_year)
            else:
                sub = hist.tail(p['days'])
            
            if len(sub) < 5: # データが少なすぎる場合はスキップ
                results[f'HV_{label}'] = None
                results[f'Ret_{label}'] = None
                continue

            # 年率換算 (252営業日)
            results[f'HV_{label}'] = sub['Log_Return'].std() * np.sqrt(252)
            results[f'Ret_{label}'] = sub['Log_Return'].mean() * 252
            
        return results
    except: return None

def calculate_market_metrics_parallel(symbols):
    """全銘柄 + ETF + 指数の指標を計算"""
    sector_etfs = ["VOX", "VCR", "VDC", "VDE", "VFH", "VHT", "VIS", "VGT", "VAW", "VNQ", "VPU", "VOO"]
    target_symbols = list(set(symbols + ['^GSPC'] + sector_etfs))

    print(f"\n{len(target_symbols)} 銘柄のリスク・リターン(多期間)を計算中...")
    results = []
    # GitHub Actions では 10、ローカルでは 1 をデフォルトにする
    default_max_workers = 10 if os.getenv("GITHUB_ACTIONS") == "true" else 1
    current_max_workers = int(os.getenv("MAX_WORKERS", default_max_workers))

    with ThreadPoolExecutor(max_workers=current_max_workers) as executor:
        future_to_symbol = {executor.submit(process_single_stock, sym): sym for sym in target_symbols}
        for future in tqdm(as_completed(future_to_symbol), total=len(target_symbols)):
            res = future.result()
            if res: results.append(res)
    return pl.DataFrame(results) if results else pl.DataFrame({'Symbol': []})

def generate_scatter_html(df_metrics, target_symbol, sector_etf_symbol):
    fig = generate_scatter_fig(df_metrics, target_symbol, sector_etf_symbol)
    return fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False, 'scrollZoom': False, 'responsive': True})

def generate_scatter_fig(df_metrics, target_symbol, sector_etf_symbol):
    """リスク・リターン散布図生成 (期間切り替えタブ付き)"""
    fig = go.Figure()
    
    # 各期間ごとにトレースを作成
    # トレースの順序: [1M(Others, Sector, Market, Target), 6M(...), ...]
    traces_per_period = 4
    total_periods = len(PERIODS)
    
    for i, p in enumerate(PERIODS):
        label = p['label']
        visible = (label == "2年") # デフォルトは2年を表示
        
        hv_col = f'HV_{label}'
        ret_col = f'Ret_{label}'
        
        # データ抽出 (HVとRetの両方が存在する銘柄のみ)
        df_p = df_metrics.filter(pl.col(hv_col).is_not_null() & pl.col(ret_col).is_not_null())
        
        df_sp500_idx = df_p.filter(pl.col('Symbol') == '^GSPC')
        df_target = df_p.filter(pl.col('Symbol') == target_symbol)
        df_sector = df_p.filter(pl.col('Symbol') == sector_etf_symbol)
        df_others = df_p.filter(~pl.col('Symbol').is_in([target_symbol, '^GSPC', sector_etf_symbol]))

        # 1. その他
        fig.add_trace(go.Scatter(
            x=df_others[hv_col], y=df_others[ret_col], text=df_others['Symbol'],
            mode='markers', name='S&P500銘柄', 
            marker=dict(size=6, color='#72777B', opacity=0.4),
            visible=visible, hovertemplate='<b>%{text}</b><br>リスク: %{x:.1%}<br>リターン: %{y:.1%}<extra></extra>'
        ))
        # 2. セクター
        fig.add_trace(go.Scatter(
            x=df_sector[hv_col], y=df_sector[ret_col], text=[sector_etf_symbol],
            mode='markers+text', textposition="top center", name=f'セクター({sector_etf_symbol})',
            marker=dict(size=12, color='blue', symbol='diamond'),
            visible=visible, hovertemplate='<b>%{text}</b><br>リスク: %{x:.1%}<br>リターン: %{y:.1%}<extra></extra>'
        ))
        # 3. 市場
        fig.add_trace(go.Scatter(
            x=df_sp500_idx[hv_col], y=df_sp500_idx[ret_col], text=['S&P 500'],
            mode='markers+text', textposition="top center", name='S&P 500',
            marker=dict(size=12, color='black', symbol='star'),
            visible=visible, hovertemplate='<b>S&P 500</b><br>リスク: %{x:.1%}<br>リターン: %{y:.1%}<extra></extra>'
        ))
        # 4. ターゲット
        fig.add_trace(go.Scatter(
            x=df_target[hv_col], y=df_target[ret_col], text=[target_symbol],
            mode='markers+text', textposition="bottom center", name=target_symbol,
            marker=dict(size=16, color='red', line=dict(width=2, color='white')),
            visible=visible, hovertemplate='<b>%{text}</b> (対象)<br>リスク: %{x:.1%}<br>リターン: %{y:.1%}<extra></extra>'
        ))

    # 切り替えボタンの作成
    buttons = []
    for i, p in enumerate(PERIODS):
        # この期間のトレースだけをTrueにし、他をFalseにするリストを作成
        visibility = [False] * (total_periods * traces_per_period)
        for j in range(traces_per_period):
            visibility[i * traces_per_period + j] = True
            
        buttons.append(dict(
            label=p['label'],
            method="update",
            args=[{"visible": visibility}]
        ))

    fig.update_layout(
        xaxis=dict(title='リスク (ボラティリティ 年率)', tickformat='.0%', fixedrange=True, gridcolor='#E5E7EB'),
        yaxis=dict(
            title=dict(text='リターン (年率換算)', font=dict(color='#374151')),
            tickformat='.0%', fixedrange=True, automargin=True, gridcolor='#E5E7EB'
        ),
        margin=dict(l=60, r=30, t=130, b=40), height=600, template='plotly_white',
        autosize=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1.0,
            font=dict(color='#374151')
        ),
        updatemenus=[dict(
            type="buttons",
            direction="right",
            active=4, # 2年 (インデックス4) をアクティブに
            x=0.5,
            y=1.2,
            xanchor="center",
            yanchor="bottom",
            showactive=True,
            buttons=buttons,
            pad={"r": 10, "t": 10},
            font=dict(size=12),
            bgcolor="white",
            bordercolor="#E5E7EB",
            borderwidth=1
        )]
    )
    
    return fig

if __name__ == "__main__":
    df_metrics = calculate_market_metrics_parallel(["MSFT", "AAPL"])
    html = generate_scatter_html(df_metrics, "MSFT", "VGT")
    with open("test_scatter.html", "w") as f:
        f.write(html)
    print("test_scatter.html generated.")
