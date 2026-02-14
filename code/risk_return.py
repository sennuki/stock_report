# -*- coding: utf-8 -*-
import yfinance as yf
import polars as pl
import numpy as np
import plotly.graph_objects as go
import plotly.io as pio
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

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
#  Part B: リスク・リターン分析 (並列計算)
# ==========================================

def process_single_stock(symbol):
    """1銘柄のHVとリターンを計算"""
    try:
        ticker = yf.Ticker(symbol)
        hist_pd = ticker.history(period="2y")
        if len(hist_pd) < 250: return None

        hist = pl.from_pandas(hist_pd.reset_index()).select(['Date', 'Close'])
        hist = hist.with_columns([(pl.col("Close") / pl.col("Close").shift(1)).log().alias("Log_Return")])
        last_250 = hist['Log_Return'].tail(250)

        return {
            'Symbol': symbol,
            'HV_250': last_250.std() * np.sqrt(250),
            'Log_Return': last_250.mean() * 250
        }
    except: return None

def calculate_market_metrics_parallel(symbols):
    """全銘柄 + ETF + 指数の指標を計算"""
    sector_etfs = ["VOX", "VCR", "VDC", "VDE", "VFH", "VHT", "VIS", "VGT", "VAW", "VNQ", "VPU", "VOO"]
    # 重複を排除し、指数とETFを追加
    target_symbols = list(set(symbols + ['^GSPC'] + sector_etfs))

    print(f"\n{len(target_symbols)} 銘柄のリスク・リターンを計算中...")
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_symbol = {executor.submit(process_single_stock, sym): sym for sym in target_symbols}
        for future in tqdm(as_completed(future_to_symbol), total=len(target_symbols)):
            res = future.result()
            if res: results.append(res)
    return pl.DataFrame(results) if results else pl.DataFrame({'Symbol': [], 'HV_250': [], 'Log_Return': []})

def generate_scatter_html(df_metrics, target_symbol, sector_etf_symbol):
    """リスク・リターン散布図生成"""
    df_sp500_idx = df_metrics.filter(pl.col('Symbol') == '^GSPC')
    df_target = df_metrics.filter(pl.col('Symbol') == target_symbol)
    df_sector = df_metrics.filter(pl.col('Symbol') == sector_etf_symbol)
    df_others = df_metrics.filter(~pl.col('Symbol').is_in([target_symbol, '^GSPC', sector_etf_symbol]))

    fig = go.Figure()
    # その他
    fig.add_trace(go.Scatter(x=df_others['HV_250'], y=df_others['Log_Return'], text=df_others['Symbol'], mode='markers', name='S&P500銘柄', marker=dict(size=6, color='#72777B', opacity=0.5)))
    # セクター
    if not df_sector.is_empty():
        fig.add_trace(go.Scatter(x=df_sector['HV_250'], y=df_sector['Log_Return'], text=[f"{sector_etf_symbol}"] , mode='markers+text', textposition="top center", name=f'{sector_etf_symbol}', marker=dict(size=12, color='blue', symbol='diamond')))
    # 市場
    if not df_sp500_idx.is_empty():
        fig.add_trace(go.Scatter(x=df_sp500_idx['HV_250'], y=df_sp500_idx['Log_Return'], text=['S&P 500'], mode='markers+text', textposition="top center",name='S&P500', marker=dict(size=12, color='black', symbol='star')))
    # ターゲット
    if not df_target.is_empty():
        fig.add_trace(go.Scatter(x=df_target['HV_250'], y=df_target['Log_Return'], text=[target_symbol], mode='markers+text', textposition="bottom center", name=target_symbol, marker=dict(size=16, color='red', line=dict(width=2, color='white'))))

    fig.update_layout(
        title=dict(text=f"リスク・リターン分析: {target_symbol} vs {sector_etf_symbol}", font=dict(size=14)),
        xaxis=dict(title='リスク (HV 250日)', tickformat='.0%', fixedrange=True),
        yaxis=dict(title='リターン (年率換算)', tickformat='.0%', fixedrange=True),
        margin=dict(l=40, r=40, t=60, b=40), height=500, template='plotly_white',
        autosize=True,
        legend=dict(orientation="h", y=1.02, x=1, xanchor="right")
    )
    # ここだけはCDNを含める（最初のグラフとしてロードさせるため）
    return fig.to_html(full_html=False, include_plotlyjs='cdn', config={'displayModeBar': False, 'scrollZoom': False, 'responsive': True})

if __name__ == "__main__":
    print("MSFTと関連指数のリスク・リターンを計算中...")
    # テスト用銘柄
    symbols = ["MSFT", "AAPL", "NVDA"]
    
    # 計算実行
    df_metrics = calculate_market_metrics_parallel(symbols)
    print("\n--- Calculated Metrics ---")
    print(df_metrics)
    
    # グラフ生成テスト
    if not df_metrics.is_empty():
        html = generate_scatter_html(df_metrics, "MSFT", "VGT")
        print(f"Scatter Plot HTML generated (len: {len(html)})")
