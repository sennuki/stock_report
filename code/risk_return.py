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

# Plotly 6.0.0+ template migration
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

PERIOD_CONFIGS = [
    {"key": "1M", "label": "1ヶ月", "days": 21},
    {"key": "3M", "label": "3ヶ月", "days": 63},
    {"key": "6M", "label": "6ヶ月", "days": 126},
    {"key": "YTD", "label": "年初来", "days": "YTD"},
    {"key": "1Y", "label": "1年", "days": 252},
    {"key": "2Y", "label": "2年", "days": 504},
    {"key": "5Y", "label": "5年", "days": 1260},
]

def process_single_stock(symbol):
    """1銘柄の各期間のリスク(HV)とリターンを計算"""
    try:
        ticker = yf.Ticker(symbol)
        # 5年以上のデータを取得
        hist_pd = ticker.history(period="10y")
        if hist_pd.empty: return None

        hist = pl.from_pandas(hist_pd.reset_index()).select(['Date', 'Close'])
        hist = hist.with_columns(pl.col("Date").dt.replace_time_zone(None))
        hist = hist.with_columns([(pl.col("Close") / pl.col("Close").shift(1)).log().alias("Log_Return")])
        
        results = {'Symbol': symbol}

        # 前日比
        if len(hist) >= 2:
            results['Daily_Change'] = float((hist['Close'][-1] - hist['Close'][-2]) / hist['Close'][-2])
        else:
            results['Daily_Change'] = 0.0

        # 最新の日付を取得
        last_date = hist['Date'][-1]
        
        for p in PERIOD_CONFIGS:
            key = p['key']
            is_valid_period = True
            if p['days'] == "YTD":
                # 年初来: その年の1月1日以降
                sub = hist.filter(pl.col("Date") >= datetime(last_date.year, 1, 1))
                if len(sub) < 5: sub = hist.tail(21)
            else:
                sub = hist.tail(p['days'])
                # 要求期間の80%以上のデータが存在しない場合は無効とする（上場直後の銘柄を長期グラフから除外）
                if len(sub) < p['days'] * 0.8:
                    is_valid_period = False
            
            if len(sub) < 5 or not is_valid_period:
                results[f'HV_{key}'] = None
                results[f'Ret_{key}'] = None
            else:
                # 年率換算リスク
                std_val = sub['Log_Return'].std()
                results[f'HV_{key}'] = float(std_val * np.sqrt(252)) if std_val is not None else 0.0
                
                # 年率換算リターン
                total_ret = (hist['Close'][-1] / sub['Close'][0]) - 1
                days_diff = (last_date - sub['Date'][0]).days
                if days_diff > 5:
                    ann_ret = (1 + total_ret) ** (365.0 / days_diff) - 1
                    results[f'Ret_{key}'] = float(ann_ret) if np.isfinite(ann_ret) else 0.0
                else:
                    results[f'Ret_{key}'] = float(total_ret)
            
        return results
    except Exception:
        return None

def calculate_market_metrics_parallel(symbols):
    """全銘柄 + ETF + 指数の指標を計算"""
    sector_etfs = ["VOX", "VCR", "VDC", "VDE", "VFH", "VHT", "VIS", "VGT", "VAW", "VNQ", "VPU", "VOO"]
    target_symbols = list(set(symbols + ['^GSPC'] + sector_etfs))

    print(f"\n{len(target_symbols)} 銘柄のリスク・リターンを計算中...")
    results = []
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
    """リスク・リターン散布図生成 (多期間切り替え)"""
    fig = go.Figure()
    trace_counts = []
    period_ranges = []
    
    for p in PERIOD_CONFIGS:
        key = p['key']
        hv_col = f'HV_{key}'
        ret_col = f'Ret_{key}'
        visible = (key == "1Y") # 1年をデフォルト表示
        
        if hv_col not in df_metrics.columns:
            trace_counts.append(0)
            period_ranges.append({'min_x': 0, 'max_x': 2, 'min_y': -2, 'max_y': 2})
            continue

        df_p = df_metrics.filter(pl.col(hv_col).is_not_null() & pl.col(ret_col).is_not_null())
        if df_p.is_empty():
            trace_counts.append(0)
            period_ranges.append({'min_x': 0, 'max_x': 2, 'min_y': -2, 'max_y': 2})
            continue

        # 外れ値対策のためのクリップ範囲を計算
        if len(df_p) > 10:
            import math
            q1_x = df_p[hv_col].quantile(0.05)
            q3_x = df_p[hv_col].quantile(0.95)
            q1_y = df_p[ret_col].quantile(0.05)
            q3_y = df_p[ret_col].quantile(0.95)
            
            # None フォールバック
            q1_x = q1_x if q1_x is not None else 0.0
            q3_x = q3_x if q3_x is not None else 1.0
            q1_y = q1_y if q1_y is not None else -1.0
            q3_y = q3_y if q3_y is not None else 1.0
            
            iqr_x = q3_x - q1_x
            iqr_y = q3_y - q1_y
            
            # IQRが0の場合は少し余裕を持たせる
            if iqr_x == 0: iqr_x = 0.1
            if iqr_y == 0: iqr_y = 0.1
            
            raw_max_x = q3_x + iqr_x * 1.5
            raw_max_y = q3_y + iqr_y * 1.5
            raw_min_y = q1_y - iqr_y * 1.5
            
            # X軸 (下限0.0固定)
            # ステップを算出して綺麗な倍数に切り上げ (5分割を想定)
            step_x = 0.1 # 10%
            if raw_max_x > 1.0: step_x = 0.2
            if raw_max_x > 2.0: step_x = 0.5
            if raw_max_x > 5.0: step_x = 1.0
            max_x = math.ceil(raw_max_x / step_x) * step_x
            min_x = 0.0
            
            # Y軸
            # 下限は-1.0(-100%)までとする。綺麗なステップ幅を算出
            min_y = max(-1.0, math.floor(raw_min_y * 10) / 10.0)
            
            range_y = raw_max_y - min_y
            step_y = 0.2
            if range_y > 1.5: step_y = 0.5
            if range_y > 3.0: step_y = 1.0
            if range_y > 10.0: step_y = 2.0
            if range_y > 20.0: step_y = 5.0
            
            # 最大値を step_y の倍数になるように設定
            ticks = math.ceil(range_y / step_y)
            # 最小5目盛りは確保する
            ticks = max(5, ticks)
            max_y = min_y + ticks * step_y
            
            # ただし、グラフが潰れないように最大500% (5.0) でキャップする
            if max_y > 5.0:
                max_y = 5.0
                
            # X軸も同様に極端な値をキャップ (最大でも300% (3.0)程度で十分)
            if max_x > 3.0:
                max_x = 3.0
        else:
            max_x, min_x = 2.0, 0.0
            max_y, min_y = 2.0, -1.0

        period_ranges.append({'min_x': min_x, 'max_x': max_x, 'min_y': min_y, 'max_y': max_y})

        # プロット用データ抽出 (クリップ処理とオリジナルデータの保持)
        def get_data(df):
            orig_x = df[hv_col].to_list()
            orig_y = df[ret_col].to_list()
            txt = df['Symbol'].to_list()
            
            clipped_x = [min(max_x, max(min_x, val)) if val is not None else None for val in orig_x]
            clipped_y = [min(max_y, max(min_y, val)) if val is not None else None for val in orig_y]
            
            cdata = [[ox, oy, t] for ox, oy, t in zip(orig_x, orig_y, txt)]
            return clipped_x, clipped_y, cdata, txt

        hovertemplate_str = "<b>%{customdata[2]}</b><br>リスク: %{customdata[0]:.1%}<br>リターン: %{customdata[1]:.1%}<extra></extra>"

        # 1. その他
        df_others = df_p.filter(~pl.col('Symbol').is_in([target_symbol, '^GSPC', sector_etf_symbol]))
        x, y, cdata, txt = get_data(df_others)
        fig.add_trace(go.Scatter(x=x, y=y, customdata=cdata, text=txt, mode='markers', name='S&P500銘柄', 
                                hovertemplate=hovertemplate_str,
                                marker=dict(size=6, color='#72777B', opacity=0.4), visible=visible))
        
        # 2. セクター
        df_sector = df_p.filter(pl.col('Symbol') == sector_etf_symbol)
        if not df_sector.is_empty():
            x, y, cdata, txt = get_data(df_sector)
            fig.add_trace(go.Scatter(x=x, y=y, customdata=cdata, text=txt, mode='markers+text', textposition="top center", name='セクターETF',
                                    hovertemplate=hovertemplate_str,
                                    marker=dict(size=12, color='blue', symbol='diamond'), visible=visible))
        else:
            fig.add_trace(go.Scatter(x=[None], y=[None], name='セクターETF', visible=visible))
        
        # 3. 市場
        df_sp500_idx = df_p.filter(pl.col('Symbol') == '^GSPC')
        if not df_sp500_idx.is_empty():
            x, y, cdata, _ = get_data(df_sp500_idx)
            for cd in cdata:
                cd[2] = 'S&P 500'
            fig.add_trace(go.Scatter(x=x, y=y, customdata=cdata, text=['S&P 500'], mode='markers+text', textposition="top center", name='S&P 500',
                                    hovertemplate=hovertemplate_str,
                                    marker=dict(size=12, color='black', symbol='star'), visible=visible))
        else:
            fig.add_trace(go.Scatter(x=[None], y=[None], name='S&P 500', visible=visible))
        
        # 4. ターゲット
        df_target = df_p.filter(pl.col('Symbol') == target_symbol)
        if not df_target.is_empty():
            x, y, cdata, txt = get_data(df_target)
            fig.add_trace(go.Scatter(x=x, y=y, customdata=cdata, text=txt, mode='markers+text', textposition="bottom center", name='対象銘柄',
                                    hovertemplate=hovertemplate_str,
                                    marker=dict(size=16, color='red', line=dict(width=2, color='white')), visible=visible))
        else:
            fig.add_trace(go.Scatter(x=[None], y=[None], name='対象銘柄', visible=visible))
        
        trace_counts.append(4)

    # ボタン作成と初期レイアウト設定
    buttons = []
    default_xaxis_range = None
    default_yaxis_range = None
    
    for i, p in enumerate(PERIOD_CONFIGS):
        vis_array = []
        for j, count in enumerate(trace_counts):
            vis_array.extend([i == j] * count)
        
        pr = period_ranges[i] if i < len(period_ranges) else {'min_x': 0, 'max_x': 2, 'min_y': -2, 'max_y': 2}
        layout_update = {
            "title": f"リスク・リターン分析 ({p['label']})",
            "xaxis.range": [pr['min_x'], pr['max_x']],
            "yaxis.range": [pr['min_y'], pr['max_y']]
        }
        
        if p['key'] == "1Y":
            default_xaxis_range = [pr['min_x'], pr['max_x']]
            default_yaxis_range = [pr['min_y'], pr['max_y']]

        buttons.append(dict(label=p['label'], method="update", args=[{"visible": vis_array}, layout_update]))

    fig.update_layout(
        updatemenus=[dict(type="buttons", direction="right", x=0.5, y=1.2, xanchor="center", buttons=buttons)],
        xaxis=dict(title='リスク (ボラティリティ 年率)', tickformat='.0%', gridcolor='#E5E7EB', range=default_xaxis_range),
        yaxis=dict(title='リターン (年率換算)', tickformat='.0%', gridcolor='#E5E7EB', range=default_yaxis_range),
        margin=dict(l=60, r=30, t=80, b=40), height=550, template='plotly_white',
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1.0)
    )
    return fig
