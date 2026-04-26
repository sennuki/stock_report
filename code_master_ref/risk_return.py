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
import pytz
import time
import utils
from yfinance.exceptions import YFRateLimitError
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
    {"key": "3Y", "label": "3年", "days": 756},
    {"key": "5Y", "label": "5年", "days": 1260},
    {"key": "10Y", "label": "10年", "days": 2520},
]

def process_single_stock(symbol):
    """1銘柄の各期間のリスク(HV)とリターンを計算"""
    try:
        ticker = utils.get_ticker(symbol)
        # 5年以上のデータを取得
        hist_pd = utils.safe_call(ticker, "history", period="10y")
        if hist_pd is None or hist_pd.empty: return None

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
        
        # 決算日の取得 (直近の過去の決算日を探す)
        try:
            # yfinance 1.1.0+ では earnings_dates が取得可能
            earnings_df = utils.safe_get(ticker, 'earnings_dates')
            if earnings_df is not None and not earnings_df.empty:
                # タイムゾーンの有無を確認
                now = datetime.now()
                if earnings_df.index.tzinfo is not None:
                    # インデックスがタイムゾーン付きの場合、now もタイムゾーン付きにする（UTCベース）
                    now = datetime.now(pytz.timezone('UTC'))
                    # インデックスも比較のためにタイムゾーンを調整
                    earnings_df.index = earnings_df.index.tz_convert('UTC')
                
                # インデックスを日付として扱い、現在時刻より前の最新のものを探す
                past_earnings = earnings_df[earnings_df.index <= now]
                if not past_earnings.empty:
                    recent_earnings_date = past_earnings.index.max()
                    results['Earnings_Date'] = recent_earnings_date.strftime('%Y-%m-%d')
                else:
                    results['Earnings_Date'] = None
            else:
                results['Earnings_Date'] = None
        except YFRateLimitError:
            # print(f"Rate limited for {symbol}")
            results['Earnings_Date'] = None
            time.sleep(1)
        except Exception as e:
            # print(f"Earnings Date Fetch Error for {symbol}: {e}")
            results['Earnings_Date'] = None
        
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
    # SPDR Sector ETFs + S&P 500 ETF (SPY)
    sector_etfs = ["XLC", "XLY", "XLP", "XLE", "XLF", "XLV", "XLI", "XLK", "XLB", "XLRE", "XLU", "SPY"]
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
            
            # ターゲット銘柄とセクターETF、指数の値も考慮に入れる（これらが外枠を広げるように）
            special_symbols = [target_symbol, sector_etf_symbol, '^GSPC']
            df_special = df_p.filter(pl.col('Symbol').is_in(special_symbols))
            if not df_special.is_empty():
                target_max_y = df_special[ret_col].max()
                target_min_y = df_special[ret_col].min()
                if target_max_y is not None: raw_max_y = max(raw_max_y, target_max_y * 1.1)
                if target_min_y is not None: raw_min_y = min(raw_min_y, target_min_y * 1.1)

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
            
            # グラフが潰れないようにキャップする
            # 1ヶ月(1M)の場合は最大2000%(20.0)まで動的に許容し、それ以外は500%(5.0)でキャップする
            cap_y = 20.0 if key == "1M" else 5.0
            if max_y > cap_y:
                max_y = cap_y
                
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
            
            sec = df["Security"].to_list() if "Security" in df.columns else [""] * len(txt)
            cdata = [[ox, oy, t, s] for ox, oy, t, s in zip(orig_x, orig_y, txt, sec)]
            return clipped_x, clipped_y, cdata, txt

        hovertemplate_str = "<b>%{customdata[2]}</b><br>%{customdata[3]}<br>リスク: %{customdata[0]:.1%}<br>リターン: %{customdata[1]:.1%}<extra></extra>"

        # 1. ターゲット
        df_target = df_p.filter(pl.col('Symbol') == target_symbol)
        if not df_target.is_empty():
            x, y, cdata, txt = get_data(df_target)
            fig.add_trace(go.Scatter(x=x, y=y, customdata=cdata, text=txt, mode='markers+text', textposition="bottom center", name=f'{target_symbol} ({p["label"]})',
                                    hovertemplate=hovertemplate_str,
                                    marker=dict(size=16, color='red', line=dict(width=2, color='white')), visible=visible))
        else:
            fig.add_trace(go.Scatter(x=[None], y=[None], name=f'{target_symbol} ({p["label"]})', visible=visible))

        # 2. セクター
        df_sector = df_p.filter(pl.col('Symbol') == sector_etf_symbol)
        if not df_sector.is_empty():
            x, y, cdata, txt = get_data(df_sector)
            fig.add_trace(go.Scatter(x=x, y=y, customdata=cdata, text=txt, mode='markers+text', textposition="top center", name=f'{sector_etf_symbol} ({p["label"]})',
                                    hovertemplate=hovertemplate_str,
                                    marker=dict(size=12, color='blue'), visible=visible))
        else:
            fig.add_trace(go.Scatter(x=[None], y=[None], name=f'{sector_etf_symbol} ({p["label"]})', visible=visible))
        
        # 3. 市場 (S&P 500 Index)
        df_sp500_idx = df_p.filter(pl.col('Symbol') == '^GSPC')
        if not df_sp500_idx.is_empty():
            x, y, cdata, _ = get_data(df_sp500_idx)
            for cd in cdata:
                cd[2] = 'S&P 500'
                cd[3] = 'S&P 500 Index'
            fig.add_trace(go.Scatter(x=x, y=y, customdata=cdata, text=['S&P 500'], mode='markers+text', textposition="top center", name=f'S&P 500 ({p["label"]})',
                                    hovertemplate=hovertemplate_str,
                                    marker=dict(size=12, color='black'), visible=visible))
        else:
            fig.add_trace(go.Scatter(x=[None], y=[None], name=f'S&P 500 ({p["label"]})', visible=visible))

        # 4. その他 (S&P 500 銘柄)
        df_others = df_p.filter(~pl.col('Symbol').is_in([target_symbol, '^GSPC', sector_etf_symbol]))
        x, y, cdata, txt = get_data(df_others)
        fig.add_trace(go.Scatter(x=x, y=y, customdata=cdata, text=txt, mode='markers', name=f'S&P500銘柄 ({p["label"]})', 
                                hovertemplate=hovertemplate_str,
                                marker=dict(size=6, color='#72777B', opacity=0.4), visible=visible))
        
        trace_counts.append(4)

    # 初期レイアウト設定
    default_xaxis_range = None
    default_yaxis_range = None
    
    for i, p in enumerate(PERIOD_CONFIGS):
        if p['key'] == "1Y":
            pr = period_ranges[i] if i < len(period_ranges) else {'min_x': 0, 'max_x': 2, 'min_y': -2, 'max_y': 2}
            default_xaxis_range = [pr['min_x'], pr['max_x']]
            default_yaxis_range = [pr['min_y'], pr['max_y']]

    fig.update_layout(
        xaxis=dict(title='リスク (ボラティリティ 年率)', tickformat='.0%', gridcolor='#E5E7EB', range=default_xaxis_range),
        yaxis=dict(title='リターン (年率換算)', tickformat='.0%', gridcolor='#E5E7EB', range=default_yaxis_range),
        margin=dict(l=60, r=30, t=80, b=40), height=550, template='plotly_white',
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1.0)
    )
    return fig
