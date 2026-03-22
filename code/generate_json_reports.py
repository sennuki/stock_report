# -*- coding: utf-8 -*-
import concurrent.futures
import os
import json
import polars as pl
import pandas as pd
import numpy as np
import time
from yfinance.exceptions import YFRateLimitError
import base64
from tqdm import tqdm
import plotly.io as pio
import logging
import datetime
# Suppress noisy yfinance errors
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

import fundamentals
import risk_return
import performance_comparison
import utils
import market_data

import time
import random

# Force Plotly to use standard JSON output
pio.json.config.default_engine = 'json'

from decimal import Decimal

def clean_plotly_data(obj):
    """Recursively remove 'bdata' and convert to standard lists."""
    if isinstance(obj, dict):
        if "bdata" in obj and "dtype" in obj:
            # Decode base64 binary data to list
            dtype = obj["dtype"]
            bdata = base64.b64decode(obj["bdata"])
            return np.frombuffer(bdata, dtype=dtype).tolist()
        return {k: clean_plotly_data(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_plotly_data(v) for v in obj]
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, (np.generic, datetime.datetime, datetime.date)):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return obj.item()
    return obj

def fig_to_dict(fig):
    if isinstance(fig, str):
        return {"error": fig}
    
    if hasattr(fig, 'to_plotly_json'):
        data = fig.to_plotly_json()
    else:
        data = fig
        
    # Thoroughly clean bdata and numpy types
    return clean_plotly_data(data)

def generate_json_for_ticker(row, df_info, df_metrics, output_dir, monex_symbols=None, rakuten_symbols=None, sbi_symbols=None, mufg_symbols=None, matsui_symbols=None, dmm_symbols=None, paypay_symbols=None):
    # Add a small random delay to mimic human behavior and avoid rate limits
    time.sleep(random.uniform(0.5, 1.5))
    
    ticker_display = row['Symbol']
    chart_target_symbol = row['Symbol_YF']
    current_sector = row['GICS Sector']
    current_sub_industry = row['GICS Sub-Industry']
    exchange = row['Exchange']
    
    # Check availability
    def check_availability(target, symbol_list):
        if not symbol_list: return False
        if target in symbol_list: return True
        # Handle variations like BRK-B vs BRKB or BRK.B
        variations = [target.replace("-", ""), target.replace("-", ".")]
        for v in variations:
            if v in symbol_list: return True
        return False

    is_available_monex = check_availability(ticker_display, monex_symbols)
    is_available_rakuten = check_availability(ticker_display, rakuten_symbols)
    is_available_sbi = check_availability(ticker_display, sbi_symbols)
    is_available_mufg = check_availability(ticker_display, mufg_symbols)
    is_available_matsui = check_availability(ticker_display, matsui_symbols)
    is_available_dmm = check_availability(ticker_display, dmm_symbols)
    is_available_paypay = check_availability(ticker_display, paypay_symbols)
    # TradingView symbol
    tv_ticker = ticker_display.replace("-", ".")
    full_symbol = f"{exchange}:{tv_ticker}"
    
    # Sector ETF (SPDR)
    sector_map = {
        "Communication Services": "XLC", "Consumer Discretionary": "XLY",
        "Consumer Staples": "XLP", "Energy": "XLE", "Financials": "XLF",
        "Health Care": "XLV", "Industrials": "XLI", "Information Technology": "XLK",
        "Materials": "XLB", "Real Estate": "XLRE", "Utilities": "XLU"
    }
    sector_etf_ticker = sector_map.get(current_sector, "SPY")

    # 1. Financial Data & Charts
    report_data = {
        "symbol": ticker_display,
        "symbol_yf": chart_target_symbol,
        "security": row['Security'],
        "security_ja": row.get('Security_JA'),
        "sector": current_sector,
        "sub_industry": current_sub_industry,
        "exchange": exchange,
        "full_symbol": full_symbol,
        "sector_etf": sector_etf_ticker,
        "is_available_monex": is_available_monex,
        "is_available_rakuten": is_available_rakuten,
        "is_available_sbi": is_available_sbi,
        "is_available_mufg": is_available_mufg,
        "is_available_matsui": is_available_matsui,
        "is_available_dmm": is_available_dmm,
        "is_available_paypay": is_available_paypay,
        "charts": {}
    }

    try:
        ticker_obj = utils.get_ticker(chart_target_symbol)
        fin_data = fundamentals.get_financial_data(ticker_obj)
        
        report_data["charts"]["bs"] = fig_to_dict(fundamentals.get_bs_chart_data(fin_data.get('bs', {})))
        report_data["charts"]["is"] = fig_to_dict(fundamentals.get_is_chart_data(fin_data.get('is', {})))
        report_data["charts"]["cf"] = fig_to_dict(fundamentals.get_cf_chart_data(fin_data.get('cf', {})))
        report_data["charts"]["tp"] = fig_to_dict(fundamentals.get_tp_chart_data(fin_data.get('tp', {})))
        report_data["charts"]["dps"] = fig_to_dict(fundamentals.get_dps_eps_chart_data(fin_data.get('dps', {}), fin_data.get('is', {})))
        report_data["charts"]["dps_history"] = fig_to_dict(fundamentals.get_dps_history_chart_data(fin_data.get('dps', {})))
        
        # --- Add Valuation Data ---
        # if "valuation" in fin_data:
        #    report_data["valuation"] = fin_data["valuation"]
        #    report_data["charts"]["pe_valuation"] = fig_to_dict(fundamentals.get_valuation_plotly_fig(fin_data["valuation"]))
        # ----------------------------

        # --- Add Earnings Surprise ---
        def format_date(d):
            if d is None: return None
            if hasattr(d, 'strftime'): return d.strftime('%Y-%m-%d')
            s = str(d)
            if ' ' in s: s = s.split(' ')[0]
            return s

        try:
            # yfinance property access for earnings_dates is notoriously flaky
            ed = utils.safe_get(ticker_obj, 'earnings_dates')
            
            if ed is not None and not ed.empty:
                # Ensure it's a DataFrame and has required columns
                required_cols = ['Reported EPS', 'EPS Estimate', 'Surprise(%)']
                if all(col in ed.columns for col in required_cols):
                    # Latest reported
                    valid_ed = ed[ed['Reported EPS'].notnull()].sort_index(ascending=False)
                    if not valid_ed.empty:
                        latest = valid_ed.iloc[0]
                        report_data["earnings_surprise"] = {
                            "date": format_date(valid_ed.index[0]),
                            "actual": float(latest['Reported EPS']),
                            "estimate": float(latest['EPS Estimate']) if not np.isnan(latest['EPS Estimate']) else None,
                            "surprise_pct": float(latest['Surprise(%)']) if not np.isnan(latest['Surprise(%)']) else None
                        }
                    
                    # Next earnings (where Reported EPS is NaN)
                    next_ed = ed[ed['Reported EPS'].isnull()].sort_index(ascending=True)
                    if not next_ed.empty:
                        # Check if the date is in the future
                        now_str = datetime.datetime.now().strftime('%Y-%m-%d')
                        next_date_cand = format_date(next_ed.index[0])
                        if next_date_cand >= now_str:
                            next_item = next_ed.iloc[0]
                            report_data["next_earnings"] = {
                                "date": next_date_cand,
                                "estimate": float(next_item['EPS Estimate']) if not np.isnan(next_item['EPS Estimate']) else None
                            }
                    
                    # If next_earnings is still missing or estimate is None OR date is in the past, 
                    # try ticker.calendar and ticker.info
                    now_str = datetime.datetime.now().strftime('%Y-%m-%d')
                    if not report_data.get("next_earnings") or report_data["next_earnings"].get("date", "") < now_str:
                        cal = utils.safe_get(ticker_obj, 'calendar')
                        next_date = None
                        cal_est = None
                        
                        if cal is not None:
                            # Handle DataFrame format (older yfinance)
                            if hasattr(cal, 'empty') and not cal.empty:
                                if 'Earnings Date' in cal.index:
                                    dates = cal.loc['Earnings Date']
                                    if isinstance(dates, (list, pd.Series, pd.Index)) and len(dates) > 0:
                                        next_date = format_date(dates[0])
                                    else:
                                        next_date = format_date(dates)
                                if 'EPS Estimate' in cal.index:
                                    cal_est = cal.loc['EPS Estimate'].iloc[0] if hasattr(cal.loc['EPS Estimate'], 'iloc') else cal.loc['EPS Estimate']
                            # Handle dictionary format (newer yfinance)
                            elif isinstance(cal, dict):
                                dates = cal.get('Earnings Date')
                                if dates and isinstance(dates, list) and len(dates) > 0:
                                    next_date = format_date(dates[0])
                                cal_est = cal.get('Earnings Average') or cal.get('EPS Estimate')
                        
                        # Fallback to info
                        est = cal_est or info.get("earningsAverage")
                        
                        # Only use if it's in the future
                        if next_date and next_date >= now_str:
                            report_data["next_earnings"] = {"date": next_date, "estimate": float(est) if est is not None else None}
                        elif est and report_data.get("next_earnings"):
                            # Update estimate if missing even if date was found elsewhere
                            if report_data["next_earnings"].get("estimate") is None:
                                report_data["next_earnings"]["estimate"] = float(est)

                    if report_data.get("next_earnings") and report_data["next_earnings"].get("estimate") is None:
                        info = utils.safe_get(ticker_obj, 'info', default={})
                        est = info.get("earningsAverage")
                        if est:
                            report_data["next_earnings"]["estimate"] = float(est)

            # --- Additional Fallback for Recent Earnings (earnings_surprise) ---
            if not report_data.get("earnings_surprise") or report_data["earnings_surprise"].get("actual") is None:
                qis = utils.safe_get(ticker_obj, 'quarterly_income_stmt')
                if qis is not None and not qis.empty:
                    # Look for EPS rows
                    eps_row = None
                    for label in ['Basic EPS', 'Diluted EPS', 'BasicEPS', 'DilutedEPS']:
                        if label in qis.index:
                            eps_row = qis.loc[label]
                            break
                    
                    if eps_row is not None and not eps_row.empty:
                        # Get most recent non-null value
                        for date_idx, val in eps_row.items():
                            if val is not None and not np.isnan(val):
                                # If we didn't have any surprise data, create a basic one
                                if not report_data.get("earnings_surprise"):
                                    report_data["earnings_surprise"] = {
                                        "date": format_date(date_idx),
                                        "actual": float(val),
                                        "estimate": None,
                                        "surprise_pct": None
                                    }
                                elif report_data["earnings_surprise"].get("actual") is None:
                                    report_data["earnings_surprise"]["actual"] = float(val)
                                break
            # -----------------------------------------------------------------

        except Exception:
            # Silently skip earnings surprise errors as they are very common with yfinance
            pass
        # ----------------------------

        # --- Add Consensus Data ---
        try:
            def df_to_dict_safe(df):
                if df is None or not hasattr(df, 'empty') or df.empty: return None
                return df.replace({np.nan: None}).to_dict('index')

            # Fetch consensus from ticker.info first (more reliable for current)
            info = utils.safe_get(ticker_obj, 'info', default={})
            
            # Helper to extract data from estimate dataframe rows
            def get_row_val(df, period, col_name_base):
                if df is None or period not in df.index:
                    return None
                row = df.loc[period]
                # Try various case variants (e.g., 'Avg', 'avg')
                for variant in [col_name_base, col_name_base.lower(), col_name_base.capitalize()]:
                    if variant in row:
                        val = row[variant]
                        if val is not None and not pd.isna(val):
                            return float(val)
                return None

            # Create a simplified consensus structure
            consensus = {
                "earnings": {},
                "revenue": {}
            }

            # Map for periods we want to extract
            periods = ["0q", "+1q", "0y", "+1y"]
            
            # Initial load from info (mostly for 0q)
            consensus["earnings"]["0q"] = {
                "avg": info.get("earningsAverage"),
                "low": info.get("earningsLow"),
                "high": info.get("earningsHigh"),
                "growth": info.get("earningsGrowth"),
                "numberOfAnalysts": info.get("numberOfAnalystOpinions")
            }
            consensus["revenue"]["0q"] = {
                "avg": info.get("revenueAverage"),
                "low": info.get("revenueLow"),
                "high": info.get("revenueHigh"),
                "growth": info.get("revenueGrowth"),
                "numberOfAnalysts": info.get("numberOfAnalystOpinions")
            }

            # Fallback and additional periods from dedicated properties
            e_est = utils.safe_get(ticker_obj, 'earnings_estimate')
            r_est = utils.safe_get(ticker_obj, 'revenue_estimate')
            
            if e_est is not None and not e_est.empty:
                consensus["earnings_full"] = df_to_dict_safe(e_est)
                for p in periods:
                    if p not in consensus["earnings"] or consensus["earnings"][p].get("avg") is None:
                        consensus["earnings"][p] = {
                            "avg": get_row_val(e_est, p, "avg"),
                            "low": get_row_val(e_est, p, "low"),
                            "high": get_row_val(e_est, p, "high"),
                            "growth": get_row_val(e_est, p, "growth"),
                            "numberOfAnalysts": get_row_val(e_est, p, "numberOfAnalysts")
                        }

            if r_est is not None and not r_est.empty:
                consensus["revenue_full"] = df_to_dict_safe(r_est)
                for p in periods:
                    if p not in consensus["revenue"] or consensus["revenue"][p].get("avg") is None:
                        consensus["revenue"][p] = {
                            "avg": get_row_val(r_est, p, "avg"),
                            "low": get_row_val(r_est, p, "low"),
                            "high": get_row_val(r_est, p, "high"),
                            "growth": get_row_val(r_est, p, "growth"),
                            "numberOfAnalysts": get_row_val(r_est, p, "numberOfAnalysts")
                        }

            report_data["consensus"] = consensus
            report_data["consensus_raw"] = {
                "eps_trend": df_to_dict_safe(utils.safe_get(ticker_obj, 'eps_trend')),
                "eps_revisions": df_to_dict_safe(utils.safe_get(ticker_obj, 'eps_revisions'))
            }
        except Exception as cons_err:
            print(f"Error fetching consensus for {ticker_display}: {cons_err}")
        # ----------------------------

        # --- Add Highlights ---
        try:
            info = utils.safe_get(ticker_obj, 'info', default={})
            
            def get_growth_val(field):
                val = info.get(field)
                if val is None: return None
                # Growth values are sometimes 10.5 (for 10.5%) and sometimes 0.105
                if val > 1.0 or val < -1.0: return val / 100.0
                return val

            def get_yield_val(field):
                val = info.get(field)
                if val is None: return None
                # Dividend yield is almost always a ratio (0.0089) in yfinance, but sometimes 0.89
                if val > 0.1: return val / 100.0
                return val
                
            report_data["highlights"] = {
                "revenue_growth": get_growth_val("revenueGrowth"),
                "earnings_growth": get_growth_val("earningsGrowth"),
                "profit_margins": info.get("profitMargins"),
                "operating_margins": info.get("operatingMargins"),
                "roe": info.get("returnOnEquity"),
                "roa": info.get("returnOnAssets"),
                "eps_ttm": info.get("trailingEps"),
                "eps_forward": info.get("forwardEps"),
                "pe_ttm": info.get("trailingPE"),
                "pe_forward": info.get("forwardPE"),
                "dividend_yield": get_yield_val("dividendYield"),
                "payout_ratio": info.get("payoutRatio"),
                "debt_to_equity": info.get("debtToEquity"),
                "current_ratio": info.get("currentRatio")
            }
            
            # Additional fallback for payout ratio if missing
            if report_data["highlights"]["payout_ratio"] is None:
                div_rate = info.get("dividendRate")
                eps_ttm = info.get("trailingEps")
                if div_rate and eps_ttm and eps_ttm > 0:
                    report_data["highlights"]["payout_ratio"] = div_rate / eps_ttm

        except Exception as h_err:
            print(f"Error fetching highlights for {ticker_display}: {h_err}")
        # ----------------------------

        # --- Add Analyst Ratings ---
        try:
            # yfinance 1.1.0+ may output 404 or other errors for some symbols
            recs = utils.safe_get(ticker_obj, 'recommendations_summary')
            analyst_data = {}
            if recs is not None and not recs.empty:
                # Use current month (period '0m')
                current_recs = recs[recs['period'] == '0m']
                if not current_recs.empty:
                    analyst_data = current_recs.to_dict('records')[0]
            
            # Add target prices from info (already fetched)
            target_keys = [
                'targetHighPrice', 'targetLowPrice', 'targetMeanPrice', 
                'targetMedianPrice', 'currentPrice', 'numberOfAnalystOpinions'
            ]
            for key in target_keys:
                if key in info:
                    analyst_data[key] = info[key]
            
            # Add recent rating changes (upgrades/downgrades)
            try:
                ud = utils.safe_get(ticker_obj, 'upgrades_downgrades')
                if ud is not None and not ud.empty:
                    # Take the last 10 changes, convert index to string
                    recent_ud = ud.sort_index(ascending=False).head(10).reset_index()
                    # Convert Timestamp to string
                    recent_ud['GradeDate'] = recent_ud['GradeDate'].dt.strftime('%Y-%m-%d')
                    # Replace NaN with null for JSON compatibility
                    recent_ud = recent_ud.replace({np.nan: None})
                    report_data["rating_changes"] = recent_ud.to_dict('records')
            except Exception as ud_err:
                print(f"Error fetching upgrades/downgrades for {ticker_display}: {ud_err}")

            if analyst_data:
                report_data["analyst_ratings"] = analyst_data

        except Exception:
            # Silently skip if recommendations are unavailable
            pass
        # ----------------------------

    except Exception as e:
        error_msg = str(e)
        print(f"Error fetching financials for {ticker_display}: {error_msg}")
        report_data["error"] = error_msg
        
        # If rate limited, sleep longer for the next one
        if "Rate limited" in error_msg or "429" in error_msg:
            print(f"Rate limit detected for {ticker_display}. Sleeping for 30 seconds...")
            time.sleep(30)

    # 2. Risk Return Chart
    try:
        # Ensure Symbol column in df_metrics is string
        df_metrics = df_metrics.with_columns(pl.col("Symbol").cast(pl.String))
        
        # Join df_info to df_metrics to add Security column for hover tooltips
        df_metrics_with_name = df_metrics.join(
            df_info.select(["Symbol_YF", "Security"]), 
            left_on="Symbol", 
            right_on="Symbol_YF", 
            how="left"
        )
        fig_rr = risk_return.generate_scatter_fig(df_metrics_with_name, chart_target_symbol, sector_etf_ticker)
        report_data["charts"]["risk_return"] = fig_to_dict(fig_rr)
    except Exception as e:
        print(f"Error generating risk-return for {ticker_display}: {e}")

    # 3. Performance Comparison Chart
    try:
        fig_perf = performance_comparison.generate_performance_chart_fig(chart_target_symbol, sector_etf_ticker)
        report_data["charts"]["performance"] = fig_to_dict(fig_perf)
    except Exception as e:
        print(f"Error generating performance comparison for {ticker_display}: {e}")

    # 4. Peers
    # Get metrics for all peers
    peer_metrics = df_metrics.select(["Symbol", "Daily_Change"])

    def get_peer_list(df_p):
        if df_p.is_empty(): return []
        # Join with metrics to get Daily_Change
        df_p_with_metrics = df_p.join(peer_metrics, left_on="Symbol_YF", right_on="Symbol", how="left")
        return df_p_with_metrics.select(["Symbol", "Symbol_YF", "Daily_Change"]).to_dicts()

    sub_peers = df_info.filter((pl.col("GICS Sub-Industry")==current_sub_industry) & (pl.col("Symbol_YF")!=chart_target_symbol))
    other_peers = df_info.filter((pl.col("GICS Sector")==current_sector) & (pl.col("GICS Sub-Industry")!=current_sub_industry) & (pl.col("Symbol_YF")!=chart_target_symbol))

    report_data["peers"] = {
        "sub_industry": get_peer_list(sub_peers),
        "sector": get_peer_list(other_peers)
    }

    # Write to JSON atomically
    output_path = os.path.join(output_dir, f"{chart_target_symbol}.json")
    temp_path = output_path + ".tmp"
    try:
        # Clean the entire report_data object before serialization
        cleaned_report_data = clean_plotly_data(report_data)
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(cleaned_report_data, f, ensure_ascii=False)
        os.replace(temp_path, output_path)
    except Exception as write_err:
        print(f"Error writing JSON for {ticker_display}: {write_err}")
        if os.path.exists(temp_path):
            os.remove(temp_path)

def export_json_reports(df_info, df_metrics, output_dir="../stock-blog/public/reports"):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, output_dir)
    if not os.path.exists(output_dir): os.makedirs(output_dir)

    print(f"\nJSONレポート生成開始: {output_dir}")
    
    # 全銘柄の直近の価格データをまとめて取得 (一括ダウンロード)
    symbols_list = df_info['Symbol_YF'].to_list()
    print(f"全 {len(symbols_list)} 銘柄の価格データを一括取得中...")
    try:
        # yf.download は内部でセッションを共有すると効率的
        import yfinance as yf
        from utils import get_session
        price_data = yf.download(symbols_list, period="5d", interval="1d", session=get_session(), group_by='ticker', progress=False)
        # 必要な指標（前日比など）を事前に計算して辞書に保持しておくと、個別のリクエストをスキップできる場合がある
    except Exception as e:
        print(f"一括データ取得エラー (スキップして続行します): {e}")

    # 取扱銘柄リストを取得 (キャッシュされるため高速)
    monex_symbols = market_data.get_monex_available_symbols()
    rakuten_symbols = market_data.get_rakuten_available_symbols()
    sbi_symbols = market_data.get_sbi_available_symbols()
    mufg_symbols = market_data.get_mufg_available_symbols()
    matsui_symbols = market_data.get_matsui_available_symbols()
    dmm_symbols = market_data.get_dmm_available_symbols()
    paypay_symbols = market_data.get_paypay_available_symbols()

    rows = df_info.to_dicts()
    # 制限を回避するため、デフォルトの並列度を 2 に抑える (環境変数で変更可能)
    max_workers = int(os.environ.get("PYTHON_MAX_WORKERS", 2)) 

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(generate_json_for_ticker, row, df_info, df_metrics, output_dir, monex_symbols, rakuten_symbols, sbi_symbols, mufg_symbols, matsui_symbols, dmm_symbols, paypay_symbols): row['Symbol'] for row in rows}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(rows)):
            try:
                future.result()
            except Exception as e:
                ticker = futures[future]
                print(f"Error processing {ticker}: {e}")

if __name__ == "__main__":
    print("Testing JSON generation for MSFT...")
    test_symbol = "MSFT"
    df_info = pl.DataFrame({
        "Symbol": [test_symbol],
        "Symbol_YF": [test_symbol],
        "Security": ["Microsoft Corp"],
        "GICS Sector": ["Information Technology"],
        "GICS Sub-Industry": ["Systems Software"],
        "Exchange": ["NASDAQ"]
    })
    df_metrics = risk_return.calculate_market_metrics_parallel([test_symbol])
    export_json_reports(df_info, df_metrics, output_dir="../stock-blog/public/reports")
