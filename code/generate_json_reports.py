# -*- coding: utf-8 -*-
import concurrent.futures
import os
import json
import polars as pl
import numpy as np
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

def generate_json_for_ticker(row, df_info, df_metrics, output_dir, monex_symbols=None, rakuten_symbols=None, sbi_symbols=None):
    # Add a small random delay to mimic human behavior and avoid rate limits
    time.sleep(random.uniform(0.5, 1.5))
    
    ticker_display = row['Symbol']
    chart_target_symbol = row['Symbol_YF']
    current_sector = row['GICS Sector']
    current_sub_industry = row['GICS Sub-Industry']
    exchange = row['Exchange']
    
    # Check availability
    is_available_monex = ticker_display in monex_symbols if monex_symbols else False
    is_available_rakuten = ticker_display in rakuten_symbols if rakuten_symbols else False
    is_available_sbi = ticker_display in sbi_symbols if sbi_symbols else False

    # TradingView symbol
    tv_ticker = ticker_display.replace("-", ".")
    full_symbol = f"{exchange}:{tv_ticker}"
    
    # Sector ETF
    sector_map = {
        "Communication Services": "VOX", "Consumer Discretionary": "VCR",
        "Consumer Staples": "VDC", "Energy": "VDE", "Financials": "VFH",
        "Health Care": "VHT", "Industrials": "VIS", "Information Technology": "VGT",
        "Materials": "VAW", "Real Estate": "VNQ", "Utilities": "VPU"
    }
    sector_etf_ticker = sector_map.get(current_sector, "VOO")

    # 1. Financial Data & Charts
    report_data = {
        "symbol": ticker_display,
        "symbol_yf": chart_target_symbol,
        "security": row['Security'],
        "sector": current_sector,
        "sub_industry": current_sub_industry,
        "exchange": exchange,
        "full_symbol": full_symbol,
        "sector_etf": sector_etf_ticker,
        "is_available_monex": is_available_monex,
        "is_available_rakuten": is_available_rakuten,
        "is_available_sbi": is_available_sbi,
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
        try:
            ed = ticker_obj.earnings_dates
            if ed is not None and not ed.empty:
                # Latest reported
                valid_ed = ed[ed['Reported EPS'].notnull()].sort_index(ascending=False)
                if not valid_ed.empty:
                    latest = valid_ed.iloc[0]
                    report_data["earnings_surprise"] = {
                        "date": valid_ed.index[0].strftime('%Y-%m-%d'),
                        "actual": float(latest['Reported EPS']),
                        "estimate": float(latest['EPS Estimate']) if not np.isnan(latest['EPS Estimate']) else None,
                        "surprise_pct": float(latest['Surprise(%)']) if not np.isnan(latest['Surprise(%)']) else None
                    }
                
                # Next earnings (where Reported EPS is NaN)
                next_ed = ed[ed['Reported EPS'].isnull()].sort_index(ascending=True)
                if not next_ed.empty:
                    next_item = next_ed.iloc[0]
                    report_data["next_earnings"] = {
                        "date": next_ed.index[0].strftime('%Y-%m-%d'),
                        "estimate": float(next_item['EPS Estimate']) if not np.isnan(next_item['EPS Estimate']) else None
                    }
        except Exception as es_err:
            print(f"Error fetching earnings surprise for {ticker_display}: {es_err}")
        # ----------------------------

        # --- Add Consensus Data ---
        try:
            def df_to_dict_safe(df):
                if df is None or df.empty: return None
                return df.replace({np.nan: None}).to_dict('index')

            report_data["consensus"] = {
                "earnings": df_to_dict_safe(ticker_obj.earnings_estimate),
                "revenue": df_to_dict_safe(ticker_obj.revenue_estimate),
                "eps_trend": df_to_dict_safe(ticker_obj.eps_trend),
                "eps_revisions": df_to_dict_safe(ticker_obj.eps_revisions)
            }
        except Exception as cons_err:
            print(f"Error fetching consensus for {ticker_display}: {cons_err}")
        # ----------------------------

        # --- Add Highlights ---
        try:
            info = ticker_obj.info
            
            def normalize_ratio(val):
                if val is None: return None
                # If value is > 1.0 (like 0.89 being 0.89%), it might be percentage.
                # But dividend yield can be very small (0.0089).
                # Actually, many yfinance fields are inconsistent.
                # For MSFT: dividendYield=0.89 (%), payoutRatio=0.21 (ratio).
                # It seems dividendYield is often percentage while growth is ratio.
                return val
                
            report_data["highlights"] = {
                "revenue_growth": info.get("revenueGrowth"),
                "earnings_growth": info.get("earningsGrowth"),
                "profit_margins": info.get("profitMargins"),
                "operating_margins": info.get("operatingMargins"),
                "roe": info.get("returnOnEquity"),
                "roa": info.get("returnOnAssets"),
                "eps_ttm": info.get("trailingEps"),
                "eps_forward": info.get("forwardEps"),
                "pe_ttm": info.get("trailingPE"),
                "pe_forward": info.get("forwardPE"),
                "dividend_yield": info.get("dividendYield") / 100 if info.get("dividendYield") is not None and info.get("dividendYield") > 0.05 else info.get("dividendYield"),
                "payout_ratio": info.get("payoutRatio"),
                "debt_to_equity": info.get("debtToEquity"),
                "current_ratio": info.get("currentRatio")
            }
        except Exception as h_err:
            print(f"Error fetching highlights for {ticker_display}: {h_err}")
        # ----------------------------

        # --- Add Analyst Ratings ---
        try:
            # yfinance 1.1.0+ may output 404 or other errors for some symbols
            recs = getattr(ticker_obj, 'recommendations_summary', None)
            analyst_data = {}
            if recs is not None and not recs.empty:
                # Use current month (period '0m')
                current_recs = recs[recs['period'] == '0m']
                if not current_recs.empty:
                    analyst_data = current_recs.to_dict('records')[0]
            
            # Add target prices from info
            info = ticker_obj.info
            target_keys = [
                'targetHighPrice', 'targetLowPrice', 'targetMeanPrice', 
                'targetMedianPrice', 'currentPrice', 'numberOfAnalystOpinions'
            ]
            for key in target_keys:
                if key in info:
                    analyst_data[key] = info[key]
            
            # Add recent rating changes (upgrades/downgrades)
            try:
                ud = ticker_obj.upgrades_downgrades
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

    # Write to JSON
    output_path = os.path.join(output_dir, f"{chart_target_symbol}.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report_data, f, ensure_ascii=False)

def export_json_reports(df_info, df_metrics, output_dir="../stock-blog/public/reports"):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, output_dir)
    if not os.path.exists(output_dir): os.makedirs(output_dir)

    print(f"\nJSONレポート生成開始: {output_dir}")
    
    # 取扱銘柄リストを取得
    monex_symbols = market_data.get_monex_available_symbols()
    print(f"マネックス証券 取扱銘柄数: {len(monex_symbols)}")
    
    rakuten_symbols = market_data.get_rakuten_available_symbols()
    print(f"楽天証券 取扱銘柄数: {len(rakuten_symbols)}")
    
    sbi_symbols = market_data.get_sbi_available_symbols()
    print(f"SBI証券 取扱銘柄数: {len(sbi_symbols)}")

    rows = df_info.to_dicts()
    max_workers = 1 
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(generate_json_for_ticker, row, df_info, df_metrics, output_dir, monex_symbols, rakuten_symbols, sbi_symbols): row['Symbol'] for row in rows}
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
