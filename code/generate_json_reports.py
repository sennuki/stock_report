# -*- coding: utf-8 -*-
import concurrent.futures
import os
import json
import polars as pl
import numpy as np
import base64
from tqdm import tqdm
import plotly.io as pio

import fundamentals
import risk_return
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
    elif isinstance(obj, np.generic):
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

def generate_json_for_ticker(row, df_info, df_metrics, output_dir):
    # Add a small random delay to mimic human behavior and avoid rate limits
    time.sleep(random.uniform(0.5, 1.5))
    
    ticker_display = row['Symbol']
    chart_target_symbol = row['Symbol_YF']
    current_sector = row['GICS Sector']
    current_sub_industry = row['GICS Sub-Industry']
    exchange = row['Exchange']
    
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
        "charts": {}
    }

    try:
        ticker_obj = utils.get_ticker(chart_target_symbol)
        fin_data = fundamentals.get_financial_data(ticker_obj)
        
        report_data["charts"]["bs"] = fig_to_dict(fundamentals.get_bs_plotly_fig(fin_data.get('bs', {})))
        report_data["charts"]["is"] = fig_to_dict(fundamentals.get_is_plotly_fig(fin_data.get('is', {})))
        report_data["charts"]["cf"] = fig_to_dict(fundamentals.get_cf_plotly_fig(fin_data.get('cf', {})))
        report_data["charts"]["tp"] = fig_to_dict(fundamentals.get_tp_plotly_fig(fin_data.get('tp', {})))
        report_data["charts"]["dps"] = fig_to_dict(fundamentals.get_dps_eps_plotly_fig(fin_data.get('dps', {}), fin_data.get('is', {})))
        
        # --- Add Valuation Data ---
        if "valuation" in fin_data:
            report_data["valuation"] = fin_data["valuation"]
            report_data["charts"]["pe_valuation"] = fig_to_dict(fundamentals.get_valuation_plotly_fig(fin_data["valuation"]))
        # ----------------------------

        # --- Add Analyst Ratings ---
        try:
            recs = ticker_obj.recommendations_summary
            if recs is not None and not recs.empty:
                # Use current month (period '0m')
                current_recs = recs[recs['period'] == '0m']
                if not current_recs.empty:
                    report_data["analyst_ratings"] = current_recs.to_dict('records')[0]
        except Exception as ree:
            print(f"Error fetching recommendations for {ticker_display}: {ree}")
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
        fig_rr = risk_return.generate_scatter_fig(df_metrics, chart_target_symbol, sector_etf_ticker)
        report_data["charts"]["risk_return"] = fig_to_dict(fig_rr)
    except Exception as e:
        print(f"Error generating risk-return for {ticker_display}: {e}")

    # 3. Peers
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
    rows = df_info.to_dicts()
    max_workers = 1 
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(generate_json_for_ticker, row, df_info, df_metrics, output_dir): row['Symbol'] for row in rows}
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
