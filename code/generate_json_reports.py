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
import threading
# Suppress noisy yfinance errors
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

import fundamentals
import risk_return
import performance_comparison
import utils
import market_data
from utils import get_gemini_model

import time
import random

# Force Plotly to use standard JSON output
pio.json.config.default_engine = 'json'

# Initialize Gemini Client
from utils import get_gemini_client
gemini_client = get_gemini_client()
GEMINI_MODEL_NAME = "models/gemma-4-26b-a4b-it"

translation_cache = {}
initial_translation_counter = 0
translation_lock = threading.Lock()
MAX_INITIAL_TRANSLATIONS = 250

def translate_summary(symbol, summary):
    if not summary or not gemini_client:
        return None
    
    if symbol in translation_cache:
        return translation_cache[symbol]
        
    for attempt in range(2):
        try:
            # 原文に忠実な翻訳を指示するプロンプト
            prompt = f"以下の英文の会社概要を、内容を省略・補完することなく、原文に忠実かつ正確な日本語に翻訳してください。専門用語は日本の投資家が理解できる適切な用語を用い、自然な日本語の文章として整えてください。情報の追加や主観的な要約は行わないでください。\n\n{summary}"
            response = gemini_client.models.generate_content(
                model=GEMINI_MODEL_NAME,
                contents=prompt,
                config={
                    "system_instruction": "あなたはプロの翻訳者および証券アナリストです。提供されたテキストを、正確かつ忠実に日本語へ翻訳してください。"
                }
            )
            translation_cache[symbol] = response.text
            return response.text
        except Exception as e:
            if "429" in str(e):
                print(f"Rate limited for {symbol}, waiting longer...")
                time.sleep(30)
                continue
            print(f"Translation error for {symbol}: {e}")
            break
    return None

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

def generate_json_for_ticker(row, df_info, df_metrics, output_dir, force_translate=False, monex_symbols=None, rakuten_symbols=None, sbi_symbols=None, mufg_symbols=None, matsui_symbols=None, dmm_symbols=None, paypay_symbols=None, moomoo_symbols=None, iwaicosmo_symbols=None):
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
    is_available_moomoo = check_availability(ticker_display, moomoo_symbols)
    is_available_iwaicosmo = check_availability(ticker_display, iwaicosmo_symbols)
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
    
    # Financial sector flag for FCF warning
    is_financial = current_sector in ["Financials", "Real Estate"]

    # --- Fetch Info & Translate Summary ---
    ticker_obj = utils.get_ticker(chart_target_symbol)
    info = utils.safe_get(ticker_obj, 'info', default={})
    
    # Check if we already have a translation to save API tokens
    business_summary_ja = None
    output_path = os.path.join(output_dir, f"{chart_target_symbol}.json")
    if os.path.exists(output_path):
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                old_data = json.load(f)
                business_summary_ja = old_data.get("business_summary_ja")
                # Format existing summary just in case
                if business_summary_ja:
                    business_summary_ja = utils.format_summary(business_summary_ja)
        except: pass

    # If force_translate is True OR we don't have a translation yet, call Gemini
    if (not business_summary_ja or force_translate) and info.get("longBusinessSummary"):
        # Calculate safer delay based on max_workers to stay under 15 RPM
        # 60s / 15 requests = 4s per request. With N workers, we need 4s * N delay.
        max_workers = int(os.environ.get("PYTHON_MAX_WORKERS", 2))
        
        do_translate = False
        if force_translate:
            # Periodic rotation update
            print(f" [{ticker_display}] 定期ローテーションによる再翻訳を実行します...")
            time.sleep(2.0)
            do_translate = True
        elif not business_summary_ja:
            # Initial translation: ensure we are well under 15 RPM and limit to 100
            global initial_translation_counter
            with translation_lock:
                if initial_translation_counter < MAX_INITIAL_TRANSLATIONS:
                    initial_translation_counter += 1
                    do_translate = True
                else:
                    print(f" [{ticker_display}] 初回翻訳制限({MAX_INITIAL_TRANSLATIONS})に達したためスキップします。")
                    do_translate = False
            
            if do_translate:
                wait_time = 4.5 * max_workers
                print(f" [{ticker_display}] 初回翻訳を開始します ({initial_translation_counter}/{MAX_INITIAL_TRANSLATIONS}) (Wait: {wait_time}s)...")
                time.sleep(wait_time) 
            
        if do_translate:
            business_summary_ja = translate_summary(chart_target_symbol, info.get("longBusinessSummary"))
            if business_summary_ja:
                business_summary_ja = utils.format_summary(business_summary_ja)

    # Calculate DCF Valuation
    dcf_valuation = utils.calculate_dcf(chart_target_symbol, ticker=ticker_obj)

    # 1. Financial Data & Charts
    report_data = {
        "symbol": ticker_display,
        "symbol_yf": chart_target_symbol,
        "security": row['Security'],
        "security_ja": row.get('Security_JA'),
        "business_summary_ja": business_summary_ja,
        "dcf_valuation": dcf_valuation,
        "sector": current_sector,
        "sub_industry": current_sub_industry,
        "exchange": exchange,
        "full_symbol": full_symbol,
        "sector_etf": sector_etf_ticker,
        "is_financial": is_financial,
        "is_available_monex": is_available_monex,
        "is_available_rakuten": is_available_rakuten,
        "is_available_sbi": is_available_sbi,
        "is_available_mufg": is_available_mufg,
        "is_available_matsui": is_available_matsui,
        "is_available_dmm": is_available_dmm,
        "is_available_paypay": is_available_paypay,
        "is_available_moomoo": is_available_moomoo,
        "is_available_iwaicosmo": is_available_iwaicosmo,
        "charts": {}
    }

    try:
        fin_data = fundamentals.get_financial_data(ticker_obj)
        
        report_data["charts"]["bs"] = fig_to_dict(fundamentals.get_bs_chart_data(fin_data.get('bs', {})))
        report_data["charts"]["is"] = fig_to_dict(fundamentals.get_is_chart_data(fin_data.get('is', {})))
        report_data["charts"]["cf"] = fig_to_dict(fundamentals.get_cf_chart_data(fin_data.get('cf', {})))
        report_data["charts"]["tp"] = fig_to_dict(fundamentals.get_tp_chart_data(fin_data.get('tp', {})))
        report_data["charts"]["dps"] = fig_to_dict(fundamentals.get_dps_eps_chart_data(fin_data.get('dps', {}), fin_data.get('is', {})))
        report_data["charts"]["dps_history"] = fig_to_dict(fundamentals.get_dps_history_chart_data(fin_data.get('dps', {})))
        report_data["charts"]["segment"] = fig_to_dict(fundamentals.get_segment_chart_data(fin_data.get('segment', pl.DataFrame())))
        report_data["charts"]["geo"] = fig_to_dict(fundamentals.get_geo_chart_data(fin_data.get('geography', pl.DataFrame())))
        
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
                    # Sort by date descending
                    recent_ud = ud.sort_index(ascending=False).head(10).reset_index()
                    
                    # Fetch historical data to get prices at those dates
                    # We fetch 10 years of data to cover older ratings
                    hist_for_rating = ticker_obj.history(period="10y")
                    
                    # Ensure index is UTC and normalized for comparison
                    if hist_for_rating.index.tz is None:
                        hist_for_rating.index = hist_for_rating.index.tz_localize('UTC').normalize()
                    else:
                        hist_for_rating.index = hist_for_rating.index.tz_convert('UTC').normalize()
                    
                    def get_price_at_date(date_ts):
                        try:
                            # Ensure date_ts is UTC and normalized
                            if date_ts.tzinfo is None:
                                date_only = pd.Timestamp(date_ts).tz_localize('UTC').normalize()
                            else:
                                date_only = pd.Timestamp(date_ts).tz_convert('UTC').normalize()
                            
                            if date_only in hist_for_rating.index:
                                return float(hist_for_rating.loc[date_only]['Close'])
                            # If not found (e.g. weekend), find the closest previous business day
                            prev_dates = hist_for_rating.index[hist_for_rating.index <= date_only]
                            if not prev_dates.empty:
                                return float(hist_for_rating.loc[prev_dates[-1]]['Close'])
                        except:
                            pass
                        return None

                    # Add price at the time of rating
                    recent_ud['PriceAtRating'] = recent_ud['GradeDate'].apply(get_price_at_date)
                    
                    # Convert Timestamp to string for JSON
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

    # 5. Movement Reason (from df_metrics if exists)
    if "movement_reason" in df_metrics.columns:
        symbol_reason = df_metrics.filter(pl.col("Symbol") == chart_target_symbol).select("movement_reason").to_series().to_list()
        if symbol_reason and symbol_reason[0] is not None:
            report_data["movement_reason"] = symbol_reason[0]

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

    # Also update the master list used by Astro for page generation
    stocks_json_path = os.path.join(base_dir, "../stock-blog/src/data/stocks.json")
    
    # Merge df_info with df_metrics to have Daily_Change for the index page
    try:
        df_master = df_info.join(df_metrics.select(["Symbol", "Daily_Change"]), left_on="Symbol_YF", right_on="Symbol", how="left")
        # Ensure it's sorted by Symbol for a consistent UI
        df_master = df_master.sort("Symbol")
        
        with open(stocks_json_path, "w", encoding="utf-8") as f:
            json.dump(df_master.to_dicts(), f, ensure_ascii=False, indent=2)
        print(f"マスター銘柄リスト更新完了: {stocks_json_path}")
    except Exception as me:
        print(f"マスター銘柄リスト更新エラー: {me}")

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
    moomoo_symbols = market_data.get_moomoo_available_symbols()
    iwaicosmo_symbols = market_data.get_iwaicosmo_available_symbols()

    rows = df_info.to_dicts()
    # Ensure consistent order by sorting by Symbol
    rows = sorted(rows, key=lambda x: x['Symbol'])
    num_stocks = len(rows)
    
    # Calculate daily rotation for translation (365 days cycle)
    # Day of year (1 to 365)
    day_of_year = datetime.datetime.now().timetuple().tm_yday
    batch_size = max(1, num_stocks // 365 + (1 if num_stocks % 365 > 0 else 0))
    start_idx = ((day_of_year - 1) * batch_size) % num_stocks
    end_idx = min(start_idx + batch_size, num_stocks)
    
    # Identify which stocks are in today's rotation batch
    target_symbols = [rows[i]['Symbol'] for i in range(start_idx, end_idx)]
    print(f"\n--- 日次翻訳ローテーション ---")
    print(f"全銘柄数: {num_stocks}, 1日のノルマ: {batch_size}")
    print(f"本日の再翻訳対象 ({start_idx+1}-{end_idx}番目): {', '.join(target_symbols)}")

    # 制限を回避するため、デフォルトの並列度を 2 に抑える (環境変数で変更可能)
    max_workers = int(os.environ.get("PYTHON_MAX_WORKERS", 2))

    # Prefetch common data for performance charts
    common_etfs = ["XLC", "XLY", "XLP", "XLE", "XLF", "XLV", "XLI", "XLK", "XLB", "XLRE", "XLU", "SPY", "^GSPC"]
    performance_comparison.prefetch_common_data(common_etfs)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for i, row in enumerate(rows):
            # Check if this stock is in today's batch
            force_translate = (i >= start_idx and i < end_idx)
            futures[executor.submit(generate_json_for_ticker, row, df_info, df_metrics, output_dir, force_translate, monex_symbols, rakuten_symbols, sbi_symbols, mufg_symbols, matsui_symbols, dmm_symbols, paypay_symbols, moomoo_symbols, iwaicosmo_symbols)] = row['Symbol']
            
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(rows)):
            try:
                future.result()
            except Exception as e:
                ticker = futures[future]
                print(f"Error processing {ticker}: {e}")

if __name__ == "__main__":
    print("Testing JSON generation for all sectors (2 stocks per sector + MSFT)...")
    
    test_data = [
        # Communication Services
        {"Symbol": "GOOGL", "Symbol_YF": "GOOGL", "Security": "Alphabet Inc", "Security_JA": "アルファベット", "GICS Sector": "Communication Services", "GICS Sub-Industry": "Interactive Media", "Exchange": "NASDAQ"},
        {"Symbol": "META", "Symbol_YF": "META", "Security": "Meta Platforms Inc", "Security_JA": "メタ・プラットフォームズ", "GICS Sector": "Communication Services", "GICS Sub-Industry": "Interactive Media", "Exchange": "NASDAQ"},
        # Consumer Discretionary
        {"Symbol": "AMZN", "Symbol_YF": "AMZN", "Security": "Amazon.com Inc", "Security_JA": "アマゾン・ドット・コム", "GICS Sector": "Consumer Discretionary", "GICS Sub-Industry": "Broadline Retail", "Exchange": "NASDAQ"},
        {"Symbol": "TSLA", "Symbol_YF": "TSLA", "Security": "Tesla Inc", "Security_JA": "テスラ", "GICS Sector": "Consumer Discretionary", "GICS Sub-Industry": "Automobile Manufacturers", "Exchange": "NASDAQ"},
        # Consumer Staples
        {"Symbol": "PG", "Symbol_YF": "PG", "Security": "Procter & Gamble", "Security_JA": "プロクター・アンド・ギャンブル", "GICS Sector": "Consumer Staples", "GICS Sub-Industry": "Household Products", "Exchange": "NYSE"},
        {"Symbol": "KO", "Symbol_YF": "KO", "Security": "Coca-Cola Co", "Security_JA": "コカ・コーラ", "GICS Sector": "Consumer Staples", "GICS Sub-Industry": "Soft Drinks", "Exchange": "NYSE"},
        # Energy
        {"Symbol": "XOM", "Symbol_YF": "XOM", "Security": "Exxon Mobil Corp", "Security_JA": "エクソンモービル", "GICS Sector": "Energy", "GICS Sub-Industry": "Integrated Oil & Gas", "Exchange": "NYSE"},
        {"Symbol": "CVX", "Symbol_YF": "CVX", "Security": "Chevron Corp", "Security_JA": "シェブロン", "GICS Sector": "Energy", "GICS Sub-Industry": "Integrated Oil & Gas", "Exchange": "NYSE"},
        # Financials
        {"Symbol": "JPM", "Symbol_YF": "JPM", "Security": "JPMorgan Chase & Co", "Security_JA": "JPモルガン・チェース", "GICS Sector": "Financials", "GICS Sub-Industry": "Diversified Banks", "Exchange": "NYSE"},
        {"Symbol": "V", "Symbol_YF": "V", "Security": "Visa Inc", "Security_JA": "ビザ", "GICS Sector": "Financials", "GICS Sub-Industry": "Transaction & Payment Processing Services", "Exchange": "NYSE"},
        # Health Care
        {"Symbol": "JNJ", "Symbol_YF": "JNJ", "Security": "Johnson & Johnson", "Security_JA": "ジョンソン・エンド・ジョンソン", "GICS Sector": "Health Care", "GICS Sub-Industry": "Pharmaceuticals", "Exchange": "NYSE"},
        {"Symbol": "LLY", "Symbol_YF": "LLY", "Security": "Eli Lilly & Co", "Security_JA": "イーライリリー", "GICS Sector": "Health Care", "GICS Sub-Industry": "Pharmaceuticals", "Exchange": "NYSE"},
        # Industrials
        {"Symbol": "CAT", "Symbol_YF": "CAT", "Security": "Caterpillar Inc", "Security_JA": "キャタピラー", "GICS Sector": "Industrials", "GICS Sub-Industry": "Construction Machinery & Heavy Transportation Equipment", "Exchange": "NYSE"},
        {"Symbol": "HON", "Symbol_YF": "HON", "Security": "Honeywell International", "Security_JA": "ハネウェル・インターナショナル", "GICS Sector": "Industrials", "GICS Sub-Industry": "Industrial Conglomerates", "Exchange": "NASDAQ"},
        # Information Technology
        {"Symbol": "MSFT", "Symbol_YF": "MSFT", "Security": "Microsoft Corp", "Security_JA": "マイクロソフト", "GICS Sector": "Information Technology", "GICS Sub-Industry": "Systems Software", "Exchange": "NASDAQ"},
        {"Symbol": "AAPL", "Symbol_YF": "AAPL", "Security": "Apple Inc", "Security_JA": "アップル", "GICS Sector": "Information Technology", "GICS Sub-Industry": "Technology Hardware Storage & Peripherals", "Exchange": "NASDAQ"},
        # Materials
        {"Symbol": "LIN", "Symbol_YF": "LIN", "Security": "Linde plc", "Security_JA": "リンデ", "GICS Sector": "Materials", "GICS Sub-Industry": "Industrial Gases", "Exchange": "NYSE"},
        {"Symbol": "SHW", "Symbol_YF": "SHW", "Security": "Sherwin-Williams Co", "Security_JA": "シャーウィン・ウィリアムズ", "GICS Sector": "Materials", "GICS Sub-Industry": "Specialty Chemicals", "Exchange": "NYSE"},
        # Real Estate
        {"Symbol": "PLD", "Symbol_YF": "PLD", "Security": "Prologis Inc", "Security_JA": "プロロジス", "GICS Sector": "Real Estate", "GICS Sub-Industry": "Industrial REITs", "Exchange": "NYSE"},
        {"Symbol": "AMT", "Symbol_YF": "AMT", "Security": "American Tower Corp", "Security_JA": "アメリカン・タワー", "GICS Sector": "Real Estate", "GICS Sub-Industry": "Telecom Tower REITs", "Exchange": "NYSE"},
        # Utilities
        {"Symbol": "NEE", "Symbol_YF": "NEE", "Security": "NextEra Energy", "Security_JA": "ネクステラ・エナジー", "GICS Sector": "Utilities", "GICS Sub-Industry": "Electric Utilities", "Exchange": "NYSE"},
        {"Symbol": "SO", "Symbol_YF": "SO", "Security": "Southern Co", "Security_JA": "サザンカンパニー", "GICS Sector": "Utilities", "GICS Sub-Industry": "Electric Utilities", "Exchange": "NYSE"}
    ]
    
    df_info = pl.DataFrame(test_data)
    test_symbols = df_info["Symbol_YF"].to_list()
    
    df_metrics = risk_return.calculate_market_metrics_parallel(test_symbols)
    export_json_reports(df_info, df_metrics, output_dir="../stock-blog/public/reports")
