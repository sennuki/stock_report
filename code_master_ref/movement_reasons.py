# -*- coding: utf-8 -*-
import os
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from google import genai
from dotenv import load_dotenv
import yfinance as yf
import pandas as pd
import polars as pl

from google.genai import types
import utils

# .envの読み込み
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

# 2026年時点のモデル設定
GEMINI_MODEL = "gemini-3.1-flash-lite-preview"

def get_gemini_client():
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return None
    return genai.Client(api_key=api_key)

def get_recent_news(symbol):
    """
    yfinanceから銘柄に関連する最新ニュースを取得する
    """
    try:
        ticker = yf.Ticker(symbol)
        news = ticker.news
        if not news:
            return ""
        news_summary = "\n".join([f"- {n.get('title')}" for n in news[:3]])
        return news_summary
    except:
        return ""

def generate_styled_reason(client, symbol, stats, original_reason, recent_news=""):
    """
    指定されたニューススタイルで株価変動理由を生成する。
    recent_news は呼び出し元で取得済みのものを渡す（yfinance呼び出しを含まない）。
    """
    is_up = stats['diff_pct'] >= 0
    up_down_word = "高" if is_up else "安"

    intraday_price = stats.get('high', stats['close']) if is_up else stats.get('low', stats['close'])
    prev_close = stats['close'] / (1 + stats['diff_pct']) if stats['diff_pct'] != -1 else stats['close']
    intraday_diff = intraday_price - prev_close
    intraday_pct = intraday_diff / prev_close if prev_close != 0 else 0
    
    prompt = f"""
以下の銘柄情報と背景理由を元に、プロの証券アナリストが執筆する金融ニュース記事のようなスタイルで文章を作成してください。
必要に応じて、最新の市場動向を検索して補完してください。

【銘柄】: {symbol}
【日付】: {stats['date']}
【前日比】: {stats['diff']:.2f}ドル ({stats['diff_pct']:.2%})
【終値】: {stats['close']:.2f}ドル
【一時的な株価】: {intraday_price:.2f}ドル (前日比 {intraday_diff:.2f}ドル{up_down_word} / {intraday_pct:.2%})
【年初来騰落率】: {stats['ytd_pct']:.2%}
【主な背景理由】: {original_reason}
【関連ニュース】: 
{recent_news}

【構成案】
1. 一行目に「年初来・株価騰落率：[+0.00]％」と記載。 (※{stats['ytd_pct']:.2%})
2. 本文は「{stats['date_ja']}の取引で、[会社概要や業界での立ち位置]の[社名]が大幅に[上昇/下落]。...」と開始し、背景理由を詳しく、プロフェッショナルな日本語で説明。
3. 最後に「株価は一時、前日比[0.00]ドル[高/安]([0.00]％)の[0.00]ドルまで[上昇/下落]し、[0.00]ドル[高/安]([0.00]％)の[0.00]ドルで終了。S&P500の[上昇/下落]率{stats['rank']}位にランクインし、年初来では[0.00]％[高/安]となった。」という形式で締める。

※必ず指定のスタイルを守り、事実に基づいた格調高い文章にしてください。改行（\n）を適切に使用して読みやすくしてください。
"""

    max_retries = 3
    base_delay = 10 

    for attempt in range(max_retries):
        try:
            # 最新の SDK 形式に合わせた呼び出し
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    system_instruction="あなたは日経新聞やロイター通信のシニア編集者です。正確で客観的、かつ洞察に富んだ金融ニュース記事を執筆します。",
                    thinking_config=types.ThinkingConfig(
                        thinking_level="MINIMAL",
                    ),
                )
            )
            return response.text.strip()
        except Exception as e:
            error_msg = str(e)
            if ("429" in error_msg or "503" in error_msg) and attempt < max_retries - 1:
                delay = base_delay * (attempt + 1)
                print(f"Server error for {symbol} ({error_msg[:10]}...). Retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(delay)
                continue
            print(f"Error generating reason for {symbol}: {e}")
            return None

def process_top_movers(df_metrics):
    """
    上昇・下落トップ10銘柄に対して理由を生成し、結果を辞書で返す。

    Phase 1 (逐次): yfinance でデータ取得 → rate limit 対策で 2 秒待機
    Phase 2 (並列): Gemini API 呼び出しのみを ThreadPoolExecutor で並列化
    """
    client = get_gemini_client()
    if not client:
        print("Skipping reason generation: Gemini API Key not found.")
        return {}

    df_sorted = df_metrics.sort("Daily_Change", descending=True)
    top_gainers = df_sorted.head(10).to_dicts()
    top_losers = df_sorted.tail(10).sort("Daily_Change").to_dicts()

    today_str = time.strftime("%Y-%m-%d")
    today_ja = time.strftime("%m月%d日")

    movers = []
    for i, row in enumerate(top_gainers):
        row['rank'] = i + 1; row['type'] = 'gain'; movers.append(row)
    for i, row in enumerate(top_losers):
        row['rank'] = i + 1; row['type'] = 'loss'; movers.append(row)

    # --- Phase 1: yfinance データ収集（逐次・2秒スリープ） ---
    print(f"Phase 1: Collecting market data for {len(movers)} movers (sequential)...")
    movers_data = []  # list of (row, stats, recent_news)
    for row in movers:
        symbol = row['Symbol']
        print(f"  [{symbol}] fetching price + news...")
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="5d")
            if hist.empty:
                continue

            last_close = hist.iloc[-1]['Close']
            prev_close = hist.iloc[-2]['Close']
            stats = {
                "date": today_str,
                "date_ja": today_ja,
                "close": last_close,
                "high": hist.iloc[-1]['High'],
                "low": hist.iloc[-1]['Low'],
                "diff": last_close - prev_close,
                "diff_pct": row['Daily_Change'],
                "ytd_pct": row.get('Ret_YTD', 0.0),
                "rank": row['rank']
            }
            recent_news = get_recent_news(symbol)
            movers_data.append((row, stats, recent_news))
            time.sleep(2)
        except Exception as e:
            print(f"  [{symbol}] data fetch failed: {e}")
            continue

    # --- Phase 2: Gemini 呼び出し（並列・yfinance なし） ---
    print(f"Phase 2: Generating AI analysis for {len(movers_data)} movers (parallel, max_workers=3)...")
    results = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_map = {
            executor.submit(generate_styled_reason, client, row['Symbol'], stats, "", recent_news): (row, stats)
            for row, stats, recent_news in movers_data
        }
        for future in as_completed(future_map):
            row, _ = future_map[future]
            symbol = row['Symbol']
            try:
                reason_text = future.result()
                if reason_text:
                    results[symbol] = {
                        "date": today_str,
                        "change_pct": row['Daily_Change'],
                        "reason": reason_text
                    }
                    print(f"  [{symbol}] done")
            except Exception as e:
                print(f"  [{symbol}] generation failed: {e}")

    return results
