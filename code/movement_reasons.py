# -*- coding: utf-8 -*-
import os
import json
import time
from google import genai
from dotenv import load_dotenv
import yfinance as yf
import pandas as pd
import polars as pl

# .envの読み込み
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

# 2026年時点のモデル設定
# GEMINI_MODEL = "models/gemini-3.1-flash-lite-preview" # 一時的に制限中のため以下を使用
GEMINI_MODEL = "models/gemini-2.5-flash-lite"

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

def generate_styled_reason(client, symbol, stats, original_reason):
    """
    指定されたニューススタイルで株価変動理由を生成する
    """
    # 前日比の符号に応じた語句の選択
    is_up = stats['diff_pct'] >= 0
    up_down_word = "高" if is_up else "安"
    
    # ニュース風の演出用の一時的な価格
    intraday_price = stats.get('high', stats['close']) if is_up else stats.get('low', stats['close'])
    prev_close = stats['close'] / (1 + stats['diff_pct']) if stats['diff_pct'] != -1 else stats['close']
    intraday_diff = intraday_price - prev_close
    intraday_pct = intraday_diff / prev_close if prev_close != 0 else 0
    
    # 最新ニュースの補強
    recent_news = get_recent_news(symbol)
    
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
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config={
                    "tools": [{"google_search": {}}],
                    "system_instruction": "あなたは日経新聞やロイター通信のシニア編集者です。正確で客観的、かつ洞察に富んだ金融ニュース記事を執筆します。"
                }
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
    上昇・下落トップ10銘柄に対して理由を生成し、結果を辞書で返す
    """
    client = get_gemini_client()
    if not client:
        print("Skipping reason generation: Gemini API Key not found.")
        return {}

    # 上昇率・下落率でソート
    df_sorted = df_metrics.sort("Daily_Change", descending=True)
    top_gainers = df_sorted.head(10).to_dicts()
    top_losers = df_sorted.tail(10).sort("Daily_Change").to_dicts()
    
    results = {}
    today_str = time.strftime("%Y-%m-%d")
    today_ja = time.strftime("%m月%d日")

    # 処理対象をまとめる
    movers = []
    for i, row in enumerate(top_gainers):
        row['rank'] = i + 1
        row['type'] = 'gain'
        movers.append(row)
    for i, row in enumerate(top_losers):
        row['rank'] = i + 1
        row['type'] = 'loss'
        movers.append(row)

    print(f"Generating professional reasons for {len(movers)} top movers using {GEMINI_MODEL}...")
    
    for row in movers:
        symbol = row['Symbol']
        print(f"Processing {symbol} (Rank {row['rank']} {row['type']})...")
        
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="5d")
            if hist.empty: continue
            
            last_close = hist.iloc[-1]['Close']
            prev_close = hist.iloc[-2]['Close']
            high_val = hist.iloc[-1]['High']
            low_val = hist.iloc[-1]['Low']
            
            stats = {
                "date": today_str,
                "date_ja": today_ja,
                "close": last_close,
                "high": high_val,
                "low": low_val,
                "diff": last_close - prev_close,
                "diff_pct": row['Daily_Change'],
                "ytd_pct": row.get('Ret_YTD', 0.0),
                "rank": row['rank']
            }
            
            reason_text = generate_styled_reason(client, symbol, stats, "")
            
            if reason_text:
                results[symbol] = {
                    "date": today_str,
                    "change_pct": row['Daily_Change'],
                    "reason": reason_text
                }
                print(f"Successfully generated reason for {symbol}")
            
            # APIクォータ対策: 5秒待機
            time.sleep(5)
        except Exception as e:
            print(f"Failed to fetch detailed data for {symbol}: {e}")
            continue
            
    return results
