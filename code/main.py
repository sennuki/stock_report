# -*- coding: utf-8 -*-
import os
import json
import logging
import warnings
import market_data
import fetch_raw_data
import utils
import boto3
from dotenv import load_dotenv

logging.getLogger('yfinance').setLevel(logging.CRITICAL)
warnings.filterwarnings("ignore", message=".*Timestamp.utcnow is deprecated.*")

load_dotenv()

# R2 接続設定
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "stock-data-c1")

s3_client = None
if R2_ACCOUNT_ID and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY:
    s3_client = boto3.client(
        's3',
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto"
    )

import math

def sanitize_json(obj):
    """
    Recursively convert NaN, Infinity, -Infinity to None (null in JSON).
    """
    if isinstance(obj, dict):
        return {k: sanitize_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_json(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    return obj

def export_stocks_json(df):
    """S&P 500/400/600 の銘柄リストを Astro 用の JSON データとして保存する"""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    dest_path = os.path.join(base_dir, "../stock-blog/src/data/stocks.json")
    try:
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        data = df.to_dicts()
        sanitized_data = sanitize_json(data)
        with open(dest_path, 'w', encoding='utf-8') as f:
            json.dump(sanitized_data, f, ensure_ascii=False, indent=2)
        print(f"銘柄リストJSONを保存しました: {dest_path}")
    except Exception as e:
        print(f"JSON保存エラー: {e}")

def upload_base_stocks_list_to_r2(df_stocks):
    """
    S&P 500/400/600 の基本銘柄リストを R2 の raw/stocks_list.json としてアップロードする
    """
    try:
        data = df_stocks.to_dicts()
        sanitized_data = sanitize_json(data)
        json_data = json.dumps(sanitized_data, ensure_ascii=False, indent=2)

        if s3_client:
            s3_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key="raw/stocks_list.json",
                Body=json_data.encode('utf-8'),
                ContentType='application/json'
            )
            utils.log_event("SUCCESS", "SYSTEM", "Uploaded base stocks_list.json to R2")
        else:
            print("R2 is not configured. Skipping base stocks list upload.")
    except Exception as e:
        utils.log_event("ERROR", "SYSTEM", f"Failed to upload stocks list to R2: {e}")

if __name__ == "__main__":
    utils.log_event("INFO", "SYSTEM", "--- Python Data Fetch Pipeline Started ---")

    # TEST_MODE が true の場合は少数銘柄に限定。
    # TEST_SYMBOLS 環境変数があればそのリストを使う (セクター ETF は
    # fetch_raw_data 側で自動付与される)。
    # 例外: TEST_SYMBOLS="all" の場合は本番と同じ全 S&P 500/400/600 を取得
    #   (c1 ブランチで E2E 検証を回したいときに使う)。
    if os.getenv("TEST_MODE") == "true":
        test_symbols_env = os.getenv("TEST_SYMBOLS", "").strip()
        if test_symbols_env.lower() != "all":
            if test_symbols_env:
                test_symbols = [s.strip().upper() for s in test_symbols_env.split(",") if s.strip()]
            else:
                test_symbols = ["MSFT", "AAPL", "NVDA", "SPY"]
            utils.log_event("INFO", "SYSTEM", f"Test mode active: processing {test_symbols}.")
            fetch_raw_data.main(symbols_override=test_symbols)
            utils.log_event("SUCCESS", "SYSTEM", "Test mode fetch finished.")
            exit(0)
        # TEST_SYMBOLS=all のときは下の「本番フル取得」経路に流す。
        utils.log_event("INFO", "SYSTEM", "TEST_MODE=true with TEST_SYMBOLS=all: running full S&P 500/400/600 fetch.")

    # 1. ベース銘柄リストの取得（S&P 500 / 400 / 600 を統合）
    #    先に取得しておき、stocks.json と raw データ取得の両方に使い回す
    df_stocks = None
    symbols = None
    try:
        df_stocks = market_data.fetch_sp_indices_companies()
        if df_stocks is not None and not df_stocks.is_empty():
            from collections import Counter
            cnt = Counter(df_stocks['Index'].to_list())
            utils.log_event("SUCCESS", "SYSTEM",
                            f"Fetched {len(df_stocks)} stocks from Wikipedia: {dict(cnt)}")
            # stocks.json にエクスポート（TypeScript側で読み込む）
            export_stocks_json(df_stocks)
            upload_base_stocks_list_to_r2(df_stocks)
            symbols = df_stocks['Symbol_YF'].to_list()
        else:
            utils.log_event("WARNING", "SYSTEM",
                            "fetch_sp_indices_companies returned empty - falling back to default behavior")
    except Exception as e:
        utils.log_event("ERROR", "SYSTEM", f"Failed to fetch base stocks list: {e}")

    # 2. fetch_raw_data: 取得した銘柄リストを引き継いで生データを取得 → R2 にアップ
    try:
        fetch_raw_data.main(symbols_override=symbols)
        utils.log_event("SUCCESS", "SYSTEM", "Fetched all raw data and uploaded to R2")
    except Exception as e:
        utils.log_event("ERROR", "SYSTEM", f"Failed during fetch_raw_data.main(): {e}")

    utils.log_event("INFO", "SYSTEM", "--- Python Data Fetch Pipeline Finished ---")
