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

def upload_base_stocks_list_to_r2(df_sp500):
    """
    S&P500の基本銘柄リストをR2の raw/stocks_list.json としてアップロードする
    """
    try:
        data = df_sp500.to_dicts()
        json_data = json.dumps(data, ensure_ascii=False, indent=2)
        
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
    
    # fetch_raw_data.py の main を実行して、全銘柄データをR2に保存
    try:
        fetch_raw_data.main()
        utils.log_event("SUCCESS", "SYSTEM", "Fetched all raw data and uploaded to R2")
    except Exception as e:
        utils.log_event("ERROR", "SYSTEM", f"Failed during fetch_raw_data.main(): {e}")
        
    # ベース銘柄リストのアップロード (TypeScript側で一覧表示等に利用するため)
    try:
        df_sp500 = market_data.fetch_sp500_companies_optimized()
        if not df_sp500.is_empty():
            upload_base_stocks_list_to_r2(df_sp500)
    except Exception as e:
        pass
    
    utils.log_event("INFO", "SYSTEM", "--- Python Data Fetch Pipeline Finished ---")
