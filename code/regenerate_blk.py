# -*- coding: utf-8 -*-
import polars as pl
import report_generator
import risk_return
import os

# BLKのデータのみを定義
df_info = pl.DataFrame({
    "Symbol": ["BLK"],
    "Symbol_YF": ["BLK"],
    "Security": ["BlackRock, Inc."],
    "GICS Sector": ["Financials"],
    "GICS Sub-Industry": ["Asset Management & Custodial Banks"],
    "Exchange": ["NYSE"]
})

# リスク指標 (ダミー)
df_metrics = pl.DataFrame([
    {"Symbol": "BLK", "HV_250": 0.25, "Log_Return": 0.15},
    {"Symbol": "VFH", "HV_250": 0.20, "Log_Return": 0.10},
    {"Symbol": "^GSPC", "HV_250": 0.15, "Log_Return": 0.12}
])

output_dir = "output_reports_full"
print(f"Regenerating report for BLK into {output_dir}...")
report_generator.generate_report_for_ticker(df_info.to_dicts()[0], df_info, df_metrics, output_dir)
print("Done.")
