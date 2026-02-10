# -*- coding: utf-8 -*-
import polars as pl
import report_generator
import risk_return
import os

df_info = pl.DataFrame({
    "Symbol": ["SPGI"],
    "Symbol_YF": ["SPGI"],
    "Security": ["S&P Global Inc."],
    "GICS Sector": ["Financials"],
    "GICS Sub-Industry": ["Financial Exchanges & Data"],
    "Exchange": ["NYSE"]
})

df_metrics = pl.DataFrame([
    {"Symbol": "SPGI", "HV_250": 0.25, "Log_Return": 0.15},
    {"Symbol": "VFH", "HV_250": 0.20, "Log_Return": 0.10},
    {"Symbol": "^GSPC", "HV_250": 0.15, "Log_Return": 0.12}
])

output_dir = "output_reports_full"
print(f"Regenerating report for SPGI into {output_dir}...")
report_generator.generate_report_for_ticker(df_info.to_dicts()[0], df_info, df_metrics, output_dir)
print("Done.")
