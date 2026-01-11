# -*- coding: utf-8 -*-
import concurrent.futures
import os

import polars as pl
import yfinance as yf
from tqdm import tqdm

import fundamentals
import market_data  # For type checking or if we want to run full generation here
import risk_return

# ==========================================
#  Part C (後半): レポート生成
# ==========================================

sector_map = {
    "Communication Services": "VOX",
    "Consumer Discretionary": "VCR",
    "Consumer Staples": "VDC",
    "Energy": "VDE",
    "Financials": "VFH",
    "Health Care": "VHT",
    "Industrials": "VIS",
    "Information Technology": "VGT",
    "Materials": "VAW",
    "Real Estate": "VNQ",
    "Utilities": "VPU"
}

TEMPLATE = """<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>銘柄分析レポート: {ticker} ({security})</title>
    <script type="module" src="https://widgets.tradingview-widget.com/w/ja/tv-ticker-tag.js"></script>
    <style>
        body {{ font-family: sans-serif; margin: 20px; }}
        h1 {{ font-size: 24px; }}
        h2 {{ font-size: 20px; border-bottom: 1px solid #ddd; padding-bottom: 5px; margin-top: 30px; }}
        hr {{ border: none; border-top: 1px solid #eee; margin: 20px 0; }}
    </style>
</head>
<body>

<h1>銘柄分析レポート: {ticker} ({security})</h1>

<div class="tradingview-widget-container"><div class="tradingview-widget-container__widget"></div><script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-symbol-info.js" async>{{ "symbol": "{full_symbol}", "colorTheme": "light", "isTransparent": false, "locale": "ja", "width": "100%" }}</script></div>

<hr>
<h2>🎯 リスク・リターン分析</h2>
<p>🔴 <strong>{ticker}</strong> (対象) vs 🔷 <strong>{sector_etf_ticker}</strong> (セクター) vs ★ <strong>S&P 500</strong></p>
{volatility_chart_html}

<hr>
<h2>🏢 同業種・競合 ({sub_industry})</h2>
<div style="display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 20px;">{sub_industry_peers_html}</div>

<h2>🏭 同セクター他社 ({sector_name})</h2>
<details><summary>クリックして展開</summary><div style="display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; margin-bottom: 20px;">{sector_other_peers_html}</div></details>

<hr>
<h2>📈 パフォーマンス比較</h2>
<div class="tradingview-widget-container"><div class="tradingview-widget-container__widget"></div><script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js" async>{{ "allow_symbol_change": false, "interval": "D", "width": "100%", "height": 500, "symbol": "{full_symbol}", "theme": "light", "style": "2", "locale": "ja", "withdateranges": true, "hide_volume": true, "compareSymbols": [ {{ "symbol": "{sector_etf_tv}", "position": "SameScale" }}, {{ "symbol": "FRED:SP500", "position": "SameScale" }} ] }}</script></div>

<hr>
<h2>📊 ファンダメンタルズ分析</h2>

<div class="tradingview-widget-container">
  <div class="tradingview-widget-container__widget"></div>
  <div class="tradingview-widget-copyright"><a href="https://jp.tradingview.com/symbols/NASDAQ-AAPL/financials-overview/" rel="noopener nofollow" target="_blank"><span class="blue-text">Track all markets on TradingView</span></a></div>
  <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-financials.js" async>
  {{
  "symbol": "{full_symbol}",
  "colorTheme": "light",
  "displayMode": "regular",
  "isTransparent": false,
  "locale": "ja",
  "width": "100%",
  "height": 950
}}
  </script>
</div>

<div style="display: flex; flex-direction: column; gap: 40px;">
  <div>{chart_bs}</div>
  <div>{chart_is}</div>
  <div>{chart_cf}</div>
  <div>{chart_tp}</div>
</div>

</body>
</html>
"""

def generate_report_for_ticker(row, df_info, df_metrics, output_dir):
    current_ticker_raw = row['Symbol']
    ticker_display = current_ticker_raw.replace("-", ".")
    chart_target_symbol = row['Symbol_YF']
    current_sector = row['GICS Sector']
    current_sub_industry = row['GICS Sub-Industry']
    exchange = row['Exchange']
    full_symbol = f"{exchange}:{ticker_display}"
    sector_etf_ticker = sector_map.get(current_sector, "VOO")
    sector_etf_tv = f"AMEX:{sector_etf_ticker}"

    # 1. 財務チャート生成
    try:
        fin_data = fundamentals.get_financial_data(yf.Ticker(chart_target_symbol))
        chart_bs = fundamentals.get_bs_plotly_html(fin_data['bs'])
        chart_is = fundamentals.get_is_plotly_html(fin_data['is'])
        chart_cf = fundamentals.get_cf_plotly_html(fin_data['cf'])
        chart_tp = fundamentals.get_tp_plotly_html(fin_data['tp'])
    except Exception as e:
        # print(f"Error generating charts for {ticker_display}: {e}")
        chart_bs = chart_is = chart_cf = chart_tp = "<p>データ取得エラー</p>"

    # 2. リスクリターンチャート生成
    volatility_chart_html = risk_return.generate_scatter_html(df_metrics, chart_target_symbol, sector_etf_ticker)

    # 3. タグ生成
    def create_tags(target_df):
        tags = [f'<tv-ticker-tag symbol="{item["Exchange"]}:{item["Symbol"].replace("-", ".")}"></tv-ticker-tag>' for item in target_df.to_dicts()]
        return "\n".join(tags) if tags else "なし"

    sub_peers = df_info.filter((pl.col("GICS Sub-Industry")==current_sub_industry) & (pl.col("Symbol")!=current_ticker_raw))
    other_peers = df_info.filter((pl.col("GICS Sector")==current_sector) & (pl.col("GICS Sub-Industry")!=current_sub_industry) & (pl.col("Symbol")!=current_ticker_raw))

    # 4. 書き出し
    content = TEMPLATE.format(
        ticker=ticker_display, security=row['Security'], full_symbol=full_symbol,
        sector_etf_tv=sector_etf_tv, sector_etf_ticker=sector_etf_ticker,
        sector_name=current_sector, sub_industry=current_sub_industry,
        sub_industry_peers_html=create_tags(sub_peers), sector_other_peers_html=create_tags(other_peers),
        volatility_chart_html=volatility_chart_html,
        chart_bs=chart_bs, chart_is=chart_is, chart_cf=chart_cf, chart_tp=chart_tp
    )
    
    output_path = os.path.join(output_dir, f"{ticker_display}.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)

def export_full_analysis_reports(df_info, df_metrics, output_dir="output_reports_full"):
    if not os.path.exists(output_dir): os.makedirs(output_dir)

    print(f"\nレポート生成開始: {output_dir}")
    rows = df_info.to_dicts()

    # ThreadPoolExecutorによる並列処理
    # ユーザー要望により4コアに合わせて4に設定 (もしKilledされる場合は 2~3 に下げてください)
    max_workers = 20
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 各タスクの引数に df_info, df_metrics, output_dir を渡す
        futures = {executor.submit(generate_report_for_ticker, row, df_info, df_metrics, output_dir): row['Symbol'] for row in rows}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(rows)):
            try:
                future.result()
            except Exception as e:
                ticker = futures[future]
                print(f"Error processing {ticker}: {e}")

    print("完了しました。")

if __name__ == "__main__":
    print("MSFTのレポート生成テストを実行します...")
    
    # ダミーデータ作成 (MSFT)
    df_info = pl.DataFrame({
        "Symbol": ["MSFT"],
        "Symbol_YF": ["MSFT"],
        "Security": ["Microsoft Corp"],
        "GICS Sector": ["Information Technology"],
        "GICS Sub-Industry": ["Systems Software"],
        "Exchange": ["NASDAQ"]
    })
    
    # ダミーのリスク指標
    df_metrics = pl.DataFrame([
        {"Symbol": "MSFT", "HV_250": 0.25, "Log_Return": 0.30},
        {"Symbol": "^GSPC", "HV_250": 0.15, "Log_Return": 0.10},
        {"Symbol": "VGT", "HV_250": 0.20, "Log_Return": 0.25}
    ])
    
    export_full_analysis_reports(df_info, df_metrics, output_dir="test_reports")
