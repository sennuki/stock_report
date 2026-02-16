# -*- coding: utf-8 -*-
import concurrent.futures
import os

import polars as pl
import yfinance as yf
from tqdm import tqdm

import fundamentals
import market_data  # For type checking or if we want to run full generation here
import risk_return
import utils

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
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Noto+Color+Emoji&family=Noto+Emoji:wght@300..700&display=swap" rel="stylesheet">
    <script src="https://cdn.plot.ly/plotly-2.35.2.min.js" charset="utf-8"></script>
    <script type="module" src="https://widgets.tradingview-widget.com/w/ja/tv-ticker-tag.js"></script>
    <style>
        body {{ 
            font-family: sans-serif, "Noto Emoji", "Noto Color Emoji"; 
            margin: 20px; 
            line-height: 1.5;
            color: inherit;
            background-color: transparent;
        }}
        h1 {{ font-size: 24px; color: inherit; }}
        h2 {{ font-size: 20px; border-bottom: 1px solid #ddd; padding-bottom: 5px; margin-top: 30px; color: inherit; }}
        hr {{ border: none; border-top: 1px solid #eee; margin: 20px 0; }}
        .toc {{ background-color: rgba(0, 0, 0, 0.05); padding: 15px; border-radius: 5px; margin-bottom: 20px; border: 1px solid #eee; }}
        .toc h3 {{ margin-top: 0; font-size: 18px; color: inherit; }}
        .toc ul {{ list-style-type: disc; padding-left: 20px; margin-bottom: 0; }}
        .toc li {{ margin-bottom: 5px; }}
        .toc a {{ text-decoration: none; color: #007bff; }}
        .toc a:hover {{ text-decoration: underline; }}
        .tradingview-widget-container {{ margin-bottom: 20px; width: 100%; overflow: hidden; }}
        
        /* Dark mode overrides for injected content */
        @media (prefers-color-scheme: dark) {{
            h2 {{ border-bottom-color: #444; }}
            hr {{ border-top-color: #333; }}
            .toc {{ background-color: rgba(255, 255, 255, 0.05); border-color: #444; }}
        }}
        
        @media (max-width: 600px) {{
            body {{ margin: 10px; }}
            h1 {{ font-size: 20px; }}
            h2 {{ font-size: 18px; }}
        }}
    </style>
</head>
<body data-pagefind-body>

<h1>銘柄分析レポート: {ticker} ({security})</h1>

<div class="tradingview-widget-container">
    <div class="tradingview-widget-container__widget"></div>
    <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-symbol-info.js" async>
    {{ "symbol": "{full_symbol}", "colorTheme": "light", "isTransparent": false, "locale": "ja", "width": "100%" }}
    </script>
</div>

<hr>

<nav class="toc">
    <h3>目次</h3>
    <ul>
        <li><a href="#stock-chart">株価推移・概要</a></li>
        <li><a href="#risk-return">リスク・リターン分析</a></li>
        <li><a href="#peers">同業種・競合</a></li>
        <li><a href="#sector-peers">同セクター他社</a></li>
        <li><a href="#performance">パフォーマンス比較</a></li>
        <li><a href="#fundamentals">ファンダメンタルズ分析 (概要)</a></li>
        <li><a href="#fundamentals-detail">財務データ推移 (詳細)</a>
            <ul>
                <li><a href="#balance-sheet">貸借対照表</a></li>
                <li><a href="#income-statement">損益計算書</a></li>
                <li><a href="#cash-flow">キャッシュフロー</a></li>
                <li><a href="#shareholder-return">株主還元</a></li>
                <li><a href="#dividend-history">1株あたり配当金</a></li>
            </ul>
        </li>
    </ul>
</nav>

<h2 id="risk-return">🎯 リスク・リターン分析</h2>
<p>🔴 <strong>{ticker}</strong> (対象) vs 🔷 <strong>{sector_etf_ticker}</strong> (セクター) vs ★ <strong>S&P 500</strong></p>
{volatility_chart_html}

<hr>
<h2 id="peers">🏢 同業種・競合 ({sub_industry})</h2>
<div style="display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 20px;">{sub_industry_peers_html}</div>

<h2 id="sector-peers">🏭 同セクター他社 ({sector_name})</h2>
<details><summary>クリックして展開</summary><div style="display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; margin-bottom: 20px;">{sector_other_peers_html}</div></details>

<hr>
<h2 id="performance">📈 パフォーマンス比較</h2>
<div id="tv-advanced-chart-placeholder"></div>

<hr>
<h2 id="fundamentals">📊 ファンダメンタルズ分析</h2>
<div id="tv-financials-placeholder"></div>

<h2 id="fundamentals-detail">📈 財務データ推移 (詳細分析)</h2>

<div style="display: flex; flex-direction: column; gap: 40px;">
  <div>{chart_bs}</div>
  <div>{chart_is}</div>
  <div>{chart_cf}</div>
  <div>{chart_tp}</div>
  <div>{chart_dps}</div>
</div>

</body>
</html>
"""

def generate_report_for_ticker(row, df_info, df_metrics, output_dir):
    # Symbolは表示用 (BRK.B), Symbol_YFはシステム/ファイル用 (BRK-B)
    ticker_display = row['Symbol']
    chart_target_symbol = row['Symbol_YF']
    
    current_sector = row['GICS Sector']
    current_sub_industry = row['GICS Sub-Industry']
    exchange = row['Exchange']
    
    # TradingView用はドット形式を使用
    tv_ticker = ticker_display.replace("-", ".")
    full_symbol = f"{exchange}:{tv_ticker}"
    
    sector_etf_ticker = sector_map.get(current_sector, "VOO")
    sector_etf_tv = f"AMEX:{sector_etf_ticker}"

    # 1. 財務チャート生成
    try:
        ticker_obj = utils.get_ticker(chart_target_symbol)
        fin_data = fundamentals.get_financial_data(ticker_obj)
        
        # 個別にエラーハンドリングして、一部のグラフが失敗しても他を表示できるようにする
        def safe_gen(func, *args):
            try:
                return func(*args)
            except Exception as e:
                return f"<p>グラフ生成エラー ({func.__name__}): {str(e)}</p>"

        chart_bs = safe_gen(fundamentals.get_bs_plotly_html, fin_data.get('bs', {}))
        chart_is = safe_gen(fundamentals.get_is_plotly_html, fin_data.get('is', {}))
        chart_cf = safe_gen(fundamentals.get_cf_plotly_html, fin_data.get('cf', {}))
        chart_tp = safe_gen(fundamentals.get_tp_plotly_html, fin_data.get('tp', {}))
        chart_dps = safe_gen(fundamentals.get_dps_eps_plotly_html, fin_data.get('dps', {}), fin_data.get('is', {}))
        
    except Exception as e:
        error_msg = f"<p>データ取得エラー: {str(e)}</p>"
        chart_bs = chart_is = chart_cf = chart_tp = chart_dps = error_msg
        print(f"CRITICAL ERROR for {ticker_display}: {e}")

    # 2. リスクリターンチャート生成
    volatility_chart_html = risk_return.generate_scatter_html(df_metrics, chart_target_symbol, sector_etf_ticker)

    # 3. タグ生成
    def create_tags(target_df):
        # Symbol_YFをファイル名の参照に使用し、Symbolを表示に使用する
        tags = [f'<tv-ticker-tag symbol="{item["Exchange"]}:{item["Symbol"].replace("-", ".")}"></tv-ticker-tag>' for item in target_df.to_dicts()]
        return "\n".join(tags) if tags else "なし"

    sub_peers = df_info.filter((pl.col("GICS Sub-Industry")==current_sub_industry) & (pl.col("Symbol_YF")!=chart_target_symbol))
    other_peers = df_info.filter((pl.col("GICS Sector")==current_sector) & (pl.col("GICS Sub-Industry")!=current_sub_industry) & (pl.col("Symbol_YF")!=chart_target_symbol))

    # 特殊文字のエスケープ (簡単のため)
    safe_sub_industry = current_sub_industry.replace("&", "&amp;")
    safe_sector_name = current_sector.replace("&", "&amp;")
    safe_security = row['Security'].replace("&", "&amp;")

    # 4. 書き出し
    content = TEMPLATE.format(
        ticker=ticker_display, security=safe_security, full_symbol=full_symbol,
        sector_etf_tv=sector_etf_tv, sector_etf_ticker=sector_etf_ticker,
        sector_name=safe_sector_name, sub_industry=safe_sub_industry,
        sub_industry_peers_html=create_tags(sub_peers), sector_other_peers_html=create_tags(other_peers),
        volatility_chart_html=volatility_chart_html,
        chart_bs=chart_bs, chart_is=chart_is, chart_cf=chart_cf, chart_tp=chart_tp, chart_dps=chart_dps
    )
    
    # Use Symbol_YF for filename to match Astro's expected path (e.g., BRK-B.html)
    output_path = os.path.join(output_dir, f"{chart_target_symbol}.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)

def export_full_analysis_reports(df_info, df_metrics, output_dir="output_reports_full"):
    if not os.path.exists(output_dir): os.makedirs(output_dir)

    print(f"\nレポート生成開始: {output_dir}")
    rows = df_info.to_dicts()

    # GitHub Actions の共有IPからのアクセスは Yahoo Finance にブロックされやすいため、
    # GitHub Actions 環境では負荷を最小限にするため並列数を 1 (直列) に制限する。
    default_max_workers = 1 if os.getenv("GITHUB_ACTIONS") == "true" else 1
    max_workers = int(os.getenv("MAX_WORKERS", default_max_workers))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 各タスクの引数に df_info, df_metrics, output_dir を渡す
        futures = {executor.submit(generate_report_for_ticker, row, df_info, df_metrics, output_dir): row['Symbol'] for row in rows}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(rows)):
            try:
                future.result()
            except Exception as e:
                ticker = futures[future]
                utils.log_event("ERROR", ticker, str(e))
                print(f"Error processing {ticker}: {e}")

    print("完了しました。")

if __name__ == "__main__":
    print("MSFTのレポート生成テストを実行します...")
    
    # テスト対象銘柄
    test_symbol = "MSFT"
    
    # 銘柄基本情報
    df_info = pl.DataFrame({
        "Symbol": [test_symbol],
        "Symbol_YF": [test_symbol],
        "Security": ["Microsoft Corp"],
        "GICS Sector": ["Information Technology"],
        "GICS Sub-Industry": ["Systems Software"],
        "Exchange": ["NASDAQ"]
    })
    
    # 実際の指標を計算 (risk_returnのロジックを使用)
    df_metrics = risk_return.calculate_market_metrics_parallel([test_symbol])
    
    export_full_analysis_reports(df_info, df_metrics, output_dir="test_reports")
    print(f"\nテスト完了: test_reports/{test_symbol}.html を確認してください。")
