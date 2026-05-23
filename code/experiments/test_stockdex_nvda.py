"""
stockdex を NVDA でテストし、現状の yfinance / defeatbeta-api と比較するスクリプト。

実行: python3 code/experiments/test_stockdex_nvda.py
事前準備: pip install stockdex

出力: code/experiments/stockdex_nvda_report.txt
"""
import sys
import traceback
from pathlib import Path

OUT = Path(__file__).parent / "stockdex_nvda_report.txt"
lines = []

def log(msg=""):
    print(msg)
    lines.append(msg)

def section(title):
    log("\n" + "=" * 70)
    log(f"  {title}")
    log("=" * 70)

def try_call(label, fn):
    log(f"\n--- {label} ---")
    try:
        df = fn()
        if df is None:
            log("  → None")
            return
        if hasattr(df, "shape"):
            log(f"  shape: {df.shape}")
        if hasattr(df, "columns"):
            cols = list(df.columns)[:15]
            log(f"  columns: {cols}{'...' if len(list(df.columns)) > 15 else ''}")
        if hasattr(df, "index"):
            idx = list(df.index)
            if idx:
                log(f"  index range: {idx[0]} ... {idx[-1]}  (n={len(idx)})")
        # head + tail で時系列範囲を確認
        if hasattr(df, "head"):
            log("  head(5):")
            for line in str(df.head(5)).splitlines():
                log("    " + line)
            log("  tail(3):")
            for line in str(df.tail(3)).splitlines():
                log("    " + line)
    except Exception as e:
        log(f"  ERROR: {type(e).__name__}: {e}")

# --- stockdex ---
section("stockdex (NVDA)")
try:
    from stockdex import Ticker
    t = Ticker("NVDA")

    # Macrotrends: 通常 20+ 年の長期データ
    try_call("macrotrends_revenue (annual)",      lambda: t.macrotrends_revenue(frequency="annual"))
    try_call("macrotrends_revenue (quarterly)",   lambda: t.macrotrends_revenue(frequency="quarterly"))
    try_call("macrotrends_income_statement(A)",   lambda: t.macrotrends_income_statement(frequency="annual"))
    try_call("macrotrends_balance_sheet(A)",      lambda: t.macrotrends_balance_sheet(frequency="annual"))
    try_call("macrotrends_cash_flow(A)",          lambda: t.macrotrends_cash_flow(frequency="annual"))

    # Finviz: セグメント別・地域別
    try_call("finviz_revenue_by_segment",         lambda: t.finviz_revenue_by_segment())
    try_call("finviz_revenue_by_regions",         lambda: t.finviz_revenue_by_regions())
    try_call("finviz_revenue_by_products_and_services",
                                                  lambda: t.finviz_revenue_by_products_and_services())

    # Yahoo API: 5年程度 (yfinance とほぼ同等)
    try_call("yahoo_api_income_statement(A)",     lambda: t.yahoo_api_income_statement(frequency="annual"))
    try_call("yahoo_api_balance_sheet(A)",        lambda: t.yahoo_api_balance_sheet(frequency="annual"))

    # Digrin: 配当・FCF履歴
    try_call("digrin_dividend",                   lambda: t.digrin_dividend())
    try_call("digrin_free_cash_flow",             lambda: t.digrin_free_cash_flow())
    try_call("digrin_net_income",                 lambda: t.digrin_net_income())
except Exception as e:
    log(f"stockdex import/init failed: {type(e).__name__}: {e}")
    traceback.print_exc()

# --- 現状の yfinance ---
section("yfinance (NVDA) - 現在の主データ源")
try:
    import yfinance as yf
    yft = yf.Ticker("NVDA")
    try_call("yf income_stmt (annual)",           lambda: yft.income_stmt)
    try_call("yf quarterly_income_stmt",          lambda: yft.quarterly_income_stmt)
    try_call("yf balance_sheet",                  lambda: yft.balance_sheet)
    try_call("yf cashflow",                       lambda: yft.cashflow)
    try_call("yf dividends",                      lambda: yft.dividends.to_frame() if yft.dividends is not None else None)
except Exception as e:
    log(f"yfinance failed: {e}")

# --- 現状の defeatbeta-api ---
section("defeatbeta-api (NVDA) - 現在のセグメント/長期データ源")
try:
    from defeatbeta_api.data.ticker import Ticker as DBTicker
    db = DBTicker("NVDA")
    try_call("db revenue_by_segment",             lambda: db.revenue_by_segment())
    try_call("db revenue_by_geography",           lambda: db.revenue_by_geography())
    try_call("db annual_revenue_yoy_growth",      lambda: db.annual_revenue_yoy_growth())
    try_call("db price (10y?)",                   lambda: db.price())
except Exception as e:
    log(f"defeatbeta-api failed: {e}")

# --- 書き出し ---
OUT.write_text("\n".join(lines), encoding="utf-8")
log(f"\n(saved to {OUT})")
