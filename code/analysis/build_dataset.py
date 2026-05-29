# -*- coding: utf-8 -*-
"""銘柄レポート JSON 群を横断分析用のフラットなテーブルに変換するスクリプト。

サイト構築用に R2 / ローカルへ蓄積した `reports/{symbol}.json` と
`reports/transcripts/index.json` を読み込み、AI に渡して横断的な考察を
行いやすい「1 銘柄 = 1 行」のスカラー指標テーブルに平坦化する。

出力:
    code/analysis/analysis.duckdb   ... DuckDB データベース
        - stocks       : 銘柄ごとの最新スナップショット（バリュエーション/
                          成長性・収益性/アナリスト/最新四半期サマリ）
        - transcripts  : 決算トランスクリプト 1 四半期 = 1 行
                          （財務ハイライト + センチメント + 定型テキスト指標）
    code/analysis/stocks.parquet / transcripts.parquet ... 可搬なエクスポート

データの用意:
    `reports/` は git 管理外（R2 が正）。先に既存スクリプトでローカルへ取得する:
        node worker-processor/download-reports-from-r2.mjs
    取得済みディレクトリを --reports-dir で指定する（既定は自動探索）。

使い方:
    python code/analysis/build_dataset.py
    python code/analysis/build_dataset.py --reports-dir reports --out code/analysis/analysis.duckdb
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from typing import Any, Iterable

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ANALYSIS_DIR = os.path.dirname(os.path.abspath(__file__))


# --------------------------------------------------------------------------
# パス探索ヘルパ
# --------------------------------------------------------------------------
def find_reports_dir(explicit: str | None) -> str:
    """レポート JSON が置かれたディレクトリを探す。"""
    candidates = []
    if explicit:
        candidates.append(explicit)
    candidates += [
        os.path.join(REPO_ROOT, "reports"),
        os.path.join(REPO_ROOT, "stock-blog", "public", "reports"),
    ]
    for c in candidates:
        if c and os.path.isdir(c):
            # *.json が 1 つでもあれば採用
            if any(f.endswith(".json") for f in os.listdir(c)):
                return c
    # 見つからない場合は最初の候補を返し、呼び出し側で警告
    return candidates[0] if candidates else os.path.join(REPO_ROOT, "reports")


def load_stocks_meta() -> dict[str, dict]:
    """stocks.json から銘柄メタ情報（指数所属・日本語名等）を読み込む。"""
    path = os.path.join(REPO_ROOT, "stock-blog", "src", "data", "stocks.json")
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    # JSON 内に NaN / Infinity が混じることがあるため null へ置換
    raw = raw.replace("NaN", "null").replace("-Infinity", "null").replace("Infinity", "null")
    try:
        rows = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"warning: stocks.json の解析に失敗しました: {e}", file=sys.stderr)
        return {}
    meta: dict[str, dict] = {}
    for r in rows:
        sym = r.get("Symbol_YF") or r.get("Symbol")
        if not sym:
            continue
        meta[sym] = {
            "index_membership": r.get("Index"),
            "daily_change": _num(r.get("Daily_Change")),
            "security_ja": r.get("Security_JA"),
        }
    return meta


# --------------------------------------------------------------------------
# 値の取り出しユーティリティ
# --------------------------------------------------------------------------
def _num(v: Any) -> float | None:
    """数値へ安全に変換。NaN/Inf/変換不能は None。"""
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def dig(obj: Any, *path: str) -> Any:
    """ネストした dict を安全にたどる。"""
    cur = obj
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def first_present(obj: dict, *keys: str) -> Any:
    """候補キーのうち最初に存在する値を返す（DCF などキー名が不定な場合用）。"""
    for k in keys:
        if isinstance(obj, dict) and obj.get(k) is not None:
            return obj.get(k)
    return None


# --------------------------------------------------------------------------
# 1 銘柄レポート -> stocks 行
# --------------------------------------------------------------------------
def flatten_stock(report: dict, meta: dict[str, dict], latest_tx: dict | None) -> dict:
    h = report.get("highlights") or {}
    # 実レポートのアナリスト情報は "analyst_ratings"（"analyst" はフォールバック）
    analyst = report.get("analyst_ratings") or report.get("analyst") or {}
    dcf = report.get("dcf_valuation") or {}
    if not isinstance(analyst, dict):
        analyst = {}
    if not isinstance(dcf, dict):
        dcf = {}

    sym = report.get("symbol_yf") or report.get("symbol")
    m = meta.get(sym, {})

    # 現在株価は DCF / アナリストどちらにも入りうる
    current_price = _num(first_present(analyst, "currentPrice")) or _num(dcf.get("current_price"))
    target_mean = _num(first_present(analyst, "targetMeanPrice"))
    target_upside = None
    if target_mean and current_price:
        target_upside = target_mean / current_price - 1.0

    # DCF 公正価値（calculate_dcf は "fair_price" で出力。他実装に備え候補も試す）
    dcf_fair = _num(first_present(
        dcf, "fair_price", "intrinsic_value_per_share", "intrinsic_value",
        "fair_value", "value_per_share", "dcf_value",
    ))
    dcf_upside = _num(first_present(dcf, "upside", "margin_of_safety"))
    if dcf_upside is None and dcf_fair and current_price:
        dcf_upside = dcf_fair / current_price - 1.0

    row = {
        # --- 識別・メタ ---
        "symbol": report.get("symbol"),
        "symbol_yf": sym,
        "security": report.get("security"),
        "security_ja": report.get("security_ja") or m.get("security_ja"),
        "sector": report.get("sector"),
        "sub_industry": report.get("sub_industry"),
        "exchange": report.get("exchange"),
        "index_membership": m.get("index_membership"),
        "is_financial": report.get("is_financial"),
        "daily_change": m.get("daily_change"),
        # --- バリュエーション ---
        "pe_ttm": _num(h.get("pe_ttm")),
        "pe_forward": _num(h.get("pe_forward")),
        "dividend_yield": _num(h.get("dividend_yield")),
        "payout_ratio": _num(h.get("payout_ratio")),
        "dcf_fair_value": dcf_fair,
        "dcf_upside": dcf_upside,
        # --- 成長性・収益性 ---
        "revenue_growth": _num(h.get("revenue_growth")),
        "earnings_growth": _num(h.get("earnings_growth")),
        "profit_margin": _num(h.get("profit_margins")),
        "operating_margin": _num(h.get("operating_margins")),
        "roe": _num(h.get("roe")),
        "roa": _num(h.get("roa")),
        "eps_ttm": _num(h.get("eps_ttm")),
        "eps_forward": _num(h.get("eps_forward")),
        "debt_to_equity": _num(h.get("debt_to_equity")),
        "current_ratio": _num(h.get("current_ratio")),
        # --- アナリスト ---
        "current_price": current_price,
        "target_mean": target_mean,
        "target_high": _num(first_present(analyst, "targetHighPrice")),
        "target_low": _num(first_present(analyst, "targetLowPrice")),
        "target_upside": target_upside,
        "num_analysts": _num(first_present(analyst, "numberOfAnalystOpinions")),
        "rec_strong_buy": _num(analyst.get("strongBuy")),
        "rec_buy": _num(analyst.get("buy")),
        "rec_hold": _num(analyst.get("hold")),
        "rec_sell": _num(analyst.get("sell")),
        "rec_strong_sell": _num(analyst.get("strongSell")),
        # --- 最新四半期トランスクリプト由来 ---
        "latest_period": dig(latest_tx, "period") if latest_tx else None,
        "latest_sentiment_overall": _num(dig(latest_tx, "sentiment", "overall", "score")) if latest_tx else None,
        "latest_sentiment_analyst": _num(dig(latest_tx, "sentiment", "analyst", "score")) if latest_tx else None,
        "latest_rev_yoy": _num(dig(latest_tx, "financials", "revenue_yoy")) if latest_tx else None,
        "latest_eps_yoy": _num(dig(latest_tx, "financials", "eps_yoy")) if latest_tx else None,
        "latest_operating_margin": _num(dig(latest_tx, "financials", "operating_margin")) if latest_tx else None,
        "latest_hedge_density": _num(dig(latest_tx, "hedge_density")) if latest_tx else None,
        "latest_qa_ratio": _num(dig(latest_tx, "qa_ratio")) if latest_tx else None,
    }
    return row


# --------------------------------------------------------------------------
# トランスクリプト索引 -> transcripts 行群
# --------------------------------------------------------------------------
def flatten_transcripts(tx_index: dict) -> list[dict]:
    rows: list[dict] = []
    for sym, entries in (tx_index or {}).items():
        if not isinstance(entries, list):
            continue
        for e in entries:
            if not isinstance(e, dict):
                continue
            fin = e.get("financials") or {}
            sent = e.get("sentiment") or {}
            if not isinstance(fin, dict):
                fin = {}
            if not isinstance(sent, dict):
                sent = {}
            rows.append({
                "symbol": sym,
                "fy": e.get("fy"),
                "fq": e.get("fq"),
                "period": e.get("period"),
                "period_end": fin.get("period_end"),
                "generated": e.get("generated"),
                # 財務ハイライト
                "revenue": _num(fin.get("revenue")),
                "revenue_yoy": _num(fin.get("revenue_yoy")),
                "gross_margin": _num(fin.get("gross_margin")),
                "operating_income": _num(fin.get("operating_income")),
                "operating_margin": _num(fin.get("operating_margin")),
                "operating_income_yoy": _num(fin.get("operating_income_yoy")),
                "net_income": _num(fin.get("net_income")),
                "net_margin": _num(fin.get("net_margin")),
                "net_income_yoy": _num(fin.get("net_income_yoy")),
                "eps": _num(fin.get("eps")),
                "eps_yoy": _num(fin.get("eps_yoy")),
                # センチメント
                "sentiment_overall": _num(dig(sent, "overall", "score")),
                "sentiment_management": _num(dig(sent, "management", "score")),
                "sentiment_analyst": _num(dig(sent, "analyst", "score")),
                "analyst_concern_level": dig(sent, "analyst", "concern_level"),
                # 定型テキスト指標
                "word_count": _num(e.get("word_count")),
                "hedge_density": _num(e.get("hedge_density")),
                "qa_ratio": _num(e.get("qa_ratio")),
                "analyst_count": _num(e.get("analyst_count")),
                "management_count": _num(e.get("management_count")),
            })
    return rows


def load_transcript_index(reports_dir: str) -> dict:
    path = os.path.join(reports_dir, "transcripts", "index.json")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"warning: transcripts/index.json の読み込みに失敗: {e}", file=sys.stderr)
        return {}


def iter_report_files(reports_dir: str) -> Iterable[str]:
    for name in sorted(os.listdir(reports_dir)):
        if not name.endswith(".json"):
            continue
        if name == "stocks.json":
            continue
        yield os.path.join(reports_dir, name)


# --------------------------------------------------------------------------
# DuckDB への書き込み
# --------------------------------------------------------------------------
def _column_type(values: list) -> str:
    """非 NULL 値からおおまかな DuckDB 型を推定する。"""
    non_null = [v for v in values if v is not None]
    if not non_null:
        return "VARCHAR"
    if all(isinstance(v, bool) for v in non_null):
        return "BOOLEAN"
    # bool は int のサブクラスなので bool を除いた数値判定
    if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in non_null):
        return "DOUBLE"
    return "VARCHAR"


def _write_table(con, name: str, rows: list[dict]) -> None:
    """dict のリストを DuckDB テーブルへ書き込む（既存は置き換え）。

    追加依存を避けるため duckdb だけで完結させる（列型を推定して CREATE TABLE
    し、executemany でパラメータ化 INSERT する）。
    """
    con.execute(f"DROP TABLE IF EXISTS {name}")
    if not rows:
        print(f"warning: {name} に書き込むデータがありません", file=sys.stderr)
        return
    # 全行のキー集合をそろえる（欠損は None で補完）。列順を安定させる。
    columns: list[str] = []
    seen: set[str] = set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k)
                columns.append(k)

    coltypes = {c: _column_type([r.get(c) for r in rows]) for c in columns}
    col_defs = ", ".join(f'"{c}" {coltypes[c]}' for c in columns)
    con.execute(f"CREATE TABLE {name} ({col_defs})")

    placeholders = ", ".join("?" for _ in columns)
    data = [tuple(r.get(c) for c in columns) for r in rows]
    con.executemany(f'INSERT INTO {name} VALUES ({placeholders})', data)


# --------------------------------------------------------------------------
# メイン
# --------------------------------------------------------------------------
def build(reports_dir: str, out_path: str) -> tuple[int, int]:
    import duckdb

    meta = load_stocks_meta()
    tx_index = load_transcript_index(reports_dir)

    # 銘柄ごとの最新四半期トランスクリプト（索引は新しい四半期が先頭）
    latest_tx_by_symbol = {
        sym: entries[0]
        for sym, entries in tx_index.items()
        if isinstance(entries, list) and entries
    }

    stock_rows: list[dict] = []
    skipped = 0
    for path in iter_report_files(reports_dir):
        try:
            with open(path, "r", encoding="utf-8") as f:
                report = json.load(f)
        except (json.JSONDecodeError, OSError):
            skipped += 1
            continue
        if not isinstance(report, dict) or not (report.get("symbol") or report.get("symbol_yf")):
            skipped += 1
            continue
        sym = report.get("symbol_yf") or report.get("symbol")
        stock_rows.append(flatten_stock(report, meta, latest_tx_by_symbol.get(sym)))

    tx_rows = flatten_transcripts(tx_index)

    if skipped:
        print(f"warning: {skipped} 件のレポートを読み飛ばしました（解析失敗 / 形式不一致）", file=sys.stderr)

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    con = duckdb.connect(out_path)
    try:
        _write_table(con, "stocks", stock_rows)
        _write_table(con, "transcripts", tx_rows)
        # 可搬な Parquet も併せて出力
        if stock_rows:
            con.execute(
                f"COPY stocks TO '{os.path.join(ANALYSIS_DIR, 'stocks.parquet')}' (FORMAT PARQUET)"
            )
        if tx_rows:
            con.execute(
                f"COPY transcripts TO '{os.path.join(ANALYSIS_DIR, 'transcripts.parquet')}' (FORMAT PARQUET)"
            )
    finally:
        con.close()

    return len(stock_rows), len(tx_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--reports-dir", default=None, help="レポート JSON のディレクトリ（既定は自動探索）")
    parser.add_argument("--out", default=os.path.join(ANALYSIS_DIR, "analysis.duckdb"), help="出力 DuckDB パス")
    args = parser.parse_args()

    reports_dir = find_reports_dir(args.reports_dir)
    if not os.path.isdir(reports_dir):
        print(f"error: レポートディレクトリが見つかりません: {reports_dir}", file=sys.stderr)
        print("先に `node worker-processor/download-reports-from-r2.mjs` で R2 から取得してください。", file=sys.stderr)
        return 1

    print(f"reports-dir: {reports_dir}")
    n_stocks, n_tx = build(reports_dir, args.out)
    print(f"完了: stocks={n_stocks} 行, transcripts={n_tx} 行 -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
