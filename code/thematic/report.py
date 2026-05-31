# -*- coding: utf-8 -*-
"""コホート比較レポートの組み立てと出力(Markdown / CSV / JSON)。標準ライブラリのみ。"""
from __future__ import annotations

import csv
import io
import json
import statistics
from datetime import datetime, timezone

# (キー, 表示名, 整形種別) — 出力列の定義。
PRICE_FIELDS = [
    ("drawdown_52w", "52w高値比", "pct"),
    ("ret_since_event", "イベント後", "pct"),
    ("excess_event", "対BM超過", "pct"),
    ("ret_ytd", "YTD", "pct"),
    ("ret_12m", "12M", "pct"),
]
FUND_FIELDS = [
    ("revenue_yoy_latest", "売上YoY", "pct"),
    ("revenue_yoy_prev", "前Q YoY", "pct"),
    ("operating_margin", "営業利益率", "pct"),
    ("net_margin", "純利益率", "pct"),
]
SIGNAL_FIELDS = [
    ("bear_density", "bear密度", "num"),
    ("bull_density", "bull密度", "num"),
    ("net_signal", "net", "num"),
]
ALL_FIELDS = PRICE_FIELDS + FUND_FIELDS + SIGNAL_FIELDS


def _fmt(v, kind: str) -> str:
    if v is None or (isinstance(v, float) and v != v):  # None / NaN
        return ""
    if kind == "pct":
        return f"{v * 100:.1f}%"
    if kind == "num":
        return f"{v:.2f}"
    return str(v)


def build_rows(theme, per_ticker: dict, bench=None) -> list:
    """per_ticker: {symbol: {price:{}, fund:{}, signal:{}, transcript_period, error}}

    bench: 基準ベンチマークの price_metrics(あれば excess_event を算出)。
    """
    bench_event = (bench or {}).get("ret_since_event")
    rows = []
    for c in theme.cohorts:
        for sym in c.tickers:
            d = per_ticker.get(sym, {})
            price = d.get("price") or {}
            fund = d.get("fund") or {}
            sig = d.get("signal") or {}
            row = {"symbol": sym, "cohort": c.key, "cohort_label": c.label}
            for key, *_ in PRICE_FIELDS:
                row[key] = price.get(key)
            # 対ベンチマーク超過(イベント後)
            if bench_event is not None and price.get("ret_since_event") is not None:
                row["excess_event"] = price["ret_since_event"] - bench_event
            else:
                row["excess_event"] = None
            for key, *_ in FUND_FIELDS:
                row[key] = fund.get(key)
            for key, *_ in SIGNAL_FIELDS:
                row[key] = sig.get(key)
            row["transcript_period"] = d.get("transcript_period")
            row["error"] = d.get("error")
            rows.append(row)
    return rows


def _median(vals):
    xs = [v for v in vals if isinstance(v, (int, float)) and not isinstance(v, bool) and v == v]
    return round(statistics.median(xs), 4) if xs else None


def cohort_aggregates(theme, rows: list) -> dict:
    """コホートごとに各指標の中央値を取る(これが見出しの答えになる)。"""
    aggs = {}
    for c in theme.cohorts:
        crows = [r for r in rows if r["cohort"] == c.key]
        covered = [r for r in crows if r.get("error") is None and r.get("price")]
        a = {"label": c.label, "n": len(crows), "n_covered": len(covered)}
        for key, *_ in ALL_FIELDS:
            a[key] = _median([r.get(key) for r in crows])
        aggs[c.key] = a
    return aggs


# --------------------------------------------------------------------------
# Markdown
# --------------------------------------------------------------------------
def _md_table(headers, rows_of_cells) -> str:
    head = "| " + " | ".join(headers) + " |"
    sep = "| " + " | ".join("---" for _ in headers) + " |"
    body = "\n".join("| " + " | ".join(r) + " |" for r in rows_of_cells)
    return "\n".join([head, sep, body]) if rows_of_cells else "\n".join([head, sep])


def to_markdown(theme, rows, aggs, meta=None) -> str:
    meta = meta or {}
    lines = [f"# {theme.title}", ""]
    if theme.thesis:
        lines += [f"> **仮説**: {theme.thesis}", ""]
    info = []
    if meta.get("asof"):
        info.append(f"基準日: {meta['asof']}")
    if theme.event_date:
        info.append(f"イベント日: {theme.event_date}")
    if theme.benchmarks:
        info.append(f"ベンチマーク: {', '.join(theme.benchmarks)}")
    if info:
        lines += [" / ".join(info), ""]

    # 1) コホート別サマリ(中央値)
    lines += ["## コホート別サマリ(各指標の中央値)", ""]
    headers = ["コホート", "n(被覆)"] + [name for _, name, _ in ALL_FIELDS]
    cells = []
    for key, a in aggs.items():
        row = [f"{a['label']} ({key})", f"{a['n_covered']}/{a['n']}"]
        row += [_fmt(a.get(k), kind) for k, _, kind in ALL_FIELDS]
        cells.append(row)
    lines += [_md_table(headers, cells), ""]

    # 2) 銘柄別
    lines += ["## 銘柄別", ""]
    headers = ["銘柄", "コホート", "決算期"] + [name for _, name, _ in ALL_FIELDS]
    cells = []
    for r in rows:
        if r.get("error"):
            row = [r["symbol"], r["cohort"], "—"] + ["—"] * len(ALL_FIELDS)
            cells.append(row)
            continue
        row = [r["symbol"], r["cohort"], r.get("transcript_period") or ""]
        row += [_fmt(r.get(k), kind) for k, _, kind in ALL_FIELDS]
        cells.append(row)
    lines += [_md_table(headers, cells), ""]

    errs = [r for r in rows if r.get("error")]
    if errs:
        lines += ["## 取得に失敗した銘柄", ""]
        for r in errs:
            lines.append(f"- {r['symbol']} ({r['cohort']}): {r['error']}")
        lines.append("")

    lines += [
        "## 読み方",
        "",
        "- **52w高値比 / イベント後 / 対BM超過**: 価格面で本当に売られているか(対照群と比較)。",
        "- **売上YoY / 前Q YoY / 各マージン**: 懸念が決算ファンダに現れているか(成長鈍化・利益率低下)。",
        "- **bear/bull密度・net**: 経営陣が懸念(bear: 座席等)と反証(bull: 従量/NRR/AI等)のどちらを多く語ったか。netが負ほど懸念寄り。",
        "",
    ]
    if theme.notes:
        lines += [f"_注: {theme.notes}_", ""]
    lines += ["_出典: 価格=yfinance、ファンダ/トランスクリプト=defeatbeta-api。投資助言ではありません。_"]
    return "\n".join(lines)


# --------------------------------------------------------------------------
# CSV / JSON
# --------------------------------------------------------------------------
def to_csv(rows) -> str:
    cols = ["symbol", "cohort"] + [k for k, *_ in ALL_FIELDS] + ["transcript_period", "error"]
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(cols)
    for r in rows:
        w.writerow(["" if r.get(c) is None else r.get(c) for c in cols])
    return buf.getvalue().rstrip("\n")


def to_json(theme, rows, aggs, meta=None) -> str:
    payload = {
        "theme": theme.name,
        "title": theme.title,
        "thesis": theme.thesis,
        "event_date": theme.event_date,
        "benchmarks": theme.benchmarks,
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "meta": meta or {},
        "cohort_aggregates": aggs,
        "rows": rows,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)
