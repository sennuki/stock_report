# -*- coding: utf-8 -*-
"""thematic の出力を既存 DuckDB(code/analysis/analysis.duckdb)へ取り込むローダ。

仕組みは build_dataset.py と同型: output/<theme>/report.json を全て走査し、
「1 テーマ × 1 銘柄」の行へ平坦化して、build_dataset._write_table を再利用して
`theme_metrics` テーブルを作る(既存 stocks / transcripts は触らない)。これにより
analysis/query.py から symbol で JOIN して横断クエリできる。

build_dataset と同様、出力ディレクトリにある **全テーマの report.json から毎回
作り直す**(report.json が正)。特定テーマだけ残したいときは output/ を整理する。

使い方:
    uv run python thematic/to_duckdb.py
    uv run python thematic/to_duckdb.py --db analysis/analysis.duckdb \
        --output-dir thematic/output

その後:
    uv run python analysis/query.py --named theme_cohort_summary
    uv run python analysis/query.py --named theme_price_fundamental_divergence

注意: stocks / transcripts を伴う JOIN クエリ(theme_oversold_value など)は、別途
`uv run python analysis/build_dataset.py`(R2 レポートが前提)で stocks を作る必要がある。
theme_metrics 単独のクエリは stocks 無しでも動く。
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import os
import sys

THEMATIC_DIR = os.path.dirname(os.path.abspath(__file__))
CODE_DIR = os.path.dirname(THEMATIC_DIR)
ANALYSIS_DIR = os.path.join(CODE_DIR, "analysis")
DEFAULT_DB = os.path.join(ANALYSIS_DIR, "analysis.duckdb")
DEFAULT_OUTPUT_DIR = os.path.join(THEMATIC_DIR, "output")

# build_dataset._write_table を再利用するため analysis/ を import パスへ。
if ANALYSIS_DIR not in sys.path:
    sys.path.insert(0, ANALYSIS_DIR)

# theme_metrics に載せる数値列(report.json の rows のキーと一致)。順序固定。
NUMERIC_FIELDS = [
    "drawdown_52w", "ret_since_event", "excess_event", "ret_ytd", "ret_12m",
    "revenue_yoy_latest", "revenue_yoy_prev", "operating_margin", "net_margin",
    "bear_density", "bull_density", "net_signal",
    "sentiment_overall", "sentiment_mgmt", "hedge_density",
]
TEXT_FIELDS = ["theme", "title", "asof", "symbol", "cohort", "cohort_label",
               "transcript_period", "error"]


def _num(v):
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if (math.isnan(f) or math.isinf(f)) else f


def iter_report_files(output_dir: str):
    yield from sorted(glob.glob(os.path.join(output_dir, "*", "report.json")))


def flatten_reports(output_dir: str):
    """output/*/report.json を 1 テーマ × 1 銘柄の行へ平坦化する。"""
    rows = []
    per_theme = []
    for path in iter_report_files(output_dir):
        try:
            with open(path, encoding="utf-8") as f:
                payload = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"warning: {path} を読めません: {e}", file=sys.stderr)
            continue
        tname = payload.get("theme") or os.path.basename(os.path.dirname(path))
        asof = (payload.get("meta") or {}).get("asof")
        n = 0
        for r in payload.get("rows", []) or []:
            if not isinstance(r, dict) or not r.get("symbol"):
                continue
            row = {
                "theme": tname,
                "title": payload.get("title"),
                "asof": asof,
                "symbol": r.get("symbol"),
                "cohort": r.get("cohort"),
                "cohort_label": r.get("cohort_label"),
                "transcript_period": r.get("transcript_period"),
            }
            for k in NUMERIC_FIELDS:
                row[k] = _num(r.get(k))
            row["error"] = r.get("error")
            rows.append(row)
            n += 1
        per_theme.append((tname, n))
    return rows, per_theme


def _coerce_numeric_columns(con, table: str) -> None:
    """数値列を DOUBLE に揃える。

    _write_table は値から型推定するため、ある列が全テーマで全 NULL の場合に
    VARCHAR になり median() や数値比較が失敗しうる。それを防ぐため明示的に
    DOUBLE へ寄せる(既に DOUBLE なら実質 no-op)。best-effort。
    """
    for c in NUMERIC_FIELDS:
        try:
            con.execute(
                f'ALTER TABLE {table} ALTER COLUMN "{c}" TYPE DOUBLE '
                f'USING TRY_CAST("{c}" AS DOUBLE)'
            )
        except Exception:
            pass


def build(db_path: str, output_dir: str):
    import duckdb
    from build_dataset import _write_table  # 既存ローダの書き込みヘルパを再利用

    rows, per_theme = flatten_reports(output_dir)
    if not rows:
        print(
            f"取り込む行がありません(output-dir: {output_dir})。\n"
            f"先に `python thematic/run.py --theme <name>` を実行してください。",
            file=sys.stderr,
        )
        return 0, []

    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    con = duckdb.connect(db_path)
    try:
        _write_table(con, "theme_metrics", rows)
        _coerce_numeric_columns(con, "theme_metrics")
        # 可搬な Parquet も db と同じディレクトリへ出力(best-effort)
        parquet = os.path.join(
            os.path.dirname(os.path.abspath(db_path)), "theme_metrics.parquet"
        )
        try:
            con.execute(f"COPY theme_metrics TO '{parquet}' (FORMAT PARQUET)")
        except Exception as e:
            print(f"  [warn] parquet 書き出しに失敗: {e}")
    finally:
        con.close()
    return len(rows), per_theme


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--db", default=DEFAULT_DB,
                   help="出力 DuckDB(既定 code/analysis/analysis.duckdb)")
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR,
                   help="thematic の出力ディレクトリ(既定 thematic/output)")
    args = p.parse_args()

    n, per_theme = build(args.db, args.output_dir)
    if n:
        detail = ", ".join(f"{t}({c})" for t, c in per_theme)
        print(f"完了: theme_metrics={n} 行 [{detail}] -> {args.db}")
        print("クエリ例:")
        print("  uv run python analysis/query.py --named theme_cohort_summary")
        print("  uv run python analysis/query.py --named theme_price_fundamental_divergence")
    return 0 if n else 1


if __name__ == "__main__":
    raise SystemExit(main())
