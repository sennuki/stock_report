# -*- coding: utf-8 -*-
"""横断分析用クエリ実行 CLI。

build_dataset.py が作った analysis.duckdb に対して SQL を実行し、結果を
AI へそのまま貼り付けやすい Markdown 表 / CSV で出力する。

使い方:
    # 名前付きクエリの一覧
    python code/analysis/query.py --list

    # 名前付きクエリを実行（queries.sql 内の `-- name: xxx`）
    python code/analysis/query.py --named valuation_cheap

    # 任意の SQL を実行
    python code/analysis/query.py --sql "SELECT sector, count(*) FROM stocks GROUP BY 1 ORDER BY 2 DESC"

    # CSV で出力 / ファイル保存
    python code/analysis/query.py --named growth_quality --format csv --out screen.csv
"""
from __future__ import annotations

import argparse
import os
import re
import sys

ANALYSIS_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(ANALYSIS_DIR, "analysis.duckdb")
QUERIES_PATH = os.path.join(ANALYSIS_DIR, "queries.sql")


def load_named_queries() -> dict[str, str]:
    """queries.sql を `-- name: <id>` 区切りでパースする。"""
    if not os.path.exists(QUERIES_PATH):
        return {}
    with open(QUERIES_PATH, "r", encoding="utf-8") as f:
        text = f.read()
    queries: dict[str, str] = {}
    current: str | None = None
    buf: list[str] = []
    for line in text.splitlines():
        m = re.match(r"^--\s*name:\s*(\S+)", line)
        if m:
            if current and buf:
                queries[current] = "\n".join(buf).strip()
            current = m.group(1)
            buf = []
        elif current is not None:
            buf.append(line)
    if current and buf:
        queries[current] = "\n".join(buf).strip()
    return queries


def to_markdown(columns: list[str], rows: list[tuple]) -> str:
    if not rows:
        return "(0 rows)"

    def fmt(v):
        if v is None:
            return ""
        if isinstance(v, float):
            return f"{v:.4g}"
        return str(v)

    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join("---" for _ in columns) + " |"
    body = "\n".join("| " + " | ".join(fmt(v) for v in r) + " |" for r in rows)
    return "\n".join([header, sep, body])


def to_csv(columns: list[str], rows: list[tuple]) -> str:
    import csv
    import io

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(columns)
    for r in rows:
        w.writerow(["" if v is None else v for v in r])
    return buf.getvalue().rstrip("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = parser.add_mutually_exclusive_group()
    g.add_argument("--sql", help="実行する任意の SQL")
    g.add_argument("--named", help="queries.sql 内の名前付きクエリ ID")
    g.add_argument("--list", action="store_true", help="名前付きクエリ一覧を表示")
    parser.add_argument("--db", default=DEFAULT_DB, help="DuckDB パス")
    parser.add_argument("--format", choices=["markdown", "csv"], default="markdown", help="出力形式")
    parser.add_argument("--limit", type=int, default=50, help="表示行数の上限（0 で無制限）")
    parser.add_argument("--out", help="出力先ファイル（省略時は標準出力）")
    args = parser.parse_args()

    named = load_named_queries()

    if args.list:
        if not named:
            print("名前付きクエリがありません（queries.sql を確認）", file=sys.stderr)
            return 1
        print("利用可能な名前付きクエリ:")
        for name in named:
            print(f"  - {name}")
        return 0

    if args.sql:
        sql = args.sql
    elif args.named:
        if args.named not in named:
            print(f"error: 名前付きクエリ '{args.named}' が見つかりません。--list で確認してください。", file=sys.stderr)
            return 1
        sql = named[args.named]
    else:
        parser.print_help()
        return 1

    if not os.path.exists(args.db):
        print(f"error: DB が見つかりません: {args.db}", file=sys.stderr)
        print("先に `python code/analysis/build_dataset.py` を実行してください。", file=sys.stderr)
        return 1

    if args.limit and args.limit > 0 and not re.search(r"\blimit\b", sql, re.IGNORECASE):
        sql = sql.rstrip().rstrip(";") + f"\nLIMIT {args.limit}"

    import duckdb

    con = duckdb.connect(args.db, read_only=True)
    try:
        cur = con.execute(sql)
        columns = [d[0] for d in cur.description]
        rows = cur.fetchall()
    except duckdb.Error as e:
        print(f"SQL エラー: {e}", file=sys.stderr)
        return 1
    finally:
        con.close()

    rendered = to_csv(columns, rows) if args.format == "csv" else to_markdown(columns, rows)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(rendered + "\n")
        print(f"{len(rows)} 行を {args.out} に書き出しました。")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
