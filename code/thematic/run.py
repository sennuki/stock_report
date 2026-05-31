# -*- coding: utf-8 -*-
"""テーマ別「下落の妥当性」検証フレームワーク — CLI エントリポイント。

あるマーケット・テーマ(例: SaaS apocalypse)について、該当銘柄(affected)と
類似だが堅調な対照群(resilient)を比較し、懸念が「価格・決算ファンダ・決算説明会
トランスクリプト」に裏づけられるかを定量化してレポート(MD/CSV/JSON)を出力する。

使い方(ネットワークが必要 = ローカル / GitHub Actions 等で実行):
    uv run python thematic/run.py --list-themes
    uv run python thematic/run.py --theme saas_apocalypse
    uv run python thematic/run.py --theme saas_apocalypse --no-transcripts
    uv run python thematic/run.py --theme saas_apocalypse --refresh --period 3y
    uv run python thematic/run.py --validate --theme saas_apocalypse   # 取得せず検証のみ

注意: この環境(Claude Code on the web の制限ポリシー)では Yahoo Finance /
HuggingFace が遮断され実データ取得はできない。--list-themes / --validate /
--help はオフラインで動く。実データ取得はネットワーク開放環境で実行すること。
"""
from __future__ import annotations

import argparse
import os
import sys
import time

# スクリプトのあるディレクトリ(thematic/)を import パスに追加。
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import metrics  # noqa: E402  (pandas/numpy のみ。ネットワーク非依存)
import report  # noqa: E402  (標準ライブラリのみ)
from theme import list_themes, load_theme  # noqa: E402

DEFAULT_OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")


def _collect_symbol(sources, sym, theme, period, do_transcripts, refresh) -> dict:
    """1 銘柄分の price / fund / signal を集める。失敗は error に格納して継続。

    refresh=True のときはキャッシュを無視(max_age_hours=None)。通常時は各
    get_* の既定鮮度(価格 12h / ファンダ・トランスクリプト 72h)を使う。
    """
    price_age = None if refresh else 12
    data_age = None if refresh else 72
    out: dict = {}
    errors = []
    # 価格(yfinance)
    try:
        close = sources.get_price_history(sym, period=period, max_age_hours=price_age)
        if close is not None and len(close):
            out["price"] = metrics.price_metrics(close, event_date=theme.event_date)
    except Exception as e:
        errors.append(f"price: {e}")
    # ファンダ(defeatbeta 四半期損益計算書)
    try:
        qis = sources.get_quarterly_income_statement(sym, max_age_hours=data_age)
        if qis is not None:
            out["fund"] = metrics.fundamental_trend(qis)
    except Exception as e:
        errors.append(f"fund: {e}")
    # トランスクリプト(defeatbeta)
    if do_transcripts:
        try:
            tx = sources.get_latest_transcript(sym, max_age_hours=data_age)
            if tx and tx.get("df") is not None:
                out["signal"] = metrics.transcript_signal_scan(
                    tx["df"], theme.bear_terms, theme.bull_terms
                )
                out["transcript_period"] = f"FY{tx.get('fy')} Q{tx.get('fq')}"
        except Exception as e:
            errors.append(f"transcript: {e}")
    if errors and not out:
        out["error"] = "; ".join(errors)
    elif errors:
        out["warnings"] = errors
    return out


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--theme", help="テーマ名(themes/<name>.json)またはパス")
    p.add_argument("--list-themes", action="store_true", help="利用可能なテーマを一覧")
    p.add_argument("--validate", action="store_true", help="テーマ定義の検証のみ(取得しない)")
    p.add_argument("--no-transcripts", action="store_true", help="トランスクリプト取得を省略(高速)")
    p.add_argument("--refresh", action="store_true", help="キャッシュを無視して再取得")
    p.add_argument("--period", default="2y", help="価格の取得期間(yfinance 表記。既定 2y)")
    p.add_argument("--out", default=DEFAULT_OUT, help="出力ディレクトリ(既定 thematic/output)")
    p.add_argument("--limit", type=int, default=0, help="先頭 N 銘柄だけ処理(動作確認用。0=全件)")
    p.add_argument("--sleep", type=float, default=0.5, help="銘柄間の待機秒(レート配慮。既定 0.5)")
    args = p.parse_args()

    if args.list_themes:
        names = list_themes()
        if not names:
            print("テーマがありません(thematic/themes/ を確認)", file=sys.stderr)
            return 1
        print("利用可能なテーマ:")
        for n in names:
            try:
                t = load_theme(n)
                tickers = t.all_tickers()
                print(f"  - {n}: {t.title}  [{len(tickers)} 銘柄 / {len(t.cohorts)} コホート]")
            except Exception as e:
                print(f"  - {n}: (読み込みエラー: {e})")
        return 0

    if not args.theme:
        p.print_help()
        return 1

    try:
        theme = load_theme(args.theme)
    except (FileNotFoundError, ValueError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    tickers = theme.all_tickers()
    print(f"テーマ: {theme.title}  ({len(tickers)} 銘柄 / {len(theme.cohorts)} コホート)")
    for c in theme.cohorts:
        print(f"  [{c.key}] {c.label}: {', '.join(c.tickers)}")

    if args.validate:
        print("検証 OK(取得はしていません)。")
        return 0

    # ここから先はネットワークが必要。重い依存は sources の関数内で遅延 import。
    import sources  # noqa: E402

    bench_age = None if args.refresh else 12
    if args.limit and args.limit > 0:
        tickers = tickers[: args.limit]

    # ベンチマーク(対 BM 超過の基準)
    bench_metrics = None
    if theme.benchmarks:
        bsym = theme.benchmarks[0]
        try:
            bclose = sources.get_price_history(bsym, period=args.period, max_age_hours=bench_age)
            if bclose is not None and len(bclose):
                bench_metrics = metrics.price_metrics(bclose, event_date=theme.event_date)
                print(f"ベンチマーク {bsym}: イベント後 "
                      f"{(bench_metrics.get('ret_since_event') or 0) * 100:.1f}%")
        except Exception as e:
            print(f"  [warn] benchmark {bsym} の取得に失敗: {e}")

    per_ticker: dict = {}
    n = len(tickers)
    for i, sym in enumerate(tickers, 1):
        print(f"[{i}/{n}] {sym} ...", flush=True)
        per_ticker[sym] = _collect_symbol(
            sources, sym, theme, args.period, not args.no_transcripts, args.refresh
        )
        if args.sleep and i < n:
            time.sleep(args.sleep)

    rows = report.build_rows(theme, per_ticker, bench=bench_metrics)
    aggs = report.cohort_aggregates(theme, rows)
    asof = (bench_metrics or {}).get("asof")
    if not asof:
        asof = next(
            (per_ticker[s]["price"]["asof"] for s in tickers
             if per_ticker.get(s, {}).get("price", {}).get("asof")),
            None,
        )
    meta = {"asof": asof, "benchmark_metrics": bench_metrics, "n_tickers": n}

    out_dir = os.path.join(args.out, theme.name)
    os.makedirs(out_dir, exist_ok=True)
    md_path = os.path.join(out_dir, "report.md")
    csv_path = os.path.join(out_dir, "per_ticker.csv")
    json_path = os.path.join(out_dir, "report.json")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(report.to_markdown(theme, rows, aggs, meta) + "\n")
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write(report.to_csv(rows) + "\n")
    with open(json_path, "w", encoding="utf-8") as f:
        f.write(report.to_json(theme, rows, aggs, meta) + "\n")

    # 標準出力にコホート別サマリ(中央値)を表示
    print("\n=== コホート別サマリ(中央値) ===")
    for key, a in aggs.items():
        dd = a.get("drawdown_52w")
        ev = a.get("ret_since_event")
        yoy = a.get("revenue_yoy_latest")
        net = a.get("net_signal")
        def pct(x):
            return f"{x * 100:.1f}%" if isinstance(x, (int, float)) else "—"
        print(f"  [{key}] {a['label']} (被覆 {a['n_covered']}/{a['n']}): "
              f"52w高値比 {pct(dd)} / イベント後 {pct(ev)} / 売上YoY {pct(yoy)} / net {net}")
    print(f"\n出力: {md_path}\n      {csv_path}\n      {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
