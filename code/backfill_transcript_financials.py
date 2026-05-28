"""既存の決算トランスクリプト索引 (R2: reports/transcripts/index.json) を走査して、
各エントリに四半期財務ハイライト (financials) を追記するワンショットスクリプト。

- 新規生成パイプライン (generate_transcript_report.py) には既に組み込み済み。
- このスクリプトは過去に生成済みのエントリを後追いで埋めるためのもの。
- report_date が無いエントリ、または defeatbeta API が値を返さないエントリは
  スキップする（financials を持たない状態のまま）。

実行: python code/backfill_transcript_financials.py [--force] [SYMBOL ...]
  --force         既に financials を持つエントリも再取得する
  SYMBOL          指定シンボルのみ処理（複数可）。省略時は索引全件。
"""
import argparse
import sys
import time

from defeatbeta_api.data.ticker import Ticker

from generate_transcript_report import (
    fetch_quarter_financials,
    load_transcript_index,
    save_transcript_index,
)


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("symbols", nargs="*", help="対象シンボル（省略時は全件）")
    p.add_argument(
        "--force",
        action="store_true",
        help="既に financials を持つエントリも再取得する",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="銘柄ごとのスリープ秒数（API 過負荷防止、デフォルト 0.5）",
    )
    return p.parse_args()


def main():
    args = parse_args()
    index = load_transcript_index()
    if not index:
        print("索引が空です。終了します。")
        return 0

    targets = args.symbols or sorted(index.keys())
    total = 0
    updated = 0
    failed = 0
    skipped = 0
    for symbol in targets:
        entries = index.get(symbol)
        if not entries:
            print(f"[{symbol}] 索引に登録なし。スキップ。")
            continue

        # この銘柄の中で更新対象となるエントリ
        targets_for_symbol = [
            e for e in entries
            if e.get("report_date")
            and (args.force or not e.get("financials"))
        ]
        if not targets_for_symbol:
            continue

        print(
            f"[{symbol}] {len(targets_for_symbol)}件のエントリで財務ハイライトを取得"
        )
        try:
            ticker = Ticker(symbol)
        except Exception as e:
            print(f"  Ticker 作成に失敗: {e}")
            failed += len(targets_for_symbol)
            continue

        for entry in targets_for_symbol:
            total += 1
            fy = entry.get("fy")
            fq = entry.get("fq")
            report_date = entry.get("report_date")
            try:
                fin = fetch_quarter_financials(ticker, report_date)
            except Exception as e:
                print(f"  FY{fy} Q{fq}: 例外 {e}")
                failed += 1
                continue
            if not fin:
                print(f"  FY{fy} Q{fq} (report_date={report_date}): データなし")
                skipped += 1
                continue
            entry["financials"] = fin
            updated += 1
            print(
                f"  FY{fy} Q{fq}: period_end={fin.get('period_end')} "
                f"revenue={fin.get('revenue')}"
            )

        # 銘柄単位で都度保存（中断時に途中までの結果を残す）
        save_transcript_index(index)
        time.sleep(args.sleep)

    print(
        f"\n完了: 対象 {total} 件 / 更新 {updated} 件 / "
        f"データなし {skipped} 件 / 失敗 {failed} 件"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
