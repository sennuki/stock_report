"""S&P 500 全銘柄で revenue_by_segment / revenue_by_geography をバッチ検証する。

検出する問題:
- segment_pollution : セグメントに地域名 (_is_geo_item=True) のカラムが混入
- geo_pollution     : 地域に非地域名 (_is_geo_item=False) のカラムが混入
- segment_empty     : セグメントが空 (defeatbeta データ無し or 採用ロジックで全弾かれ)
- geo_empty         : 地域が空
- error             : 例外発生

判定対象は **最新 3 期で値 ≠ 0 のアクティブカラム** のみ (古い時代のレガシー
カラムを誤検知しないため)。
"""
from __future__ import annotations

import csv
import os
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd

from utils import YFinanceAdapterTicker, _is_geo_item

CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'stock_list_az.csv')


def load_symbols() -> list[str]:
    syms = []
    with open(CSV_PATH, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sym = row.get('Symbol_YF') or row.get('Symbol')
            if sym:
                syms.append(sym.strip())
    return syms


def active_columns(df, lookback: int = 3) -> list[str]:
    """直近 lookback 期で値 != 0 のカラム名のみ返す。"""
    if df is None or df.empty:
        return []
    value_cols = [c for c in df.columns if c not in ('symbol', 'report_date')]
    if not value_cols:
        return []
    recent = df.sort_values('report_date').tail(lookback)
    return [c for c in value_cols if recent[c].fillna(0).abs().sum() > 0]


def main() -> int:
    symbols = load_symbols()
    print(f'Loaded {len(symbols)} symbols from {CSV_PATH}')

    results = []
    issues = defaultdict(list)
    start = time.time()

    for i, sym in enumerate(symbols, 1):
        if i % 25 == 0 or i == len(symbols):
            elapsed = time.time() - start
            print(f'  [{i}/{len(symbols)}] {elapsed:.1f}s elapsed', flush=True)
        try:
            adapter = YFinanceAdapterTicker(sym)
            df_seg = adapter.revenue_by_segment()
            df_geo = adapter.revenue_by_geography()
        except Exception as e:
            issues['error'].append((sym, repr(e)))
            results.append({'symbol': sym, 'error': repr(e)})
            continue

        seg_active = active_columns(df_seg)
        geo_active = active_columns(df_geo)

        # 汚染検出
        seg_polluted_with_geo = [c for c in seg_active if _is_geo_item(c)]
        geo_polluted_with_non_geo = [c for c in geo_active if not _is_geo_item(c)]

        rec = {
            'symbol': sym,
            'seg_active': seg_active,
            'geo_active': geo_active,
            'seg_polluted_with_geo': seg_polluted_with_geo,
            'geo_polluted_with_non_geo': geo_polluted_with_non_geo,
            'seg_empty': not seg_active,
            'geo_empty': not geo_active,
        }
        results.append(rec)

        if seg_polluted_with_geo:
            issues['segment_pollution'].append((sym, seg_polluted_with_geo))
        if geo_polluted_with_non_geo:
            issues['geo_pollution'].append((sym, geo_polluted_with_non_geo))
        if not seg_active and not geo_active:
            issues['both_empty'].append(sym)
        elif not seg_active:
            issues['segment_empty_only'].append(sym)
        elif not geo_active:
            issues['geo_empty_only'].append(sym)

    elapsed = time.time() - start
    print(f'\nDone in {elapsed:.1f}s. {len(symbols)} symbols processed.')

    # --- レポート ---
    print('\n' + '=' * 80)
    print('SUMMARY')
    print('=' * 80)
    print(f'  errors                : {len(issues["error"])}')
    print(f'  segment_pollution     : {len(issues["segment_pollution"])}')
    print(f'  geo_pollution         : {len(issues["geo_pollution"])}')
    print(f'  both_empty            : {len(issues["both_empty"])}')
    print(f'  segment_empty_only    : {len(issues["segment_empty_only"])}')
    print(f'  geo_empty_only        : {len(issues["geo_empty_only"])}')

    if issues['segment_pollution']:
        print('\n--- segment に地域名が混入 ---')
        for sym, cols in issues['segment_pollution'][:50]:
            print(f'  {sym}: {cols}')
        if len(issues['segment_pollution']) > 50:
            print(f'  ... ({len(issues["segment_pollution"]) - 50} more)')

    if issues['geo_pollution']:
        print('\n--- geography に非地域名が混入 ---')
        for sym, cols in issues['geo_pollution'][:50]:
            print(f'  {sym}: {cols}')
        if len(issues['geo_pollution']) > 50:
            print(f'  ... ({len(issues["geo_pollution"]) - 50} more)')

    if issues['error']:
        print('\n--- errors ---')
        for sym, err in issues['error'][:20]:
            print(f'  {sym}: {err}')

    # CSV 出力
    out_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'data', 'revenue_breakdown_batch_report.csv',
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow(['symbol', 'seg_polluted_with_geo', 'geo_polluted_with_non_geo',
                    'seg_empty', 'geo_empty', 'seg_active', 'geo_active'])
        for r in results:
            if 'error' in r:
                w.writerow([r['symbol'], 'ERROR', r['error'], '', '', '', ''])
                continue
            w.writerow([
                r['symbol'],
                '|'.join(r['seg_polluted_with_geo']),
                '|'.join(r['geo_polluted_with_non_geo']),
                int(r['seg_empty']),
                int(r['geo_empty']),
                '|'.join(r['seg_active']),
                '|'.join(r['geo_active']),
            ])
    print(f'\nCSV report written to: {out_path}')

    # 汚染が 1 件でもあれば exit 1
    return 0 if not (issues['segment_pollution'] or issues['geo_pollution']) else 1


if __name__ == '__main__':
    raise SystemExit(main())
