"""defeatbeta-api 0.0.57 の revenue_by_breakdown() を実データで検証するテスト。

目的:
- utils._classify_breakdown_type() / _pivot_breakdown_long_to_wide() / YFinanceAdapterTicker
  の revenue_by_segment / revenue_by_geography が、 実際の戻り値で正しく動くか確認する。
- 期待値: AAPL/MSFT/NVDA はセグメント & 地域の両方が取得できる。 TSCO/VLO は空。
"""
from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd

from defeatbeta_api.data.ticker import Ticker as DBTicker
from utils import (
    YFinanceAdapterTicker,
    _classify_breakdown_type,
    _pivot_breakdown_long_to_wide,
)

pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 200)

SYMBOLS = ['AAPL', 'MSFT', 'NVDA', 'TSCO', 'VLO']


def header(title: str) -> None:
    print()
    print('=' * 80)
    print(title)
    print('=' * 80)


def inspect_classification(symbol: str) -> dict:
    """各 breakdown_type に対する分類結果と件数を表示する。"""
    raw = DBTicker(symbol).revenue_by_breakdown()
    if raw is None or raw.empty:
        print(f'[{symbol}] revenue_by_breakdown() empty')
        return {}
    print(f'[{symbol}] total rows = {len(raw)}')
    summary = (
        raw.groupby('breakdown_type')
        .agg(
            rows=('item_name', 'size'),
            unique_items=('item_name', 'nunique'),
            example_items=('item_name', lambda s: ', '.join(sorted(set(s))[:4])),
        )
        .reset_index()
    )
    summary['classification'] = summary['breakdown_type'].apply(_classify_breakdown_type)
    print(summary.to_string(index=False))
    return {row['breakdown_type']: row['classification'] for _, row in summary.iterrows()}


def inspect_adapter(symbol: str) -> None:
    adapter = YFinanceAdapterTicker(symbol)
    for method in ('revenue_by_segment', 'revenue_by_geography'):
        df = getattr(adapter, method)()
        print(f'\n--- [{symbol}] {method} ---')
        if df is None or df.empty:
            print('  (empty)')
            continue
        print('  shape:', df.shape)
        print('  columns:', list(df.columns))
        latest = df.sort_values('report_date').tail(3)
        print(latest.to_string(index=False))


def run() -> int:
    failures: list[str] = []

    # 期待値:
    #  - non_empty: 取得 (True/False)
    #  - geo_must_include / geo_must_not_include: 地域カラムに含めたい/含めたくない値の部分一致
    #  - seg_must_include / seg_must_not_include: セグメントカラムについて同様
    expectations = {
        'AAPL': {
            'segment': True, 'geography': True,
            'geo_must_include': ['US'],
            'geo_must_not_include': ['Iphone', 'Mac', 'Ipad', 'Service', 'Product'],
            'seg_must_not_include': ['US', 'CN'],
        },
        'MSFT': {
            'segment': True, 'geography': True,
            'geo_must_include': ['US'],
            'geo_must_not_include': ['Office', 'Xbox', 'Surface', 'Windows', 'Gaming',
                                     'Advertising', 'Linked In', 'Dynamics', 'Server',
                                     'Devices', 'Phone'],
            'seg_must_include': ['Productivity And Business Processes', 'Intelligent Cloud'],
            'seg_must_not_include': ['US', 'Non Us'],
        },
        'NVDA': {
            'segment': True, 'geography': True,
            'geo_must_include': ['US'],
            'geo_must_not_include': ['Data Center', 'Datacenter', 'Gaming',
                                     'Automotive', 'Compute', 'Networking'],
            'seg_must_not_include': ['US', 'TW', 'CN'],
        },
        'TSCO': {'segment': False, 'geography': False},
        'VLO':  {'segment': False, 'geography': False},
    }

    def _contains_substring(columns, needles):
        """needle がいずれかの列名の部分文字列か判定 (case-insensitive)。"""
        cols = [str(c).lower() for c in columns]
        return [n for n in needles if any(n.lower() in c for c in cols)]

    def _has_exact(columns, needle):
        """列名がちょうど needle と等しいか判定 (case-insensitive, strip)。"""
        return any(str(c).strip().lower() == needle.lower() for c in columns)

    for symbol in SYMBOLS:
        header(f'{symbol}: classification of breakdown_type values')
        try:
            inspect_classification(symbol)
        except Exception as e:
            print(f'  classification error: {e}')
            failures.append(f'{symbol}: classification raised {e}')

        header(f'{symbol}: adapter.revenue_by_segment / revenue_by_geography')
        try:
            adapter = YFinanceAdapterTicker(symbol)
            df_seg = adapter.revenue_by_segment()
            df_geo = adapter.revenue_by_geography()
        except Exception as e:
            print(f'  adapter error: {e}')
            failures.append(f'{symbol}: adapter raised {e}')
            continue

        print(f'segment   empty={df_seg.empty if df_seg is not None else "None"}  '
              f'shape={None if df_seg is None else df_seg.shape}')
        print(f'geography empty={df_geo.empty if df_geo is not None else "None"}  '
              f'shape={None if df_geo is None else df_geo.shape}')

        if not df_seg.empty:
            print('\nsegment columns:', list(df_seg.columns))
            print(df_seg.sort_values('report_date').tail(3).to_string(index=False))
        if not df_geo.empty:
            print('\ngeography columns:', list(df_geo.columns))
            print(df_geo.sort_values('report_date').tail(3).to_string(index=False))

        exp = expectations[symbol]
        got_seg = df_seg is not None and not df_seg.empty
        got_geo = df_geo is not None and not df_geo.empty
        if exp['segment'] != got_seg:
            failures.append(f'{symbol}: segment expected={exp["segment"]} got={got_seg}')
        if exp['geography'] != got_geo:
            failures.append(f'{symbol}: geography expected={exp["geography"]} got={got_geo}')

        # セマンティック検証:
        #  - geo_must_include / seg_must_not_include: 地域略号(US/TW/CN) は完全一致で判定
        #  - geo_must_not_include / seg_must_include: 単語/フレーズは部分一致で判定
        if got_geo and 'geo_must_include' in exp:
            cols = [c for c in df_geo.columns if c not in ('symbol', 'report_date')]
            missing = [n for n in exp['geo_must_include'] if not _has_exact(cols, n)]
            if missing:
                failures.append(f'{symbol}: geography missing expected cols {missing}; have {cols}')
        if got_geo and 'geo_must_not_include' in exp:
            cols = [c for c in df_geo.columns if c not in ('symbol', 'report_date')]
            polluted = _contains_substring(cols, exp['geo_must_not_include'])
            if polluted:
                failures.append(
                    f'{symbol}: geography polluted by product-like cols matching {polluted}; '
                    f'all cols={cols}'
                )
        if got_seg and 'seg_must_include' in exp:
            cols = [c for c in df_seg.columns if c not in ('symbol', 'report_date')]
            missing = [n for n in exp['seg_must_include']
                       if not _contains_substring(cols, [n])]
            if missing:
                failures.append(f'{symbol}: segment missing expected cols {missing}; have {cols}')
        if got_seg and 'seg_must_not_include' in exp:
            cols = [c for c in df_seg.columns if c not in ('symbol', 'report_date')]
            polluted = [n for n in exp['seg_must_not_include'] if _has_exact(cols, n)]
            if polluted:
                failures.append(
                    f'{symbol}: segment polluted by geo-like cols matching {polluted}; '
                    f'all cols={cols}'
                )

    header('SUMMARY')
    if not failures:
        print('PASS: all expectations met')
        return 0
    print('FAIL:')
    for f in failures:
        print(f'  - {f}')
    return 1


if __name__ == '__main__':
    raise SystemExit(run())
