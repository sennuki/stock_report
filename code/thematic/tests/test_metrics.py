# -*- coding: utf-8 -*-
"""ネットワーク不要の単体テスト(合成データ)。

実データ源(yfinance/HuggingFace)が遮断された環境でも、指標計算と
キャッシュの往復が正しいことを検証できる。

実行:
    uv run python thematic/tests/test_metrics.py
    (または pytest があれば: uv run pytest thematic/tests/ -q)
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

# thematic/ を import パスに追加(tests/ の 1 つ上)。
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import metrics  # noqa: E402
import sources  # noqa: E402


def _approx(a, b, tol=1e-6):
    return a is not None and abs(a - b) <= tol


def test_price_metrics():
    # 2025-01-01(100) → 2026-02-23(300) へ上昇、その後 2026-05-29(150) へ下落。
    idx = pd.date_range("2025-01-01", "2026-05-29", freq="D")
    peak = pd.Timestamp("2026-02-23")
    o = idx.map(lambda d: d.toordinal()).to_numpy(dtype="float64")
    o0, op, oe = idx[0].toordinal(), peak.toordinal(), idx[-1].toordinal()
    rising = 100 + (300 - 100) * (o - o0) / (op - o0)
    falling = 300 + (150 - 300) * (o - op) / (oe - op)
    vals = np.where(o <= op, rising, falling)
    close = pd.Series(vals, index=idx)

    m = metrics.price_metrics(close, event_date="2026-02-24")
    assert _approx(m["price"], 150.0, 1e-6), m["price"]
    assert _approx(m["high_52w"], 300.0, 1e-6), m["high_52w"]
    # 高値 300 → 現在 150 のドローダウンは -50%
    assert _approx(m["drawdown_52w"], -0.5, 1e-6), m["drawdown_52w"]
    # イベント(ピーク直後)以降は下落
    assert m["ret_since_event"] is not None and m["ret_since_event"] < 0, m["ret_since_event"]
    # 12M 前(上昇途中)より現在は安い
    assert m["ret_12m"] is not None and m["ret_12m"] < 0, m["ret_12m"]
    # tz-aware でも落ちないこと
    m2 = metrics.price_metrics(close.tz_localize("UTC"), event_date="2026-02-24")
    assert _approx(m2["drawdown_52w"], -0.5, 1e-6), m2["drawdown_52w"]
    print("  ok: price_metrics")


def test_fundamental_trend():
    cols = [
        "2024-03-31", "2024-06-30", "2024-09-30", "2024-12-31",
        "2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31",
    ]
    data = {
        "Total Revenue":    [100, 110, 120, 130, 140, 154, 168, 200],
        "Gross Profit":     [60,  66,  72,  78,  84,  92,  100, 120],
        "Operating Income": [20,  22,  24,  26,  28,  30,  32,  40],
        "Net Income":       [10,  11,  12,  13,  14,  15,  16,  30],
    }
    df = pd.DataFrame(data, index=cols).T  # index=指標, columns=四半期末
    out = metrics.fundamental_trend(df)
    assert out["latest_period_end"] == "2025-12-31", out["latest_period_end"]
    # 直近 YoY = 200/130 - 1 ≈ 0.5385、前Q = 168/120 - 1 = 0.40
    assert _approx(out["revenue_yoy_latest"], 200 / 130 - 1, 1e-3), out["revenue_yoy_latest"]
    assert _approx(out["revenue_yoy_prev"], 0.40, 1e-6), out["revenue_yoy_prev"]
    assert out["revenue_accelerating"] is True, out["revenue_accelerating"]
    assert _approx(out["operating_margin"], 40 / 200, 1e-6), out["operating_margin"]
    assert _approx(out["gross_margin"], 120 / 200, 1e-6), out["gross_margin"]
    assert _approx(out["net_margin"], 30 / 200, 1e-6), out["net_margin"]
    # Breakdown を列に持つ形式でも動くこと
    df2 = df.copy()
    df2.index.name = "Breakdown"
    df2 = df2.reset_index()
    out2 = metrics.fundamental_trend(df2)
    assert _approx(out2["operating_margin"], 40 / 200, 1e-6), out2
    print("  ok: fundamental_trend")


def test_transcript_signal_scan():
    df = pd.DataFrame({
        "speaker": ["CEO", "CFO"],
        "content": [
            "Our usage-based pricing and NRR are strong.",
            "We reduced seat counts; seat compression is real.",
        ],
    })
    bear = ["seat", "per-seat"]
    bull = ["usage-based", "NRR"]
    out = metrics.transcript_signal_scan(df, bear, bull)
    assert out["bear_count"] == 2, out  # "seat counts", "seat compression"
    assert out["bull_count"] == 2, out  # "usage-based", "nrr"
    assert out["bear_terms"] == {"seat": 2}, out["bear_terms"]
    assert out["bull_terms"] == {"usage-based": 1, "NRR": 1}, out["bull_terms"]
    assert _approx(out["net_signal"], 0.0, 1e-9), out["net_signal"]
    # 空入力で落ちないこと
    assert metrics.transcript_signal_scan(None, bear, bull) == {}
    print("  ok: transcript_signal_scan")


def test_cache_roundtrip():
    sym = "__TEST__"
    # QIS(Breakdown を index に持つ DataFrame)の往復
    df = pd.DataFrame(
        {"2025-09-30": [168.0, 32.0], "2025-12-31": [200.0, 40.0]},
        index=pd.Index(["Total Revenue", "Operating Income"], name="Breakdown"),
    )
    sources._write_df_cache("qis", sym, df, index_label="Breakdown")
    back = sources._read_df_cache("qis", sym, max_age_hours=999, index_label="Breakdown")
    assert back is not None, "cache read returned None"
    assert _approx(float(back.loc["Total Revenue", "2025-12-31"]), 200.0), back
    # fundamental_trend が往復後の df でも動くこと
    out = metrics.fundamental_trend(back)
    assert _approx(out["operating_margin"], 40 / 200, 1e-6), out
    # クリーンアップ
    for ext in ("parquet", "json"):
        p = sources._cache_path("qis", sym, ext)
        if os.path.exists(p):
            os.remove(p)
    print("  ok: cache_roundtrip")


def main() -> int:
    tests = [
        test_price_metrics,
        test_fundamental_trend,
        test_transcript_signal_scan,
        test_cache_roundtrip,
    ]
    failed = 0
    for t in tests:
        try:
            t()
        except AssertionError as e:
            failed += 1
            print(f"  FAIL: {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"  ERROR: {t.__name__}: {type(e).__name__}: {e}")
    if failed:
        print(f"\n{failed} 件失敗")
        return 1
    print(f"\n{len(tests)} 件すべて成功")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
