# -*- coding: utf-8 -*-
"""テーマ分析の指標計算(純関数)。

本モジュールの関数はすべてネットワーク非依存で、pandas / numpy / 標準ライブラリ
だけで完結する。データ取得は sources.py が担い、ここは取得済みの DataFrame /
Series を指標へ変換する責務に限定する。これにより tests/test_metrics.py により
ネットワークのない環境でも検証できる。
"""
from __future__ import annotations

import math
import re
from typing import Optional

import numpy as np
import pandas as pd

# 損益計算書の行ラベル(defeatbeta / yfinance 双方の表記揺れに対応)。
# generate_transcript_report._IS_ROW_ALIASES と整合させている。
IS_ROW_ALIASES = {
    "revenue": [
        "Total Revenue", "Revenue", "Operating Revenue",
        "TotalRevenue", "OperatingRevenue",
    ],
    "gross_profit": ["Gross Profit", "GrossProfit"],
    "operating_income": ["Operating Income", "OperatingIncome", "Operating Profit"],
    "net_income": [
        "Net Income", "NetIncome", "Net Income Common Stockholders",
        "Net Income Continuous Operations", "Net Income from Continuing Operations",
        "Net Income Continuing Operations",
    ],
}


# --------------------------------------------------------------------------
# 小さなヘルパ
# --------------------------------------------------------------------------
def _to_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def _ret(curr, base) -> Optional[float]:
    """単純リターン (curr/base - 1)。不正値は None。"""
    curr = _to_float(curr)
    base = _to_float(base)
    if curr is None or base is None or base == 0:
        return None
    return curr / base - 1.0


def _as_close_series(s) -> pd.Series:
    """index を tz-naive の DatetimeIndex にそろえ、昇順・重複排除した float Series。"""
    s = pd.Series(s).dropna()
    if s.empty:
        return pd.Series(dtype="float64")
    idx = pd.DatetimeIndex(pd.to_datetime(s.index))
    if idx.tz is not None:
        idx = idx.tz_convert(None)
    out = pd.Series(np.asarray(s.values, dtype="float64"), index=idx).sort_index()
    return out[~out.index.duplicated(keep="last")]


def _naive(ts: pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(ts)
    return ts.tz_convert(None) if ts.tz is not None else ts


# --------------------------------------------------------------------------
# 価格パフォーマンス
# --------------------------------------------------------------------------
def price_metrics(close, event_date=None, asof=None) -> dict:
    """終値 Series からリターン/ドローダウン系の指標を計算する。

    引数:
        close: index が日付の終値 Series(tz 有無不問)
        event_date: 指定すると『その日以降の最初の終値』を基準に当日比リターンを出す
        asof: 評価基準日(既定は系列の最終日)
    """
    close = _as_close_series(close)
    if close.empty:
        return {}
    asof_ts = _naive(asof) if asof is not None else close.index[-1]
    close = close[close.index <= asof_ts]
    if close.empty:
        return {}

    price = float(close.iloc[-1])
    last_date = close.index[-1]

    def on_or_before(date):
        sub = close[close.index <= date]
        return float(sub.iloc[-1]) if len(sub) else None

    def on_or_after(date):
        sub = close[close.index >= date]
        return float(sub.iloc[0]) if len(sub) else None

    out = {"asof": last_date.strftime("%Y-%m-%d"), "price": price}

    for key, days in (("ret_1m", 30), ("ret_3m", 91), ("ret_6m", 182), ("ret_12m", 365)):
        out[key] = _ret(price, on_or_before(last_date - pd.Timedelta(days=days)))

    # 年初来(前年末の終値が基準。無ければ系列先頭にフォールバック)
    base_ytd = on_or_before(pd.Timestamp(year=last_date.year, month=1, day=1) - pd.Timedelta(days=1))
    if base_ytd is None:
        base_ytd = float(close.iloc[0])
    out["ret_ytd"] = _ret(price, base_ytd)

    # 52週高値からのドローダウン(負値)
    w52 = close[close.index >= last_date - pd.Timedelta(days=365)]
    high_52w = float(w52.max()) if len(w52) else None
    out["high_52w"] = high_52w
    out["drawdown_52w"] = _ret(price, high_52w)

    # イベント以降のリターン
    if event_date:
        out["ret_since_event"] = _ret(price, on_or_after(_naive(event_date)))

    return out


# --------------------------------------------------------------------------
# ファンダメンタル推移(四半期損益計算書)
# --------------------------------------------------------------------------
def _find_row(df: pd.DataFrame, aliases):
    """行ラベル(大小・前後空白無視)を探して該当行を返す。"""
    for a in aliases:
        if a in df.index:
            return df.loc[a]
    lower = {str(i).strip().lower(): i for i in df.index}
    for a in aliases:
        k = a.strip().lower()
        if k in lower:
            return df.loc[lower[k]]
    return None


def fundamental_trend(qis_df, max_quarters: int = 8) -> dict:
    """四半期損益計算書から売上 YoY 系列・直近マージン・加速/減速を抽出する。

    qis_df は Breakdown を index(または列)、四半期末日を列に持つ DataFrame。
    """
    if qis_df is None or getattr(qis_df, "empty", True):
        return {}
    df = qis_df
    if "Breakdown" in getattr(df, "columns", []):
        df = df.set_index("Breakdown")

    # 日付に変換できる列だけを四半期列として昇順に並べる
    date_cols = []
    for c in df.columns:
        try:
            d = pd.to_datetime(c)
        except (ValueError, TypeError):
            continue
        date_cols.append((d, c))
    if not date_cols:
        return {}
    date_cols.sort()

    rev_row = _find_row(df, IS_ROW_ALIASES["revenue"])
    gp_row = _find_row(df, IS_ROW_ALIASES["gross_profit"])
    op_row = _find_row(df, IS_ROW_ALIASES["operating_income"])
    ni_row = _find_row(df, IS_ROW_ALIASES["net_income"])

    def val(row, col):
        return _to_float(row.get(col)) if row is not None else None

    # 直近 max_quarters 四半期の売上 YoY 系列(4 列前=前年同期)
    yoy_series = []
    for i in range(4, len(date_cols)):
        d, c = date_cols[i]
        _, cprev = date_cols[i - 4]
        yoy = _ret(val(rev_row, c), val(rev_row, cprev))
        if yoy is not None:
            yoy_series.append((d.strftime("%Y-%m-%d"), round(yoy, 4)))
    yoy_series = yoy_series[-max_quarters:]

    latest_d, latest_c = date_cols[-1]
    rev = val(rev_row, latest_c)

    def margin(row):
        x = val(row, latest_c)
        if x is None or not rev:
            return None
        return x / rev

    out = {
        "latest_period_end": latest_d.strftime("%Y-%m-%d"),
        "revenue": rev,
        "gross_margin": margin(gp_row),
        "operating_margin": margin(op_row),
        "net_margin": margin(ni_row),
        "revenue_yoy_series": yoy_series,
        "revenue_yoy_latest": yoy_series[-1][1] if yoy_series else None,
        "revenue_yoy_prev": yoy_series[-2][1] if len(yoy_series) >= 2 else None,
    }
    if out["revenue_yoy_latest"] is not None and out["revenue_yoy_prev"] is not None:
        out["revenue_accelerating"] = out["revenue_yoy_latest"] > out["revenue_yoy_prev"]
    else:
        out["revenue_accelerating"] = None
    return out


# --------------------------------------------------------------------------
# トランスクリプトのシグナル・スキャン
# --------------------------------------------------------------------------
def _count_terms(text_lower: str, terms):
    """語/フレーズの出現回数を数える。空白を含む語は部分一致、単語は単語境界一致。"""
    total = 0
    per = {}
    for t in terms or []:
        t2 = str(t).lower().strip()
        if not t2:
            continue
        if re.search(r"\s", t2):
            c = text_lower.count(t2)
        else:
            c = len(re.findall(r"\b" + re.escape(t2) + r"\b", text_lower))
        per[t] = c
        total += c
    return total, per


def transcript_signal_scan(transcript_df, bear_terms, bull_terms) -> dict:
    """決算説明会トランスクリプト本文から bear/bull シグナルの密度を算出する。

    transcript_df は 'content' 列(defeatbeta の get_transcript 形式)を想定。
    密度は出現数/1000語。net_signal>0 なら経営陣が反証(従量/NRR/AI 等)を
    懸念(座席圧縮等)より多く語っていることを示す。
    """
    if transcript_df is None or getattr(transcript_df, "empty", True):
        return {}
    cols = getattr(transcript_df, "columns", [])
    if "content" in cols:
        texts = [str(x) for x in transcript_df["content"].tolist()]
    else:
        texts = [str(x) for x in pd.Series(transcript_df).tolist()]
    full = " ".join(texts)
    lower = full.lower()
    words = len(re.findall(r"\b[\w']+\b", full))

    bear_total, bear_per = _count_terms(lower, bear_terms)
    bull_total, bull_per = _count_terms(lower, bull_terms)

    def dens(n):
        return round(n / words * 1000, 2) if words else 0.0

    return {
        "word_count": words,
        "bear_count": bear_total,
        "bull_count": bull_total,
        "bear_density": dens(bear_total),
        "bull_density": dens(bull_total),
        "net_signal": round(dens(bull_total) - dens(bear_total), 2),
        "bear_terms": {k: v for k, v in bear_per.items() if v},
        "bull_terms": {k: v for k, v in bull_per.items() if v},
    }
