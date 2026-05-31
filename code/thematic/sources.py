# -*- coding: utf-8 -*-
"""データ取得アダプタ。

  - 価格: yfinance(取得不可なら既存 utils の defeatbeta ベース adapter にフォールバック)
  - 四半期損益計算書 / 決算説明会トランスクリプト: defeatbeta-api

重い依存(utils / defeatbeta_api / yfinance)は import 時にネットワークへアクセス
するため、すべて関数内で遅延 import する。これにより本モジュールを import しても
オフラインで安全(run.py の --help やテーマ検証、cache 単体テストが動く)。

取得結果は code/thematic/.cache/ に保存し、再実行を高速化する(既定の鮮度を超えると
再取得)。--refresh 相当は max_age_hours=None を渡すとキャッシュを無視する。
"""
from __future__ import annotations

import json
import os
import sys
import time

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache")
# prebuilt(R2 由来)テーブルの既定 DuckDB(code/analysis/build_dataset.py が生成)
DEFAULT_PREBUILT_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "analysis", "analysis.duckdb",
)


def _ensure_code_on_path() -> None:
    """親ディレクトリ(code/)を sys.path に追加し、utils 等を import 可能にする。"""
    code_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if code_dir not in sys.path:
        sys.path.insert(0, code_dir)


# --------------------------------------------------------------------------
# キャッシュ(parquet for DataFrame, json for メタ)
# --------------------------------------------------------------------------
def _cache_path(kind: str, symbol: str, ext: str) -> str:
    d = os.path.join(CACHE_DIR, kind)
    os.makedirs(d, exist_ok=True)
    safe = "".join(ch if (ch.isalnum() or ch in "._-") else "_" for ch in symbol)
    return os.path.join(d, f"{safe}.{ext}")


def _fresh(path: str, max_age_hours) -> bool:
    if max_age_hours is None or not os.path.exists(path):
        return False
    return (time.time() - os.path.getmtime(path)) / 3600 <= max_age_hours


def _write_df_cache(kind: str, symbol: str, df, index_label: str | None = None) -> None:
    try:
        out = df.copy()
        if index_label is not None:
            out = out.rename_axis(index_label).reset_index()
        out.columns = [str(c) for c in out.columns]
        out.to_parquet(_cache_path(kind, symbol, "parquet"), index=False)
    except Exception as e:  # キャッシュは best-effort
        print(f"  [warn] cache write failed ({kind}/{symbol}): {e}")


def _read_df_cache(kind: str, symbol: str, max_age_hours, index_label: str | None = None):
    path = _cache_path(kind, symbol, "parquet")
    if not _fresh(path, max_age_hours):
        return None
    try:
        import pandas as pd

        df = pd.read_parquet(path)
        if index_label is not None and index_label in df.columns:
            df = df.set_index(index_label)
        return df
    except Exception:
        return None


def _write_json_cache(kind: str, symbol: str, obj: dict) -> None:
    try:
        with open(_cache_path(kind, symbol, "json"), "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False)
    except Exception as e:
        print(f"  [warn] cache write failed ({kind}/{symbol}): {e}")


def _read_json_cache(kind: str, symbol: str, max_age_hours):
    path = _cache_path(kind, symbol, "json")
    if not _fresh(path, max_age_hours):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# --------------------------------------------------------------------------
# 価格(yfinance 主、defeatbeta フォールバック)
# --------------------------------------------------------------------------
def _fetch_price(symbol: str, period: str):
    """終値 Series を返す。yfinance を優先し、ダメなら defeatbeta adapter。"""
    _ensure_code_on_path()
    close = None
    try:
        import yfinance as yf
        from utils import get_session

        hist = yf.Ticker(symbol, session=get_session()).history(
            period=period, auto_adjust=True
        )
        if hist is not None and not hist.empty and "Close" in hist.columns:
            close = hist["Close"].dropna()
    except Exception as e:
        print(f"  [warn] yfinance price failed for {symbol}: {e}")
    if close is None or len(close) == 0:
        try:
            from utils import get_ticker, safe_call

            hist = safe_call(get_ticker(symbol), "history", period=period, max_retries=3)
            if hist is not None and not hist.empty and "Close" in hist.columns:
                close = hist["Close"].dropna()
        except Exception as e:
            print(f"  [warn] defeatbeta price fallback failed for {symbol}: {e}")
    return close


def get_price_history(symbol: str, period: str = "2y", max_age_hours=12):
    """日次終値の pandas Series(index=日付)。取得不可なら None。"""
    import pandas as pd

    df = _read_df_cache("price", symbol, max_age_hours)
    if df is not None and "Close" in df.columns and "Date" in df.columns:
        return pd.Series(df["Close"].values, index=pd.to_datetime(df["Date"]))

    close = _fetch_price(symbol, period)
    if close is None or len(close) == 0:
        return None
    cache_df = pd.DataFrame({"Date": pd.to_datetime(close.index), "Close": close.values})
    _write_df_cache("price", symbol, cache_df)
    return pd.Series(close.values, index=pd.to_datetime(close.index))


# --------------------------------------------------------------------------
# 四半期損益計算書(defeatbeta)
# --------------------------------------------------------------------------
def _fetch_qis(symbol: str):
    _ensure_code_on_path()
    try:
        from defeatbeta_api.data.ticker import Ticker as DBTicker

        obj = DBTicker(symbol).quarterly_income_statement()
        df = obj.df() if hasattr(obj, "df") else obj
    except Exception as e:
        print(f"  [warn] defeatbeta income stmt failed for {symbol}: {e}")
        return None
    if df is None or getattr(df, "empty", True):
        return None
    if "Breakdown" in df.columns:
        df = df.set_index("Breakdown")
    return df


def get_quarterly_income_statement(symbol: str, max_age_hours=72):
    """Breakdown を index に持つ四半期損益計算書 DataFrame。取得不可なら None。"""
    df = _read_df_cache("qis", symbol, max_age_hours, index_label="Breakdown")
    if df is not None:
        return df
    df = _fetch_qis(symbol)
    if df is None:
        return None
    _write_df_cache("qis", symbol, df, index_label="Breakdown")
    return df


# --------------------------------------------------------------------------
# 決算説明会トランスクリプト(defeatbeta)
# --------------------------------------------------------------------------
def _fetch_latest_transcript(symbol: str):
    _ensure_code_on_path()
    try:
        from defeatbeta_api.data.ticker import Ticker as DBTicker

        transcripts = DBTicker(symbol).earning_call_transcripts()
        lst = transcripts.get_transcripts_list()
        if lst is None or lst.empty:
            return None
        latest = lst.sort_values(["fiscal_year", "fiscal_quarter"]).iloc[-1]
        fy, fq = int(latest["fiscal_year"]), int(latest["fiscal_quarter"])
        report_date = None
        if "report_date" in lst.columns and latest["report_date"] is not None:
            report_date = str(latest["report_date"])[:10] or None
        df = transcripts.get_transcript(fy, fq)
        if df is None or getattr(df, "empty", True):
            return None
        return {"fy": fy, "fq": fq, "report_date": report_date, "df": df}
    except Exception as e:
        print(f"  [warn] defeatbeta transcript failed for {symbol}: {e}")
        return None


def get_latest_transcript(symbol: str, max_age_hours=72):
    """最新四半期のトランスクリプト。{fy, fq, report_date, df} か None。

    df は speaker/content 列を持つ(metrics.transcript_signal_scan に渡せる)。
    """
    meta = _read_json_cache("transcript_meta", symbol, max_age_hours)
    df = _read_df_cache("transcript", symbol, max_age_hours)
    if meta is not None and df is not None:
        meta = dict(meta)
        meta["df"] = df
        return meta

    res = _fetch_latest_transcript(symbol)
    if res is None:
        return None
    _write_df_cache("transcript", symbol, res["df"])
    _write_json_cache(
        "transcript_meta", symbol, {k: v for k, v in res.items() if k != "df"}
    )
    return res


# --------------------------------------------------------------------------
# prebuilt(R2 由来)テーブルからの読み取り
# --------------------------------------------------------------------------
def load_prebuilt(symbols, db_path: str | None = None) -> dict:
    """既存 DuckDB の transcripts テーブル(R2 由来・build_dataset.py で構築)から
    ファンダとトーンを一括で読む。{symbol: {"fund": {...}, "tone": {...}}}。

    defeatbeta を叩かずに済むので prebuilt/auto モードを高速化する。価格は
    時系列が無いため対象外(常に yfinance)。DB / テーブルが無い、対象外銘柄は
    結果に含めない(呼び出し側で live フォールバックする)。
    """
    if not symbols:
        return {}
    db_path = db_path or DEFAULT_PREBUILT_DB
    if not os.path.exists(db_path):
        print(f"  [warn] prebuilt DB が見つかりません: {db_path}"
              f"（先に `python code/analysis/build_dataset.py` を実行）")
        return {}

    import duckdb

    try:
        con = duckdb.connect(db_path, read_only=True)
    except Exception as e:
        print(f"  [warn] prebuilt DB を開けません: {e}")
        return {}
    try:
        tables = {r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()}
        if "transcripts" not in tables:
            print("  [warn] prebuilt に transcripts テーブルがありません"
                  "（build_dataset.py を実行してください）")
            return {}
        placeholders = ",".join("?" for _ in symbols)
        rows = con.execute(
            f"""SELECT symbol, fy, fq, revenue_yoy, operating_margin, net_margin,
                       period_end, sentiment_overall, sentiment_management,
                       hedge_density, qa_ratio
                FROM transcripts
                WHERE symbol IN ({placeholders})
                ORDER BY symbol, fy, fq""",
            list(symbols),
        ).fetchall()
    except Exception as e:
        print(f"  [warn] prebuilt の読み込みに失敗: {e}")
        return {}
    finally:
        con.close()

    from collections import defaultdict

    by_sym = defaultdict(list)
    for r in rows:
        by_sym[r[0]].append(r)

    out = {}
    for sym, rs in by_sym.items():
        rs.sort(key=lambda x: ((x[1] or 0), (x[2] or 0)))  # (fy, fq) 昇順
        last = rs[-1]
        prev = rs[-2] if len(rs) >= 2 else None
        out[sym] = {
            "fund": {
                "revenue_yoy_latest": last[3],
                "revenue_yoy_prev": prev[3] if prev else None,
                "operating_margin": last[4],
                "net_margin": last[5],
                "latest_period_end": last[6],
            },
            "tone": {
                "sentiment_overall": last[7],
                "sentiment_mgmt": last[8],
                "hedge_density": last[9],
                "qa_ratio": last[10],
                "transcript_period": f"FY{last[1]} Q{last[2]}" if last[1] else None,
            },
        }
    return out
