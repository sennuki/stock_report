# -*- coding: utf-8 -*-
"""テーマ定義(JSON)のロードと検証。

標準ライブラリのみに依存し、ネットワークにも重い依存にも触れない。
これにより run.py の --list-themes / --validate やテストはオフラインで動く。
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass

THEMES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "themes")


@dataclass
class Cohort:
    key: str
    label: str
    tickers: list


@dataclass
class Theme:
    name: str
    title: str
    thesis: str
    event_date: str | None
    benchmarks: list
    cohorts: list  # list[Cohort]
    bear_terms: list
    bull_terms: list
    notes: str = ""

    def all_tickers(self) -> list:
        """全コホートのティッカーを重複なく順序保持で返す。"""
        seen: list = []
        for c in self.cohorts:
            for t in c.tickers:
                if t not in seen:
                    seen.append(t)
        return seen

    def cohort_of(self, ticker: str) -> str | None:
        for c in self.cohorts:
            if ticker in c.tickers:
                return c.key
        return None


def theme_path(name: str) -> str:
    if name.endswith(".json"):
        return name if os.path.isabs(name) else os.path.join(THEMES_DIR, name)
    return os.path.join(THEMES_DIR, name + ".json")


def list_themes() -> list:
    """themes/ 内の利用可能なテーマ名(先頭'_'を除く)を返す。"""
    if not os.path.isdir(THEMES_DIR):
        return []
    return [
        f[:-5]
        for f in sorted(os.listdir(THEMES_DIR))
        if f.endswith(".json") and not f.startswith("_")
    ]


def _validate(d: dict, path: str) -> None:
    if not isinstance(d, dict):
        raise ValueError(f"{path}: トップレベルは object である必要があります")
    cohorts = d.get("cohorts")
    if not isinstance(cohorts, dict) or not cohorts:
        raise ValueError(f"{path}: 'cohorts' に1つ以上のコホートが必要です")
    for key, c in cohorts.items():
        tickers = c.get("tickers") if isinstance(c, dict) else None
        if not isinstance(tickers, list) or not tickers:
            raise ValueError(f"{path}: cohort '{key}' に非空の tickers 配列が必要です")
    signals = d.get("signals", {})
    if signals and not isinstance(signals, dict):
        raise ValueError(f"{path}: 'signals' は object である必要があります")


def load_theme(name: str) -> Theme:
    """テーマ JSON を読み込み、検証して Theme を返す。"""
    path = theme_path(name)
    if not os.path.exists(path):
        avail = ", ".join(list_themes()) or "(なし)"
        raise FileNotFoundError(
            f"テーマ定義が見つかりません: {path}\n利用可能: {avail}"
        )
    with open(path, encoding="utf-8") as f:
        d = json.load(f)
    _validate(d, path)

    cohorts = [
        Cohort(key=key, label=c.get("label", key), tickers=list(c.get("tickers", [])))
        for key, c in d["cohorts"].items()
    ]
    signals = d.get("signals", {}) or {}
    return Theme(
        name=d.get("name") or os.path.basename(path)[:-5],
        title=d.get("title") or d.get("name", ""),
        thesis=d.get("thesis", ""),
        event_date=d.get("event_date"),
        benchmarks=list(d.get("benchmarks", [])),
        cohorts=cohorts,
        bear_terms=list(signals.get("bear", [])),
        bull_terms=list(signals.get("bull", [])),
        notes=d.get("notes", ""),
    )
