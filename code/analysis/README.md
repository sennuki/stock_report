# 横断分析環境 (DuckDB)

サイト構築用に蓄積した銘柄レポート JSON を、**1 銘柄 = 1 行**のフラットな
テーブルに変換し、DuckDB で SQL を投げて横断的に分析するためのツール群です。
結果を Markdown / CSV で書き出し、そのまま AI に渡して考察させる運用を想定しています。

```
code/analysis/
├── build_dataset.py   レポート JSON → analysis.duckdb (+ parquet) へ変換
├── query.py           SQL 実行 CLI（Markdown / CSV 出力）
├── queries.sql        再利用可能な名前付きクエリ集（割安・成長・セクター・決算）
└── README.md          この説明
```

依存は `duckdb` のみ（プロジェクトに既存。`requirements.txt` 参照）。

## 1. データを用意する

`reports/` は git 管理外で、正データは Cloudflare R2 にあります。先に既存の
ダウンロードスクリプトでローカルへ取得してください（R2 認証情報が必要）。

```bash
node worker-processor/download-reports-from-r2.mjs
```

取得対象:
- `reports/{symbol}.json` … 銘柄ごとの統合レポート
- `reports/transcripts/index.json` … 決算トランスクリプト索引（任意）

## 2. データセットを構築する

```bash
python code/analysis/build_dataset.py
# 取得先を明示する場合
python code/analysis/build_dataset.py --reports-dir reports
```

生成物:
- `code/analysis/analysis.duckdb` … `stocks` / `transcripts` テーブル
- `code/analysis/stocks.parquet`, `transcripts.parquet` … 可搬なエクスポート

### テーブル概要

| テーブル | 粒度 | 主な列 |
| --- | --- | --- |
| `stocks` | 1 銘柄 | `pe_ttm` `pe_forward` `dividend_yield` `dcf_fair_value` `dcf_upside` `revenue_growth` `earnings_growth` `roe` `roa` `operating_margin` `profit_margin` `target_upside` `latest_sentiment_overall` ほか |
| `transcripts` | 1 四半期 | `revenue_yoy` `eps_yoy` `operating_margin` `net_margin` `sentiment_overall` `sentiment_management` `sentiment_analyst` `hedge_density` `qa_ratio` ほか |

> 比率系（margin / growth / yield / roe など）は小数で格納（`0.15` = 15%）。
> `debt_to_equity` は yfinance 由来でパーセント表記（例: 120 = 1.2 倍）。

## 3. 分析する

```bash
# 名前付きクエリの一覧
python code/analysis/query.py --list

# 割安候補（予想 PER 低 × 黒字 × 成長）
python code/analysis/query.py --named valuation_cheap

# 成長 × 収益性の両立
python code/analysis/query.py --named growth_quality

# セクター横断のバリュエーション中央値
python code/analysis/query.py --named sector_valuation_summary

# 任意 SQL
python code/analysis/query.py --sql \
  "SELECT sector, median(pe_forward) FROM stocks GROUP BY 1 ORDER BY 2"

# CSV でファイルに保存（AI へ添付用）
python code/analysis/query.py --named growth_quality --format csv --out screen.csv
```

主な名前付きクエリ:

| 切り口 | クエリ ID |
| --- | --- |
| バリュエーション | `valuation_cheap` `dcf_undervalued` `cheap_vs_sector` |
| 成長性・収益性 | `growth_quality` `high_quality_value` `analyst_upside` |
| インカム | `dividend_income` |
| セクター横断 | `sector_valuation_summary` `cheap_vs_sector` |
| 決算トランスクリプト | `transcript_sentiment_movers` `earnings_accelerating` `sentiment_vs_growth` |
| メタ | `schema` `coverage` |

## 4. AI に渡して考察させる

横断データは 1000 銘柄超でそのままでは AI の文脈に入りません。**SQL で
母集団を絞ってから**結果（Markdown / CSV）を渡すのがコツです。

おすすめの渡し方:
1. `query.py --named coverage` でデータ充足を確認
2. 目的のクエリで 30〜50 行程度に絞り、`--format csv --out` で書き出し
3. その CSV を AI に添付し「この母集団の特徴・外れ値・投資仮説を考察して」と依頼

さらに進めたい場合は、この DuckDB を **DuckDB MCP サーバー**に接続すれば、
AI 自身が SQL を書いて横断分析〜考察まで一気通貫で行えます（別途設定）。

## 補足

- `dcf_fair_value` / `dcf_upside` は `dcf_valuation.fair_price` が取得できた
  銘柄のみ埋まります（DCF 非適用銘柄は NULL）。
- レポートの `charts`（base64 のチャートデータ）は分析に不要なため取り込みません。
