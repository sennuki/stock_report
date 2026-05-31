# テーマ別「下落の妥当性」検証フレームワーク (`code/thematic/`)

あるマーケット・テーマ（例: **SaaS apocalypse**）について、

1. **どの銘柄が該当するか**（`affected` コホート）
2. **類似だが下落していない銘柄**（`resilient` / 対照群）
3. **その懸念が正しいか**を、**決算ファンダ**と**決算説明会トランスクリプト**から検証

を、**yfinance（価格）+ defeatbeta-api（ファンダ・トランスクリプト）**で定量化し、
コホート比較レポート（Markdown / CSV / JSON）として出力するツール群です。

テーマ定義は **JSON 1 ファイル**なので、SaaS apocalypse 以外にも差し替えるだけで
流用できます（GLP-1、関税、金利感応など）。

---

## なぜ別ツールなのか / 既存資産との関係

- 既存の `code/utils.py`（`get_session` / `get_ticker` / defeatbeta アダプタ）と
  defeatbeta のトランスクリプト API をそのまま再利用します（車輪の再発明をしない）。
- 既存 `code/analysis/`（DuckDB 横断分析）は「サイト用に蓄積した全銘柄レポート JSON」を
  母集団にします。本ツールは **テーマで絞った任意バスケット**を、レポート JSON の有無に
  関係なく**その場で取得して比較**する点が異なります（R2 不要）。

## ネットワーク要件（重要）

価格は Yahoo Finance、ファンダ/トランスクリプトは HuggingFace（defeatbeta のデータ源）に
アクセスします。**Claude Code on the web の制限ネットワークポリシーではこれらが遮断され、
実データ取得はできません。** ローカル、または既存パイプラインと同じ GitHub Actions など
**ネットワークが開放された環境で実行**してください。

> 取得を伴わない `--list-themes` / `--validate` / `--help` と、`tests/` の単体テストは
> オフラインでも動きます（重い依存は関数内で遅延 import）。

## セットアップ & 実行

```bash
cd code
uv sync                       # 依存（yfinance / defeatbeta-api / pandas / duckdb 等）

# テーマ一覧
uv run python thematic/run.py --list-themes

# テーマ定義だけ検証（取得しない・オフライン可）
uv run python thematic/run.py --validate --theme saas_apocalypse

# 本実行（要ネットワーク）
uv run python thematic/run.py --theme saas_apocalypse

# よく使うオプション
uv run python thematic/run.py --theme saas_apocalypse --no-transcripts   # 高速（価格+ファンダのみ）
uv run python thematic/run.py --theme saas_apocalypse --limit 4          # 先頭4銘柄で動作確認
uv run python thematic/run.py --theme saas_apocalypse --refresh --period 3y
```

出力は `thematic/output/<theme>/` に:

| ファイル | 内容 |
| --- | --- |
| `report.md` | コホート別サマリ（中央値）＋銘柄別表＋読み方。**まずこれを見る** |
| `per_ticker.csv` | 銘柄別の全指標（AI に貼って追加考察させる用） |
| `report.json` | 機械可読（集計＋明細＋メタ） |

単体テスト（オフライン可）:

```bash
uv run python thematic/tests/test_metrics.py
```

## 算出する指標

| 区分 | 指標 | 意味 / 検証する問い |
| --- | --- | --- |
| 価格 | `drawdown_52w` | 52週高値からの下落率。「本当に売られたか」 |
| 価格 | `ret_since_event` | イベント日（例 Claude Cowork 発表）以降のリターン |
| 価格 | `excess_event` | 上記のベンチマーク超過（テーマ要因かを切り分け） |
| 価格 | `ret_ytd` / `ret_12m` | 年初来 / 12か月リターン |
| ファンダ | `revenue_yoy_latest` / `_prev` | 直近・前四半期の売上 YoY。「成長は鈍化したか」 |
| ファンダ | `revenue_accelerating` | 売上 YoY が加速したか |
| ファンダ | `operating_margin` / `net_margin` | 直近四半期の利益率 |
| トランスクリプト | `bear_density` / `bull_density` | 経営陣が懸念語 / 反証語をどれだけ語ったか（出現数/1000語） |
| トランスクリプト | `net_signal` | `bull_density - bear_density`。負ほど懸念寄り |

**読み筋**: 懸念が正しいなら、`affected` は `resilient` より ①ドローダウンが深く
②売上 YoY が鈍化／減速し ③`net_signal` が低い（座席等の懸念を多く語る）はず。
そうなっていなければ「売られ過ぎ（割安候補）」の可能性が示唆されます。

## 新しいテーマの追加（流用手順）

1. `themes/_template.json` をコピーして `themes/<your_theme>.json` を作成。
2. 編集する項目:
   - `cohorts`: `affected` と `resilient`（対照群）に銘柄（yfinance 表記）を入れる。キー名・コホート数は自由。
   - `event_date`: テーマの転機となった日（任意）。`ret_since_event` の基準。
   - `benchmarks`: 相対比較用 ETF/指数（先頭が `excess_event` の基準）。
   - `signals.bear` / `signals.bull`: トランスクリプトで数える語/フレーズ。
     空白入りは部分一致、単語は単語境界一致（小文字化して照合）。
3. `uv run python thematic/run.py --validate --theme <your_theme>` で検証 → 本実行。

スキーマの各項目は `themes/_template.json` の `_field_help` を参照。

## 既存 DuckDB 横断分析への取り込み（`code/analysis/`）

thematic の出力を `theme_metrics` テーブルとして既存 `code/analysis/analysis.duckdb`
に載せ、`query.py` から `stocks` / `transcripts` と **symbol で JOIN** して横断分析できる。

```bash
# 1) テーマを実行して output/<theme>/report.json を作る（要ネットワーク）
uv run python thematic/run.py --theme saas_apocalypse

# 2) output/*/report.json を全て theme_metrics テーブルへ取り込む
uv run python thematic/to_duckdb.py

# 3) 名前付きクエリで横断分析（既存 query.py を再利用）
uv run python code/analysis/query.py --named theme_cohort_summary
uv run python code/analysis/query.py --named theme_price_fundamental_divergence
```

- `theme_metrics` の粒度は 1 テーマ × 1 銘柄（列: `theme` `symbol` `cohort`
  `drawdown_52w` `ret_since_event` `excess_event` `revenue_yoy_latest`
  `operating_margin` `net_signal` ほか）。
- 取り込みは `build_dataset._write_table` を再利用し、`output/*/report.json` を
  **毎回全走査して作り直す**（report.json が正）。
- 結合キーは `theme_metrics.symbol = stocks.symbol_yf`（thematic は yfinance 表記）。

`queries.sql` に追加した名前付きクエリ:

| クエリ ID | 内容 | `stocks` 要否 |
| --- | --- | --- |
| `theme_cohort_summary` | テーマ×コホートの中央値（下落幅 / 成長 / 論調） | 不要 |
| `theme_price_fundamental_divergence` | 暴落だが売上 YoY 未減速 ＝ 売られ過ぎ候補 | 不要 |
| `theme_oversold_value` | affected で割安かつ DCF 上振れ大 ＝ 逆張り候補 | 要 |
| `theme_signal_vs_sentiment` | 語彙 `net_signal` と LLM センチメントの乖離 | 要 |

> `stocks` / `transcripts` は別途 `python code/analysis/build_dataset.py`（R2 レポートが前提）
> で作る。`theme_metrics` 単独のクエリ（上表「不要」）は R2 無しでも動く。

## ファイル構成

| ファイル | 役割 |
| --- | --- |
| `run.py` | CLI。テーマを読み→全銘柄の指標を集め→レポート出力 |
| `theme.py` | テーマ JSON のロード/検証（stdlib のみ・オフライン可） |
| `sources.py` | yfinance/defeatbeta アダプタ（既存 `utils` 再利用・遅延 import・キャッシュ） |
| `metrics.py` | 価格/ファンダ/トランスクリプトの指標計算（**純関数**・オフライン単体テスト可） |
| `report.py` | コホート比較表・サマリ生成（MD/CSV/JSON・stdlib のみ） |
| `to_duckdb.py` | 出力を `theme_metrics` として `code/analysis/analysis.duckdb` へ取り込む |
| `themes/*.json` | テーマ定義（`saas_apocalypse.json` ＝実例、`_template.json` ＝雛形） |
| `tests/test_metrics.py` | ネットワーク不要の単体テスト |

`.cache/`（取得データ）と `output/`（生成物）は `.gitignore` 済み。

## 制限・注意

- 投資助言ではありません。`themes/saas_apocalypse.json` のバスケット/シグナルは
  2026-05 時点の調査に基づく**出発点**で、実行前に見直してください。
- defeatbeta は四半期損益計算書とトランスクリプトの提供範囲に銘柄差があり、欠損は
  空欄になります（銘柄ごと try/except で継続。失敗は `report.md` 末尾に列挙）。
- センチメントは LLM ではなく**語彙ベースのシグナル密度**です。深掘りは既存の
  `generate_transcript_report.py`（Gemini センチメント）と併用してください。
- トランスクリプト取得は重いため、まず `--no-transcripts` で価格+ファンダだけ回し、
  目星をつけてから本実行すると速いです。
