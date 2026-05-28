import os
import sys
import time
import random
import json
import re
import numpy as np
import pandas as pd
from utils import get_gemini_client
from defeatbeta_api.data.ticker import Ticker

# 翻訳・要約・センチメント分析に使うモデル（GEMINI.md 既定）。
# gemma は応答が遅い代わりに quota が広い（1 日 1500 リクエスト）。
# generate_json_reports.py の翻訳を flash-lite へ移したため、この 1500 回は
# トランスクリプト生成が専有できる。1 本あたり概算 14 回消費。
MODEL_NAME = "models/gemma-4-26b-a4b-it"

# 生成したレポートは stock-blog サイトから参照される。md 本体・索引とも
# 「正」は R2 に置き、銘柄ページ／トランスクリプトページが SSR で実行時に読む。
# ローカル開発用に public/ にも同じものを書く（Git 管理しない）。
#   - Markdown 本体: reports/transcripts/{symbol}_{FY}_Q{FQ}.md
#   - 索引:          reports/transcripts/index.json
#     {symbol: [{fy, fq, period, generated, sentiment?}, ...]}（新しい四半期が先頭）
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRANSCRIPT_PUBLIC_DIR = os.path.join(
    REPO_ROOT, "stock-blog", "public", "reports", "transcripts"
)
TRANSCRIPT_INDEX_PATH = os.path.join(TRANSCRIPT_PUBLIC_DIR, "index.json")
TRANSCRIPT_INDEX_KEY = "reports/transcripts/index.json"


def get_r2_client():
    """main.py と同じ要領で Cloudflare R2 (S3 互換) クライアントを返す。
    認証情報が無い、または boto3 が未インストールの場合は None を返す。"""
    account_id = os.getenv("R2_ACCOUNT_ID")
    access_key = os.getenv("R2_ACCESS_KEY_ID")
    secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
    if not (account_id and access_key and secret_key):
        return None
    try:
        import boto3
    except ImportError:
        print("boto3 が見つかりません。R2 アップロードをスキップします。")
        return None
    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def upload_transcript_to_r2(symbol, fiscal_year, fiscal_quarter, content):
    """本番の SSR トランスクリプトページが参照できるよう R2 へアップロードする。"""
    client = get_r2_client()
    if not client:
        print("R2 未設定のためアップロードをスキップ（ローカルコピーは保存済み）。")
        return
    bucket = os.getenv("R2_BUCKET_NAME", "stock-data-c1")
    key = f"reports/transcripts/{symbol}_{fiscal_year}_Q{fiscal_quarter}.md"
    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType="text/markdown; charset=utf-8",
        )
        print(f"Uploaded to R2: {bucket}/{key}")
    except Exception as e:
        print(f"R2 アップロードに失敗しました: {e}")


def load_transcript_index():
    """トランスクリプト索引を読み込む。R2 を正とし、未設定ならローカルコピー。

    R2 は設定済みなのに取得に失敗した場合は例外を送出する。索引が空のまま
    マージ・保存して既存エントリを失わないための安全策。"""
    client = get_r2_client()
    if client:
        bucket = os.getenv("R2_BUCKET_NAME", "stock-data-c1")
        try:
            obj = client.get_object(Bucket=bucket, Key=TRANSCRIPT_INDEX_KEY)
            return json.loads(obj["Body"].read().decode("utf-8"))
        except Exception as e:
            code = (
                getattr(e, "response", {}) or {}
            ).get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404", "NoSuchBucket"):
                return {}  # 初回実行: 索引はまだ存在しない
            raise RuntimeError(f"R2 から索引を取得できませんでした: {e}") from e
    # R2 未設定（ローカル開発）: public/ のコピーを読む
    if os.path.exists(TRANSCRIPT_INDEX_PATH):
        try:
            with open(TRANSCRIPT_INDEX_PATH, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"ローカル索引を読めませんでした（空で再作成します）: {e}")
    return {}


def save_transcript_index(index):
    """索引を public/（ローカル開発配信用）と R2（本番 SSR 用）の両方へ書き出す。"""
    payload = json.dumps(index, ensure_ascii=False, indent=2) + "\n"
    os.makedirs(os.path.dirname(TRANSCRIPT_INDEX_PATH), exist_ok=True)
    with open(TRANSCRIPT_INDEX_PATH, "w", encoding="utf-8") as f:
        f.write(payload)
    client = get_r2_client()
    if client:
        bucket = os.getenv("R2_BUCKET_NAME", "stock-data-c1")
        try:
            client.put_object(
                Bucket=bucket,
                Key=TRANSCRIPT_INDEX_KEY,
                Body=payload.encode("utf-8"),
                ContentType="application/json; charset=utf-8",
            )
            print(f"Index uploaded to R2: {bucket}/{TRANSCRIPT_INDEX_KEY}")
        except Exception as e:
            print(f"R2 への索引アップロードに失敗しました: {e}")
    print(f"Index updated: {TRANSCRIPT_INDEX_PATH}")


# --- 四半期財務ハイライト ------------------------------------------------
# トランスクリプトページに、その期の主要 KPI（売上高・営業利益・純利益・EPS）と
# 前年同期比 (YoY) を載せるためのデータを defeatbeta から取得する。
# 索引 (index.json) の各エントリに `financials` として保存する。

# defeatbeta / yfinance 両対応のための行ラベルエイリアス。
_IS_ROW_ALIASES = {
    "revenue": [
        "Total Revenue", "Revenue", "Operating Revenue",
        "TotalRevenue", "OperatingRevenue",
    ],
    "gross_profit": ["Gross Profit", "GrossProfit"],
    "operating_income": [
        "Operating Income", "OperatingIncome", "Operating Profit",
    ],
    "net_income": [
        "Net Income", "NetIncome", "Net Income Common Stockholders",
        "Net Income Continuous Operations",
        "Net Income from Continuing Operations",
        "Net Income Continuing Operations",
    ],
    "eps_diluted": ["Diluted EPS", "DilutedEPS", "Earnings Per Share Diluted"],
    "eps_basic": ["Basic EPS", "BasicEPS", "Earnings Per Share Basic"],
}


def _find_is_row(df, aliases):
    """行ラベル（大文字小文字・余分な空白を無視）を探して該当行を返す。"""
    for alias in aliases:
        if alias in df.index:
            return df.loc[alias]
    lower_idx = {str(i).strip().lower(): i for i in df.index}
    for alias in aliases:
        key = alias.strip().lower()
        if key in lower_idx:
            return df.loc[lower_idx[key]]
    return None


def _to_float(val):
    if val is None:
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    if np.isnan(f) or np.isinf(f):
        return None
    return f


def _yoy(curr, prev):
    if curr is None or prev is None or prev == 0:
        return None
    return (curr - prev) / abs(prev)


def fetch_quarter_financials(ticker, report_date):
    """report_date に対応する四半期の主要 KPI を defeatbeta から取得する。

    quarterly_income_statement の列（四半期末日）のうち、report_date 以前で
    最も近いものを当該四半期とみなす。前年同期は 4 列前（=約 1 年前）。
    取得できない場合は None を返す（呼び出し側で索引に保存しない）。
    """
    if not report_date:
        return None
    try:
        qis_obj = ticker.quarterly_income_statement()
        df = qis_obj.df() if hasattr(qis_obj, "df") else None
        if df is None or df.empty:
            return None
        if "Breakdown" in df.columns:
            df = df.set_index("Breakdown")
    except Exception as e:
        print(f"  quarterly_income_statement の取得に失敗: {e}")
        return None

    try:
        rd = pd.to_datetime(report_date)
    except Exception:
        return None

    # 列名（四半期末日）を昇順に並べる
    date_cols = []
    for col in df.columns:
        try:
            d = pd.to_datetime(col)
            date_cols.append((d, col))
        except Exception:
            continue
    if not date_cols:
        return None
    date_cols.sort()

    # report_date 以前で最新の四半期末を当該四半期とみなす
    candidates = [(d, c) for d, c in date_cols if d <= rd]
    if not candidates:
        return None
    period_date, period_col = candidates[-1]
    # 四半期末と発表日の差が大きすぎる場合は対応する四半期データ無しとみなす
    if (rd - period_date).days > 200:
        return None

    # 前年同期: period_date から約 1 年前に最も近い列
    yoy_target = period_date - pd.Timedelta(days=365)
    prior = [(d, c) for d, c in date_cols if d < period_date]
    yoy_col = None
    if prior:
        yp, yc = min(prior, key=lambda x: abs((x[0] - yoy_target).days))
        if abs((yp - yoy_target).days) <= 90:
            yoy_col = yc

    def val(metric_key, col):
        if col is None:
            return None
        row = _find_is_row(df, _IS_ROW_ALIASES[metric_key])
        if row is None:
            return None
        return _to_float(row.get(col))

    revenue = val("revenue", period_col)
    gross_profit = val("gross_profit", period_col)
    op_income = val("operating_income", period_col)
    net_income = val("net_income", period_col)
    eps_diluted = val("eps_diluted", period_col)
    eps_basic = val("eps_basic", period_col)
    eps_type = "diluted" if eps_diluted is not None else "basic"
    eps = eps_diluted if eps_diluted is not None else eps_basic

    revenue_prev = val("revenue", yoy_col)
    op_prev = val("operating_income", yoy_col)
    ni_prev = val("net_income", yoy_col)
    eps_prev = val(
        "eps_diluted" if eps_type == "diluted" else "eps_basic", yoy_col
    )

    def margin(num, denom):
        if num is None or not denom:
            return None
        return num / denom

    result = {
        "period_end": period_date.strftime("%Y-%m-%d"),
        "revenue": revenue,
        "revenue_yoy": _yoy(revenue, revenue_prev),
        "gross_profit": gross_profit,
        "gross_margin": margin(gross_profit, revenue),
        "operating_income": op_income,
        "operating_margin": margin(op_income, revenue),
        "operating_income_yoy": _yoy(op_income, op_prev),
        "net_income": net_income,
        "net_margin": margin(net_income, revenue),
        "net_income_yoy": _yoy(net_income, ni_prev),
        "eps": eps,
        "eps_type": eps_type,
        "eps_yoy": _yoy(eps, eps_prev),
    }
    # 全項目が None の場合は保存しない
    informative_keys = [k for k in result if k not in ("period_end", "eps_type")]
    if all(result[k] is None for k in informative_keys):
        return None
    return result


def update_transcript_index(
    symbol,
    fiscal_year,
    fiscal_quarter,
    sentiment=None,
    report_date=None,
    financials=None,
):
    """1 銘柄分のエントリを索引に追記（同一 FY/Q は上書き）して保存する。"""
    index = load_transcript_index()

    # 同じ FY/Q の重複エントリを除いてから追記する（再生成時の上書き）
    entries = [
        e for e in index.get(symbol, [])
        if not (e.get("fy") == fiscal_year and e.get("fq") == fiscal_quarter)
    ]
    entry = {
        "fy": fiscal_year,
        "fq": fiscal_quarter,
        "period": f"{fiscal_year}-Q{fiscal_quarter}",
        "generated": time.strftime("%Y-%m-%d"),
    }
    if report_date:
        entry["report_date"] = report_date
    if sentiment:
        entry["sentiment"] = sentiment
    if financials:
        entry["financials"] = financials
    entries.append(entry)
    entries.sort(key=lambda e: (e["fy"], e["fq"]), reverse=True)
    index[symbol] = entries

    save_transcript_index(index)


# --- 感情分析（センチメント分析）------------------------------------------
# 決算電話会議は「オペレーター挨拶 → 経営陣のプレゼン → Q&A」という定型構造を
# 持つ。speaker 列には役職が無いため、この構造を利用して話者を分類する。

# Q&A セッションの開始をオペレーターが告げる際の定型表現。
_QA_MARKERS = re.compile(
    r"question-and-answer|q&a session|first question|begin the q|"
    r"take (?:your |our )?questions|open (?:it |the (?:call|line)s? )?(?:up )?for questions",
    re.I,
)

# ヘッジ表現・不確実性マーカー（E: LLM を使わない語彙ベース指標）。
_HEDGE_TERMS = [
    "uncertain", "uncertainty", "challenging", "challenge", "challenges",
    "headwind", "headwinds", "cautious", "caution", "difficult", "soft",
    "softness", "weak", "weakness", "pressure", "volatile", "volatility",
    "hopefully", "potentially", "possibly", "roughly", "approximately",
    "somewhat", "modest", "modestly", "we'll see", "hard to say",
    "hard to predict", "hard to tell", "to some extent", "if anything",
    "remains to be seen",
]


def classify_speakers(df):
    """プレゼン部分で話す非オペレーター = 経営陣、Q&A 以降のそれ以外 = アナリスト。
    返り値: (management:set, analysts:set, qa_start_index:int)。"""
    speakers = [str(r.get("speaker", "")).strip() for _, r in df.iterrows()]
    contents = [str(r.get("content", "")) for _, r in df.iterrows()]
    is_operator = [s.lower() == "operator" for s in speakers]

    qa_start = None
    for i, (op, c) in enumerate(zip(is_operator, contents)):
        if op and _QA_MARKERS.search(c):
            qa_start = i
            break
    if qa_start is None:  # フォールバック: 構造を検出できない場合は前半 40%
        qa_start = max(1, int(len(df) * 0.4))

    management = {
        speakers[i] for i in range(qa_start)
        if not is_operator[i] and speakers[i]
    }
    analysts = {
        speakers[i] for i in range(qa_start, len(df))
        if not is_operator[i] and speakers[i] and speakers[i] not in management
    }
    return management, analysts, qa_start


def compute_hedging_metrics(df, management, analysts, qa_start):
    """E: ヘッジ語の出現頻度や Q&A 比率など、LLM を使わない定量指標。"""
    full_text = " ".join(str(r.get("content", "")) for _, r in df.iterrows())
    lower = full_text.lower()
    words = len(re.findall(r"\b[\w']+\b", full_text))

    hedge_hits = 0
    for term in _HEDGE_TERMS:
        if " " in term:
            hedge_hits += lower.count(term)
        else:
            hedge_hits += len(re.findall(r"\b" + re.escape(term) + r"\b", lower))

    total = len(df)
    qa_paras = max(0, total - qa_start)
    return {
        "word_count": words,
        "hedge_count": hedge_hits,
        "hedge_density": round(hedge_hits / words * 1000, 1) if words else 0.0,
        "qa_ratio": round(qa_paras / total, 2) if total else 0.0,
        "analyst_count": len(analysts),
        "management_count": len(management),
    }


def parse_sentiment_json(raw):
    """LLM の出力からセンチメント JSON を抽出・検証する。失敗時は None。"""
    if not raw:
        return None
    text = raw.strip()
    fence = re.search(r"```(?:json)?\s*(.+?)\s*```", text, re.S)
    if fence:
        text = fence.group(1).strip()
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end == -1:
        return None
    try:
        data = json.loads(text[start:end + 1])
    except Exception as e:
        print(f"感情分析 JSON のパースに失敗しました: {e}")
        return None

    def clamp(v):
        try:
            return max(-100, min(100, int(round(float(v)))))
        except Exception:
            return 0

    for key in ("overall", "management", "analyst"):
        node = data.get(key)
        if isinstance(node, dict) and "score" in node:
            node["score"] = clamp(node["score"])
    if isinstance(data.get("topics"), list):
        for t in data["topics"]:
            if isinstance(t, dict) and "score" in t:
                t["score"] = clamp(t["score"])
    return data


def analyze_call_sentiment(symbol, fiscal_year, fiscal_quarter,
                           df, management, analysts, call_with_retry):
    """A/B/C: 経営陣トーン・経営陣 vs アナリスト・トピック別の感情を LLM で分析。"""
    mgmt_text, analyst_text = "", ""
    for _, row in df.iterrows():
        speaker = str(row.get("speaker", "")).strip()
        content = str(row.get("content", ""))
        if speaker in management:
            mgmt_text += content + "\n\n"
        elif speaker in analysts:
            analyst_text += content + "\n\n"
    if not mgmt_text:  # 話者分類に失敗した場合は全文で代替
        mgmt_text = "\n\n".join(str(r.get("content", "")) for _, r in df.iterrows())

    schema = (
        "出力スキーマ（この JSON のみを出力し、コードフェンスや前後の文章は付けない）:\n"
        "{\n"
        '  "overall":    {"score": <int -100..100>, "label": "<ラベル>", '
        '"summary": "<全体トーンの1〜2文の説明>"},\n'
        '  "management": {"score": <int -100..100>, "label": "<ラベル>"},\n'
        '  "analyst":    {"score": <int -100..100>, "label": "<ラベル>", '
        '"concern_level": "低|中|高"},\n'
        '  "topics": [\n'
        '    {"topic": "<テーマ名>", "score": <int -100..100>, '
        '"label": "<ラベル>", "note": "<そのテーマに関する1文>"}\n'
        "  ]\n"
        "}"
    )
    prompt = (
        f"以下は {symbol} FY{fiscal_year} Q{fiscal_quarter} の決算電話会議"
        "（Earnings Call）の内容です。投資家向けに感情分析（センチメント分析）"
        "を行ってください。\n\n"
        "スコアは -100（非常に弱気・否定的）〜 +100（非常に強気・楽観的）の整数。\n"
        "label は「強気」「やや強気」「中立」「やや慎重」「慎重」のいずれか。\n"
        "analyst の score はアナリストの質問の論調（懸念が強いほど低い）、"
        "concern_level は市場の懸念度。\n"
        "topics は経営陣が言及した主要テーマ（ガイダンス、マージン、需要、"
        "AI・成長戦略、競争環境、設備投資 など）を 4〜6 個挙げてください。\n\n"
        + schema
        + "\n\n## 経営陣の発言（プレゼン＋質疑応答での回答）\n"
        + mgmt_text[:80000]
        + "\n\n## アナリストの質問\n"
        + (analyst_text[:30000] or "（質疑応答パートを検出できませんでした）")
    )
    try:
        raw = call_with_retry(
            prompt,
            "あなたは決算電話会議を分析する金融アナリストです。"
            "客観的に感情分析を行い、指定された JSON 形式のみで回答してください。",
        )
    except Exception as e:
        print(f"感情分析の生成に失敗しました: {e}")
        return None
    return parse_sentiment_json(raw)


# --- 逐次翻訳 -------------------------------------------------------------
# 翻訳結果は Markdown としてサイトに表示される。話者を `[名前]:` 形式のまま
# にすると Markdown のリンク参照定義 `[label]: dest` と衝突して壊れたリンクに
# なるため、翻訳後に話者を太字 `**名前**` へ変換する。話者名はチャンク翻訳
# ごとに訳が揺れないよう、事前に一度だけ表記を確定させる。

# 行頭の話者ラベル `[名前]:` を捉える正規表現（: は全角・半角どちらも許容）。
_SPEAKER_LINE_RE = re.compile(r"^[ \t]*\[(.+?)\][ \t]*[:：][ \t]*", re.MULTILINE)


def translate_speaker_names(names, call_with_retry):
    """話者名（原語）-> 表示名 の対応を一度だけ作る。チャンク翻訳での表記揺れ
    （例: ヒルトン・シュロスバーグ / Hilton Schlosberg）を防ぐ。
    失敗・取りこぼしは原語名をそのまま使う。"""
    names = [n for n in names if n]
    if not names:
        return {}
    listing = "\n".join(f"{i + 1}. {n}" for i, n in enumerate(names))
    prompt = (
        "以下は決算電話会議の話者名の一覧です。各名前を、日本語の決算資料で"
        "一般的な表記に変換してください（人名はカタカナ、Operator は"
        "「オペレーター」）。\n"
        "出力は「番号. 変換後の表記」の形式のみとし、入力と同じ順序・同じ"
        "件数で返してください。\n\n" + listing
    )
    mapping = {}
    try:
        resp = call_with_retry(
            prompt,
            "あなたは金融分野の翻訳者です。指示された形式のみで回答してください。",
        ) or ""
        for line in resp.splitlines():
            m = re.match(r"\s*(\d+)\s*[.．]\s*(.+)", line)
            if m:
                idx = int(m.group(1)) - 1
                if 0 <= idx < len(names):
                    mapping[names[idx]] = m.group(2).strip()
    except Exception as e:
        print(f"話者名の表記変換に失敗（原語名のまま使用）: {e}")
    for n in names:  # 取りこぼしは原語名で補完
        mapping.setdefault(n, n)
    return mapping


def speakers_to_markdown(text, speaker_display):
    """翻訳結果中の行頭話者ラベル `[名前]: ` を、Markdown 安全な太字段落
    `**表示名**` に変換する。表示名は speaker_display で正規化し、未知の
    名前はそのまま使う（いずれにせよ角括弧を除去してリンク衝突を防ぐ）。"""
    def repl(m):
        raw = m.group(1).strip()
        display = speaker_display.get(raw, raw)
        return f"**{display}**\n\n"
    return _SPEAKER_LINE_RE.sub(repl, text)


def generate_transcript_report(symbol, fiscal_year, fiscal_quarter):
    print(f"--- Generating Detailed Transcript Report for {symbol} FY{fiscal_year} Q{fiscal_quarter} ---")
    
    # 1. Fetch Transcript
    ticker = Ticker(symbol)
    try:
        transcripts = ticker.earning_call_transcripts()
        df = transcripts.get_transcript(fiscal_year, fiscal_quarter)
        if df.empty:
            print(f"No transcript found for {symbol} FY{fiscal_year} Q{fiscal_quarter}")
            return
    except Exception as e:
        print(f"Error fetching transcript: {e}")
        return

    # 1-b. 決算発表日 (report_date) を一覧 API から取得して索引に記録する。
    # 取得失敗時は致命的ではない（索引には report_date を含めない）。
    report_date = None
    try:
        lst = transcripts.get_transcripts_list()
        mask = (lst["fiscal_year"] == fiscal_year) & (lst["fiscal_quarter"] == fiscal_quarter)
        match = lst.loc[mask]
        if not match.empty:
            raw = match["report_date"].iloc[0]
            report_date = str(raw)[:10] if raw is not None else None
    except Exception as e:
        print(f"  report_date の取得に失敗: {e}")

    # 2. AI Client
    client = get_gemini_client()
    if not client:
        print("Failed to initialize Gemini client.")
        return

    def call_with_retry(prompt, system_instruction, max_retries=5):
        for attempt in range(max_retries):
            try:
                response = client.models.generate_content(
                    model=MODEL_NAME,
                    contents=prompt,
                    config={"system_instruction": system_instruction}
                )
                return response.text
            except Exception as e:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"  Attempt {attempt+1} failed: {e}. Retrying in {wait_time:.1f}s...")
                if attempt == max_retries - 1:
                    raise e
                time.sleep(wait_time)

    # 3. 話者名の表記をここで一意に確定する（チャンク翻訳ごとの揺れを防ぐ）。
    print("Resolving speaker names...")
    unique_speakers = list(dict.fromkeys(
        str(r.get("speaker", "")).strip() for _, r in df.iterrows()
    ))
    speaker_display = translate_speaker_names(unique_speakers, call_with_retry)

    # 4. 逐次翻訳（チャンク単位）。話者ラベルは原語のまま残させて本文だけ翻訳し、
    #    後段で speakers_to_markdown が Markdown 安全な太字 **名前** に変換する。
    print("Starting high-fidelity translation...")
    translated_full_text = ""
    chunk_size = 8  # 1 リクエストで翻訳する段落数

    for i in range(0, len(df), chunk_size):
        chunk_df = df.iloc[i:i + chunk_size]
        chunk_text_en = ""
        for _, row in chunk_df.iterrows():
            chunk_text_en += f"[{row['speaker']}]: {row['content']}\n\n"

        prompt_trans = (
            "以下は決算電話会議のトランスクリプトの一部です。各発言は "
            "`[話者名]: 本文` の形式です。本文を一字一句忠実に、かつ自然な"
            "日本語に翻訳してください。意訳しすぎないでください。\n"
            "話者名（角括弧 [] の部分）は翻訳せず英語の原文のまま残し、"
            "各発言を `[話者名]: ` で始まる形式のまま出力してください。\n\n"
            "---\n" + chunk_text_en
        )
        print(f"Translating paragraphs {i} to {min(i + chunk_size, len(df))}...")
        try:
            translation = call_with_retry(
                prompt_trans,
                "あなたはプロの翻訳家です。金融・ビジネス用語を正確に使い、"
                "原文に忠実な翻訳を提供してください。",
            )
            if not translation:
                translation = f"（チャンク {i} の翻訳に失敗しました）"
            translated_full_text += translation + "\n\n"
            time.sleep(1)  # 連続リクエストによる負荷を避ける
        except Exception as e:
            print(f"Error translating chunk after retries: {e}")
            translated_full_text += f"（チャンク {i} の翻訳に失敗しました）\n\n"

    # 話者ラベル `[名前]:` を Markdown 安全な太字 **名前** に変換する。
    translated_full_text = speakers_to_markdown(translated_full_text, speaker_display)

    # 4. Generate Summary based on the full English text
    print("Generating overall summary...")
    full_text_en = ""
    for _, row in df.iterrows():
        full_text_en += f"[{row['speaker']}]: {row['content']}\n\n"
    
    # 全体の要約（contextは256Kと大きいため、先頭120K文字まで使用）
    prompt_sum = f"""
以下の決算電話会議（Earnings Call）のトランスクリプト全体の内容を分析し、
投資家向けに重要なポイントを日本語で要約してください。

対象: {symbol} FY{fiscal_year} Q{fiscal_quarter}

要約の構成：
1. 決算の要旨（全体的な業績と評価）
2. セグメント別・地域別の動向
3. 経営陣が強調した戦略、成長ドライバー（AI、投資計画等）
4. アナリストの質問と回答の重要点
5. 今後の見通しとガイダンス

---
トランスクリプト本文（原文）:
{full_text_en[:120000]}
"""
    try:
        summary_ja = call_with_retry(
            prompt_sum,
            "あなたはシニア・アナリストです。投資家が迅速に理解できるよう、客観的で要点を得た日本語の要約を作成してください。"
        )
    except Exception as e:
        print(f"Error generating summary after retries: {e}")
        summary_ja = "要約の生成に失敗しました。"

    # 5. Final Assembly
    final_report = f"# {symbol} FY{fiscal_year} Q{fiscal_quarter} Earnings Call Report\n\n"
    final_report += "## 全体要約 (Summary)\n\n"
    final_report += summary_ja + "\n\n"
    final_report += "---" * 10 + "\n\n"
    final_report += "## 逐次翻訳 (Faithful Translation)\n\n"
    final_report += translated_full_text

    # 6. Publish: サイト (stock-blog) から参照できる場所へ保存する
    os.makedirs(TRANSCRIPT_PUBLIC_DIR, exist_ok=True)
    file_name = f"{symbol}_{fiscal_year}_Q{fiscal_quarter}.md"
    file_path = os.path.join(TRANSCRIPT_PUBLIC_DIR, file_name)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(final_report)
    print(f"\nReport saved to: {file_path}")

    # 7. 感情分析（A/B/C: LLM によるトーン分析、E: 語彙ベースの定量指標）
    print("Analyzing call sentiment...")
    management, analysts, qa_start = classify_speakers(df)
    print(f"  経営陣 {len(management)}名 / アナリスト {len(analysts)}名 を検出")
    sentiment = analyze_call_sentiment(
        symbol, fiscal_year, fiscal_quarter, df, management, analysts, call_with_retry
    ) or {}
    sentiment["hedging"] = compute_hedging_metrics(df, management, analysts, qa_start)

    # 7-b. その期の財務ハイライト（売上高・営業利益・純利益・EPS と YoY）
    print("Fetching quarterly financials...")
    financials = fetch_quarter_financials(ticker, report_date)
    if financials:
        print(f"  四半期末 {financials.get('period_end')}: revenue={financials.get('revenue')}")
    else:
        print("  財務ハイライトは取得できませんでした")

    # 8. 銘柄ページがリンクを出すための索引を更新（Git にコミットして配信）
    update_transcript_index(
        symbol, fiscal_year, fiscal_quarter, sentiment, report_date, financials
    )

    # 9. 本番 SSR ページ用に R2 へアップロード（R2 未設定ならスキップ）
    upload_transcript_to_r2(symbol, fiscal_year, fiscal_quarter, final_report)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python generate_transcript_report.py <SYMBOL> <FY> <FQ>")
        # デフォルトで MSFT の最新をテスト
        generate_transcript_report("MSFT", 2026, 3)
    else:
        symbol = sys.argv[1]
        fy = int(sys.argv[2])
        fq = int(sys.argv[3])
        generate_transcript_report(symbol, fy, fq)
