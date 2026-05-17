import os
import sys
import time
import random
import json
import re
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


def update_transcript_index(symbol, fiscal_year, fiscal_quarter, sentiment=None):
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
    if sentiment:
        entry["sentiment"] = sentiment
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

    # 3. High-Fidelity Translation (Chunked)
    print("Starting high-fidelity translation...")
    translated_full_text = ""
    chunk_size = 8 # 少し小さめにして負荷を調整
    
    for i in range(0, len(df), chunk_size):
        chunk_df = df.iloc[i:i+chunk_size]
        chunk_text_en = ""
        for _, row in chunk_df.iterrows():
            chunk_text_en += f"[{row['speaker']}]: {row['content']}\n\n"
        
        prompt_trans = f"""
以下の英文（決算電話会議のトランスクリプトの一部）を、一字一句忠実に、かつ自然な日本語に翻訳してください。
意訳しすぎず、発言の内容を正確に反映させてください。

---
{chunk_text_en}
"""
        print(f"Translating paragraphs {i} to {min(i+chunk_size, len(df))}...")
        try:
            translation = call_with_retry(
                prompt_trans,
                "あなたはプロの翻訳家です。金融・ビジネス用語を正確に使い、原文に忠実な翻訳を提供してください。"
            )
            if translation is None:
                translation = f"[Translation Error: empty response for chunk starting at {i}]"
            translated_full_text += translation + "\n\n"
            # 連続リクエストによる負荷を避ける
            time.sleep(1)
        except Exception as e:
            print(f"Error translating chunk after retries: {e}")
            translated_full_text += f"[Translation Error for chunk starting at {i}]\n\n"

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

    # 8. 銘柄ページがリンクを出すための索引を更新（Git にコミットして配信）
    update_transcript_index(symbol, fiscal_year, fiscal_quarter, sentiment)

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
