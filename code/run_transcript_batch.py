"""決算説明会トランスクリプトの定期蓄積バッチ。GitHub Actions から呼ばれる。

挙動:
  - 対象銘柄を R2 の reports/stocks.json（無ければローカルの stocks.json）から取得
  - 各銘柄の「最新四半期」のトランスクリプトを 1 本だけ対象とする
  - 索引 (reports/transcripts/index.json) に既出ならスキップ
  - 1 回の実行で生成する本数を TRANSCRIPT_LIMIT で制限
  - 経過時間が TRANSCRIPT_MAX_MINUTES を超えたら打ち切る

制約への配慮:
  - gemma-4-26b-a4b-it は 1 日 1500 回まで。1 本あたり概算 8〜20 回消費し、
    generate_json_reports.py と quota を共有するため TRANSCRIPT_LIMIT は控えめに。
  - GitHub Actions は 1 ジョブ 6 時間まで。TRANSCRIPT_MAX_MINUTES で安全に打ち切る。
  - R2 は 10GB まで。md 1 本は約 90KB と小さく、最新四半期のみのため増加は緩やか。

環境変数:
  TRANSCRIPT_LIMIT        1 回の実行で生成する最大本数（既定 15）
  TRANSCRIPT_MAX_MINUTES  この分数を超えたら新規生成を打ち切る（既定 300）
  TRANSCRIPT_SCAN_LIMIT   走査する銘柄数の上限。0 で全件（既定 0）
"""
import os
import sys
import json
import time
import random

from defeatbeta_api.data.ticker import Ticker
from generate_transcript_report import (
    generate_transcript_report,
    load_transcript_index,
    get_r2_client,
    REPO_ROOT,
)

LOCAL_STOCKS_PATH = os.path.join(
    REPO_ROOT, "stock-blog", "src", "data", "stocks.json"
)
STOCKS_R2_KEY = "reports/stocks.json"


def load_symbols():
    """対象銘柄リストを取得する。R2 の reports/stocks.json を正とし、
    取得できなければリポジトリ同梱の stocks.json にフォールバックする。"""
    client = get_r2_client()
    if client:
        bucket = os.getenv("R2_BUCKET_NAME", "stock-data-c1")
        try:
            obj = client.get_object(Bucket=bucket, Key=STOCKS_R2_KEY)
            data = json.loads(obj["Body"].read().decode("utf-8"))
            syms = [s.get("Symbol_YF") for s in data if s.get("Symbol_YF")]
            if syms:
                print(f"R2 {STOCKS_R2_KEY} から {len(syms)} 銘柄を取得")
                return syms
        except Exception as e:
            print(f"R2 stocks.json を取得できませんでした（ローカルへ）: {e}")
    with open(LOCAL_STOCKS_PATH, encoding="utf-8") as f:
        data = json.load(f)
    syms = [s.get("Symbol_YF") for s in data if s.get("Symbol_YF")]
    print(f"ローカル stocks.json から {len(syms)} 銘柄を取得")
    return syms


def latest_period(symbol):
    """defeatbeta から銘柄の最新トランスクリプトの (fy, fq) を返す。無ければ None。"""
    try:
        transcripts = Ticker(symbol).earning_call_transcripts()
        lst = transcripts.get_transcripts_list()
        if lst is None or lst.empty:
            return None
        latest = lst.sort_values(["fiscal_year", "fiscal_quarter"]).iloc[-1]
        return int(latest["fiscal_year"]), int(latest["fiscal_quarter"])
    except Exception as e:
        print(f"  [{symbol}] トランスクリプト一覧の取得に失敗: {e}")
        return None


def main():
    limit = int(os.getenv("TRANSCRIPT_LIMIT", "15"))
    max_minutes = float(os.getenv("TRANSCRIPT_MAX_MINUTES", "300"))
    scan_limit = int(os.getenv("TRANSCRIPT_SCAN_LIMIT", "0"))
    started = time.time()

    def elapsed_min():
        return (time.time() - started) / 60

    symbols = load_symbols()
    # 走査開始位置をシャッフルし、長期的に全銘柄をカバーする
    # （走査上限を設けても特定銘柄に偏らないため）。
    random.shuffle(symbols)
    if scan_limit > 0:
        symbols = symbols[:scan_limit]

    index = load_transcript_index()
    print(
        f"--- 走査開始: {len(symbols)} 銘柄 / 生成上限 {limit} 本 "
        f"/ 時間上限 {max_minutes:.0f} 分 ---"
    )

    # 1) 未生成の最新四半期を探す
    todo = []
    skipped = 0
    for sym in symbols:
        if len(todo) >= limit:
            break
        if elapsed_min() > max_minutes:
            print("時間上限に達したため走査を打ち切ります。")
            break
        period = latest_period(sym)
        if period is None:
            continue
        fy, fq = period
        if any(
            e.get("fy") == fy and e.get("fq") == fq
            for e in index.get(sym, [])
        ):
            skipped += 1
            continue
        todo.append((sym, fy, fq))
        print(f"  [対象] {sym} FY{fy} Q{fq}")

    print(f"--- 生成対象 {len(todo)} 本 / 既出スキップ {skipped} 件 ---")

    # 2) 生成（索引・md は generate_transcript_report 内で R2 に保存される）
    generated = 0
    failed = 0
    for sym, fy, fq in todo:
        if elapsed_min() > max_minutes:
            print("時間上限に達したため生成を打ち切ります。")
            break
        try:
            generate_transcript_report(sym, fy, fq)
            generated += 1
        except Exception as e:
            print(f"[{sym} FY{fy} Q{fq}] 生成に失敗: {e}")
            failed += 1

    mins = elapsed_min()
    print(
        f"=== 完了: 生成 {generated} / 失敗 {failed} "
        f"/ 既出スキップ {skipped} / 所要 {mins:.1f} 分 ==="
    )

    # GitHub Actions のジョブサマリへ
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_path:
        try:
            with open(summary_path, "a", encoding="utf-8") as f:
                f.write("### 決算説明会トランスクリプト生成\n\n")
                f.write(f"- 生成: {generated}\n")
                f.write(f"- 失敗: {failed}\n")
                f.write(f"- 既出スキップ: {skipped}\n")
                f.write(f"- 所要時間: {mins:.1f} 分\n")
        except Exception:
            pass

    # 生成 0・失敗ありのときだけ非ゼロ終了（全部スキップは正常）
    if generated == 0 and failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
