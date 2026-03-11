import urllib.request
import json
import zipfile
import io
import os
import sys
import shutil

# Configuration
REPO = "sennuki/stock_report"
TOKEN = "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"
ARTIFACT_NAME = "generated-reports"
BRANCH = "feature/workflow-test-no-deploy"
# Root of the project (parent of stock-blog)
BASE_DIR = "/home/linux/gemini/stock_report"

class GitHubRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        new_req = super().redirect_request(req, fp, code, msg, headers, newurl)
        if 'Authorization' in new_req.headers:
            del new_req.headers['Authorization']
        return new_req

def get_latest_run_id():
    url = f"https://api.github.com/repos/{REPO}/actions/runs?branch={BRANCH}&status=success&per_page=1"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
            runs = data.get("workflow_runs", [])
            if not runs:
                print(f"Error: No successful workflow runs found for branch {BRANCH}")
                sys.exit(1)
            return runs[0]["id"]
    except Exception as e:
        print(f"Error fetching runs: {e}")
        sys.exit(1)

def get_artifact_info(run_id):
    url = f"https://api.github.com/repos/{REPO}/actions/runs/{run_id}/artifacts"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
            artifacts = data.get("artifacts", [])
            for artifact in artifacts:
                if artifact["name"] == ARTIFACT_NAME:
                    return artifact["archive_download_url"], artifact["size_in_bytes"]
            print(f"Error: Artifact '{ARTIFACT_NAME}' not found in run {run_id}")
            sys.exit(1)
    except Exception as e:
        print(f"Error fetching artifacts: {e}")
        sys.exit(1)

def download_and_extract(url, total_size):
    print(f"Downloading artifact...")
    opener = urllib.request.build_opener(GitHubRedirectHandler)
    urllib.request.install_opener(opener)
    req = urllib.request.Request(url)
    req.add_header('Authorization', f'Bearer {TOKEN}')

    try:
        with urllib.request.urlopen(req) as response:
            buffer = io.BytesIO()
            downloaded = 0
            while True:
                chunk = response.read(64*1024)
                if not chunk: break
                buffer.write(chunk)
                downloaded += len(chunk)
                done = int(50 * downloaded / total_size)
                percent = 100 * downloaded / total_size
                sys.stdout.write(f"\r[{'=' * done}{' ' * (50-done)}] {percent:3.1f}% ({downloaded}/{total_size} bytes)")
                sys.stdout.flush()
            
            print("\nDownload complete. Extracting...")
            zip_content = buffer.getvalue()
            with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
                # アーティファクト内の各ファイルをルート(BASE_DIR)に対して展開
                for info in z.infolist():
                    target_path = os.path.join(BASE_DIR, info.filename)
                    # 既存のファイルがあったら削除して確実に上書き
                    if os.path.exists(target_path) and not info.is_dir():
                        os.remove(target_path)
                    z.extract(info, BASE_DIR)
                    # 更新日時を現在のシステム時刻に合わせる（OSの表示上の違和感をなくすため）
                    if not info.is_dir():
                        os.utime(target_path, None)

        print(f"Successfully extracted artifact to {BASE_DIR}")
    except Exception as e:
        print(f"\nError downloading/extracting artifact: {e}")
        sys.exit(1)

if __name__ == "__main__":
    print(f"Syncing reports from GitHub Actions ({BRANCH})...")
    run_id = get_latest_run_id()
    print(f"Found latest successful run: ID {run_id}")
    download_url, total_size = get_artifact_info(run_id)
    download_and_extract(download_url, total_size)
    print("\nSync complete! You can now run 'npm run dev' in stock-blog directory.")
