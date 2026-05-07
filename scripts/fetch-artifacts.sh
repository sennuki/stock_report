#!/bin/bash
set -e

# 最新のワークフロー実行から成果物をダウンロード
# GitHub CLI (gh) を使用して、stock-raw-data という名前のアーティファクトを取得します
echo "🚀 GitHub Actions から最新の生データ成果物をダウンロードしています..."
gh run download --name stock-raw-data --dir code/data/raw_data/

# stocks.json も必要であれば別途取得するか、main.py を実行して生成してください。
echo "💡 stocks.json を更新するには、'python code/main.py' を実行してください（データ取得はスキップ可能）。"

echo "✨ 完了しました。"
