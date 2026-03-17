#!/bin/bash
set -e

# 最新のワークフロー実行から成果物をダウンロード
# GitHub CLI (gh) を使用して、stock-reports-json という名前のアーティファクトを取得します
echo "🚀 GitHub Actions から最新の成果物をダウンロードしています..."
gh run download --name stock-reports-json --dir stock-blog/

# ダウンロードされたファイルを確認
if [ -f "stock-blog/src/data/stocks.json" ]; then
  echo "✅ stock-blog/src/data/stocks.json を更新しました。"
fi

if [ -d "stock-blog/public/output_reports_full" ]; then
  echo "✅ stock-blog/public/output_reports_full ディレクトリを更新しました。"
fi

echo "✨ 完了しました。"
