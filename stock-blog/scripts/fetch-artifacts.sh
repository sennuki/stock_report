#!/bin/bash
set -e

# プロジェクトのルートディレクトリ（.gitがある場所）を特定
ROOT_DIR=$(git rev-parse --show-toplevel)
BLOG_DIR="$ROOT_DIR/stock-blog"

# 作業ディレクトリを stock-blog に設定
cd "$BLOG_DIR"

echo "🚀 GitHub Actions から最新の成果物をダウンロードしています..."
echo "📍 作業ディレクトリ: $BLOG_DIR"

# アーティファクトの展開エラーを防ぐため、既存のファイルを一時的に削除
echo "🗑️  既存の成果物ファイルを整理中..."
rm -f src/data/stocks.json
rm -rf public/reports

# GitHub CLI (gh) を使用して、現在のディレクトリ (stock-blog/) に直接展開
# 注意: アーティファクトに public/reports が含まれていない場合はスキップ
gh run download --name stock-reports-json --dir .

# ダウンロードされたファイルを確認
if [ -f "src/data/stocks.json" ]; then
  echo "✅ stock-blog/src/data/stocks.json を更新しました。"
fi

if [ -d "public/reports" ]; then
  count=$(ls -1 public/reports | wc -l)
  echo "✅ stock-blog/public/reports ディレクトリを更新しました ($count 個のファイル)。"
else
  echo "⚠️  警告: public/reports ディレクトリが見つかりません。次回の GitHub Action 実行を待つ必要があるかもしれません。"
fi

echo "✨ 完了しました。"
