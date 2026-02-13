import fs from 'fs';
import path from 'path';

const INPUT_DIR = 'public/output_reports_full';
const OUTPUT_DIR = 'src/data/blog';
const AUTHOR = 'Gemini Stock Bot';

if (!fs.existsSync(OUTPUT_DIR)) {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

const files = fs.readdirSync(INPUT_DIR).filter(f => f.endsWith('.html'));
const now = new Date().toISOString();

console.log(`Found ${files.length} HTML reports. Generating blog posts...`);

files.forEach(file => {
  const ticker = path.basename(file, '.html');
  const filePath = path.join(INPUT_DIR, file);
  const htmlContent = fs.readFileSync(filePath, 'utf-8');

  // タイトルの抽出
  const titleMatch = htmlContent.match(/<title>(.*?)<\/title>/);
  let title = titleMatch ? titleMatch[1] : `銘柄分析レポート: ${ticker}`;
  // ダブルクォートをエスケープ
  title = title.replace(/"/g, '"');

  // ブログ記事（Markdown）の作成
  const mdContent = `---
author: ${AUTHOR}
pubDatetime: ${now}
title: "${title}"
postSlug: ${ticker.toLowerCase()}
featured: false
draft: false
tags:
  - stocks
  - ${ticker}
description: "${title}。最新の株価パフォーマンス、ファンダメンタルズ、テクニカル分析を網羅した詳細レポートです。"
---

<iframe src="/output_reports_full/${file}" style="width: 100%; height: 1000px; border: none;"></iframe>

[レポートを全画面で見る](/output_reports_full/${file})
`;

  fs.writeFileSync(path.join(OUTPUT_DIR, `${ticker}.md`), mdContent);
});

console.log('Successfully generated all blog posts.');
