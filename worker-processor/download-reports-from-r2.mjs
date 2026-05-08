/**
 * Cloudflare REST API を使って R2 の reports/*.json を
 * ../stock-blog/public/reports/ にダウンロードする。
 *
 * Astro [symbol].astro は getStaticPaths でビルド時にローカル
 * public/reports/{symbol}.json を読む。S&P 500 のレポートは git に
 * コミット済みだが、S&P 400/600 のレポートはコミットされていない
 * ため、毎回 R2 から取り直してビルドに含める必要がある。
 *
 * 既にローカルに同名ファイルがある場合も上書きする（最新の R2 内容で
 * 更新するため）。stocks.json をソースに対象シンボルを決めることで、
 * R2 list のページング問題を回避する。
 */
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dir = dirname(fileURLToPath(import.meta.url));

const ACCOUNT_ID = process.env.CLOUDFLARE_ACCOUNT_ID;
const API_TOKEN  = process.env.CLOUDFLARE_API_TOKEN;
const BUCKET     = process.env.R2_BUCKET_NAME || 'stock-data-c1';
const STOCKS_JSON = join(__dir, '..', 'stock-blog', 'src', 'data', 'stocks.json');
const OUT_DIR     = join(__dir, '..', 'stock-blog', 'public', 'reports');
const CONCURRENCY = 30;

if (!ACCOUNT_ID || !API_TOKEN) {
  console.error('CLOUDFLARE_ACCOUNT_ID / CLOUDFLARE_API_TOKEN が未設定です');
  process.exit(1);
}

const stocksRaw = await readFile(STOCKS_JSON, 'utf-8');
const stocks = JSON.parse(stocksRaw);
const symbols = stocks
  .map(s => s.Symbol_YF || s.Symbol)
  .filter(Boolean);

console.log(`stocks.json: ${symbols.length} symbols`);

await mkdir(OUT_DIR, { recursive: true });

async function downloadOne(symbol) {
  const key = `reports/${symbol}.json`;
  const url = `https://api.cloudflare.com/client/v4/accounts/${ACCOUNT_ID}/r2/buckets/${BUCKET}/objects/${encodeURIComponent(key)}`;
  const resp = await fetch(url, {
    headers: { 'Authorization': `Bearer ${API_TOKEN}` },
  });
  if (resp.status === 404) return { symbol, status: 'missing' };
  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`HTTP ${resp.status}: ${txt.slice(0, 200)}`);
  }
  const body = await resp.text();
  await writeFile(join(OUT_DIR, `${symbol}.json`), body);
  return { symbol, status: 'ok' };
}

let ok = 0, missing = 0, failed = 0;
for (let i = 0; i < symbols.length; i += CONCURRENCY) {
  const batch = symbols.slice(i, i + CONCURRENCY);
  const results = await Promise.allSettled(batch.map(downloadOne));
  for (const r of results) {
    if (r.status === 'fulfilled') {
      if (r.value.status === 'ok') ok++;
      else missing++;
    } else {
      failed++;
      process.stderr.write(`\n  ✗ ${r.reason.message}`);
    }
  }
  process.stdout.write(`\r  ${ok + missing + failed}/${symbols.length} (ok=${ok} missing=${missing} failed=${failed})`);
}

console.log(`\n完了: 成功=${ok}, R2に未生成=${missing}, 失敗=${failed}`);

if (ok === 0) {
  console.error('1件もダウンロードできませんでした。R2 / 認証 / Worker /batch を確認してください。');
  process.exit(1);
}
