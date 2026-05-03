/**
 * Cloudflare REST API を使って code/raw_data/*.json を R2 にアップロードする。
 * CLOUDFLARE_API_TOKEN と CLOUDFLARE_ACCOUNT_ID のみ必要（S3互換キー不要）。
 */
import { readdir, readFile } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dir = dirname(fileURLToPath(import.meta.url));

const ACCOUNT_ID = process.env.CLOUDFLARE_ACCOUNT_ID;
const API_TOKEN  = process.env.CLOUDFLARE_API_TOKEN;
const BUCKET     = process.env.R2_BUCKET_NAME || 'stock-data-c1';
const RAW_DIR    = join(__dir, '..', 'code', 'raw_data');
const CONCURRENCY = 20;

if (!ACCOUNT_ID || !API_TOKEN) {
  console.error('CLOUDFLARE_ACCOUNT_ID / CLOUDFLARE_API_TOKEN が未設定です');
  process.exit(1);
}

async function uploadFile(filename) {
  const symbol = filename.replace(/_raw\.json$/, '').replace(/\.json$/, '');
  const key    = `raw/${symbol}.json`;
  const body   = await readFile(join(RAW_DIR, filename));
  const url    = `https://api.cloudflare.com/client/v4/accounts/${ACCOUNT_ID}/r2/buckets/${BUCKET}/objects/${encodeURIComponent(key)}`;

  const resp = await fetch(url, {
    method: 'PUT',
    headers: { 'Authorization': `Bearer ${API_TOKEN}`, 'Content-Type': 'application/json' },
    body,
  });

  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`HTTP ${resp.status}: ${txt.slice(0, 300)}`);
  }
  return symbol;
}

const files = (await readdir(RAW_DIR).catch(() => [])).filter(f => f.endsWith('.json'));

if (files.length === 0) {
  console.log('raw_data/ にファイルがありません。スキップします。');
  process.exit(0);
}

console.log(`R2 へアップロード開始: ${files.length} ファイル (並列数=${CONCURRENCY})`);

let done = 0, failed = 0;
for (let i = 0; i < files.length; i += CONCURRENCY) {
  const batch   = files.slice(i, i + CONCURRENCY);
  const results = await Promise.allSettled(batch.map(uploadFile));
  for (const r of results) {
    if (r.status === 'fulfilled') done++;
    else { failed++; process.stderr.write(`\n  ✗ ${r.reason.message}`); }
  }
  process.stdout.write(`\r  ${done + failed}/${files.length} 完了 (失敗: ${failed})`);
}

console.log(`\n完了: 成功=${done}, 失敗=${failed}`);

// 全件失敗 = 認証エラーの可能性が高い → 終了コード1
if (done === 0 && failed > 0) process.exit(1);
