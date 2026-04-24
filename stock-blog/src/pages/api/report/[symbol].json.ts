import type { APIRoute } from 'astro';
import { transformRawToReport } from '@/utils/report-generator';

export const GET: APIRoute = async ({ params, locals }) => {
  const { symbol } = params;
  const runtime = (locals as any).runtime;

  if (!runtime || !runtime.env.STOCK_DATA) {
    return new Response(JSON.stringify({ error: 'R2 bucket not configured' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  try {
    const object = await runtime.env.STOCK_DATA.get(`raw/${symbol}.json`);
    if (!object) {
      return new Response(JSON.stringify({ error: 'Stock data not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    const content = await object.text();
    // NaNをnullに置換
    const rawData = JSON.parse(content.replace(/\bNaN\b/g, "null"));
    
    // レポート形式に変換
    const reportData = transformRawToReport(rawData);

    return new Response(JSON.stringify(reportData), {
      status: 200,
      headers: { 
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=3600' // 1時間キャッシュ
      }
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: (e as Error).message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
};
