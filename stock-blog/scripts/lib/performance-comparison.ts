import { yahooFinance } from './risk-analysis.ts';

/**
 * 対象銘柄、セクターETF、S&P 500の累積リターン比較データを期間別に生成
 */
export async function generatePerformanceChartData(symbol: string, sectorEtfSymbol: string) {
  const spySymbol = 'SPY';
  const symbols = [symbol, sectorEtfSymbol, spySymbol];
  
  // 銘柄名の中に括弧 () を使わないようにして、期間指定の括弧と干渉するのを防ぐ
  const labels: Record<string, string> = { 
    [symbol]: symbol, 
    [sectorEtfSymbol]: `Sector ${sectorEtfSymbol}`, 
    [spySymbol]: 'S&P 500' 
  };
  
  const colors: Record<string, string> = { 
    [symbol]: '#ff6b01', 
    [sectorEtfSymbol]: '#006cac', 
    [spySymbol]: '#22c55e' 
  };

  const periods = [
    { label: '1ヶ月', days: 30 },
    { label: '3ヶ月', days: 91 },
    { label: '6ヶ月', days: 182 },
    { label: '年初来', days: 'ytd' },
    { label: '1年', days: 365 },
    { label: '3年', days: 365 * 3 },
    { label: '5年', days: 365 * 5 },
    { label: '10年', days: 365 * 10 }
  ];

  try {
    // 10年分のデータを一括取得
    const results = await Promise.all(
      symbols.map(s => 
        yahooFinance.chart(s, { period1: new Date(Date.now() - 3650 * 24 * 60 * 60 * 1000), interval: '1d' })
          .catch(() => null)
      )
    );

    const validResults = results.filter(r => r && r.quotes && r.quotes.length > 0);
    if (validResults.length === 0) return null;

    const datasets: any[] = [];
    const now = new Date();

    periods.forEach(p => {
      let startDate: Date;
      if (p.days === 'ytd') {
        startDate = new Date(now.getFullYear(), 0, 1);
      } else {
        startDate = new Date(now.getTime() - (p.days as number) * 24 * 60 * 60 * 1000);
      }

      for (let i = 0; i < symbols.length; i++) {
        const res = results[i];
        if (!res || !res.quotes) continue;

        const quotes = res.quotes.filter((q: any) => 
          q.close !== null && q.date >= startDate
        ) as { date: Date, close: number }[];

        if (quotes.length < 2) continue;

        const firstPrice = quotes[0].close;
        
        // データの軽量化（間引きロジック）
        let samplingInterval = 1;
        if (p.label === '3年' || p.label === '5年') samplingInterval = 3; // 3日おき
        if (p.label === '10年') samplingInterval = 7; // 1週間おき

        const sampledQuotes = quotes.filter((_, idx) => idx % samplingInterval === 0);
        // 最後のデータポイントは必ず含める
        if (quotes.length > 0 && (quotes.length - 1) % samplingInterval !== 0) {
          sampledQuotes.push(quotes[quotes.length - 1]);
        }

        const dates = sampledQuotes.map(q => q.date.toISOString().split('T')[0]);
        const values = sampledQuotes.map(q => (q.close / firstPrice) - 1);

        datasets.push({
          name: `${labels[symbols[i]]} (${p.label})`,
          x: dates,
          y: values,
          type: 'scatter',
          mode: 'lines',
          line: { color: colors[symbols[i]], width: 2 },
          visible: p.label === '1年',
          _periodVisible: p.label === '1年'
        });
      }
    });

    return {
      data: datasets,
      layout: {
        xaxis: { title: '日付' },
        yaxis: { title: '累積リターン', tickformat: '.0%' },
        hovermode: 'x unified',
        legend: { orientation: 'h', y: -0.2 }
      }
    };
  } catch (e) {
    console.error(`Error generating performance chart for ${symbol}:`, e);
    return null;
  }
}
