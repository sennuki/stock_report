import { yahooFinance } from './risk-analysis.ts';

/**
 * 対象銘柄、セクターETF、S&P 500の累積リターン比較データを生成
 */
export async function generatePerformanceChartData(symbol: string, sectorEtfSymbol: string) {
  const symbols = [symbol, sectorEtfSymbol, '^GSPC'];
  const labels: Record<string, string> = { [symbol]: symbol, [sectorEtfSymbol]: sectorEtfSymbol, '^GSPC': 'S&P 500' };
  const colors: Record<string, string> = { [symbol]: '#ff6b01', [sectorEtfSymbol]: '#006cac', '^GSPC': '#22c55e' };

  try {
    const results = await Promise.all(
      symbols.map(s => 
        yahooFinance.chart(s, { period1: '2020-01-01', interval: '1d' })
          .catch(() => null)
      )
    );

    const datasets = [];
    let commonDates: string[] = [];

    for (let i = 0; i < symbols.length; i++) {
      const res = results[i];
      if (!res || !res.quotes || res.quotes.length === 0) continue;

      const quotes = res.quotes.filter((q: any) => q.close !== null) as { date: Date, close: number }[];
      if (quotes.length === 0) continue;

      const firstPrice = quotes[0].close;
      const data = quotes.map(q => ({
        x: q.date.toISOString().split('T')[0],
        y: (q.close / firstPrice) - 1
      }));

      datasets.push({
        label: labels[symbols[i]],
        data: data.map(d => d.y),
        dates: data.map(d => d.x),
        borderColor: colors[symbols[i]],
        backgroundColor: 'transparent',
        borderWidth: 2,
        pointRadius: 0
      });

      if (commonDates.length === 0 || data.length < commonDates.length) {
        commonDates = data.map(d => d.x);
      }
    }

    // 簡易的に Plotly 形式の構造を返す
    return {
      data: datasets.map(ds => ({
        x: ds.dates,
        y: ds.data,
        name: ds.label,
        type: 'scatter',
        mode: 'lines',
        line: { color: ds.borderColor, width: 2 }
      })),
      layout: {
        xaxis: { title: '日付' },
        yaxis: { title: '累積リターン', tickformat: '.0%' },
        hovermode: 'x unified'
      }
    };
  } catch (e) {
    console.error(`Error generating performance chart for ${symbol}:`, e);
    return null;
  }
}
