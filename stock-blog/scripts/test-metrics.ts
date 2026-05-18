// import { fetchSp500Companies } from './lib/market-data.ts';
import { calculateRiskMetrics, yahooFinance } from './lib/risk-analysis.ts';

async function test() {
  console.log('--- Debugging yahooFinance object ---');
  console.log('Type of yahooFinance:', typeof yahooFinance);
  if (yahooFinance) {
    console.log('Available keys:', Object.keys(yahooFinance).slice(0, 10));
  }

  console.log('\n--- Testing Risk Metrics Calculation for MSFT ---');
  const metrics = await calculateRiskMetrics('MSFT');
  console.log('MSFT Metrics:', JSON.stringify(metrics, null, 2));

  console.log('\n--- Testing Quote Fetch for MSFT ---');
  try {
    const quote = await yahooFinance.quote('MSFT');
    console.log('MSFT Quote:', quote.symbol, quote.regularMarketPrice);
  } catch (e) {
    console.error('Quote fetch failed:', e);
  }
}

test().catch(console.error);
