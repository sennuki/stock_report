import * as fs from 'fs';
import * as path from 'path';
import { transformRawToReport } from '../src/utils/report-generator.ts';

async function testTransform() {
  console.log('--- Testing Transform: Raw JSON to Report JSON ---');
  
  const rawDataDir = path.join(process.cwd(), '../code/raw_data');
  if (!fs.existsSync(rawDataDir)) {
    console.error('Raw data directory not found at:', rawDataDir);
    return;
  }
  
  const files = fs.readdirSync(rawDataDir).filter(f => f.endsWith('_raw.json'));
  console.log(`Found ${files.length} raw files to transform.`);

  const reportsDir = path.join(process.cwd(), 'public/reports');
  if (!fs.existsSync(reportsDir)) fs.mkdirSync(reportsDir, { recursive: true });

  for (const file of files) {
    const symbol = file.replace('_raw.json', '');
    console.log(`\nProcessing: ${symbol}`);
    
    const rawData = JSON.parse(fs.readFileSync(path.join(rawDataDir, file), 'utf8'));
    
    // 加工ロジックを実行
    const metadata = {
      'Security': rawData.info?.longName || symbol,
      'GICS Sector': rawData.info?.sector || 'Unknown',
      'GICS Sub-Industry': rawData.info?.industry || 'Unknown'
    };
    
    const report = transformRawToReport(rawData, metadata);
    
    console.log(`- Security: ${report.security}`);
    console.log(`- DCF: ${report.dcf_valuation ? 'OK' : 'N/A'}`);
    
    fs.writeFileSync(path.join(reportsDir, `${symbol}.json`), JSON.stringify(report, null, 2));
  }
  
  console.log('\n--- All transformations completed! ---');
}

testTransform().catch(console.error);
