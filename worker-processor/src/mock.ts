import * as fs from 'fs';
import * as path from 'path';

export const mockEnv = {
  STOCK_DATA: {
    async list(options: any) {
      const prefix = options.prefix || 'raw/';
      const rawDir = path.join(__dirname, '../../code/raw_data');
      if (!fs.existsSync(rawDir)) return { objects: [] };
      
      const files = fs.readdirSync(rawDir);
      return {
        objects: files.map(f => ({ key: `raw/${f.replace('_raw', '')}` }))
      };
    },
    async get(key: string) {
      if (key === 'raw/stocks_list.json') {
        const p = path.join(__dirname, '../../code/raw_data/stocks_list.json');
        if (fs.existsSync(p)) return { text: async () => fs.readFileSync(p, 'utf-8') };
        return null;
      }
      
      if (key.startsWith('raw/')) {
        const symbol = key.replace('raw/', '').replace('.json', '');
        const rawDir = path.join(__dirname, '../../code/raw_data');
        const file = path.join(rawDir, `${symbol}_raw.json`);
        if (fs.existsSync(file)) {
          return { text: async () => fs.readFileSync(file, 'utf-8') };
        }
      }
      return null;
    },
    async put(key: string, data: any, options?: any) {
      const reportsDir = path.join(__dirname, '../../stock-blog/public/reports');
      if (!fs.existsSync(reportsDir)) {
        fs.mkdirSync(reportsDir, { recursive: true });
      }
      const file = path.join(reportsDir, key.replace('reports/', ''));
      fs.writeFileSync(file, data);
      console.log(`Mock R2 put: ${file}`);
    }
  },
  GEMINI_API_KEY: undefined // Disable Gemini API
};
