import { processAllStocks } from './index';
import { mockEnv } from './mock';

processAllStocks(mockEnv as any)
  .then(() => console.log('Mock processing complete!'))
  .catch(e => console.error(e));
