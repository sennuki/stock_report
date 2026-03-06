const fs = require('fs');
const html = fs.readFileSync('test_output.html', 'utf8');
const match = html.match(/class="apexcharts-legend-series"[^>]*>/g);
console.log(match);
