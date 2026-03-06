import sys

with open('stock-blog/src/components/report/ApexChart.astro', 'r') as f:
    content = f.read()

start_idx = content.find('  function setupControls(wrapper: HTMLElement, chart: ApexCharts, plotlyData: any) {')
end_idx = content.find('  function transformPlotlyToApex(plotlyData: any, isDark: boolean) {')

old_func = content[start_idx:end_idx]

new_func = """  function setupControls(wrapper: HTMLElement, chart: ApexCharts, plotlyData: any) {
    const controlsContainer = wrapper.querySelector('.chart-controls');
    if (!controlsContainer || !plotlyData.layout?.updatemenus) return;
    controlsContainer.innerHTML = '';
    const fullSeries = (chart as any).w.config._fullSeries || [];
    const isDark = document.documentElement.getAttribute('data-theme') === 'dark';

    const getRange = (obj: any, axis: string) => {
      if (!obj) return null;
      if (obj[`${axis}.range`]) return obj[`${axis}.range`];
      if (obj[axis] && obj[axis].range) return obj[axis].range;
      return null;
    };

    const isScatterChart = plotlyData.data && plotlyData.data.some((d: any) => d.type === 'scatter' && d.mode && d.mode.includes('markers')) && !plotlyData.data.some((d: any) => d.type === 'bar');

    const updateChartData = (btn: any) => {
      if (btn.args && btn.args[0] && (btn.args[0].visible || btn.args[0].method === 'update')) {
        const visMask = btn.args[0].visible;
        const layoutUpdate = btn.args[1] || {};
        const filtered = fullSeries.filter((s: any) => !visMask || visMask[s.originalIndex] === true);
        const isScatter = filtered.some((s:any) => s.type === 'scatter');
        
        let cats = [];
        if (!isScatter && filtered.length > 0) {
          cats = plotlyData.data[filtered[0].originalIndex]?.x || [];
        }

        let xaxisOptions: any = isScatter ? { type: 'numeric', categories: [] } : { categories: cats, type: 'category' };
        const xRange = getRange(layoutUpdate, 'xaxis');
        if (isScatter) {
           const allX = filtered.flatMap((s:any) => s.data.map((p:any) => p.x));
           xaxisOptions.min = 0;
           xaxisOptions.max = xRange ? xRange[1] : (allX.length > 0 ? Math.max(...allX) : undefined);
        }

        const yRange = getRange(layoutUpdate, 'yaxis');

        chart.updateOptions({
          series: filtered,
          colors: filtered.map((s: any) => s.color),
          xaxis: xaxisOptions,
          annotations: isScatter ? {
            position: 'back',
            yaxis: [{
              y: 0,
              borderColor: isDark ? 'rgba(156, 163, 175, 0.4)' : 'rgba(107, 114, 128, 0.4)',
              strokeDashArray: 0,
              borderWidth: 2,
              label: { show: false }
            }]
          } : { yaxis: [] },
          yaxis: filtered.map((s: any, i: number) => {
            const is2 = s.yaxisIndex === 1;
            const first0 = filtered.findIndex((fs:any) => fs.yaxisIndex === 0);
            const first1 = filtered.findIndex((fs:any) => fs.yaxisIndex === 1);
            let yAxisOpt: any = {
              seriesName: is2 ? (first1 !== -1 ? filtered[first1].name : s.name) : (first0 !== -1 ? filtered[first0].name : s.name),
              show: i === first0 || i === first1,
              opposite: is2,
              labels: { formatter: (v: any) => formatYValueAbbreviated(v, is2 || isScatter), style: { colors: isDark ? '#9ca3af' : '#6b7280' } }
            };
            if (isScatter && !is2) {
               const allY = filtered.flatMap((s:any) => s.data.map((p:any) => p.y));
               const dataMinY = allY.length > 0 ? Math.min(...allY) : 0;
               const dataMaxY = allY.length > 0 ? Math.max(...allY) : 0.2;

               yAxisOpt.min = yRange ? Math.min(yRange[0], -1.0) : Math.min(dataMinY, -1.0);
               yAxisOpt.max = yRange ? yRange[1] : dataMaxY;
               
               yAxisOpt.forceNiceScale = false;
               yAxisOpt.tickAmount = 8;
            }
            return yAxisOpt;
          })
        }, false, true);
      }
    };

    if (isScatterChart) {
      const select = document.createElement('select');
      select.className = "sm:hidden px-4 py-2 text-sm font-medium rounded-md border bg-background text-foreground border-border outline-none focus:ring-2 focus:ring-accent w-[200px] text-center mb-4";
      
      const btnGroup = document.createElement('div');
      btnGroup.className = "hidden sm:flex justify-center gap-2 mb-4";

      const buttons: HTMLButtonElement[] = [];

      plotlyData.layout.updatemenus[0].buttons.forEach((btn: any, btnIdx: number) => {
        const option = document.createElement('option');
        option.value = btnIdx.toString();
        option.textContent = btn.label;
        select.appendChild(option);

        const button = document.createElement('button');
        button.textContent = btn.label;
        button.className = `px-4 py-1.5 text-sm font-medium rounded-md border transition-colors ${btnIdx === 0 ? 'bg-foreground text-white border-foreground' : 'bg-background text-foreground border-border hover:border-accent'}`;
        buttons.push(button);
        btnGroup.appendChild(button);

        button.onclick = () => {
          select.value = btnIdx.toString();
          buttons.forEach(b => {
            b.classList.remove('bg-foreground', 'text-white', 'border-foreground');
            b.classList.add('bg-background', 'text-foreground', 'border-border');
          });
          button.classList.add('bg-foreground', 'text-white', 'border-foreground');
          updateChartData(btn);
        };
      });

      select.onchange = (e) => {
        const idx = parseInt((e.target as HTMLSelectElement).value, 10);
        buttons[idx].click();
      };

      // Ensure select defaults to the initial active button. Wait, default is btnIdx === 0
      select.value = "0";

      // Wrap in a div to center the select on mobile
      const selectWrapper = document.createElement('div');
      selectWrapper.className = "w-full flex justify-center sm:hidden";
      selectWrapper.appendChild(select);

      controlsContainer.appendChild(selectWrapper);
      controlsContainer.appendChild(btnGroup);
    } else {
      plotlyData.layout.updatemenus[0].buttons.forEach((btn: any, btnIdx: number) => {
        const button = document.createElement('button');
        button.textContent = btn.label;
        button.className = `px-4 py-1.5 text-sm font-medium rounded-md border transition-colors ${btnIdx === 0 ? 'bg-foreground text-white border-foreground' : 'bg-background text-foreground border-border hover:border-accent'}`;
        
        button.onclick = () => {
          controlsContainer.querySelectorAll('button').forEach(b => {
            b.classList.remove('bg-foreground', 'text-white', 'border-foreground');
            b.classList.add('bg-background', 'text-foreground', 'border-border');
          });
          button.classList.add('bg-foreground', 'text-white', 'border-foreground');
          updateChartData(btn);
        };
        controlsContainer.appendChild(button);
      });
    }
  }
"""

with open('stock-blog/src/components/report/ApexChart.astro', 'w') as f:
    f.write(content.replace(old_func, new_func + "\n"))

