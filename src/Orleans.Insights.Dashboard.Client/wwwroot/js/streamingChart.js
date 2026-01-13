// Real-time streaming chart using chartjs-plugin-streaming
// The plugin handles all animation/scrolling - we just push data in onRefresh
// Y-axis scaling is handled manually since the streaming plugin doesn't auto-recalculate bounds

window.streamingChart = {
    charts: new Map(),
    dotNetRefs: new Map(),
    destroying: new Set(), // Track charts being destroyed to prevent race conditions

    // Calculate nice max value for axis scaling (like OrleansDashboard's Chart.js approach)
    calculateNiceMax: function(maxValue) {
        if (maxValue <= 0) return 1;

        // Find the order of magnitude
        const magnitude = Math.pow(10, Math.floor(Math.log10(maxValue)));
        const normalized = maxValue / magnitude;

        // Round up to nice numbers: 1, 2, 2.5, 5, 10
        let nice;
        if (normalized <= 1) nice = 1;
        else if (normalized <= 2) nice = 2;
        else if (normalized <= 2.5) nice = 2.5;
        else if (normalized <= 5) nice = 5;
        else nice = 10;

        return nice * magnitude;
    },

    // Update Y-axis scale based on visible data
    updateYScales: function(chart, canvasId) {
        if (!chart || !chart.data || !chart.data.datasets) return;

        const now = Date.now();
        const duration = 60000; // Match the realtime duration
        const cutoff = now - duration;

        let maxY = 0;
        let maxY2 = 0;

        // Find max values in visible data for each axis
        chart.data.datasets.forEach((dataset, idx) => {
            if (!dataset.data) return;
            dataset.data.forEach(point => {
                if (point.x >= cutoff) {
                    if (idx < 2) { // Requests and Failed are on 'y' axis
                        maxY = Math.max(maxY, point.y);
                    } else { // Latency is on 'y2' axis
                        maxY2 = Math.max(maxY2, point.y);
                    }
                }
            });
        });

        // Calculate nice max values with some headroom
        const niceMaxY = this.calculateNiceMax(maxY * 1.1);
        const niceMaxY2 = this.calculateNiceMax(maxY2 * 1.1);

        // Update scales if they've changed significantly
        if (chart.options.scales.y.max !== niceMaxY) {
            chart.options.scales.y.max = niceMaxY;
            chart.options.scales.y.min = 0;
        }
        if (chart.options.scales.y2.max !== niceMaxY2) {
            chart.options.scales.y2.max = niceMaxY2;
            chart.options.scales.y2.min = 0;
        }
    },

    // Create a streaming chart with onRefresh callback to fetch data
    create: function (canvasId, dotNetRef, methodKey) {
        // Don't create if already being destroyed
        if (this.destroying.has(canvasId)) return false;

        const canvas = document.getElementById(canvasId);
        if (!canvas) return false;

        // Check if canvas is still in DOM and has valid dimensions
        if (!canvas.parentNode || canvas.offsetWidth === 0) return false;

        this.destroy(canvasId);

        const ctx = canvas.getContext('2d');
        if (!ctx) return false;

        // Register streaming plugin once
        if (typeof ChartStreaming !== 'undefined' && !Chart.registry.plugins.get('streaming')) {
            Chart.register(ChartStreaming);
        }

        // Store .NET reference for this chart
        this.dotNetRefs.set(canvasId, { ref: dotNetRef, key: methodKey });

        const self = this;

        try {
            const chart = new Chart(ctx, {
                type: 'line',
                data: {
                    datasets: [
                        {
                            label: 'Requests/s',
                            data: [],
                            borderColor: 'rgba(120, 57, 136, 1)',
                            backgroundColor: 'rgba(120, 57, 136, 0.3)',
                            fill: true,
                            tension: 0.3,
                            borderWidth: 2,
                            pointRadius: 0,
                            yAxisID: 'y'
                        },
                        {
                            label: 'Failed',
                            data: [],
                            borderColor: 'rgba(239, 68, 68, 1)',
                            backgroundColor: 'rgba(239, 68, 68, 0.5)',
                            fill: true,
                            tension: 0.3,
                            borderWidth: 1,
                            pointRadius: 0,
                            yAxisID: 'y'
                        },
                        {
                            label: 'Latency (ms)',
                            data: [],
                            borderColor: 'rgba(236, 151, 31, 1)',
                            backgroundColor: 'transparent',
                            fill: false,
                            tension: 0.3,
                            borderWidth: 1.5,
                            borderDash: [4, 2],
                            pointRadius: 0,
                            yAxisID: 'y2'
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    animation: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: { enabled: false }
                    },
                    scales: {
                        x: {
                            type: 'realtime',
                            realtime: {
                                duration: 60000,  // Show 60 seconds of data
                                refresh: 1000,    // Fetch new data every second
                                delay: 1000,      // 1 second delay for data arrival
                                ttl: 70000,       // Keep data for 70 seconds
                                frameRate: 30,
                                // This is THE callback - plugin calls this every 'refresh' ms
                                onRefresh: function(chart) {
                                    // Guard against destroyed charts
                                    if (!chart || !chart.ctx || self.destroying.has(canvasId)) return;

                                    const info = self.dotNetRefs.get(canvasId);
                                    if (!info || !info.ref) return;

                                    // Call .NET to get the latest data point
                                    info.ref.invokeMethodAsync('GetLatestDataPoint')
                                        .then(data => {
                                            // Guard again after async - chart may have been destroyed
                                            if (!chart || !chart.ctx || self.destroying.has(canvasId)) return;

                                            // Always add point - .NET returns zeros when no activity
                                            if (data) {
                                                const now = Date.now();
                                                chart.data.datasets[0].data.push({ x: now, y: data.requests || 0 });
                                                chart.data.datasets[1].data.push({ x: now, y: data.failed || 0 });
                                                chart.data.datasets[2].data.push({ x: now, y: data.latency || 0 });

                                                // Recalculate Y-axis scales based on visible data
                                                self.updateYScales(chart, canvasId);
                                            }
                                        })
                                        .catch(() => { /* Component disposed */ });
                                }
                            },
                            ticks: {
                                display: true,
                                maxRotation: 0,
                                autoSkip: false,
                                font: { size: 9 },
                                color: '#888',
                                // Show labels only at 30-second intervals
                                callback: function(value, index, ticks) {
                                    const date = new Date(value);
                                    if (date.getSeconds() % 30 === 0) {
                                        return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
                                    }
                                    return null;
                                }
                            },
                            grid: { display: false }
                        },
                        y: {
                            type: 'linear',
                            position: 'left',
                            // Manual scaling: min/max are updated dynamically by updateYScales()
                            min: 0,
                            max: 1,  // Initial value, will be recalculated
                            ticks: {
                                font: { size: 9 },
                                color: '#888',
                                maxTicksLimit: 5,
                                callback: function(v) {
                                    if (v >= 1000) return (v / 1000).toFixed(1) + 'k';
                                    if (v >= 1) return Math.round(v);
                                    if (v > 0) return v.toFixed(1);
                                    return '0';
                                }
                            },
                            grid: {
                                color: 'rgba(255,255,255,0.05)',
                                drawBorder: false
                            }
                        },
                        y2: {
                            type: 'linear',
                            position: 'right',
                            // Manual scaling: min/max are updated dynamically by updateYScales()
                            min: 0,
                            max: 1,  // Initial value, will be recalculated
                            ticks: {
                                font: { size: 9 },
                                color: 'rgba(236, 151, 31, 0.8)',
                                maxTicksLimit: 5,
                                callback: function(v) {
                                    if (v >= 1000) return (v / 1000).toFixed(1) + 's';
                                    return Math.round(v) + 'ms';
                                }
                            },
                            grid: { display: false }
                        }
                    }
                }
            });

            this.charts.set(canvasId, chart);
            return true;
        } catch (e) {
            console.warn('Failed to create streaming chart:', e);
            this.dotNetRefs.delete(canvasId);
            return false;
        }
    },

    // Load historical data points (backfill the chart)
    loadHistorical: function (canvasId, dataPoints) {
        const chart = this.charts.get(canvasId);
        if (!chart || !chart.ctx || !dataPoints || dataPoints.length === 0) return false;
        if (this.destroying.has(canvasId)) return false;

        const now = Date.now();
        const count = dataPoints.length;

        // Clear existing data first
        chart.data.datasets.forEach(ds => ds.data.length = 0);

        // Add historical points with timestamps going backwards from now
        for (let i = 0; i < count; i++) {
            const point = dataPoints[i];
            // Points are oldest to newest, so first point is (count-1) seconds ago
            const timestamp = now - (count - 1 - i) * 1000;
            chart.data.datasets[0].data.push({ x: timestamp, y: point.requests });
            chart.data.datasets[1].data.push({ x: timestamp, y: point.failed });
            chart.data.datasets[2].data.push({ x: timestamp, y: point.latency });
        }

        // Calculate proper Y-axis scales for historical data
        this.updateYScales(chart, canvasId);

        // Force update without animation
        try {
            chart.update('none');
        } catch (e) {
            // Chart may have been destroyed during update
        }
        return true;
    },

    // Destroy chart and cleanup
    destroy: function (canvasId) {
        // Mark as destroying to prevent race conditions
        this.destroying.add(canvasId);

        const chart = this.charts.get(canvasId);
        if (chart) {
            try {
                // Stop the streaming plugin first by clearing realtime options
                if (chart.options && chart.options.scales && chart.options.scales.x && chart.options.scales.x.realtime) {
                    chart.options.scales.x.realtime.pause = true;
                }
                chart.destroy();
            } catch (e) {
                // Ignore errors during destruction
            }
            this.charts.delete(canvasId);
        }
        this.dotNetRefs.delete(canvasId);

        // Clear destroying flag after a short delay to allow pending callbacks to complete
        setTimeout(() => this.destroying.delete(canvasId), 100);
    },

    // Cleanup all charts
    destroyAll: function () {
        this.charts.forEach((chart, canvasId) => {
            this.destroying.add(canvasId);
            try {
                if (chart.options && chart.options.scales && chart.options.scales.x && chart.options.scales.x.realtime) {
                    chart.options.scales.x.realtime.pause = true;
                }
                chart.destroy();
            } catch (e) { }
        });
        this.charts.clear();
        this.dotNetRefs.clear();
        setTimeout(() => this.destroying.clear(), 100);
    }
};
