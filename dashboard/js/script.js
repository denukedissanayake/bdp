document.addEventListener('DOMContentLoaded', function () {
    // Chart.js global styling
    Chart.defaults.font.family = "'Outfit', sans-serif";
    Chart.defaults.color = '#64748b';
    Chart.defaults.scale.grid.color = 'rgba(0, 0, 0, 0.03)';

    // Load weather data
    fetch('data/weather_summary.json')
        .then(response => {
            if (!response.ok) throw new Error('Failed to load data');
            return response.json();
        })
        .then(data => {
            console.log("Dashboard Data Loaded:", data);
            initTopDistrictsChart(data.topDistricts);
            initPrecipTable(data.peakSeasonality);
            initTempHighChart(data.temperatureAnalysis);
            initExtremeWeatherChart(data.extremeWeather);
        })
        .catch(err => {
            console.error("Error loading dashboard data:", err);
            const tableBody = document.getElementById('precipTableBody');
            if (tableBody) {
                tableBody.innerHTML = '<tr><td colspan="3" style="color: #ff4d6d; text-align: center; padding: 2rem;">Error loading data. Run: python3 -m http.server 8000 in dashboard folder</td></tr>';
            }
        });

    // 1. Top 5 Districts by Total Precipitation (Horizontal Bar Chart)
    function initTopDistrictsChart(data) {
        const ctx = document.getElementById('topDistrictsChart').getContext('2d');
        new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    label: 'Total Precipitation (Hours)',
                    data: data.data,
                    backgroundColor: ['#4361ee', '#3a0ca3', '#7209b7', '#f72585', '#4cc9f0'],
                    borderRadius: 6,
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { x: { beginAtZero: true } }
            }
        });
    }

    // 2. Peak Precipitation per District (Table)
    function initPrecipTable(peakData) {
        const tableBody = document.getElementById('precipTableBody');
        tableBody.innerHTML = "";

        peakData.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${row.district}</td>
                <td><span class="highlight">${row.month}</span></td>
                <td>${row.averagePrecipitation} hrs</td>
            `;
            tableBody.appendChild(tr);
        });
    }

    // 3a. Temperature Analysis - Overall (Doughnut Chart)
    function initTempHighChart(data) {
        const ctx = document.getElementById('tempHighChart').getContext('2d');
        new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: ['> 30°C Months', '< 30°C Months'],
                datasets: [{
                    data: [data.overall.above30, data.overall.below30],
                    backgroundColor: ['#ff4d6d', '#e2e8f0'],
                    borderWidth: 0,
                    hoverOffset: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                cutout: '75%',
                plugins: { legend: { position: 'bottom' } }
            }
        });

        initTempYearlyChart(data);
    }

    // 3b. Temperature Analysis - By Year (Line Chart)
    function initTempYearlyChart(data) {
        const ctx = document.getElementById('tempYearlyChart').getContext('2d');

        if (!data.byYear || Object.keys(data.byYear).length === 0) return;

        const years = Object.keys(data.byYear);
        const percentages = Object.values(data.byYear);

        new Chart(ctx, {
            type: 'line',
            data: {
                labels: years,
                datasets: [{
                    label: '% Months > 30°C',
                    data: percentages,
                    borderColor: '#ff4d6d',
                    backgroundColor: 'rgba(255, 77, 109, 0.1)',
                    tension: 0.3,
                    fill: true,
                    pointBackgroundColor: '#fff',
                    pointBorderColor: '#ff4d6d',
                    pointBorderWidth: 2,
                    pointRadius: 5
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: function (context) {
                                return context.parsed.y + '% of months > 30°C';
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        max: 100,
                        ticks: { callback: function (value) { return value + '%'; } }
                    }
                }
            }
        });
    }

    // 4. Extreme Weather Events (Grouped Bar Chart by City and Year)
    function initExtremeWeatherChart(data) {
        const container = document.getElementById('extremeWeatherChart').parentElement;
        const canvas = document.getElementById('extremeWeatherChart');

        // Make container scrollable horizontally with proper size for all cities
        const widthPerCity = 250;
        const minWidth = data.cities.length * widthPerCity;
        container.style.overflowX = 'auto';
        container.style.overflowY = 'visible';
        container.style.height = '550px';
        canvas.style.width = minWidth + 'px';
        canvas.style.minWidth = minWidth + 'px';
        canvas.style.height = '520px';

        const ctx = canvas.getContext('2d');

        // Generate color palette for years
        const yearColors = [
            '#4361ee', '#3a0ca3', '#7209b7', '#f72585', '#4cc9f0',
            '#06d6a0', '#118ab2', '#ef476f', '#ffd166', '#073b4c',
            '#8338ec', '#ff6b6b', '#4ecdc4', '#45b7d1', '#96ceb4'
        ];

        // Create datasets for each year
        const datasets = data.years.map((year, index) => {
            const yearData = data.cities.map(city => data.data[city][year] || 0);
            return {
                label: year,
                data: yearData,
                backgroundColor: yearColors[index % yearColors.length],
                borderRadius: 2
            };
        });

        new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.cities,
                datasets: datasets
            },
            options: {
                responsive: false,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        position: 'top',
                        labels: { boxWidth: 15, padding: 10, font: { size: 11 } }
                    }
                },
                scales: {
                    x: {
                        stacked: false,
                        ticks: { font: { size: 11 } }
                    },
                    y: {
                        beginAtZero: true,
                        max: 80,
                        title: { display: true, text: 'Extreme Event Days' }
                    }
                }
            }
        });
    }
});
