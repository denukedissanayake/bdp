import re
import json
import os
from collections import defaultdict

# Configuration
scriptDir = os.path.dirname(os.path.abspath(__file__))
dataDir = os.path.join(scriptDir, '..', 'data')
inputFile = os.path.join(dataDir, 'total_precipitation_and_mean_temperature_result.txt')
extremeWeatherFile = os.path.join(dataDir, 'extream_weather_days')
outputFile = os.path.join(dataDir, 'weather_summary.json')

# Data parsing
def parseData(filePath):
    """Parse weather data from text file"""
    regex = r"(\w+) had a total precipitation of (\d+) hours with a mean temperature of (\d+) for (\d+)\w+ month \(Year: (\d+)\)"
    data = []
    
    if not os.path.exists(filePath):
        print(f"Error: File {filePath} not found.")
        return []

    with open(filePath, 'r') as f:
        for line in f:
            match = re.search(regex, line)
            if match:
                entry = {
                    'district': match.group(1),
                    'precipitation': float(match.group(2)),
                    'temperature': float(match.group(3)),
                    'month': int(match.group(4)),
                    'year': int(match.group(5))
                }
                data.append(entry)
    return data

# 1. Most precipitous month/season for each district
def calculatePeakMonths(data):
    """Calculate peak precipitation month per district with average values"""
    districtMonthlyData = defaultdict(lambda: defaultdict(list))
    
    for entry in data:
        districtMonthlyData[entry['district']][entry['month']].append(entry['precipitation'])
    
    results = []
    monthNames = ["", "January", "February", "March", "April", "May", "June", 
                   "July", "August", "September", "October", "November", "December"]
    
    for district, months in districtMonthlyData.items():
        monthAverages = {}
        for monthNum, precipList in months.items():
            monthAverages[monthNum] = sum(precipList) / len(precipList)
        
        bestMonth = max(monthAverages.items(), key=lambda x: x[1])
        monthIdx = bestMonth[0]

        results.append({
            "district": district,
            "month": monthNames[monthIdx],
            "averagePrecipitation": round(bestMonth[1], 2)
        })
    
    results.sort(key=lambda x: x['district'])
    return results

# 2. Top 5 districts based on total precipitation
def calculateTopDistricts(data):
    """Calculate top 5 districts by total precipitation"""
    districtPrecip = defaultdict(float)
    for entry in data:
        districtPrecip[entry['district']] += entry['precipitation']
    
    sortedDistricts = sorted(districtPrecip.items(), key=lambda x: x[1], reverse=True)[:5]
    
    return {
        "labels": [item[0] for item in sortedDistricts],
        "data": [item[1] for item in sortedDistricts]
    }

# 3. Percentage of months with mean temperature above 30°C
def calculateHighTempPercentage(data):
    """Calculate percentage of months with temperature > 30°C per year"""
    totalMonths = len(data)
    if totalMonths == 0: 
        return {"overall": {"above30": 0, "below30": 0}, "byYear": {}}

    # Overall calculation
    highTempCount = sum(1 for entry in data if entry['temperature'] > 30)
    percentHigh = (highTempCount / totalMonths) * 100
    percentLow = 100 - percentHigh
    
    # Per-year calculation
    yearData = defaultdict(lambda: {"total": 0, "above30": 0})
    for entry in data:
        year = entry['year']
        yearData[year]["total"] += 1
        if entry['temperature'] > 30:
            yearData[year]["above30"] += 1
    
    byYear = {}
    for year, counts in sorted(yearData.items()):
        pct = (counts["above30"] / counts["total"]) * 100 if counts["total"] > 0 else 0
        byYear[str(year)] = round(pct, 1)
    
    return {
        "overall": {
            "above30": round(percentHigh, 1),
            "below30": round(percentLow, 1)
        },
        "byYear": byYear
    }

# 4. Extreme weather events by city and year
def calculateExtremeWeather(filePath):
    """Parse extreme weather data - yearly breakdown per city for grouped bar chart"""
    cityYearData = defaultdict(dict)
    allYears = set()
    
    if not os.path.exists(filePath):
        print(f"Warning: Extreme weather file {filePath} not found.")
        return {"cities": [], "years": [], "data": {}}
    
    with open(filePath, 'r') as f:
        next(f)  # Skip header row
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 3:
                cityName = parts[0]
                year = parts[1]
                extremeDays = int(parts[2])
                cityYearData[cityName][year] = extremeDays
                allYears.add(year)
    
    # Sort cities alphabetically and years chronologically
    sortedCities = sorted(cityYearData.keys())
    sortedYears = sorted(allYears)
    
    # Build data structure for grouped bar chart
    data = {}
    for city in sortedCities:
        data[city] = {}
        for year in sortedYears:
            data[city][year] = cityYearData[city].get(year, 0)
    
    return {
        "cities": sortedCities,
        "years": sortedYears,
        "data": data
    }

# Main execution
def main():
    print(f"Reading data from {inputFile}...")
    rawData = parseData(inputFile)
    print(f"Parsed {len(rawData)} records.")
    
    if not rawData:
        print("No valid data found. Check regex or file format.")
        return

    # Calculate all metrics
    peakMonths = calculatePeakMonths(rawData)
    topFiveDistricts = calculateTopDistricts(rawData)
    tempAnalysis = calculateHighTempPercentage(rawData)
    extremeWeather = calculateExtremeWeather(extremeWeatherFile)
    
    # Build dashboard JSON
    dashboardData = {
        "peakSeasonality": peakMonths,
        "topDistricts": topFiveDistricts,
        "temperatureAnalysis": tempAnalysis,
        "extremeWeather": extremeWeather,
        "meta": {
            "totalRecords": len(rawData),
            "generatedAt": "Today"
        }
    }
    
    # Save to file
    os.makedirs(os.path.dirname(outputFile), exist_ok=True)
    with open(outputFile, 'w') as f:
        json.dump(dashboardData, f, indent=4)
        
    print(f"Success! Dashboard data saved to {outputFile}")
    print("Top 5:", topFiveDistricts)
    print("Temp > 30%:", tempAnalysis['overall']['above30'])
    print(f"Extreme Weather: {len(extremeWeather['cities'])} cities, {len(extremeWeather['years'])} years")

if __name__ == "__main__":
    main()
