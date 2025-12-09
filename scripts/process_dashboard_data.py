import re
import json
import os
from collections import defaultdict

# --- Configuration ---
# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Navigate relative to the script: ../dashboard/data/
DATA_DIR = os.path.join(SCRIPT_DIR, '..', 'dashboard', 'data')

INPUT_FILE = os.path.join(DATA_DIR, 'mapreduce_results.txt')
OUTPUT_FILE = os.path.join(DATA_DIR, 'weather_summary.json')

# --- Logic ---

def parse_data(file_path):
    # Regex to extract: District, Precip, Temp, Month, Year
    # Example: Ampara had a total precipitation of 53 hours with a mean temperature of 25 for 1st month (Year: 2010)
    regex = r"(\w+) had a total precipitation of (\d+) hours with a mean temperature of (\d+) for (\d+)\w+ month \(Year: (\d+)\)"
    
    data = []
    
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return []

    with open(file_path, 'r') as f:
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

def calculate_top_districts(data):
    """ Task 4.2: Top 5 districts by total precipitation """
    district_precip = defaultdict(float)
    for entry in data:
        district_precip[entry['district']] += entry['precipitation']
    
    # Sort descending and take top 5
    sorted_districts = sorted(district_precip.items(), key=lambda x: x[1], reverse=True)[:5]
    
    return {
        "labels": [item[0] for item in sorted_districts],
        "data": [item[1] for item in sorted_districts]
    }

def calculate_peak_months(data):
    """ Task 4.1: Most precipitous month per district """
    # Sum precipitation by Month for each District to find the peak month
    
    district_monthly_total = defaultdict(lambda: defaultdict(float))
    
    for entry in data:
        district_monthly_total[entry['district']][entry['month']] += entry['precipitation']
    
    results = []
    month_names = ["", "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
    
    for district, months in district_monthly_total.items():
        best_month = max(months.items(), key=lambda x: x[1])  # (month_num, total_val)
        month_idx = best_month[0]

        results.append({
            "district": district,
            "month": month_names[month_idx],
            "total_precipitation": best_month[1]
        })
        
    return results

def calculate_high_temp_percentage(data):
    """ Task 4.3: Percentage of months with mean temp > 30°C """
    total_months = len(data)
    if total_months == 0: 
        return {"overall": {"above_30": 0, "below_30": 0}, "by_year": {}}

    # Overall calculation
    high_temp_count = sum(1 for entry in data if entry['temperature'] > 30)
    percent_high = (high_temp_count / total_months) * 100
    percent_low = 100 - percent_high
    
    # Per-year calculation
    year_data = defaultdict(lambda: {"total": 0, "above_30": 0})
    for entry in data:
        year = entry['year']
        year_data[year]["total"] += 1
        if entry['temperature'] > 30:
            year_data[year]["above_30"] += 1
    
    by_year = {}
    for year, counts in sorted(year_data.items()):
        pct = (counts["above_30"] / counts["total"]) * 100 if counts["total"] > 0 else 0
        by_year[str(year)] = round(pct, 1)
    
    return {
        "overall": {
            "above_30": round(percent_high, 1),
            "below_30": round(percent_low, 1)
        },
        "by_year": by_year
    }

def main():
    print(f"Reading data from {INPUT_FILE}...")
    raw_data = parse_data(INPUT_FILE)
    print(f"Parsed {len(raw_data)} records.")
    
    if not raw_data:
        print("No valid data found. Check regex or file format.")
        return

    # Calculations
    top_5_districts = calculate_top_districts(raw_data)
    peak_months = calculate_peak_months(raw_data)
    temp_analysis = calculate_high_temp_percentage(raw_data)
    
    # Construct Final JSON
    dashboard_data = {
        "top_districts": top_5_districts,
        "peak_seasonality": peak_months,
        "temperature_analysis": temp_analysis,
        "meta": {
            "total_records": len(raw_data),
            "generated_at": "Today"
        }
    }
    
    # Save JSON
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(dashboard_data, f, indent=4)
        
    print(f"Success! Dashboard data saved to {OUTPUT_FILE}")
    print("Top 5:", top_5_districts)
    print("Temp > 30%:", temp_analysis['overall']['above_30'])

if __name__ == "__main__":
    main()
