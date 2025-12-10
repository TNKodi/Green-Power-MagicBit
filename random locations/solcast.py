import os
import requests
import pandas as pd
import time

# -----------------------------
# 🔧 USER CONFIG
# -----------------------------
API_KEY = "7b-R-FqbvkW-hQMVZ4imxV416sJaPt2h"  # paste your key

# Define multiple locations
locations = [
    {
        "name": "Gampaha",
        "latitude": 7.09,
        "longitude": 80.00
    },
    {
        "name": "Belihuloya_Ratnapura",
        "latitude": 6.7325,
        "longitude": 80.7871
    },
    {
        "name": "Haputale_Badulla",
        "latitude": 6.7650,
        "longitude": 80.9510
    },
    {
        "name": "Kandy_City",
        "latitude": 7.2906,
        "longitude": 80.6337
    },
    {
        "name": "Matara_City",
        "latitude": 5.9549,
        "longitude": 80.5550
    }
]

# Solar parameters you want
OUTPUT_PARAMETERS = "ghi,dni,dhi,gti"

# Time resolution - PT1H for hourly data
TIME_RESOLUTION = "PT1H"

# -----------------------------
# 🕒 FIXED DATE RANGE FOR NOVEMBER 2025
# -----------------------------
start_iso = "2025-10-31T18:00:00Z"
end_iso = "2025-11-30T18:29:59Z"

# -----------------------------
# 🌞 BASE API URL
# -----------------------------
BASE_URL = "https://api.solcast.com.au/data/historic/radiation_and_weather"

# -----------------------------
# 📡 LOOP THROUGH LOCATIONS
# -----------------------------
for idx, location in enumerate(locations, 1):
    location_name = location["name"]
    latitude = location["latitude"]
    longitude = location["longitude"]
    
    print(f"\n{'='*60}")
    print(f"Processing location {idx}/{len(locations)}: {location_name}")
    print(f"Requesting Solcast historic data from {start_iso} to {end_iso}")
    print(f"Location: lat={latitude}, lon={longitude}")
    print(f"{'='*60}")
    
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "output_parameters": OUTPUT_PARAMETERS,
        "start": start_iso,
        "end": end_iso,
        "period": TIME_RESOLUTION,
        "api_key": API_KEY
    }
    
    # Make request
    headers = {
        "Accept": "application/json"
    }
    response = requests.get(BASE_URL, params=params, headers=headers)
    
    print(f"HTTP Status: {response.status_code}")
    
    if response.status_code != 200:
        print(f"❌ Error from Solcast API for {location_name}")
        print("HTTP Status:", response.status_code)
        print("Response:", response.text[:1000])
        print(f"⚠️ Skipping {location_name} and continuing...")
        continue
    
    data = response.json()
    
    # Expected key
    series_key = "estimated_actuals"
    if series_key not in data:
        print(f"❌ Expected key '{series_key}' not found in response for {location_name}.")
        print("Full response:")
        print(data)
        print(f"⚠️ Skipping {location_name} and continuing...")
        continue
    
    records = data[series_key]
    
    # -----------------------------
    # 📊 CONVERT TO DATAFRAME
    # -----------------------------
    df = pd.DataFrame(records)
    
    # Convert timestamps to Asia/Colombo timezone (UTC+5:30)
    for col in ["period_end", "period_start"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True).dt.tz_convert("Asia/Colombo")
    
    print(f"✅ Retrieved rows: {len(df)}")
    
    # -----------------------------
    # 💾 SAVE TO CSV
    # -----------------------------
    output_csv = f"solcast_nov_2025_{location_name}_hourly.csv"
    df.to_csv(output_csv, index=False)
    print(f"✅ Data saved to: {os.path.abspath(output_csv)}")
    
    # Add a small delay between requests to avoid rate limiting
    if idx < len(locations):
        print("⏳ Waiting 2 seconds before next request...")
        time.sleep(2)

print(f"\n{'='*60}")
print("🎉 All locations processed successfully!")
print(f"{'='*60}")
