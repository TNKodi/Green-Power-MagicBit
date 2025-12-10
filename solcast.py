import os
import requests
import pandas as pd

# -----------------------------
# 🔧 USER CONFIG
# -----------------------------

#7.274276317883622, 80.2191266481395 - mas thulhiirya
API_KEY = "7b-R-FqbvkW-hQMVZ4imxV416sJaPt2h"  # paste your key
LATITUDE = 7.274276317883622     
LONGITUDE = 80.2191266481395

# Solar parameters you want
OUTPUT_PARAMETERS = "ghi,dni,dhi,gti"

# Time resolution - PT1H for hourly data
TIME_RESOLUTION = "PT5M"  # 5-minute intervals
# Output file name
OUTPUT_CSV = "solcast_nov_2025_1stweek_minitly.csv"

# -----------------------------
# 🕒 FIXED DATE RANGE FOR NOVEMBER 2025
# -----------------------------
start_iso = "2025-10-31T18:30:00Z"
end_iso = "2025-11-07T18:29:29Z"

print(f"Requesting Solcast historic data from {start_iso} to {end_iso}")
print(f"Location: lat={LATITUDE}, lon={LONGITUDE}")

# -----------------------------
# 🌞 BUILD API URL
# -----------------------------
BASE_URL = "https://api.solcast.com.au/data/historic/radiation_and_weather"

params = {
    "latitude": LATITUDE,
    "longitude": LONGITUDE,
    "output_parameters": OUTPUT_PARAMETERS,
    "start": start_iso,
    "end": end_iso,
    "period": TIME_RESOLUTION,
    "api_key": API_KEY
}

# -----------------------------
# 📡 MAKE REQUEST
# -----------------------------
headers = {
    "Accept": "application/json"
}
response = requests.get(BASE_URL, params=params, headers=headers)

print(f"HTTP Status: {response.status_code}")

if response.status_code != 200:
    print("❌ Error from Solcast API")
    print("HTTP Status:", response.status_code)
    print("Response:", response.text[:1000])
    raise SystemExit(1)

data = response.json()

# Expected key
series_key = "estimated_actuals"
if series_key not in data:
    print(f"❌ Expected key '{series_key}' not found in response.")
    print("Full response:")
    print(data)
    raise SystemExit(1)

records = data[series_key]

# -----------------------------
# 📊 CONVERT TO DATAFRAME
# -----------------------------
df = pd.DataFrame(records)

# Parse timestamps (skip 'period' as it contains interval like 'PT1H')

# Convert timestamps to Asia/Colombo timezone (UTC+5:30)
for col in ["period_end", "period_start"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], utc=True).dt.tz_convert("Asia/Colombo")

print("✅ Retrieved rows:", len(df))

# -----------------------------
# 💾 SAVE TO CSV
# -----------------------------
df.to_csv(OUTPUT_CSV, index=False)
print(f"✅ Data saved to: {os.path.abspath(OUTPUT_CSV)}")
