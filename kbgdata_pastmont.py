import requests
import pandas as pd

# Kebithigollewa coordinates
LAT = 8.5345
LON = 80.4923

START = "2025-11-01"
END   = "2025-11-30"

# Build the URL
base_url = "https://archive-api.open-meteo.com/v1/archive"

params = {
    "latitude": LAT,
    "longitude": LON,
    "start_date": START,
    "end_date": END,
    "hourly": ",".join([
        "shortwave_radiation",        # GHI
        "direct_radiation",           # direct on horizontal
        "direct_normal_irradiance",   # DNI
        "diffuse_radiation",          # DHI
        "global_tilted_irradiance"    # GTI (requires tilt/azimuth if you want custom)
    ]),
    "timezone": "Asia/Colombo"
}

response = requests.get(base_url, params=params)
response.raise_for_status()
data = response.json()

# 'hourly' is a dict: keys = variable names, values = lists
hourly = data["hourly"]

# Convert directly to DataFrame
df = pd.DataFrame(hourly)

# Convert timestamps and aggregate to daily sums
df["time"] = pd.to_datetime(df["time"], utc=False)
daily_df = (
    df.set_index("time")
    .resample("D")
    .sum(numeric_only=True)
    .reset_index()
)

print(daily_df.head())
print(daily_df.columns)

# Save to CSV
output_path = "kebithigollewa_openmeteo_solar_daily_2025_11.csv"
daily_df.to_csv(output_path, index=False)
print(f"Saved to {output_path}")
