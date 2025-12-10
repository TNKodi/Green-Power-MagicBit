import requests
import pandas as pd

# Define locations
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

START = "2025-11-01"
END   = "2025-11-30"

# Build the URL
base_url = "https://archive-api.open-meteo.com/v1/archive"

# Loop through each location
for location in locations:
    print(f"\nFetching data for {location['name']}...")
    
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
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

    # Convert timestamps (keep as hourly)
    df["time"] = pd.to_datetime(df["time"], utc=False)
    print(df.head())
    print(df.columns)

    # Save to CSV (hourly) with location name
    output_path = f"openmeteo_{location['name']}_nov_2025_hourly.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved to {output_path}")
