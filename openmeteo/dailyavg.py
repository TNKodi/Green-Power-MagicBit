import pandas as pd
import numpy as np
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Read the hourly data
df = pd.read_csv(os.path.join(script_dir, "kebithigollewa_openmeteo_solar_2025_11.csv"))

# Convert time to datetime and extract date
df['time'] = pd.to_datetime(df['time'])
df['date'] = df['time'].dt.date

# Define columns for solar parameters (map to actual column names)
solar_cols = {
    'shortwave_radiation': 'ghi',
    'direct_radiation': 'dri',
    'direct_normal_irradiance': 'dni',
    'diffuse_radiation': 'dhi',
    'global_tilted_irradiance': 'gti'
}

# Create daily average using only non-zero values
daily_avg = df.groupby('date').apply(
    lambda group: pd.Series({
        f'{avg_col_name}_avg': group[csv_col][group[csv_col] > 0].mean() 
                               if (group[csv_col] > 0).any() else np.nan
        for csv_col, avg_col_name in solar_cols.items()
        if csv_col in df.columns
    })
).reset_index()

print("Daily Average (Non-Zero Values Only):")
print(daily_avg)

# Save to CSV
output_file = os.path.join(script_dir, "kebithigollewa_openmeteo_solar_daily_2025_11.csv")
daily_avg.to_csv(output_file, index=False)
print(f"\n✅ Daily averages saved to: {output_file}")