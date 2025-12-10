import pandas as pd
import numpy as np

# Read the hourly data
df = pd.read_csv("solcast_oct_2025_hourly.csv")

# Convert period_end to datetime and extract date
df['period_end'] = pd.to_datetime(df['period_end'])
df['date'] = df['period_end'].dt.date

# Define columns for solar parameters
solar_cols = ['ghi', 'dni', 'dhi', 'gti']

# Create daily average using only non-zero values
daily_avg = pd.DataFrame()
daily_avg['date'] = df.groupby('date').size().index

for col in solar_cols:
    if col in df.columns:
        # Calculate mean of non-zero values only
        daily_avg[f'{col}_avg'] = df.groupby('date')[col].apply(
            lambda x: x[x > 0].mean() if (x > 0).any() else np.nan
        ).values

print("Daily Average (Non-Zero Values Only):")
print(daily_avg)

# Save to CSV
output_file = "solcast_oct_2025_daily_avg.csv"
daily_avg.to_csv(output_file, index=False)
print(f"\n✅ Daily averages saved to: {output_file}")