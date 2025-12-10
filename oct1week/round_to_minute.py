import pandas as pd
from datetime import datetime

# Read the CSV file
df = pd.read_csv('oct1week_sensor_kbg.csv', sep=';')

# Convert Timestamp to datetime and round to nearest minute
df['Timestamp'] = pd.to_datetime(df['Timestamp'])
df['Timestamp'] = df['Timestamp'].dt.round('min')  # Round to nearest minute

# Save to new CSV file
df.to_csv('oct1week_sensor_kbg_rounded.csv', sep=';', index=False)

print(f"New file created: oct1week_sensor_kbg_rounded.csv")
print(f"Total rows: {len(df)}")
print("\nFirst 10 rows:")
print(df.head(10))
