import pandas as pd
from datetime import timedelta

def handle_missing_values(input_file, output_file, max_gap_minutes=3):
    """
    Handle missing values in CSV file with time-series data.
    Only fills gaps that are <= max_gap_minutes.
    Larger gaps are considered intentional breaks and not filled.
    
    Parameters:
    -----------
    input_file : str
        Path to input CSV file
    output_file : str
        Path to output CSV file
    max_gap_minutes : int
        Maximum gap in minutes to consider for interpolation (default: 3)
    """
    
    # Read the CSV file
    print(f"Reading data from {input_file}...")
    df = pd.read_csv(input_file, sep=';', parse_dates=['Timestamp'])
    
    # Sort by timestamp to ensure chronological order
    df = df.sort_values('Timestamp').reset_index(drop=True)
    
    print(f"Original data shape: {df.shape}")
    print(f"Date range: {df['Timestamp'].min()} to {df['Timestamp'].max()}")
    
    # Create a complete time range with 1-minute frequency
    start_time = df['Timestamp'].min()
    end_time = df['Timestamp'].max()
    complete_time_range = pd.date_range(start=start_time, end=end_time, freq='1min')
    
    # Create a complete dataframe with all timestamps
    complete_df = pd.DataFrame({'Timestamp': complete_time_range})
    
    # Merge with original data
    merged_df = complete_df.merge(df, on='Timestamp', how='left')
    
    # Fill Entity Name (assuming it's constant)
    if 'Entity Name' in merged_df.columns:
        merged_df['Entity Name'] = merged_df['Entity Name'].fillna(method='ffill')
    
    # Identify gaps
    print("\nAnalyzing gaps in the data...")
    missing_mask = merged_df['Solar radiation'].isna()
    
    # Calculate time differences between consecutive rows
    merged_df['time_diff'] = merged_df['Timestamp'].diff()
    
    # Create groups based on gaps larger than max_gap_minutes
    # A new group starts when there's a gap > max_gap_minutes OR after missing data
    gap_threshold = timedelta(minutes=max_gap_minutes)
    
    # Find where large gaps occur
    large_gap_indices = merged_df[merged_df['time_diff'] > gap_threshold].index.tolist()
    
    if large_gap_indices:
        print(f"\nFound {len(large_gap_indices)} large gaps (>{max_gap_minutes} minutes)")
        for idx in large_gap_indices[:5]:  # Show first 5
            if idx > 0:
                prev_time = merged_df.loc[idx-1, 'Timestamp']
                curr_time = merged_df.loc[idx, 'Timestamp']
                gap_size = (curr_time - prev_time).total_seconds() / 60
                print(f"  Gap from {prev_time} to {curr_time}: {gap_size:.1f} minutes")
    
    # Interpolate only small gaps
    print(f"\nFilling gaps <= {max_gap_minutes} minutes using linear interpolation...")
    
    # Process each continuous segment separately
    filled_df = merged_df.copy()
    
    # Create segment identifiers
    filled_df['segment'] = (filled_df['time_diff'] > gap_threshold).cumsum()
    
    # Count missing values before and after
    missing_before = filled_df['Solar radiation'].isna().sum()
    
    # Interpolate within each segment
    for segment_id in filled_df['segment'].unique():
        segment_mask = filled_df['segment'] == segment_id
        
        # Get the segment data
        segment_data = filled_df.loc[segment_mask, 'Solar radiation']
        
        # Only interpolate if segment has at least 2 non-null values
        if segment_data.notna().sum() >= 2:
            # Interpolate within this segment
            filled_df.loc[segment_mask, 'Solar radiation'] = segment_data.interpolate(
                method='linear', 
                limit_direction='both'
            )
    
    missing_after = filled_df['Solar radiation'].isna().sum()
    filled_count = missing_before - missing_after
    
    print(f"\nMissing values before: {missing_before}")
    print(f"Missing values after: {missing_after}")
    print(f"Values filled: {filled_count}")
    
    # Remove rows that are still missing (large gaps at boundaries)
    final_df = filled_df[filled_df['Solar radiation'].notna()].copy()
    
    # Drop helper columns
    final_df = final_df.drop(columns=['time_diff', 'segment'])
    
    print(f"Final data shape: {final_df.shape}")
    
    # Save to output file
    print(f"\nSaving processed data to {output_file}...")
    final_df.to_csv(output_file, sep=';', index=False)
    
    print("Done!")
    
    # Print summary statistics
    print("\n=== Summary Statistics ===")
    print(f"Total records: {len(final_df)}")
    print(f"Records added/filled: {filled_count}")
    print(f"Solar radiation range: {final_df['Solar radiation'].min():.2f} to {final_df['Solar radiation'].max():.2f}")


if __name__ == "__main__":
    # Configuration
    input_csv = "oct1week_sensor_kbg_rounded.csv"
    output_csv = "oct1week_sensor_kbg_filled.csv"
    max_gap_minutes = 3
    
    # Run the missing value handler
    handle_missing_values(input_csv, output_csv, max_gap_minutes)
