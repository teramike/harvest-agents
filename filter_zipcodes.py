import argparse
import os
import sys

import pandas as pd


def load_zipcode_data(zipcodes_dir):
    # Load all CSV files in the specified directory
    data_frames = []
    for filename in os.listdir(zipcodes_dir):
        if filename.endswith('.csv'):
            filepath = os.path.join(zipcodes_dir, filename)
            df = pd.read_csv(filepath, dtype={'zipcode': str})
            data_frames.append(df)
    if not data_frames:
        raise ValueError("No CSV files found in the specified directory.")
    # Concatenate all data frames into one
    return pd.concat(data_frames, ignore_index=True)

def filter_zipcodes(df, state=None, city=None, county_name=None, min_population=None, max_population=None, min_density=None, max_density=None):
    # Apply filters based on the provided arguments
    if state:
        df = df[df['state_code'].str.upper() == state.upper()]
    if city:
        df = df[df['city'].str.contains(city, case=False, na=False)]
    if county_name:
        df = df[df['county_name'].str.contains(county_name, case=False, na=False)]
    if min_population is not None:
        df = df[df['population'] >= min_population]
    if max_population is not None:
        df = df[df['population'] <= max_population]
    if min_density is not None:
        df = df[df['density'] >= min_density]
    if max_density is not None:
        df = df[df['density'] <= max_density]
    return df

def main():
    parser = argparse.ArgumentParser(description='Filter zipcodes based on various criteria.')
    parser.add_argument('--zipcodes_dir', required=True, help='Directory containing zipcode CSV files.')
    parser.add_argument('--state', help='State code to filter by (e.g., "AK" for Alaska).')
    parser.add_argument('--city', help='City name to filter by.')
    parser.add_argument('--county', help='County name to filter by.')
    parser.add_argument('--min_population', type=float, help='Minimum population to filter by.')
    parser.add_argument('--max_population', type=float, help='Maximum population to filter by.')
    parser.add_argument('--min_density', type=float, help='Minimum density to filter by.')
    parser.add_argument('--max_density', type=float, help='Maximum density to filter by.')
    parser.add_argument('--output_file', default='filtered_zipcodes.txt', help='Output file to save the list of zipcodes.')
    args = parser.parse_args()

    # Load zipcode data
    try:
        df = load_zipcode_data(args.zipcodes_dir)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Convert population and density columns to numeric, handling missing values
    df['population'] = pd.to_numeric(df['population'], errors='coerce')
    df['density'] = pd.to_numeric(df['density'], errors='coerce')

    # Filter zipcodes
    filtered_df = filter_zipcodes(
        df,
        state=args.state,
        city=args.city,
        county_name=args.county,
        min_population=args.min_population,
        max_population=args.max_population,
        min_density=args.min_density,
        max_density=args.max_density
    )

    if filtered_df.empty:
        print("No zipcodes found matching the specified criteria.")
        sys.exit(0)

    # Save the list of zipcodes to the output file
    zipcodes = filtered_df['zipcode'].dropna().unique()
    with open(args.output_file, 'w') as f:
        for zipcode in zipcodes:
            f.write(f"{zipcode}\n")

    print(f"Filtered {len(zipcodes)} zipcodes. Saved to {args.output_file}")

if __name__ == '__main__':
    main()