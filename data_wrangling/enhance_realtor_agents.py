import glob
import os

import pandas as pd

PATH_DATA = 'data/realtor_agents'
PATH_ZIPCODES = 'config/usa_zipcodes.csv'
PATH_OUTPUT = 'data/realtor_agents_enhanced'

# Create output directory if it doesn't exist
os.makedirs(PATH_OUTPUT, exist_ok=True)

# Load zipcode reference data
zipcodes_df = pd.read_csv(PATH_ZIPCODES, sep=';')
# Select only needed columns
zipcodes_df = zipcodes_df[[
    'Zip Code', 'Official USPS city name', 'Primary Official County Name'
]]
zipcodes_df['Zip Code'] = zipcodes_df['Zip Code'].astype(str).str.zfill(5)

# Process each agent file
for file_path in glob.glob(os.path.join(PATH_DATA, 'agents_info_*.csv')):
    # Extract zipcode from filename
    filename = os.path.basename(file_path)
    zipcode = filename.split('_')[-1].split('.')[0]
    zipcode = str(zipcode).zfill(5)

    # Read agent data
    agents_df = pd.read_csv(file_path)

    # Get location data for this zipcode
    location_data = zipcodes_df[zipcodes_df['Zip Code'] == zipcode].iloc[0]

    # Add location columns to all rows
    agents_df['Zipcode'] = zipcode
    agents_df['City'] = location_data['Official USPS city name']
    agents_df['County'] = location_data['Primary Official County Name']

    # Save enhanced file
    output_filename = os.path.join(PATH_OUTPUT, filename)
    agents_df.to_csv(output_filename, index=False)

print("Processing complete. Enhanced files saved to:", PATH_OUTPUT)
