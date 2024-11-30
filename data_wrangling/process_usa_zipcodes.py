# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: base
#     language: python
#     name: python3
# ---

# %%
import os

import pandas as pd

# %%
PATH_ZIPCODES = 'data/config/usa_zipcodes.csv'

# %%
df = pd.read_csv(PATH_ZIPCODES, sep=';')
df.columns = [
    'zipcode', 'city', 'state_code', 'state_name', 'zcta', 'zcta_parent',
    'population', 'density', 'county_code', 'county_name', 'county_weights',
    'county_names', 'county_codes', 'imprecise', 'military', 'timezone',
    'geo_point'
]


# %%
df.shape

# %%
df.state_code.unique()

# %%
if not os.path.exists('config/zipcodes'):
    os.makedirs('config/zipcodes')

for state in df['state_code'].unique():
    state_df = df[df['state_code'] == state]
    state_df.to_csv(f'config/zipcodes/{state}.csv', index=False)

# %%
