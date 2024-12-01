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
import pandas as pd
import json
import glob
import os

# %%
PATH_REALTORS = '../data/realtor_agents_enhanced'
PATH_SEARCH_RESULTS = '../data/parsed_search_results'
PATH_OUTPUT = '../data/final_dataset.csv'

# %%
search_results_files = glob.glob(f'{PATH_SEARCH_RESULTS}/*.json')
search_results_data = []

for file_path in search_results_files:
    file_id = os.path.splitext(os.path.basename(file_path))[0]
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    # Add ID to data
    data['id'] = file_id
    
    # Append to list
    search_results_data.append(data)

# Create DataFrame
df_search_results = pd.DataFrame(search_results_data)

# %%
# Calculate percentage of non-null emails
email_percentage = (df_search_results['email'].notna().sum() / len(df_search_results)) * 100

# Calculate percentage of records with any type of email
any_email_percentage = (
    (df_search_results['email'].notna() | 
     df_search_results['other_emails'].notna() |
     df_search_results['possible_email'].notna()
    ).sum() / len(df_search_results)
) * 100

print(f"Percentage of records with primary emails: {email_percentage:.2f}%")
print(f"Percentage of records with any type of email: {any_email_percentage:.2f}%")

# %%
# Read all realtor files
realtor_files = glob.glob(f'{PATH_REALTORS}/*.csv')

# Create DataFrame by concatenating all CSV files
df_realtors = pd.concat([pd.read_csv(f) for f in realtor_files], ignore_index=True)

# Reset index and ensure id column exists in both dataframes
df_realtors = df_realtors.reset_index(drop=True)
df_search_results = df_search_results.reset_index(drop=True)

# Add id column if it doesn't exist
if 'id' not in df_search_results.columns:
    df_search_results['id'] = df_search_results.index.astype(str)

# Rename columns in search results df to avoid collisions
rename_cols = {col: f'search_{col}' for col in df_search_results.columns if col != 'id'}
df_search_results = df_search_results.rename(columns=rename_cols)

# Merge the dataframes on id
df_final = pd.merge(df_realtors, df_search_results, on='id', how='outer')


# %%
# Rename columns to snake_case
df_final.columns = [
    'name', 
    'profile_picture_url', 
    'company', 
    'brokerage_picture_url',
    'experience', 
    'phone', 
    'email_available', 
    'for_sale', 
    'sold',
    'activity_range', 
    'last_listed', 
    'certifications', 
    'id', 
    'zipcode',
    'city', 
    'county', 
    'search_email', 
    'search_other_emails',
    'search_possible_email', 
    'search_phone', 
    'search_other_phones',
    'search_city', 
    'search_age', 
    'search_gender', 
    'search_website',
    'search_social_media', 
    'search_google_review_star_rating',
    'search_most_recent_reviews', 
    'search_additional_info'
]

# Reorder columns based on importance
df_final = df_final[
    [
        'profile_picture_url',
        'name',
        'search_email',
        'search_other_emails',
        'search_possible_email',
        'phone',
        'search_phone',
        'search_other_phones',
        'search_website',
        'search_social_media',
        'search_additional_info',
        'company',
        'brokerage_picture_url',
        'experience',
        'email_available',
        'for_sale',
        'sold',
        'activity_range',
        'last_listed',
        'certifications',
        'id',
        'zipcode',
        'city',
        'county',
        'search_city',
        'search_age',
        'search_gender',
        'search_google_review_star_rating',
        'search_most_recent_reviews'
    ]
]

# %%
df_final.to_csv(PATH_OUTPUT, index=False)
