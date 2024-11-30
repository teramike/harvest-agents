#!/usr/bin/env python3

import argparse
import concurrent.futures
import http.client
import json
import os
import threading
import time

import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Lock for thread-safe printing
print_lock = threading.Lock()


def main():
    parser = argparse.ArgumentParser(description='Agent Email Search Utility')
    parser.add_argument(
        'input_dir', help='Directory containing CSV files with agent data')
    parser.add_argument('--output_dir',
                        default='data/raw_google_searches',
                        help='Directory to save the search result JSON files')
    parser.add_argument('--max_workers',
                        type=int,
                        default=10,
                        help='Maximum number of worker threads')
    args = parser.parse_args()

    # Get API key from environment variable
    api_key = os.environ['HAS_DATA_API_KEY']
    if not api_key:
        raise ValueError("HAS_DATA_API_KEY not found in environment variables")

    input_dir = args.input_dir
    output_dir = args.output_dir
    max_workers = args.max_workers

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")

    # Get list of all CSV files in the input directory
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return

    # Process each CSV file sequentially
    for csv_file in csv_files:
        csv_path = os.path.join(input_dir, csv_file)
        print(f"\nProcessing {csv_file}...")
        
        # Read CSV file
        df_agents = pd.read_csv(csv_path, encoding='utf-8')
        df_agents['Zipcode'] = df_agents['Zipcode'].astype(str)

        # Check if 'id' column exists in the CSV
        if 'id' not in df_agents.columns:
            print(
                f"Error: CSV file {csv_file} must contain an 'id' column. Skipping this file."
            )
            continue

        # Ensure 'id' column is of type string
        df_agents['id'] = df_agents['id'].astype(str)

        # Generate search tuples
        search_tuples = generate_search_tuples(df_agents, output_dir)

        if not search_tuples:
            print(f"No new searches needed for {csv_file} (all results exist)")
            continue

        # Perform searches in parallel
        results = parallel_search(search_tuples, api_key, output_dir, max_workers)

        # Print results
        with print_lock:
            print(f"\nResults for {csv_file}:")
            for result in results:
                print(result)


def generate_search_query(row):
    """
    Generates a Google search query to find a person's email based on their details.

    Parameters:
    - row (pd.Series): A row from a DataFrame containing person details.
    - include_county (bool): Whether to include county in the search query.

    Returns:
    - str: A Google search query string.
    """
    # Extract and clean data from the row
    name = row['Name'].strip() if pd.notna(row['Name']) else ''
    company = row['Company'].strip() if pd.notna(row['Company']) else ''
    city = row['City'].strip() if pd.notna(row['City']) else ''
    zipcode = row['Zipcode'].strip() if pd.notna(row['Zipcode']) else ''
    county = row['County'].strip() if pd.notna(row['County']) else ''

    query_parts = []

    # Add the name in quotes to search for the exact phrase
    if name:
        query_parts.append(f'"{name}"')

    # Add the company in quotes
    if company:
        query_parts.append(f'"{company}"')

    # Add city if available
    if city:
        query_parts.append(f'"{city}"')

    query_parts.append('(email OR contact)')

    return ' '.join(query_parts)


def generate_search_tuples(df, output_dir):
    search_tuples = []
    for _, row in df.iterrows():
        agent_uuid = row['id']
        query = generate_search_query(row)
        filename = f"{agent_uuid}.json"
        filepath = os.path.join(output_dir, filename)
        # Check if the file already exists
        if not os.path.exists(filepath):
            search_tuples.append((agent_uuid, query))
        else:
            with print_lock:
                print(
                    f"Skipped search for agent_id: {agent_uuid} (already exists)"
                )
    return search_tuples


def search(query,
           api_key,
           num_results=10,
           location='United States',
           max_retries=3):
    # Initialize variables
    attempt = 0
    while attempt < max_retries:
        conn = None
        try:
            # Create connection
            conn = http.client.HTTPSConnection("api.hasdata.com", timeout=30)

            # Prepare headers
            headers = {
                'x-api-key': api_key,
                'Content-Type': "application/json"
            }

            # Prepare the request URL
            encoded_query = query.replace(' ', '+')
            encoded_location = location.replace(' ', '+')
            url = f"/scrape/google/serp?q={encoded_query}&location={encoded_location}&deviceType=desktop&gl=us&hl=en&num={num_results}"

            # Perform the request
            conn.request("GET", url, headers=headers)
            res = conn.getresponse()
            data = res.read().decode("utf-8")

            return data

        except (http.client.HTTPException, ConnectionError, TimeoutError) as e:
            attempt += 1
            if attempt == max_retries:
                raise Exception(
                    f"Failed after {max_retries} attempts: {str(e)}")
            # Add a small delay before retrying
            time.sleep(1)
        finally:
            if conn:
                conn.close()


def search_wrapper(args):
    agent_uuid, query, api_key, output_dir = args
    filename = f"{agent_uuid}.json"
    filepath = os.path.join(output_dir, filename)

    try:
        data = search(query, api_key, num_results=10)

        # Parse the JSON data
        search_results = json.loads(data)

        # Save the results to a JSON file with the agent_uuid as the filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(search_results, f, ensure_ascii=False, indent=2)

        return f"Completed search for agent_uuid: {agent_uuid}"
    except Exception as e:
        return f"Error processing agent_uuid {agent_uuid}: {str(e)}"


def parallel_search(search_tuples, api_key, output_dir, max_workers=5):
    args_list = [(agent_uuid, query, api_key, output_dir)
                 for agent_uuid, query in search_tuples]
    results = []
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers) as executor:
        futures = [executor.submit(search_wrapper, args) for args in args_list]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)
    return results


if __name__ == "__main__":
    main()
