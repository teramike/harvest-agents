#!/usr/bin/env python3

import argparse
import asyncio
import json
import os
import time
from urllib.parse import quote_plus

import aiofiles
import aiohttp
import pandas as pd
from aiohttp import ClientSession, TCPConnector
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()


async def main():
    parser = argparse.ArgumentParser(description='Agent Email Search Utility')
    parser.add_argument('--input_dir',
                        default='data/realtor_agents_enhanced',
                        help='Directory containing CSV files with agent data')
    parser.add_argument('--output_dir',
                        default='data/raw_google_searches',
                        help='Directory to save the search result JSON files')
    parser.add_argument('--max_concurrent_requests',
                        type=int,
                        default=25,
                        help='Maximum number of concurrent HTTP requests')
    args = parser.parse_args()

    # Get API key from environment variable
    api_key = os.environ.get('HAS_DATA_API_KEY')
    if not api_key:
        raise ValueError("HAS_DATA_API_KEY not found in environment variables")

    input_dir = args.input_dir
    output_dir = args.output_dir
    max_concurrent_requests = args.max_concurrent_requests

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
        print(f"\nProcessing {csv_file}...")
        csv_path = os.path.join(input_dir, csv_file)

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
            print("No new agents to search for in this file")
            continue

        # Perform searches asynchronously
        results = await async_search(search_tuples, api_key, output_dir,
                                     max_concurrent_requests)

        # Print results
        print(f"\nResults for {csv_file}:")
        for result in results:
            print(result)


def generate_search_query(row):
    # Same as before
    name = row['Name'].strip() if pd.notna(row['Name']) else ''
    company = row['Company'].strip() if pd.notna(row['Company']) else ''
    city = row['City'].strip() if pd.notna(row['City']) else ''
    zipcode = row['Zipcode'].strip() if pd.notna(row['Zipcode']) else ''
    county = row['County'].strip() if pd.notna(row['County']) else ''

    query_parts = []

    if name:
        query_parts.append(f'"{name}"')

    if company:
        query_parts.append(f'"{company}"')

    if city:
        query_parts.append(f'"{city}"')

    query_parts.append('(email OR contact)')

    return ' '.join(query_parts)


def generate_search_tuples(df, output_dir):
    existing_files = {
        f.replace('.json', '')
        for f in os.listdir(output_dir) if f.endswith('.json')
    }
    search_tuples = []
    skipped_count = 0

    for _, row in df.iterrows():
        agent_uuid = row['id']
        if agent_uuid in existing_files:
            skipped_count += 1
            continue

        query = generate_search_query(row)
        search_tuples.append((agent_uuid, query))

    print(
        f"\nSkipping {skipped_count} agents that already have search results")
    print(f"Will search for {len(search_tuples)} new agents")
    return search_tuples


async def fetch(session, url, headers, max_retries=3):
    for attempt in range(max_retries):
        try:
            async with session.get(url, headers=headers,
                                   timeout=30) as response:
                response.raise_for_status()
                return await response.text()
        except aiohttp.ClientError as e:
            if attempt == max_retries - 1:
                raise Exception(
                    f"HTTP request failed after {max_retries} attempts: {e}")
            # Exponential backoff with base of 2 seconds
            wait_time = 2**attempt
            await asyncio.sleep(wait_time)


async def search(agent_uuid, query, api_key, output_dir, session):
    filename = f"{agent_uuid}.json"
    filepath = os.path.join(output_dir, filename)

    try:
        # Encode query to handle special characters
        encoded_query = query.encode('ascii', errors='ignore').decode()
        encoded_query = quote_plus(encoded_query)
        location = quote_plus('United States')
        num_results = 10
        url = (
            f"https://api.hasdata.com/scrape/google/serp?"
            f"q={encoded_query}&location={location}&deviceType=desktop&gl=us&hl=en&num={num_results}"
        )

        headers = {'x-api-key': api_key, 'Content-Type': "application/json"}

        data = await fetch(session, url, headers)
        search_results = json.loads(data)

        async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
            await f.write(
                json.dumps(search_results, ensure_ascii=False, indent=2))

        return f"Completed search for agent_uuid: {agent_uuid}"
    except Exception as e:
        print(f"Error processing agent_uuid {agent_uuid}: {str(e)}")
        # Return error message but don't raise exception to avoid stopping process
        return f"Error processing agent_uuid {agent_uuid}: {str(e)}"


async def async_search(search_tuples, api_key, output_dir,
                       max_concurrent_requests):
    connector = TCPConnector(limit_per_host=max_concurrent_requests)
    timeout = aiohttp.ClientTimeout(total=60)
    async with ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        for agent_uuid, query in search_tuples:
            task = asyncio.ensure_future(
                search(agent_uuid, query, api_key, output_dir, session))
            tasks.append(task)

        results = []
        for f in asyncio.as_completed(tasks):
            try:
                result = await f
                results.append(result)
            except Exception as e:
                print(f"Task failed: {str(e)}")
                # Continue processing remaining tasks even if one fails
                continue
        return results


if __name__ == "__main__":
    asyncio.run(main())
