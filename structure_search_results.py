import concurrent.futures
import glob
import json
import os
from typing import List, Optional

import pandas as pd
from openai import OpenAI
from pydantic import BaseModel, Field

PATH_INPUT = 'data/realtor_agents_enhanced/tests'
PATH_SEARCH_RESULTS = 'data/clean_google_searches'
PATH_OUTPUT = 'data/parsed_search_results'

if not os.path.exists(PATH_OUTPUT):
    os.makedirs(PATH_OUTPUT)
    print(f"Created directory: {PATH_OUTPUT}")
else:
    print(f"Directory already exists: {PATH_OUTPUT}")



PROCESSED_AGENTS = set()
for file in os.listdir(PATH_OUTPUT):
    PROCESSED_AGENTS.add(file.split('.')[0])


class AgentData(BaseModel):
    email: Optional[str] = Field(
        None,
        description=
        "The most relevant real estate agent's professional email address, if available"
    )
    other_emails: Optional[List[str]] = Field(
        None,
        description=
        "Any other email addresses found for the real estate agent or its brokerage firm"
    )
    possible_email: Optional[str] = Field(
        None,
        description=
        "The most likely email address for the real estate agent if it's main email is not available"
    )
    phone: Optional[str] = Field(
        None,
        description=
        "The most relevant real estate agent's professional phone number or its brokerage firm, if available"
    )
    other_phones: Optional[List[str]] = Field(
        None,
        description="Any other contact phone numbers for the real estate agent"
    )
    city: Optional[str] = Field(
        None,
        description="The city where the real estate agent primarily operates")
    age: Optional[int] = Field(
        None, description="The age of the real estate agent, if available")
    gender: Optional[str] = Field(
        None, description="The gender of the real estate agent, if available")
    website: Optional[str] = Field(
        None,
        description=
        "The real estate agent's professional website or listing page")
    social_media: Optional[List[str]] = Field(
        None,
        description=
        "List of social media profiles related to the agent's real estate business"
    )
    google_review_star_rating: Optional[float] = Field(
        None,
        description=
        "The Google review star rating of the real estate agent or the brokerage firm, if available"
    )
    most_recent_reviews: Optional[List[str]] = Field(
        None,
        description=
        "The most recent reviews of the real estate agent, if available")
    additional_info: Optional[str] = Field(
        None,
        description=
        "Relevant information about the real estate agent or their practice that might be interesting to know before contacting them with a personalized message so that they are more likely to respond"
    )


def extract_search_results(agent_id, search_query, google_search_results):
    # Create the filename using the id
    filename = f"{agent_id}.json"
    filepath = os.path.join(PATH_OUTPUT, filename)

    # Check if the file already exists
    if os.path.exists(filepath):
        print(f"Skipping agent_id: {agent_id} (already processed)")
        return None

    client = OpenAI()
    completion = client.beta.chat.completions.parse(
        model='gpt-4o-2024-08-06',
        temperature=0,
        max_tokens=4096,
        messages=[{
            "role":
            "system",
            "content":
            "You're a master at finding relevant real estate agent information. You stick to the provided Google search data, and only if explicitly available extract data as valid JSON according to the provided schema."
        }, {
            "role":
            "user",
            "content":
            f"When searching for the following real estate agent on Google: {search_query}, we got these results:\n{google_search_results}.\nExtract the relevant information found as valid JSON:"
        }],
        response_format=AgentData)
    if completion.choices[0].message.refusal:
        print(
            f"Model refused to respond: {completion.choices[0].message.refusal}"
        )
        return None

    agent_data = completion.choices[0].message.parsed

    # Convert the Pydantic object to a dictionary and save as JSON
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(agent_data.dict(), f, indent=2)

    print(f"Processed agent_id: {agent_id}")
    return agent_data


def process_agent(row):
    agent_id = row['id']
    if agent_id in PROCESSED_AGENTS:
        print(f"Skipping agent_id: {agent_id} (already processed)")
        return None
    
    name = row['Name']
    firm_name = row['Company'] if pd.notna(row['Company']) else ''
    city = row['City'] if pd.notna(row['City']) else ''
    county = row['County'] if pd.notna(row['County']) else ''

    # Construct the search query as a dictionary
    search_query_dict = {
        "name": name,
        "firm_name": firm_name,
        "city": city,
        "county": county,
        # TODO: make this dynamic or parameterize it if needed
        "state": "North Dakota"
    }

    # Remove empty values from the dictionary
    search_query_dict = {k: v for k, v in search_query_dict.items() if v}

    # Add the "real estate" keyword
    search_query_dict["keyword"] = "real estate"

    # Construct the final search query string
    search_query = " ".join(search_query_dict.values()).strip()

    # Load the Google search results
    search_results_file = os.path.join(PATH_SEARCH_RESULTS, f"{agent_id}.json")
    try:
        with open(search_results_file, 'r', encoding='utf-8') as f:
            google_search_results = json.load(f)
    except FileNotFoundError:
        print(f"Search results file not found for agent ID: {agent_id}")
        return None

    # Extract agent data
    return extract_search_results(agent_id, search_query,
                                  google_search_results)


def process_csv_file(csv_file):
    df = pd.read_csv(csv_file)
    if df.empty:
        print(f"CSV file {csv_file} is empty.")
        return []
    
    df_relevant = df[['id', 'Name', 'Company', 'City', 'County']]
    results = []
    
    # Process rows concurrently using ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(process_agent, row) 
            for _, row in df_relevant.iterrows()
        ]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    results.append(result)
            except Exception as e:
                print(f"Error processing agent: {str(e)}")
                continue
    
    return results


if __name__ == '__main__':
    # Get all CSV files in the input directory
    csv_files = glob.glob(os.path.join(PATH_INPUT, '*.csv'))

    all_results = []
    # Process each CSV file sequentially
    for csv_file in csv_files:
        try:
            results = process_csv_file(csv_file)
            all_results.extend(results)
        except Exception as e:
            print(f"Error processing CSV file {csv_file}: {str(e)}")
            continue

    print(f"Processed {len(all_results)} agents successfully")
