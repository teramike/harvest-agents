import argparse
import logging
import os
import re
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from zenrows import ZenRowsClient

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def extract_agent_info(card):
    agent_info = {}

    # Name
    name = card.select_one("div.agent-name span.text-bold")
    agent_info['Name'] = name.get_text(strip=True) if name else ''

    # Profile Picture URL
    profile_pic = card.select_one("div.agent-list-card-img img.profile-logo")
    agent_info['Profile Picture URL'] = profile_pic['src'] if profile_pic else ''

    # Company
    company = card.select_one("div.agent-group div")
    agent_info['Company'] = company.get_text(strip=True) if company else ''

    # Brokerage Picture URL
    brokerage_pic = card.select_one("div.agent-office-logo img")
    agent_info['Brokerage Picture URL'] = brokerage_pic['src'] if brokerage_pic else ''

    # Experience
    experience_div = card.find("div", text=re.compile(r'Experience:'))
    if experience_div:
        agent_info['Experience'] = experience_div.get_text(strip=True).replace('Experience:', '').strip()
    else:
        agent_info['Experience'] = ''

    # Phone Number
    phone_number = card.select_one("div.agent-phone")
    agent_info['Phone'] = phone_number.get_text(strip=True) if phone_number else ''

    # Email (Check if Email button exists)
    email_button = card.select_one("span.agent-email button")
    if email_button:
        # The actual email address may not be directly available due to privacy reasons.
        # Instead, we can note that the agent has an email contact option.
        agent_info['Email Available'] = 'Yes'
    else:
        agent_info['Email Available'] = 'No'

    # For Sale
    for_sale_label = card.find("span", string=re.compile(r'For sale:'))
    if for_sale_label:
        for_sale_value = for_sale_label.find_next("span", class_="bold-text")
        agent_info['For Sale'] = for_sale_value.get_text(strip=True) if for_sale_value else ''
    else:
        agent_info['For Sale'] = ''

    # Sold
    sold_label = card.find("span", string=re.compile(r'Sold:'))
    if sold_label:
        sold_value = sold_label.find_next("span", class_="bold-text")
        agent_info['Sold'] = sold_value.get_text(strip=True) if sold_value else ''
    else:
        agent_info['Sold'] = ''

    # Activity Range
    activity_range_div = card.find("div", text=re.compile(r'Activity range:'))
    if activity_range_div:
        activity_range_value = activity_range_div.find("span", class_="bold-text")
        agent_info['Activity Range'] = activity_range_value.get_text(strip=True) if activity_range_value else ''
    else:
        agent_info['Activity Range'] = ''

    # Last Listed
    listed_date_div = card.find("div", text=re.compile(r'Listed a house:'))
    if listed_date_div:
        listed_date_value = listed_date_div.find("span", class_="bold-text")
        agent_info['Last Listed'] = listed_date_value.get_text(strip=True) if listed_date_value else ''
    else:
        agent_info['Last Listed'] = ''

    # Badges / Certifications
    badges = []
    badge_icons = card.select("div.desigations_certifications-icons i")
    for badge_icon in badge_icons:
        badge_class = badge_icon.get("class", [])
        for class_name in badge_class:
            if class_name.startswith("icon-certification-"):
                badge_name = class_name.replace("icon-certification-", "").upper()
                badges.append(badge_name)
    agent_info['Certifications'] = ', '.join(badges)

    return agent_info

def scrape_realtor_agents(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    agents_info = []

    agent_cards = soup.select("div[data-testid='component-agentCard']")

    for card in agent_cards:
        try:
            agent_info = extract_agent_info(card)
            agents_info.append(agent_info)
        except Exception as e:
            logger.error(f"Error extracting agent information: {e}")

    return agents_info

def scrape_zipcode(zipcode, output_dir, api_key):
    params = {"js_render": "true", "premium_proxy": "true", "proxy_country": "us"}
    client = ZenRowsClient(api_key)
    base_url = f"https://www.realtor.com/realestateagents/{zipcode}/photo-1"
    page = 1
    all_agents = []

    while True:
        url = f"{base_url}/pg-{page}"
        response = client.get(url, params=params)

        if response.status_code != 200:
            logger.error(f"Error fetching page {page} for zipcode {zipcode}: {response.status_code}")
            break

        agents = scrape_realtor_agents(response.text)
        if not agents:
            break

        all_agents.extend(agents)
        page += 1

    if all_agents:
        df = pd.DataFrame(all_agents)
        df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(os.path.join(output_dir, f'agents_info_{zipcode}.csv'), index=False, encoding='utf-8')
        logger.info(f"Scraped information for {len(all_agents)} agents in zipcode {zipcode}")
    else:
        logger.info(f"No agents found for zipcode {zipcode}")

def main():
    parser = argparse.ArgumentParser(description='Scrape realtor.com agent data for given zipcodes.')
    parser.add_argument('zipcodes_file', help='Path to the .txt file containing zipcodes (one per line).')
    parser.add_argument('--output_dir', default='data/realtor_agents', help='Directory to save output CSV files.')
    parser.add_argument('--max_workers', type=int, default=10, help='Number of parallel workers.')
    args = parser.parse_args()

    # Get API key from environment variable
    api_key = os.environ['ZENROWS_API_KEY']
    if not api_key:
        raise ValueError("ZENROWS_API_KEY not found in environment variables")

    # Read zipcodes from the file
    with open(args.zipcodes_file, 'r') as f:
        zipcodes = [line.strip() for line in f if line.strip()]

    start_time = time.time()

    # Use ProcessPoolExecutor to process zipcodes in parallel
    with ProcessPoolExecutor(max_workers=args.max_workers) as executor:
        futures = []
        for zipcode in zipcodes:
            futures.append(executor.submit(scrape_zipcode, zipcode, args.output_dir, api_key))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"An error occurred: {e}")

    end_time = time.time()
    logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()