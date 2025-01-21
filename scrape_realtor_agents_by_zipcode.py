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
from tqdm import tqdm
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
    # Look for "Experience: ..." inside a div that contains that text and a bold-text span
    experience_div = card.find("div", text=re.compile(r'Experience:'))
    if experience_div:
        exp_span = experience_div.find("span", class_="bold-text")
        agent_info['Experience'] = exp_span.get_text(strip=True) if exp_span else ''
    else:
        agent_info['Experience'] = ''

    # Phone Number
    phone_number = card.select_one("div.agent-phone")
    agent_info['Phone'] = phone_number.get_text(strip=True) if phone_number else ''

    # Email (Check if Email button exists)
    email_button = card.select_one("span.agent-email button")
    agent_info['Email Available'] = 'Yes' if email_button else 'No'

    # For Sale and Sold:
    # They both appear in the same div, something like:
    # <div class="... pb-1 pt-16">For sale: <span>3</span> Sold: <span>6</span></div>
    for_sale_sold_div = card.find("div", class_=re.compile(r'pb-1.*pt-16'))
    if for_sale_sold_div:
        text = for_sale_sold_div.get_text(" ", strip=True)
        # Example text: "For sale: 3 Sold: 6"
        # Use regex to extract the numbers
        match = re.search(r"For sale:\s*(\d+).+Sold:\s*(\d+)", text)
        if match:
            agent_info['For Sale'] = match.group(1)
            agent_info['Sold'] = match.group(2)
        else:
            agent_info['For Sale'] = ''
            agent_info['Sold'] = ''
    else:
        agent_info['For Sale'] = ''
        agent_info['Sold'] = ''

    # Reviews and Recommendations
    # They appear as:
    # <span class="agent-reviews">1 reviews</span> | <span class="agent-recommand">1 recommendations</span>
    reviews_span = card.select_one("span.agent-reviews")
    if reviews_span:
        reviews_text = reviews_span.get_text(strip=True)
        # Extract just the digit
        reviews_num = re.sub(r'\D', '', reviews_text)
        agent_info['Reviews'] = reviews_num if reviews_num else '0'
    else:
        agent_info['Reviews'] = '0'

    recomm_span = card.select_one("span.agent-recommand")
    if recomm_span:
        recomm_text = recomm_span.get_text(strip=True)
        # Extract just the digit
        recomm_num = re.sub(r'\D', '', recomm_text)
        agent_info['Recommendations'] = recomm_num if recomm_num else '0'
    else:
        agent_info['Recommendations'] = '0'

    # Activity Range
    activity_range_div = card.find("div", text=re.compile(r'Activity range:'))
    if activity_range_div:
        activity_range_value = activity_range_div.find("span", class_="bold-text")
        agent_info['Activity Range'] = activity_range_value.get_text(strip=True) if activity_range_value else ''
    else:
        agent_info['Activity Range'] = ''

    # Last Listed (Listed a house)
    listed_date_div = card.find("div", text=re.compile(r'Listed a house:'))
    if listed_date_div:
        listed_date_value = listed_date_div.find("span", class_="bold-text")
        agent_info['Last Listed'] = listed_date_value.get_text(strip=True) if listed_date_value else ''
    else:
        agent_info['Last Listed'] = ''

    # Last Sold (Sold a house)
    sold_house_div = card.find("div", text=re.compile(r'Sold a house:'))
    if sold_house_div:
        sold_house_value = sold_house_div.find("span", class_="bold-text")
        agent_info['Last Sold'] = sold_house_value.get_text(strip=True) if sold_house_value else ''
    else:
        agent_info['Last Sold'] = ''

    # Languages
    language_div = card.find("div", class_="agent-language")
    if language_div:
        lang_span = language_div.find("span", class_="bold-text")
        agent_info['Languages'] = lang_span.get_text(strip=True) if lang_span else ''
    else:
        agent_info['Languages'] = ''

    # Certifications
    # The icons have classes like "icon-certification-gri", "icon-certification-crs", etc.
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
    parser.add_argument('zipcodes_file', default='filtered_zipcodes.txt', help='Path to the .txt file containing zipcodes (one per line).')
    parser.add_argument('--output_dir', default='data/realtor_agents', help='Directory to save output CSV files.')
    parser.add_argument('--max_workers', type=int, default=10, help='Number of parallel workers.')
    args = parser.parse_args()

    # Get API key from environment variable
    api_key = os.environ['ZENROWS_API_KEY']
    if not api_key:
        raise ValueError("ZENROWS_API_KEY not found in environment variables")

    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    # Read zipcodes from the file
    with open(args.zipcodes_file, 'r') as f:
        zipcodes = [line.strip() for line in f if line.strip()]

    # Filter out already scraped zipcodes
    existing_files = set(os.listdir(args.output_dir))
    zipcodes_to_scrape = [
        zipcode for zipcode in zipcodes 
        if f'agents_info_{zipcode}.csv' not in existing_files
    ]

    if not zipcodes_to_scrape:
        logger.info("All zipcodes have already been scraped!")
        return

    logger.info(f"Found {len(zipcodes_to_scrape)} zipcodes to scrape out of {len(zipcodes)} total zipcodes")
    start_time = time.time()

    # Add progress bar for overall zipcode processing
    with tqdm(total=len(zipcodes_to_scrape), desc="Processing zipcodes") as pbar:
        # Use ProcessPoolExecutor to process zipcodes in parallel
        with ProcessPoolExecutor(max_workers=args.max_workers) as executor:
            futures = []
            for zipcode in zipcodes_to_scrape:
                futures.append(executor.submit(scrape_zipcode, zipcode, args.output_dir, api_key))

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"An error occurred: {e}")
                pbar.update(1)

    end_time = time.time()
    logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()