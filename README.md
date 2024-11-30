# The ultimate agent harvester

A collection of tools for filtering and processing US zipcode data.

## Zipcode Filtering Tool

The `filter_zipcodes.py` script allows you to filter US zipcodes based on various criteria such as state, city, population, and density.

### Usage

#### Examples

1. Filter zipcodes for a specific state:

```bash
python filter_zipcodes.py --zipcodes_dir data/zipcodes --state CA --output_file filtered_zipcodes_ca.txt
```

2. Filter zipcodes for a specific city:

```bash
python filter_zipcodes.py --zipcodes_dir data/zipcodes --city "San Francisco" --output_file filtered_zipcodes_sf.txt
```

3. Filter zipcodes for a specific county:

```bash
python filter_zipcodes.py --zipcodes_dir data/zipcodes --county "Los Angeles" --output_file filtered_zipcodes_la.txt
```

4. Filter zipcodes for a specific population range:

```bash
python filter_zipcodes.py --zipcodes_dir data/zipcodes --min_population 100000 --max_population 200000 --output_file filtered_zipcodes_pop_100k_200k.txt
```

## Scrape Realtor Agents by Zipcode

The `scrape_realtor_agents_by_zipcode.py` script allows you to scrape realtor.com agent data for a given list of zipcodes.

### Usage

```bash
python scrape_realtor_agents_by_zipcode.py --zipcodes_file filtered_zipcodes.txt --output_dir data/realtor_agents
```
