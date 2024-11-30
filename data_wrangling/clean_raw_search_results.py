
import json
import os

PATH_INPUT = 'data/raw_google_searches'
PATH_OUTPUT = 'data/clean_google_searches'

if not os.path.exists(PATH_OUTPUT):
    os.makedirs(PATH_OUTPUT)
    print(f"Created output directory: {PATH_OUTPUT}")
else:
    print(f"Output directory already exists: {PATH_OUTPUT}")


for filename in os.listdir(PATH_INPUT):
    if filename.endswith('.json'):
        file_path = os.path.join(PATH_INPUT, filename)
        
        # Load the JSON file
        with open(file_path, 'r') as file:
            data = json.load(file)
        
        # Simplify the data according to the provided code
        results = []
        
        # Process organic results
        for result in data.get('organicResults', []):
            results.append({
                "title": result.get('title'),
                "link": result.get('link'),
                "source": result.get('source'),
                "snippet": result.get('snippet'),
                "highlighted_words": result.get('snippetHighlitedWords'),
                "type": "organic"
            })
        
        # Process knowledge graph data if it exists
        knowledge_graph = data.get('knowledgeGraph')
        if knowledge_graph:
            results.append({
                "type": "knowledge_graph",
                "data": knowledge_graph
            })
        
        # Print the filename and the number of organic results
        print(f"Processing: {filename}")
        print(f"Number of organic results: {len(results)}")
        
        # Construct the output file path
        output_file_path = os.path.join(PATH_OUTPUT, filename)

        # Save the simplified JSON data to the output file
        with open(output_file_path, 'w') as output_file:
            json.dump(results, output_file, indent=2)
