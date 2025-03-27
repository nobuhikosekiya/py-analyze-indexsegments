import os
import json
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

def fetch_elasticsearch_stats():
    """
    Fetch Elasticsearch stats using the _stats API endpoint and save to a JSON file.
    Authentication can be done via API key or username/password.
    Connection can be made using either standard URL/port or Cloud ID.
    """
    # Load environment variables from .env file
    load_dotenv()

    # Initialize Elasticsearch client based on configuration
    es_client = None

    # Check if using cloud_id
    cloud_id = os.getenv("ES_CLOUD_ID")
    if cloud_id:
        # Check authentication method
        api_key = os.getenv("ES_API_KEY")
        if api_key:
            # Connect using cloud_id and API key
            es_client = Elasticsearch(
                cloud_id=cloud_id,
                api_key=api_key
            )
        else:
            # Connect using cloud_id and username/password
            es_client = Elasticsearch(
                cloud_id=cloud_id,
                basic_auth=(os.getenv("ES_USERNAME"), os.getenv("ES_PASSWORD"))
            )
    else:
        # Connect using URL and port
        es_url = os.getenv("ES_URL", "http://localhost")
        es_port = os.getenv("ES_PORT", "9200")
        
        # Construct the base URL
        base_url = f"{es_url}:{es_port}"
        
        # Check authentication method
        api_key = os.getenv("ES_API_KEY")
        if api_key:
            # Connect using URL/port and API key
            es_client = Elasticsearch(
                hosts=[base_url],
                api_key=api_key
            )
        else:
            # Connect using URL/port and username/password
            es_client = Elasticsearch(
                hosts=[base_url],
                basic_auth=(os.getenv("ES_USERNAME"), os.getenv("ES_PASSWORD"))
            )

    # Verify connection
    if not es_client.ping():
        raise ConnectionError("Failed to connect to Elasticsearch")
    
    print(f"Successfully connected to Elasticsearch")
    
    # Fetch stats from Elasticsearch
    stats_response = es_client.indices.stats()
    
    # Convert the ObjectApiResponse to a dictionary
    stats = stats_response.body
    
    # Save stats to a JSON file
    with open("stats.json", "w", encoding="utf-8") as file:
        json.dump(stats, file, indent=2, ensure_ascii=False)
    
    print(f"Stats saved to stats.json")

if __name__ == "__main__":
    fetch_elasticsearch_stats()