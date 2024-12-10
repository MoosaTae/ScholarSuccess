import requests
import json
import time
import os
from confluent_kafka import Producer


def fetch_scopus_data(
    api_key, 
    start_year, 
    end_year, 
    query_option, 
    count=10, 
    results_per_request=25, 
    base_url="https://api.elsevier.com/content/search/scopus", 
    kafka_topic="paper", 
    kafka_config=None
):
    """
    Fetch research papers from the Scopus API and produce them to a Kafka topic.

    Args:
        api_key (str): API key for Scopus.
        start_year (int): Starting year of the publication range (inclusive).
        end_year (int): Ending year of the publication range (inclusive).
        query_option (int): Index of the predefined query options (0-3).
        count (int, optional): Number of papers to fetch. Default is 10.
        results_per_request (int, optional): Maximum results per API request. Default is 25.
        base_url (str, optional): Scopus API base URL. Default is Scopus API URL.
        kafka_topic (str, optional): Kafka topic to produce data to.
        kafka_config (dict, optional): Kafka producer configuration.

    Returns:
        list: List of research papers fetched from the Scopus API.
    """

    headers = {"X-ELS-APIKey": api_key}
    kafka_producer = None

    # Initialize Kafka producer if config is provided
    if kafka_config:
        kafka_producer = Producer(kafka_config)

    queries = [
        f"PUBYEAR > {start_year - 1} AND PUBYEAR < {end_year + 1} AND DOCTYPE(cp) AND SRCTYPE(p)",
        f"PUBYEAR > {start_year - 1} AND PUBYEAR < {end_year + 1} AND DOCTYPE(cp) AND NOT SRCTYPE(p)",
        f"PUBYEAR > {start_year - 1} AND PUBYEAR < {end_year + 1} AND SRCTYPE(p) AND NOT DOCTYPE(cp)",
        f"PUBYEAR > {start_year - 1} AND PUBYEAR < {end_year + 1} AND NOT (DOCTYPE(cp) AND SRCTYPE(p))"
    ]

    if not (0 <= query_option < len(queries)):
        raise ValueError(f"Invalid query_option {query_option}. Must be between 0 and {len(queries) - 1}.")

    papers = []
    start_index = 0

    while len(papers) < count:
        fetch_count = min(results_per_request, count - len(papers))
        params = {
            "query": queries[query_option],
            "start": start_index,
            "count": fetch_count,
            "field": (
                "prism:url,dc:identifier,eid,dc:title,prism:aggregationType,subtype,"
                "subtypeDescription,citedby-count,prism:publicationName,prism:isbn,"
                "prism:issn,prism:volume,prism:issueIdentifier,prism:pageRange,"
                "prism:coverDate,prism:coverDisplayDate,prism:doi,affiliation,"
                "dc:creator,openaccess,openaccessFlag"
            ),
        }

        try:
            response = requests.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            if start_index == 0:
                total_results = int(data.get("search-results", {}).get("opensearch:totalResults", 0))
                print(f"Total results available: {total_results}")

            entries = data.get("search-results", {}).get("entry", [])
            if not entries:
                print(f"No more entries at start index {start_index}. Exiting.")
                break

            papers.extend(entries)
            print(f"Fetched {len(entries)} papers starting from index {start_index}")

            for paper in entries:
                if kafka_producer:
                    kafka_producer.produce(
                        kafka_topic, 
                        key=paper.get('dc:identifier', 'unknown'), 
                        value=json.dumps(paper).encode('utf-8'),
                        callback=_delivery_report
                    )
                    kafka_producer.flush()

            start_index += len(entries)
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data at start index {start_index}: {e}")
            break

    return papers[:count]


def _delivery_report(err, msg):
    """Delivery report for Kafka messages."""
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


if __name__ == "__main__":
    API_KEY = os.environ.get('SCOPUS_API_KEY', '72d3da80128e29b337ae7f405f2cddf8')
    START_YEAR = 2005
    END_YEAR = 2010
    QUERY_OPTION = 0
    COUNT = 10

    kafka_config = {
        "bootstrap.servers": "localhost:29092"
    }

    research_papers = fetch_scopus_data(
        api_key=API_KEY,
        start_year=START_YEAR,
        end_year=END_YEAR,
        query_option=QUERY_OPTION,
        count=COUNT,
        kafka_topic="paper",
        kafka_config=kafka_config
    )

    print(json.dumps(research_papers, indent=2))
