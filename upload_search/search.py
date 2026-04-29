from opensearchpy import OpenSearch
import os

db_client = OpenSearch(
    hosts=[{'host': os.getenv("OS_DOMAIN"), 'port': 443}],
    http_auth=(os.getenv("OS_USERNAME"), os.getenv("OS_PASSWORD")),
    use_ssl=True,
    verify_certs=True
)

def search(num_results, q):
    query = {
        'size': num_results,
        'query': {
            'multi_match': {
                'query': q,
                'fields': ['summary']
            }
        }
    }

    responses = db_client.search(
        body = query,
        index = 'test'
    )

    num_response = responses["hits"]["total"]["value"]
    results = responses["hits"]["hits"]

    print(f"\nNumber of results: {num_response}")

    print("-" * 100)

    for result in results:
        pdf_name = result["_source"]["pdf_name"]
        summary = result["_source"]["summary"]

        print(f"PDF Name: {pdf_name}")
        print(f"Summary: {summary}")
        print("-"*100)

    # print(response)