from opensearchpy import OpenSearch
from dotenv import load_dotenv
import os

load_dotenv()

index_body = {
    "settings" : {
        "index" : {
            "number_of_shards" : 1,
            "number_of_replicas" : 1
        }
    },
    "mappings" : {
        "properties" : {
            "summary" : {"type" : "text"}
        }
    }
}

db_client = OpenSearch(
    hosts=[{'host': os.getenv("OS_DOMAIN"), 'port': 443}],
    http_auth=(os.getenv("OS_USERNAME"), os.getenv("OS_PASSWORD")),
    use_ssl=True,
    verify_certs=True
)

def check_if_index_exists(index_name):
    return db_client.indices.exists(index = index_name)

def main():
    index_name = input("Enter the index to be created: ")

    response = check_if_index_exists(index_name)

    if response == False:
        db_client.indices.create(index = index_name, body = index_body)
        print(f'{index_name} created')
    else:
        print(f'{index_name} already exists')

if __name__ == "__main__":
    main()