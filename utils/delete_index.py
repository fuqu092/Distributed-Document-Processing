from opensearchpy import OpenSearch
from dotenv import load_dotenv
import os

load_dotenv()

db_client = OpenSearch(
    hosts=[{'host': os.getenv("OS_DOMAIN"), 'port': 443}],
    http_auth=(os.getenv("OS_USERNAME"), os.getenv("OS_PASSWORD")),
    use_ssl=True,
    verify_certs=True
)

def check_if_index_exists(index_name):
    return db_client.indices.exists(index = index_name)

def main():
    index_name = input("Enter the index to be deleted: ")

    response = check_if_index_exists(index_name)

    if response == True:
        db_client.indices.delete(index = index_name)
        print(f'{index_name} deleted')
    else:
        print(f'{index_name} doesn\'t exist')

if __name__ == "__main__":
    main()