from opensearchpy import OpenSearch
from kafka import KafkaConsumer
from dotenv import load_dotenv
from PyPDF2 import PdfReader
from io import BytesIO
import requests
import boto3
import json
import os

load_dotenv()

def get_request():
    consumer = KafkaConsumer(
        'test2',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='location-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        return message.value["req_id"], message.value["pdf_name"]

def get_summary(prompt):
    invoke_url = "https://integrate.api.nvidia.com/v1/chat/completions"
    stream = False

    headers = {
        "Authorization": os.getenv("NVIDIA_API_KEY"),
        "Accept": "text/event-stream" if stream else "application/json"
    }

    payload = {
        "model": "meta/llama-4-maverick-17b-128e-instruct",
        "messages": [{"role":"user","content":prompt}],
        "max_tokens": 512,
        "temperature": 1.00,
        "top_p": 1.00,
        "frequency_penalty": 0.00,
        "presence_penalty": 0.00,
        "stream": stream
    }

    response = requests.post(invoke_url, headers=headers, json=payload)
    return response.json()["choices"][0]["message"]["content"]

def get_text(pdf_name):
    s3_client = boto3.client(
        service_name="s3",
        region_name=os.getenv("AWS_REGION"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_GET"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY_GET")
    )

    bucket_name = os.getenv("S3_BUCKET_NAME")

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=pdf_name)
        pdf_bytes = response['Body'].read()

        reader = PdfReader(BytesIO(pdf_bytes))
        text = ""

        for page in reader.pages:
            extracted = page.extract_text()
            if extracted:
                text += extracted + "\n"

        return text.strip()

    except Exception as e:
        print(f"Error: {e}")
        return None

def index_document(req_id, summary):
    db_client = OpenSearch(
        hosts=[{'host': os.getenv("OS_DOMAIN"), 'port': 443}],
        http_auth=(os.getenv("OS_USERNAME"), os.getenv("OS_PASSWORD")),
        use_ssl=True,
        verify_certs=True
    )

    document = {
        "summary" : summary
    }

    db_client.index(
        index = "test",
        body = document,
        id = req_id,
        refresh = True
    )
    print(req_id)
    print("Document indexed in opensearch")

def main():
    req_id, pdf_name = get_request()
    response = get_text(pdf_name)
    if response == None:
        print(f"Error Downloading {pdf_name}")
        return None
    
    prompt = response + "\n\nGenerate a 3 line summary of the above text\n"
    summary = get_summary(prompt)

    index_document(req_id, summary)

if __name__ == "__main__":
    main()