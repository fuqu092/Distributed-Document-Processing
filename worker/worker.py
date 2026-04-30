from kafka import KafkaConsumer, ConsumerRebalanceListener
from kafka.errors import CommitFailedError
from opensearchpy import OpenSearch
from botocore.config import Config
from dotenv import load_dotenv
import boto3
import fitz
import time
import json
import os


load_dotenv()

s3_config = Config(
    connect_timeout=5,
    read_timeout=10,
    retries={
        'max_attempts': 2,
        'mode': 'standard'
    }
)

s3_client = boto3.client(
    service_name="s3",
    region_name=os.getenv("AWS_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_GET"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY_GET"),
    config=s3_config
)

db_client = OpenSearch(
    hosts=[{'host': os.getenv("OS_DOMAIN"), 'port': 443}],
    http_auth=(os.getenv("OS_USERNAME"), os.getenv("OS_PASSWORD")),
    use_ssl=True,
    verify_certs=True
)

llm_client = boto3.client(
    "bedrock-runtime",
    region_name="ap-south-1",
    aws_access_key_id=os.getenv("BEDROCK_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("BEDROCK_SECRET_ACCESS_KEY"),
)

consumer = KafkaConsumer(
    bootstrap_servers=['kafka-svc:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='location-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    max_poll_interval_ms=300000,
    session_timeout_ms=30000,
    heartbeat_interval_ms=10000,
    max_poll_records=5,
)
print("Consumer created", flush=True)


class RebalanceHandler(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked):
        print(f"[Rebalance] Partitions revoked: {revoked}", flush=True)

    def on_partitions_assigned(self, assigned):
        print(f"[Rebalance] Partitions assigned: {assigned}", flush=True)


def get_text(pdf_name):
    bucket_name = os.getenv("S3_BUCKET_NAME")
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=pdf_name)
        print("Reading bytes", flush=True)
        pdf_bytes = response['Body'].read()

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text = "\n".join(page.get_text() for page in doc)
        
        return text.strip()

    except Exception as e:
        print(f"Error getting text: {e}", flush=True)
        return None


def get_summary(prompt):
    try:
        response = llm_client.converse( 
            modelId="google.gemma-3-4b-it", 
            messages=[ 
                { 
                    "role": "user", 
                    "content": [{"text": "Write a one-sentence bedtime story about a unicorn."}]
                } 
            ] 
        )
        return response["output"]["message"]["content"][0]["text"]
    except Exception as e:
        print(f"Error getting summary: {e}", flush=True)
        return None


def index_document(req_id, pdf_name, summary):
    document = {
        "pdf_name": pdf_name,
        "summary": summary
    }
    try:
        db_client.index(
            index="test",
            body=document,
            id=req_id,
            refresh=True
        )
        print("Document indexed in opensearch", flush=True)
        return True
    except Exception as e:
        print(f"Error indexing document: {e}", flush=True)
        return None


def wait_for_assignment(consumer, timeout_seconds=30):
    """Wait until partitions are assigned after subscribe."""
    print("Waiting for partition assignment...", flush=True)
    start = time.time()
    while not consumer.assignment():
        consumer.poll(timeout_ms=1000)
        elapsed = time.time() - start
        if elapsed > timeout_seconds:
            print("WARNING: No partitions assigned after timeout. This consumer may be idle.", flush=True)
            return False
    print(f"Assigned partitions: {consumer.assignment()}", flush=True)
    return True


def main():
    consumer.subscribe(
        [os.getenv("KAFKA_TOPIC_NAME")],
        listener=RebalanceHandler()
    )

    assignmet_response = wait_for_assignment(consumer)
    if assignmet_response == False:
        return None

    for message in consumer:
        try:
            req_id = message.value["req_id"]
            pdf_name = message.value["pdf_name"]

            print(f"Processing {req_id}", flush=True)

            text = get_text(pdf_name)
            if text == None:
                continue

            print(f"Text extracted", flush=True)

            prompt = text[:5000] + "\n\nGenerate a 3 line summary\n" + "\n\nYour output format: <summary generated>\n"
            summary = get_summary(prompt)
            if summary == None:
                continue

            print(f"Summary generated", flush=True)

            response = index_document(req_id, pdf_name, summary)
            if response == None:
                continue

            consumer.commit()

        except CommitFailedError as e:
            print(f"Lost group membership: {e}", flush=True)
            break
        except Exception as e:
            print(f"Error processing msg: {e}", flush=True)
    
    consumer.close()


if __name__ == "__main__":
    main()