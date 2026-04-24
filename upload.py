import os
from pathlib import Path
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

load_dotenv()

def check_pdf(pdf_path):
    if pdf_path.is_file() == False:
        print("PDF doesn't exist!!!")
        return False
    if pdf_path.suffix.lower() != ".pdf":
        print("Enter a valid PDF!!!")
        return False

    return True

def upload_pdf_to_aws(pdf_path):
    s3_client = boto3.client(
        service_name="s3",
        region_name=os.getenv("AWS_REGION"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_UPLOAD"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY_UPLOAD")
    )

    pdf_path_str = str(pdf_path)

    try:
        response = s3_client.upload_file(pdf_path_str, os.getenv("AWS_S3_BUCKET_NAME"), pdf_path_str)
        print(f'File uploading response: {response}')
    except ClientError as e:
        print(f'File uploading error: {e}')
        return False

    return True

def upload_pdf(pdf_name):
    if not pdf_name.lower().endswith(".pdf"):
        pdf_name += ".pdf"

    pdf_path = Path(pdf_name)

    pdf_check_response = check_pdf(pdf_path)
    if pdf_check_response == False:
        return False

    return upload_pdf_to_aws(pdf_path)
