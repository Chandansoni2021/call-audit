import os
import io
import csv
import shutil
import boto3
import json
from typing import Optional
from decimal import Decimal
from PyPDF2 import PdfReader
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from botocore.exceptions import ClientError
import pandas as pd
from nltk.tokenize import sent_tokenize

# Load environment variables
load_dotenv()

app = FastAPI()

# Configuration
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# AWS Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")

# Initialize AWS clients
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

bedrock = boto3.client(
    'bedrock-runtime',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

def chunks_string(text, tokens):
    segments = []
    len_sum = 0
    k = 0
    raw_list = sent_tokenize(text)

    for i in range(len(raw_list)):
        x1 = len(raw_list[i].split())
        len_sum += x1
        k += 1

        if len_sum > tokens:
            j = i-(k+1) if i-(k+1) >= 0 else 0
            if len(" ".join(raw_list[j: i+1]).split()) > tokens:
                j = i-k
            segments.append(" ".join(raw_list[j: i]))
            len_sum = 0
            k = 0

        if i == len(raw_list)-1:
            j = i-(k+1) if i-(k+1) >= 0 else 0
            if len(" ".join(raw_list[j: i+1]).split()) > tokens:
                j = i-k
            segments.append(" ".join(raw_list[j: i+1]))

    return segments

def chunks_string1(text, chunk_size):
    words = text.split()
    for i in range(0, len(words), chunk_size):
        yield ' '.join(words[i:i + chunk_size])

async def extract_text_from_pdf(pdf_file, file_name):
    reader = PdfReader(pdf_file)
    content_chunks = []

    for page_num, page in enumerate(reader.pages, start=1):
        page_content = page.extract_text() or ''
        chunks = chunks_string1(page_content, 200)
        content_chunks.extend([
            (page_num, file_name, chunk.strip())
            for chunk in chunks if len(chunk.split()) > 2
        ])

    return content_chunks
def generate_embeddings(text, model_id="amazon.titan-embed-text-v2:0"):
    try:
        # Titan expects input in this format
        body = json.dumps({
            "inputText": text
        })

        response = bedrock.invoke_model(
            modelId=model_id,
            body=body,
            contentType="application/json",
            accept="application/json"
        )

        # Parse the response
        response_body = json.loads(response['body'].read().decode('utf-8'))

        # Titan returns embeddings under 'embedding' key
        embedding_vector = response_body['embedding']
        return embedding_vector

    except Exception as e:
        print(f"Error generating embeddings: {str(e)}")
        return []

def upload_to_s3(local_path, s3_path):
    try:
        s3.upload_file(local_path, BUCKET_NAME, s3_path)
        return True
    except ClientError as e:
        print(f"Error uploading to S3: {str(e)}")
        return False

