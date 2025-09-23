from fastapi import FastAPI, Query, UploadFile, File
import os
import boto3
from datetime import datetime
import time
from typing import Optional
from botocore.exceptions import ClientError
from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware

import json
from dotenv import load_dotenv
from pydantic import BaseModel
import io
import csv
import shutil
from Q_A import extract_customer_qa_pairs, validate_answer
from db import fetch_agent_names, fetch_agent_score_rankings, fetch_call_audit, fetch_call_details, fetch_call_status_count, fetch_total_calls_and_agents, fetch_contact_details_count, get_calls_per_day_from_db, get_contacts_by_agent, get_customer_name_by_agent, get_email_by_agent, get_http_audio_url_from_dynamo, get_sentiment_summary_from_dynamodb

from fastapi.responses import JSONResponse
from rag import extract_text_from_pdf, generate_embeddings, upload_to_s3
from utils import get_audio_duration, transcribe_audio_aws,summarize_conversation_bedrock
from fastapi.responses import StreamingResponse
import httpx



load_dotenv()

app = FastAPI()



AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Or specify your frontend origin
    allow_credentials=True,
    allow_methods=["*"],  # Or specify ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
    allow_headers=["*"],  # Make sure "Authorization" is included
)

UPLOAD_FOLDER = "upload_data"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs("transcripts", exist_ok=True)
os.makedirs("uploads", exist_ok=True)

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

transcribe = boto3.client(
    "transcribe",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)
dynamodb = boto3.resource(
    "dynamodb",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)


table = dynamodb.Table("call_audit")

@app.post("/upload-audio-s3/")
async def upload_audio_s3(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        original_filename = file.filename
        call_id = os.path.splitext(original_filename)[0]  # e.g. mycall.wav → call_id = "mycall"
        s3_key = f"recordings/{original_filename}"
        local_path = os.path.join("uploads", original_filename)

        # Save locally
        os.makedirs("uploads", exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(contents)

        # Get duration
        call_duration = await get_audio_duration(local_path)

        # Upload to S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=contents
        )

        # Transcribe
        s3_uri = f"s3://{BUCKET_NAME}/{s3_key}"
        transcript_result = transcribe_audio_aws(s3_uri, original_filename)
        if transcript_result["status"] == "error":
            raise Exception(transcript_result["error"])

        transcript_text = transcript_result.get("transcript", "")

        # Summarize & QA
        summary_result = await summarize_conversation_bedrock(transcript_text)
        QA_pairs = await extract_customer_qa_pairs(transcript_text)
        answers = await validate_answer(QA_pairs)

        # Save to DynamoDB
        table.put_item(Item={
            "call_id": call_id,
            "call_duration": call_duration,
            "s3_uri": s3_uri,
            "CreatedOn": datetime.utcnow().isoformat(),
            "Transcript": transcript_text,
            "Summary": summary_result,
            "QA_pairs": answers
        })

        # ✅ Delete the local file after everything is done
        if os.path.exists(local_path):
            os.remove(local_path)

        # Response
        file_url = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        return {
            "message": "✅ File uploaded, transcribed, summarized, and saved to database!",
            "call_id": call_id,
            "call_duration": call_duration,
            "s3_url": file_url,
            "transcription": transcript_result,
            "summary": summary_result,
            "QA_Pairs": QA_pairs
        }

    except Exception as e:
        # Optional: clean up local file on failure as well
        if os.path.exists(local_path):
            os.remove(local_path)
        return {"error": str(e)}
 


@app.post("/upload-audio-s3/")
async def upload_audio_s3(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        original_filename = file.filename
        call_id = os.path.splitext(original_filename)[0]  # e.g. mycall.wav → call_id = "mycall"
        s3_key = f"recordings/{original_filename}"
        local_path = os.path.join("uploads", original_filename)
        print(f"Processing file: {local_path}")
        # Save locally
        os.makedirs("uploads", exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(contents)
 
        # Get duration
        call_duration = await get_audio_duration(local_path)
 
        # Upload to S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=contents
        )
 
        # Transcribe
        s3_uri = f"s3://{BUCKET_NAME}/{s3_key}"
        transcript_result = transcribe_audio_aws(s3_uri, original_filename)
        print("Transcription successful")
        if transcript_result["status"] == "error":
            raise Exception(transcript_result["error"])
 
        transcript_text = transcript_result.get("transcript", "")
 
        # Summarize & QA
        summary_result = await summarize_conversation_bedrock(transcript_text)
        print(f"Summary successful {summary_result}")
        QA_pairs = await extract_customer_qa_pairs(transcript_text)
        print("QA extraction successful")
        answers = await validate_answer(QA_pairs)
        print("Answers validated successfully")
        # Save to DynamoDB
        table.put_item(Item={
            "call_id": call_id,
            "call_duration": call_duration,
            "s3_uri": s3_uri,
            "CreatedOn": datetime.utcnow().isoformat(),
            "Transcript": transcript_text,
            "Summary": summary_result,
            "QA_pairs": answers
        })
        print("Data saved to DynamoDB successfully")
        # ✅ Delete the local file after everything is done
        if os.path.exists(local_path):
            os.remove(local_path)
 
        # Response
        file_url = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        return {
            "message": "✅ File uploaded, transcribed, summarized, and saved to database!",
            "call_id": call_id,
            "call_duration": call_duration,
            "s3_url": file_url,
            "transcription": transcript_result,
            "summary": summary_result,
            "QA_Pairs": QA_pairs
        }
 
    except Exception as e:
        # Optional: clean up local file on failure as well
        if os.path.exists(local_path):
            os.remove(local_path)
        return {"error": str(e)}
 
 
    
class CallIDRequest(BaseModel):
    call_id: str
    
@app.post("/get-call-details/")
async def get_call_details(request: CallIDRequest):
    print(request.call_id)
    data = fetch_call_details(request.call_id)
    if not data:
        raise HTTPException(status_code=404, detail="Call data not found for given call_id.")
    return {
        "message": "✅ Call details retrieved successfully!",
        "data": data
    }

@app.get("/get-call-audit/")
async def get_call_audit():
    try:
        data = fetch_call_audit()  # Fetch all call records
        # print(data, "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
        if not data:
            raise HTTPException(status_code=404, detail="No data found")
        return JSONResponse(content={"success": True, "data": data})
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})
 
 
@app.get("/get-s3-uri/{call_id}")
def generate_audio_url(call_id: str):
    http_url = get_http_audio_url_from_dynamo(call_id)

    if not http_url:
        raise HTTPException(status_code=404, detail="Call ID not found or invalid S3 URI.")

    return {
        "message": "✅ Audio URL generated successfully",
        "call_id": call_id,
        "http_url": http_url
    }

def generate_presigned_url(bucket, key, expires_in=3600):
    return s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={'Bucket': bucket, 'Key': key},
        ExpiresIn=expires_in
    )


@app.post("/get-audio/")
async def get_audio(file_name: str = Form(...)):
    # Generate the S3 URL (same as before)
    key = f"recordings/{file_name}.wav"
    bucket = "recordingsnewnew"
    url = generate_presigned_url(bucket, key)
    
    if not url:
        raise HTTPException(status_code=500, detail="Could not generate audio URL")
    
    # Fetch the audio from S3 and stream it
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            
            return StreamingResponse(
                response.iter_bytes(),
                media_type="audio/wav",
                headers={"Content-Disposition": f"inline; filename={file_name}.wav"}
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch audio: {str(e)}")


@app.get("/get-total-calls-agents/")
async def get_call_summary(agent_name: Optional[str] = None):
    try:
        data = await fetch_total_calls_and_agents(agent_name)

        if not data or not data.get("success"):
            raise HTTPException(status_code=404, detail="No call status data found.")

        return JSONResponse(content=data)

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)}
        )
@app.get("/get-contact-details-count/")
async def get_contact_details_count(agent_name: str = Query(None)):
    try:
        data = await fetch_contact_details_count(agent_name)
        print(data)
        if not data:
            raise HTTPException(status_code=404, detail="No data found.")
        return JSONResponse(content={"success": True, "data": data})
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})
    


@app.get("/fetch_contacts_agent/")
async def fetch_contacts_agent(agent_name: str = Query(None)):
    """API to fetch contacts based on the agent's name."""  
    try:
        data = await get_contacts_by_agent(agent_name)  # Function to fetch contacts
        if not data:
            raise HTTPException(status_code=404, detail="No contacts found for this agent.")
        return JSONResponse(content={"success": True, "data": data})
    
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})

@app.get("/fetch_email_agent/")
async def fetch_email_agent(agent_name: str = Query(None)):
    """API to fetch contacts based on the agent's name."""
    try:
        data = await get_email_by_agent(agent_name)  # Function to fetch contacts
        if not data:
            raise HTTPException(status_code=404, detail="No contacts found for this agent.")
        return JSONResponse(content={"success": True, "data": data})
    
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})

@app.get("/fetch_customer_name_agent/")
async def fetch_customer_name_agent(agent_name: str = Query(None)):
    """API to fetch contacts based on the agent's name."""
    print(f"Received agent_name: {agent_name}")
    try:
        data = await get_customer_name_by_agent(agent_name)  # Function to fetch contacts
        if not data:
            raise HTTPException(status_code=404, detail="No contacts found for this agent.")
        return JSONResponse(content={"success": True, "data": data})
    
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})
    

@app.get("/get-call-status-count/")
async def get_call_status_count(agent_name: str = Query(None)):
    try:
        data = await fetch_call_status_count(agent_name)
        if not data:
            raise HTTPException(status_code=404, detail="No call status data found.")
        return JSONResponse(content={"success": True, "data": data})
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})
    
    
@app.get("/get-agent-names/")
async def get_agent_names():
    """
    API route to fetch all unique agent names.
    """
    try:
        consultant_name = await fetch_agent_names()
        if not consultant_name:
            raise HTTPException(status_code=404, detail="No consultant_name.")
        return JSONResponse(content={"success": True, "agents":consultant_name})
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)}) 
    


@app.get("/api/sentiment/summary")
def get_sentiment_summary():
    try:
        sentiment_summary = get_sentiment_summary_from_dynamodb()
        print(f"Sentiment Summary: {sentiment_summary}")  # Debugging line
        return sentiment_summary
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/calls-per-day/")
def get_calls_per_day():
    try:
        result = get_calls_per_day_from_db()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    
 
@app.get("/api/sentiment/summary")
def get_sentiment_summary():
    try:
        sentiment_summary = get_sentiment_summary_from_dynamodb()
        print(f"Sentiment Summary: {sentiment_summary}")  # Debugging line
        return sentiment_summary
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/agent/score-ranking")
def get_top_bottom_agents():
    try:
        return fetch_agent_score_rankings()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))