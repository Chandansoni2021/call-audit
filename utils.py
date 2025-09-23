import os
from dotenv import load_dotenv
import boto3
import time
import json
from datetime import datetime
from urllib.parse import urlparse
from PyPDF2 import PdfReader
from decimal import Decimal
from pydub import AudioSegment
from botocore.exceptions import ClientError
import regex as re
# Load environment variables
load_dotenv()

# AWS Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")\


# Initialize AWS clients with proper configuration
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)
table = dynamodb.Table('call_audit')

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

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

bedrock_client = boto3.client(
    service_name="bedrock-runtime",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

def convert_floats_to_decimals(data):
    """Recursively convert all float values in a dictionary to Decimal"""
    if isinstance(data, float):
        return Decimal(str(data))
    elif isinstance(data, dict):
        return {k: convert_floats_to_decimals(v) for k, v in data.items()}
    elif isinstance(data, (list, tuple)):
        return [convert_floats_to_decimals(item) for item in data]
    return data

def transcribe_audio_aws(s3_uri: str, original_filename: str, save_folder: str = "transcripts") -> dict:
    """
    Transcribes audio file using AWS Transcribe service
    
    Args:
        s3_uri: S3 URI of the audio file
        original_filename: Original filename of the audio file
        save_folder: Local folder to save transcript
    
    Returns:
        Dictionary with status, transcript text, and path to saved transcript
    """
    try:
        job_name = f"transcribe_{int(time.time())}"

        # Start transcription job
        transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': s3_uri},
            MediaFormat=os.path.splitext(original_filename)[1][1:].lower(),
            LanguageCode='en-US',
            OutputBucketName=BUCKET_NAME,
            OutputKey=f"transcribe-output/{job_name}.json"
        )

        # Wait for job to complete
        while True:
            status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
            job_status = status['TranscriptionJob']['TranscriptionJobStatus']
            if job_status in ['COMPLETED', 'FAILED']:
                break
            time.sleep(5)

        if job_status == 'FAILED':
            return {"status": "error", "error": "Transcription failed."}

        # Get the transcript file from S3
        output_key = f"transcribe-output/{job_name}.json"
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=output_key)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        transcript_text = data['results']['transcripts'][0]['transcript']

        # Save transcript locally
        os.makedirs(save_folder, exist_ok=True)
        transcript_filename = os.path.splitext(original_filename)[0] + ".txt"
        transcript_path = os.path.join(save_folder, transcript_filename)

        with open(transcript_path, "w", encoding="utf-8") as f:
            f.write(transcript_text)

        return {
            "status": "done",
            "transcript": transcript_text,
            "transcript_path": transcript_path
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}

def invoke_bedrock_claude(prompt: str, model_id: str = "anthropic.claude-3-haiku-20240307-v1:0") -> dict:
    """
    Invokes Claude 3 model on AWS Bedrock
    
    Args:
        prompt: The input prompt/text to send to Claude
        model_id: The Bedrock model ID to use
    
    Returns:
        Dictionary containing the model's response
    """
    try:
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 3000,
            "messages": [{
                "role": "user",
                "content": [{"type": "text", "text": prompt}]
            }]
        })

        response = bedrock_client.invoke_model(
            modelId=model_id,
            body=body
        )

        response_body = json.loads(response['body'].read().decode('utf-8'))
        return response_body

    except Exception as e:
        return {"error": str(e)}

import json
import re
from datetime import datetime
from decimal import Decimal

def convert_floats_to_decimals(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimals(item) for item in obj]
    return obj

async def summarize_conversation_bedrock(transcript_text: str, today_date: str = None) -> dict:
    """
    Summarizes a Leap Finance call transcript using Claude 3 on AWS Bedrock
 
    Args:
        transcript_text: The conversation transcript to analyze
        today_date: Reference date for analysis (YYYY-MM-DD format)
 
    Returns:
        Dictionary containing structured analysis of the conversation
    """
    if not today_date:
        today_date = datetime.now().strftime("%Y-%m-%d")
 
    known_services = [
    "Education Loan Assistance", "GRE Preparation Support", "IELTS Coaching",
    "University Admission Guidance", "Forex Support", "Visa Counseling",
    "Pre-departure Support", "Credit Assessment", "Collateral-Free Loans",
    "Low Interest Rates", "Loan Disbursement Tracking", "Financial Planning",
    "Partnership with Global Universities", "Loan Sanction Letter",
    "Document Collection Support", "Multi-country Loan Options"
]
 
    prompt = f"""Today is {today_date}. Please analyze the following transcript of a Leap Finance sales call related to study abroad education loans.
 
Your task is to extract structured insights in JSON format for Leap Finance, focusing on the student's academic background, financial eligibility, and interest in education loan services. Follow these rules:
 
1. For contact numbers:
   - Format them as 10 digits only, without spaces or special characters
   - Pick the most relevant **customer** number only, ignore agent or others
 
2. For sentiment scores:
   - Ensure Positive + Negative + Neutral scores total exactly 10
   - Match Overall_Customer_Sentiment with the dominant sentiment
   - Statement counts should total correctly
 
3. For product interests:
   - Match only exact strings from this Leap Finance services list:
     - Education Loan Assistance
     - GRE Preparation Support
     - IELTS Coaching
     - University Admission Guidance
     - Forex Support
     - Visa Counseling
     - Pre-departure Support
     - Credit Assessment
     - Collateral-Free Loans
     - Low Interest Rates
     - Loan Disbursement Tracking
     - Financial Planning
     - Partnership with Global Universities
     - Loan Sanction Letter
     - Document Collection Support
     - Multi-country Loan Options
 
   - Return matched services as a comma-separated string or not provided
 
4. For performance scores:
   - Use whole numbers between 1-10
   - Calculate 'score' as the average of agent scores (rounded)
 
5. For education & eligibility:
   - Extract desired course (e.g., MS in CS)
   - Extract name of college/university student wants to attend
   - Identify whether student is eligible for the college (Yes/No/Unknown)
   - Capture 10th, 12th, UG academic scores (percentage or GPA)
   - Capture highest qualification (e.g., B.Tech, BBA, B.Com)
   - Capture estimated fee-paying capacity or loan need
 
If any information is missing or not clearly stated, return `not provided`. Do not assume or guess.
 
Ensure your final output is valid JSON, with no comments or explanations outside the JSON.
 
Return output in this exact JSON format:
{{
    "Customer": {{
        "Name": "Full name or not provided",
        "Contact_Details": "Phone number or not provided",
        "Email": "Valid email or not provided",
        "Address": "Full address or not provided",
        "Verification_Proof": "Mentioned proof ID or not provided",
        "Emergency_Contact_Details": "Phone number or not provided",
        "Storage_Items": "Items mentioned (if any) or not provided",
        "Pricing_Details": "Fees or loan amount details with currency",
        "Booking_Details": "Appointment or submission dates",
        "Desired_Course": "Name of course (e.g., MS in CS)",
        "Desired_College": "Name of university/college",
        "Eligible_For_College": "Yes/No/Unknown",
        "Education_Scores": {{
            "10th": "Percentage or GPA",
            "12th": "Percentage or GPA",
            "UG": "Percentage or GPA or not provided"
        }},
        "Highest_Qualification": "Latest degree (e.g., B.Tech)",
        "Fee_Paying_Capacity": "Amount student/family can pay or loan need"
    }},
    "Sales_Agent": {{
        "Name": "Agent's name or not provided",
        "Company": "Leap Finance",
        "Position": "Agent’s title or not provided"
    }},
    "Purpose_of_call": "Purpose summary in 1-2 sentences",
    "Summary": [
        "Main point (max 10 words)",
        "Second point (max 10 words)",
        "Third point (max 10 words)",
        "Fourth point (max 10 words)"
    ],
    "User_Satisfaction": "Yes/No/Partially",
    "Next_Steps": "Mentioned next steps (max 2 lines)",
    "follow_up_call": "YYYY-MM-DD HH:MM or not provided",
    "Competitor_Mention": "Other company names or not provided",
    "Customer_Tone": "Neutral/Happy/Angry/Frustrated",
    "Sales_Agent_Score": {{
        "Professionalism": 1-10,
        "Product_Knowledge": 1-10,
        "Communication_Skills": 1-10,
        "Problem_Solving": 1-10
    }},
    "Product_Interest": "Comma-separated list from known_services or not provided",
    "Call_Quality": "Good or describe issue",
    "Customer_Sentiment_Per_Statement": [
        {{
            "Statement": "Exact customer quote",
            "Sentiment": "Positive/Negative/Neutral",
            "Emotion": "Specific emotion"
        }}
    ],
    "Sentiment_Scores": {{
        "Total_Sentiment_Score": 10,
        "Positive_Sentiment_Score": X,
        "Negative_Sentiment_Score": Y,
        "Neutral_Sentiment_Score": Z
    }},
    "Overall_Customer_Sentiment": "Positive/Negative/Neutral",
    "Overall_Customer_Emotion": "Happy/Confused/Frustrated/etc.",
    "Statement_Counts": {{
        "Total_Customer_Statements": N,
        "Positive_Statements": X,
        "Negative_Statements": Y,
        "Neutral_Statements": Z
    }},
    "score": "Average of agent scores (rounded)",
    "Call_Disconnected": "True/False",
    "Call_Completion_Status": "True/False"
}}
Transcript for analysis:
{transcript_text}
"""
 
    try:
        response = invoke_bedrock_claude(prompt)
 
        if "error" in response:
            return {"error": response["error"]}
 
        content = response.get("content", [])
        if not content:
            return {"error": "Empty response from Bedrock"}
 
        summary_text = content[0].get("text", "")
        if not summary_text:
            return {"error": "No analysis generated"}
 
        try:
            summary_dict = json.loads(summary_text)
 
            if "Sentiment_Scores" in summary_dict:
                total = 10
                pos = summary_dict["Sentiment_Scores"].get("Positive_Sentiment_Score", 0)
                neg = summary_dict["Sentiment_Scores"].get("Negative_Sentiment_Score", 0)
                neu = summary_dict["Sentiment_Scores"].get("Neutral_Sentiment_Score", 0)
 
                current_sum = pos + neg + neu
                if current_sum != total and current_sum > 0:
                    scale = total / current_sum
                    summary_dict["Sentiment_Scores"]["Positive_Sentiment_Score"] = round(pos * scale, 1)
                    summary_dict["Sentiment_Scores"]["Negative_Sentiment_Score"] = round(neg * scale, 1)
                    summary_dict["Sentiment_Scores"]["Neutral_Sentiment_Score"] = round(neu * scale, 1)
 
                summary_dict["Sentiment_Scores"]["Total_Sentiment_Score"] = total
 
            if "Sales_Agent_Score" in summary_dict:
                scores = [
                    summary_dict["Sales_Agent_Score"].get("Professionalism", 0),
                    summary_dict["Sales_Agent_Score"].get("Product_Knowledge", 0),
                    summary_dict["Sales_Agent_Score"].get("Communication_Skills", 0),
                    summary_dict["Sales_Agent_Score"].get("Problem_Solving", 0)
                ]
                valid_scores = [s for s in scores if isinstance(s, (int, float))]
                if valid_scores:
                    summary_dict["score"] = round(sum(valid_scores) / len(valid_scores))
                else:
                    summary_dict["score"] = 0
 
            if "Customer" in summary_dict:
                for field in ["Contact_Details", "Emergency_Contact_Details"]:
                    if field in summary_dict["Customer"] and summary_dict["Customer"][field] not in [None, "not provided"]:
                        cleaned = re.sub(r'[^\d]', '', str(summary_dict["Customer"][field]))
                        if len(cleaned) > 10:
                            cleaned = cleaned[-10:]
                        summary_dict["Customer"][field] = cleaned if cleaned else "not provided"
 
            summary_dict = convert_floats_to_decimals(summary_dict)
            return summary_dict
 
        except json.JSONDecodeError as e:
            return {"error": f"Failed to parse response: {str(e)}", "raw_response": summary_text}
 
    except Exception as e:
        return {"error": f"Unexpected error: {str(e)}"}
 
 


def convert_floats_to_decimals(obj):
    """Recursively convert all floats in a structure to Decimals"""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_floats_to_decimals(x) for x in obj]
    return obj


async def extract_text_from_pdf(pdf_file, file_name):
    reader = PdfReader(pdf_file)
    content_chunks = []
 
    # Iterate through each page in the PDF
    for page_num, page in enumerate(reader.pages, start=1):
        # Extract text from the page, ensure it returns a string even if empty
        page_content = page.extract_text() or ''
        # print(f"Extracted content from page {page_num}:\n{page_content}")
 
        # Split content into chunks of specified word count
        chunks = chunks_string1(page_content, 200)
        # print("poiuytr",chunks)
        # print(f"Splitting page {page_num} content into chunks...")
 
        # Store each chunk with the page number and file name
        content_chunks.extend([
            (page_num, file_name, chunk.strip())
            for chunk in chunks if len(chunk.split()) > 2
        ])
 
    # print("Extracted and chunked content:", content_chunks)
    return content_chunks
   
async def get_audio_duration(file_path: str, format_type: str = "m.s") -> str:
    """
    Get the duration of an audio file in M.S format (e.g., 3.18) or float seconds.
 
    Args:
        file_path (str): Path to the audio file.
        format_type (str): Format type - "m.s" for M.S, "float" for decimal seconds.
 
    Returns:
        str: Duration in the specified format.
    """
    try:
        audio = AudioSegment.from_file(file_path)
        duration_seconds = len(audio) / 1000  # Convert milliseconds to seconds
 
        if format_type == "m.s":    
            minutes = int(duration_seconds // 60)  # Get minutes
            seconds = int(duration_seconds % 60)   # Get remaining seconds
            return f"{minutes}.{seconds}"  # Example: 3.18 (3 minutes 18 seconds)
 
        elif format_type == "float":
            return str(round(duration_seconds, 2))  # Example: 198.12
 
        else:
            raise ValueError("Invalid format_type. Choose 'm.s' or 'float'.")
 
    except Exception as e:
        print(f"Error fetching audio duration: {e}")
        return None  # Return None if there's an error
