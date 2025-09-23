from fastapi import FastAPI
import boto3
import json
from botocore.exceptions import ClientError
import os 
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from rag import generate_embeddings
import io
from dotenv import load_dotenv
load_dotenv()



AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")

# Initialize AWS clients
bedrock = boto3.client(
    'bedrock-runtime',
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
app = FastAPI()

async def extract_customer_qa_pairs(transcription):
    """Identify customer & executive, then extract Q&A pairs with complete responses using AWS Bedrock Claude."""
    print("\n=== STARTING Q&A EXTRACTION WITH CLAUDE ===")
    
    prompt = f"""
    You are analyzing a sales call transcript where two speakers are having a conversation.

    ### Task:
    1. Determine which speaker is the Customer and which is the Sales Executive.
    2. Extract only the questions asked by the Customer.
    3. Extract only the answers given by the Sales Executive.
    4. Ignore questions asked by the Sales Executive.
    5. Ensure Executive responses are complete (if cut-off, continue extracting).
    6. Do not summarize or shorten responses.
    7. Return valid JSON format (no extra text, no markdown).

    ### Speaker Identification Rules:
    - The Customer asks about products, services, pricing, or features.
    - The Sales Executive gives explanations, confirmations, and recommendations.

    ### Expected JSON Output Format:
    {{
        "qa_pairs": [
            {{
                "customer_question": "Extracted question from customer",
                "executive_answer": "Full response from sales executive"
            }}
        ]
    }}

    ### Conversation Transcript:
    {transcription}
    """

    try:
        # Invoke Claude model
        response = bedrock.invoke_model(
            modelId="anthropic.claude-3-sonnet-20240229-v1:0",
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 2000,
                "messages": [{
                    "role": "user",
                    "content": [{"type": "text", "text": prompt}]
                }]
            })
        )

        response_body = json.loads(response['body'].read().decode('utf-8'))
        response_text = response_body['content'][0]['text']
        
        print("\nRaw Claude Response:")
        print(response_text)

        # Extract JSON from response
        try:
            if "{" in response_text and "}" in response_text:
                json_start = response_text.index("{")
                json_end = response_text.rindex("}") + 1
                response_text = response_text[json_start:json_end]
            
            extracted_data = json.loads(response_text)
            print("\nSuccessfully parsed JSON:")
            print(json.dumps(extracted_data, indent=2))
            return extracted_data
            
        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {str(e)}")
            return {"qa_pairs": []}

    except ClientError as e:
        print(f"AWS Bedrock error: {str(e)}")
        return {"qa_pairs": []}
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return {"qa_pairs": []}
    


async def query_csv_and_ask_claude(query, top_n=5):
    """Search CSV for relevant info and generate answer using Claude"""
    try:
        # 1. Fetch CSV data (replace with your S3 implementation)
        df = fetch_csv_from_s3()  # Changed from Azure Blob to S3
        if df is None or df.empty:
            return "Error: Could not fetch knowledge base data."

        # 2. Generate embedding for the query
        query_embedding = generate_embeddings(query)
        if not query_embedding:
            return "Error: Failed to generate query embedding."

        # 3. Calculate cosine similarities
        similarities = []
        for idx, row in df.iterrows():
            try:
                embedding = np.array(eval(row['embedding']))
                similarity = cosine_similarity([query_embedding], [embedding])[0][0]
                similarities.append((idx, similarity, row['file_name'], row['text']))
            except:
                continue  # Skip rows with invalid embeddings

        # 4. Get top N most relevant results
        similarities.sort(key=lambda x: x[1], reverse=True)
        top_results = similarities[:top_n]
        relevant_texts = [result[3] for result in top_results]

        # 5. Prepare prompt for Claude
        prompt = f"""\n\nHuman: You are a helpful AI assistant. Based on the following context:
        
        Context:
        {''.join(relevant_texts)}

        Please provide a concise 2-3 sentence answer to this question:
        Question: {query}

        Guidelines:
        - Be factual and precise
        - Use only the provided context
        - If unsure, say "I don't have enough information"
        - Keep response under 100 words

        \n\nAssistant: Here is the answer to your question:"""

        # 6. Call Claude model
        response = bedrock.invoke_model(
            modelId="anthropic.claude-3-haiku-20240307-v1:0",  # Using Haiku for cost efficiency
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 300,
                "temperature": 0.3,
                "top_p": 0.9,
                "messages": [{
                    "role": "user",
                    "content": [{"type": "text", "text": prompt}]
                }]
            })
        )

        # 7. Process response
        response_body = json.loads(response['body'].read().decode('utf-8'))
        return response_body['content'][0]['text'].strip()

    except Exception as e:
        print(f"Error in query_csv_and_ask_claude: {str(e)}")
        return f"Error: {str(e)}"


# Helper function to fetch CSV from S3 (replace with your implementation)
def fetch_csv_from_s3():
    """Fetch CSV from S3 bucket"""
    try:
        # Example implementation - adjust based on your S3 setup
        response = s3.get_object(Bucket=BUCKET_NAME, Key="validation_data/MS_1_MS_2_MS_3_merged.csv")
        return pd.read_csv(io.StringIO(response['Body'].read().decode('utf-8')))
    except Exception as e:
        print(f"Error fetching CSV from S3: {str(e)}")
        return None 



async def validate_answer(data):
    if not isinstance(data, dict) or "qa_pairs" not in data:
        return json.dumps({"error": "Invalid data format. Expected a dictionary with key 'qa_pairs'."}, indent=4)

    qa_pairs = data["qa_pairs"]
    if not isinstance(qa_pairs, list):
        return json.dumps({"error": "Invalid data format. 'qa_pairs' should be a list of dictionaries."}, indent=4)

    results = []

    for qa_pair in qa_pairs:
        if not isinstance(qa_pair, dict):
            results.append({"error": "Invalid QA pair format. Expected a dictionary."})
            continue

        customer_question = qa_pair.get("customer_question", "").strip()
        executive_answer = qa_pair.get("executive_answer", "").strip()
       
        if not customer_question or not executive_answer:
            results.append({"error": "Missing required fields in qa_pair."})
            continue

        # Get AI-generated answer (using Claude)
        try:
            ai_answer = await query_csv_and_ask_claude(customer_question)  # Changed to Claude version
            if not ai_answer:
                ai_answer = "No AI answer available."  # Fallback in case query fails
        except Exception as e:
            ai_answer = f"Error fetching AI answer: {str(e)}"

        # Construct prompt for scoring using Claude
        scoring_prompt = f"""\n\nHuman: You are evaluating a sales conversation. Here are the details:

Customer Question: "{customer_question}"

AI-Generated Ideal Answer: "{ai_answer}"
Salesperson's Actual Answer: "{executive_answer}"

Please evaluate how well the salesperson's answer matches the ideal answer in terms of:
1. Accuracy of information
2. Clarity of explanation
3. Relevance to the question
4. Professional tone
5. Completeness of response

Provide:
1. A score from 0-10 (10 being perfect)
2. Specific improvements needed (if any)
3. What was done well

Return your evaluation in this exact JSON format:
{{
    "score": <number>,
    "improvements": ["list", "of", "suggestions"],
    "strengths": ["list", "of", "positive", "aspects"]
}}

\n\nAssistant: {{
"""

        try:
            # Call Claude model
            response = bedrock.invoke_model(
                modelId="anthropic.claude-3-sonnet-20240229-v1:0",
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 1000,
                    "temperature": 0.3,
                    "messages": [{
                        "role": "user",
                        "content": [{"type": "text", "text": scoring_prompt}]
                    }]
                })
            )

            response_body = json.loads(response['body'].read().decode('utf-8'))
            claude_response = response_body['content'][0]['text']
            
            # Extract JSON from Claude's response
            try:
                if "{" in claude_response and "}" in claude_response:
                    json_start = claude_response.index("{")
                    json_end = claude_response.rindex("}") + 1
                    claude_response = claude_response[json_start:json_end]
                
                evaluation = json.loads(claude_response)
            except json.JSONDecodeError:
                evaluation = {
                    "score": 0,
                    "improvements": ["Could not parse evaluation"],
                    "strengths": []
                }

        except Exception as e:
            evaluation = {
                "score": 0,
                "improvements": [f"Error during evaluation: {str(e)}"],
                "strengths": []
            }

        # Append results
        results.append({
            "customer_question": customer_question,
            "executive_answer": executive_answer,
            "ai_answer": ai_answer,
            "score": evaluation.get("score", 0),
            "improvements": evaluation.get("improvements", []),
            "strengths": evaluation.get("strengths", [])
        })

    return json.dumps(results, indent=4, ensure_ascii=False)
