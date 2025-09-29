# 📞 Call Audit – Voice Call Analysis using AWS Bedrock & Transcribe

**Call Audit** is a voice-based data analysis tool that leverages **AWS services** like **Transcribe**, **Bedrock**, **S3**, and **DynamoDB** to perform intelligent audio transcription and query answering — without using LangChain or other third-party orchestration frameworks.

---

##  Features

-  **Audio Transcription** using Amazon Transcribe
-  **Storage** of audio files in Amazon S3
-  **Transcript & Metadata Management** using DynamoDB
-  **LLM-based Q&A** using Amazon Bedrock (e.g., Claude, Titan, etc.)
-  Custom Python logic for chunking, embedding, and retrieval
-  Natural language interface to query call transcripts

---

## 🧰 Tech Stack

| Component      | Tech Used                 |
|----------------|---------------------------|
| Language       | Python 3.12+               |
| AI/LLM         | Amazon Bedrock            |
| Audio-to-Text  | Amazon Transcribe         |
| Storage        | Amazon S3                 |
| Database       | Amazon DynamoDB           |
| SDK            | Boto3 (AWS SDK for Python)|
| Others         | `dotenv`, `uuid`, etc.    |

---

## 📁 Project Structure

call-audit/
├── .env # Environment variables
├── Q_A.py # LLM-based Q&A logic
├── aws_transcribe_handler.py # Audio upload & transcription
├── dynamo_handler.py # DynamoDB read/write operations
├── rag.py # Embedding & basic retrieval logic
├── utils.py # Helper functions: chunking, cleaning
├── main.py # Entry point for running the app
├── requirements.txt # Python dependencies


---

## ⚙️ Setup

### 1. Clone the Repo

```bash
git clone https://github.com/Chandansoni2021/call-audit.git
cd call-audit



2. Install Dependencies
pip install -r requirements.txt

3. Configure Environment Variables

Create a .env file in the root directory with the following:

AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_REGION=your_aws_region
S3_BUCKET_NAME=your_bucket_name
DYNAMO_TABLE_NAME=your_dynamodb_table
BEDROCK_MODEL_ID=your_bedrock_model_id  # e.g. anthropic.claude-v2

🧪 Usage
▶️ Step 1: Upload and Transcribe Audio
python aws_transcribe_handler.py --file_path path/to/your/audio.wav

💬 Step 2: Ask Questions About the Call
python main.py


