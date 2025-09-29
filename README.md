# 📞 Call Audit – Voice Call Analysis using AWS Bedrock & Transcribe

**Call Audit** is a voice-based data analysis tool that leverages **AWS services** like **Transcribe**, **Bedrock**, **S3**, and **DynamoDB** to perform intelligent audio transcription and query answering — without using LangChain or other third-party orchestration frameworks.

---

## 🚀 Features

- 🎙️ **Audio Transcription** using Amazon Transcribe
- ☁️ **Storage** of audio files in Amazon S3
- 🧾 **Transcript & Metadata Management** using DynamoDB
- 🧠 **LLM-based Q&A** using Amazon Bedrock (e.g., Claude, Titan, etc.)
- 🧩 Custom Python logic for chunking, embedding, and retrieval
- 🔍 Natural language interface to query call transcripts

---

## 🧰 Tech Stack

| Component      | Tech Used                 |
|----------------|---------------------------|
| Language       | Python 3.8+               |
| AI/LLM         | Amazon Bedrock            |
| Audio-to-Text  | Amazon Transcribe         |
| Storage        | Amazon S3                 |
| Database       | Amazon DynamoDB           |
| SDK            | Boto3 (AWS SDK for Python)|
| Others         | `dotenv`, `uuid`, etc.    |

---

## 📁 Project Structure

