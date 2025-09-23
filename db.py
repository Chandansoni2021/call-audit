from dotenv import load_dotenv
import boto3
from collections import defaultdict
from datetime import datetime
from botocore.exceptions import ClientError
import os

load_dotenv()

# AWS Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")

# Initialize DynamoDB
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

# Define table name
TABLE_NAME = 'call_audit'
table = dynamodb.Table(TABLE_NAME)
response = table.scan()
# print("Raw Items:", response['Items'])

def fetch_call_details(call_id: str):
    """
    Fetch call summary and transcript from DynamoDB by call_id
    """
    try:
        response = table.get_item(Key={"call_id": call_id})
        item = response.get("Item")
 
        if item:
            print("✅ Data fetched successfully.")
            return {    
                "call_id": item.get("call_id"),
                "s3_url": item.get("s3_url"),
                "call_duration":item.get("call_duration"),
                "transcription": item.get("Transcript"),
                "summary": item.get("Summary"),
                "QA_Pairs": item.get("QA_pairs")
            }
        else:
            print("❌ No data found for this call_id.")
            return {}

    except ClientError as e:
        print(f"ClientError: {e.response['Error']['Message']}")
        return {}
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return {}


def get_http_audio_url_from_dynamo(call_id: str) -> str | None:
    """
    Fetches s3_uri from DynamoDB using call_id and returns the HTTP audio URL.
    """
    try:
        # Step 1: Fetch s3_uri
        response = table.get_item(Key={"call_id": call_id})
        item = response.get("Item")
        
        if not item or "s3_uri" not in item:
            return None

        s3_uri = item["s3_uri"]

        # Step 2: Convert to HTTP link
        parts = s3_uri.replace("s3://", "").split("/", 1)
        if len(parts) != 2:
            return None

        bucket, key = parts
        http_url = f"https://{bucket}.s3.{AWS_REGION}.amazonaws.com/{key}"

        return http_url

    except Exception as e:
        print(f"❌ Error getting audio URL from DynamoDB: {e}")
        return None
    
from decimal import Decimal
def safe_float(val):
    if isinstance(val, (float, int, Decimal)):
        return float(val)
    try:
        return float(str(val))
    except:
        return None

def parse_dynamodb_item(item):
    try:
        summary = item.get("Summary", {})
        print(summary)
        customer = summary.get("Customer", {})
        scores = summary.get("Sales_Agent_Score", {})
        sentiment_scores = summary.get("Sentiment_Scores", {})
        sales_agent = summary.get("Sales_Agent", {})
        Call_Completion_Satus=summary.get("Call_Completion_Satus",{})
        print("iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii",Call_Completion_Satus)
        return {
            "call_id": item.get("call_id"),
            "agent_name": sales_agent.get("Name"),
            "call_duration": item.get("call_duration"),
            "user_satisfaction": summary.get("User_Satisfaction"),
            "customer_name": customer.get("Name"),
            "Product_Interest": summary.get("Product_Interest"),
            "purpose_of_call": summary.get("Purpose_of_call"),
            "score": safe_float(summary.get("score")),
            "professionalism": safe_float(scores.get("Professionalism")),
            "product_knowledge": safe_float(scores.get("Product_Knowledge")),
            "communication_skills": safe_float(scores.get("Communication_Skills")),
            "problem_solving": safe_float(scores.get("Problem_Solving")),
            "Call_Completion_Satus":summary.get("Call_Completion_Satus"),
            "total_sentiment_score": safe_float(sentiment_scores.get("Total_Sentiment_Score")),
            "positive_sentiment_score": safe_float(sentiment_scores.get("Positive_Sentiment_Score")),
            "negative_sentiment_score": safe_float(sentiment_scores.get("Negative_Sentiment_Score")),
            "neutral_sentiment_score": safe_float(sentiment_scores.get("Neutral_Sentiment_Score")),
            "updated_at": item.get("created_on") or item.get("CreatedOn"),
            "transcription": item.get("Transcript"),
            "qa_pairs": item.get("QA_pairs", [])
        }
    except Exception as e:
        print(f"❌ Error parsing item: {e}")
        return None
def fetch_call_audit():
    try:
        print("Fetching all call audit data...")
        response = table.scan()
        items = response.get("Items", [])

        formatted_result = {}
        for item in items:
            parsed = parse_dynamodb_item(item)
            # print(parsed)
            if parsed:
                call_id = item.get("call_id")
                formatted_result[call_id] = parsed

        return formatted_result
    except Exception as e:
        print(f"Error fetching call audit data: {e}")
        return {}
    
async def fetch_total_calls_and_agents(agent_name=None):
    try:
        print("🔄 Fetching data from DynamoDB...")
        response = table.scan()
        items = response.get("Items", [])

        # If agent_name is provided, return data for that specific agent
        if agent_name and agent_name.lower() != "all":
            agent_name = agent_name.strip().lower()
            count = 0

            for item in items:
                summary = item.get("Summary", {})
                sales_agent = summary.get("Sales_Agent", {})
                name = sales_agent.get("Name", "")

                if name and name.strip().lower() == agent_name:
                    count += 1

            return {
                "success": True,
                "data": {
                    "total_calls": count,
                    "total_agents": 1
                }
            }

        # Else return global totals
        total_calls = len(items)
        agents_set = set()

        for item in items:
            summary = item.get("Summary", {})
            sales_agent = summary.get("Sales_Agent", {})
            name = sales_agent.get("Name", "")
            if name:
                agents_set.add(name.strip().lower())

        return {
            "success": True,
            "data": {
                "total_calls": total_calls,
                "total_agents": len(agents_set)
            }
        }

    except Exception as e:
        print(f"❌ Error in fetch_call_status_count: {e}")
        return {"success": False, "error": str(e)}
    

async def fetch_contact_details_count(agent_name=None):
    """
    Fetches total count and received/missed details for phone numbers, emails, and customer names from DynamoDB.
    """
    try:
        print("🚀 Starting fetch_contact_details_count from DynamoDB...")

        response = table.scan()
        items = response.get("Items", [])
        print(f"📄 Total items fetched: {len(items)}")

        # Filter if agent_name provided
        if agent_name and agent_name.lower() != "all":
            agent_name = agent_name.strip().lower()
            items = [
                item for item in items
                if item.get("Summary", {}).get("Sales_Agent", {}).get("Name", "").strip().lower() == agent_name
            ]
            print(f"🔍 Filtered items for agent '{agent_name}': {len(items)}")

        total_calls = len(items)
        print(f"📊 Total calls: {total_calls}")

        # Sets and counters
        phone_set, email_set, customer_set = set(), set(), set()
        received_contacts = received_emails = received_customers = 0

        for i, item in enumerate(items):
            summary = item.get("Summary", {})
            customer = summary.get("Customer", {})

            phone = str(customer.get("Contact_Details", "") or "").strip().lower()
            email = str(customer.get("Email", "") or "").strip().lower()
            name = str(customer.get("Name", "") or "").strip().lower()

            print(f"🔹 Record {i+1}: phone='{phone}', email='{email}', name='{name}'")

            if phone and phone != "not provided":
                phone_set.add(phone)
                received_contacts += 1

            if email and email != "not provided":
                email_set.add(email)
                received_emails += 1

            if name and name != "not provided":
                customer_set.add(name)
                received_customers += 1

        # Missed
        missed_contacts = max(0, total_calls - received_contacts)
        missed_emails = max(0, total_calls - received_emails)
        missed_customers = max(0, total_calls - received_customers)

        # % Calculator
        def calc_percent(value, total):
            return round((value / total * 100), 2) if total > 0 else 0

        # Final Response
        result = [
            {
                "title": "Contacts",
                "achieved": len(phone_set),
                "achievedPercent": calc_percent(received_contacts, total_calls),
                "missed": missed_contacts,
                "missedPercent": calc_percent(missed_contacts, total_calls)
            },
            {
                "title": "Emails",
                "achieved": len(email_set),
                "achievedPercent": calc_percent(received_emails, total_calls),
                "missed": missed_emails,
                "missedPercent": calc_percent(missed_emails, total_calls)
            },
            {
                "title": "Customers Name",
                "achieved": len(customer_set),
                "achievedPercent": calc_percent(received_customers, total_calls),
                "missed": missed_customers,
                "missedPercent": calc_percent(missed_customers, total_calls)
            }
        ]

        print("✅ Final Result:")
        for item in result:
            print(item)

        return result

    except Exception as e:
        print(f"❗ Error in fetch_contact_details_count: {e}")
        return []
    

async def get_contacts_by_agent(agent_name=None):
    """
    Fetches contacts (call_id, agent_name, Contact_Details) for the specified agent from DynamoDB.
    """
    try:
        response = table.scan()
        items = response.get("Items", [])

        filtered_contacts = []

        for item in items:
            summary = item.get("Summary", {})
            customer = summary.get("Customer", {})
            agent = summary.get("Sales_Agent", {})
            contact = customer.get("Contact_Details", "")
            agent_value = agent.get("Name", "")

            # Agent filter
            if agent_name and agent_name.lower() != "all":
                if not agent_value or str(agent_value).strip().lower() != str(agent_name).strip().lower():
                    continue

            # Validate contact
            if contact and str(contact).strip().lower() != "not provided":
                filtered_contacts.append({
                    "call_id": item.get("call_id", ""),
                    "agent_name": agent_value,
                    "Contact_Details": contact
                })

        return {"contacts": filtered_contacts}

    except Exception as e:
        print(f"❗ DynamoDB Error fetching contacts: {e}")
        return {"contacts": []}
    
async def get_email_by_agent(agent_name=None):
    """
    Fetches emails (call_id, agent_name, email) for the specified agent from DynamoDB.
    """
    try:
        response = table.scan()
        items = response.get("Items", [])

        filtered_emails = []

        for item in items:
            summary = item.get("Summary", {})
            customer = summary.get("Customer", {})
            agent = summary.get("Sales_Agent", {})
            email = customer.get("Email", "")
            agent_value = agent.get("Name", "")

            # Agent filter
            if agent_name and agent_name.lower() != "all":
                if not agent_value or str(agent_value).strip().lower() != str(agent_name).strip().lower():
                    continue

            # Validate email
            if email and str(email).strip().lower() != "not provided":
                filtered_emails.append({
                    "call_id": item.get("call_id", ""),
                    "agent_name": agent_value,
                    "email": email
                })

        return {"emails": filtered_emails}

    except Exception as e:
        print(f"❗ DynamoDB Error fetching emails: {e}")
        return {"emails": []}
    
async def get_customer_name_by_agent(agent_name=None):
    """
    Fetches customer names (call_id, agent_name, customer_name) for the specified agent from DynamoDB.
    """
    try:
        response = table.scan()
        items = response.get("Items", [])

        filtered_customers = []

        for item in items:
            summary = item.get("Summary", {})
            customer = summary.get("Customer", {})
            agent = summary.get("Sales_Agent", {})
            name = customer.get("Name", "")
            agent_value = agent.get("Name", "")

            # Agent filter
            if agent_name and agent_name.lower() != "all":
                if not agent_value or str(agent_value).strip().lower() != str(agent_name).strip().lower():
                    continue

            # Validate name
            if name and str(name).strip().lower() != "not provided":
                filtered_customers.append({
                    "call_id": item.get("call_id", ""),
                    "agent_name": agent_value,
                    "customer_name": name
                })

        print("✅ Customer names fetched successfully")
        return {"customer_names": filtered_customers}

    except Exception as e:
        print(f"❗ DynamoDB Error fetching customer names: {e}")
        return {"customer_names": []}
    


async def fetch_call_status_count(agent_name=None):
    """
    Fetches the count of Call_Completion_Status values: 'true' and 'not provided' from DynamoDB.
    """
    try:
        print("📞 Fetching Call_Completion_Status count from DynamoDB...")

        response = table.scan()
        items = response.get("Items", [])

        true_count = 0
        not_provided_count = 0

        for item in items:
            summary = item.get("Summary") or item.get("Summary") or {}
            agent = summary.get("Sales_Agent", {})
            agent_value = agent.get("Name", "")

            # Filter by agent_name if provided
            if agent_name and agent_name.lower() != "all":
                if not agent_value or str(agent_value).strip().lower() != str(agent_name).strip().lower():
                    continue

            # Call Completion Status (can be True, False, None, or missing)
            status = summary.get("Call_Completion_Status")
            print(status)
            if status == "True":
                true_count += 1
            else:
                not_provided_count += 1

        print(f"✅ Call_Completion_Status = true: {true_count}, not provided or false: {not_provided_count}")
        return {
            "call_completion_status_count": {
                "true": true_count,
                "not_provided_or_false": not_provided_count
            }
        }

    except Exception as e:
        print(f"❗ DynamoDB Error fetching Call_Completion_Status: {e}")
        return {
            "call_completion_status_count": {
                "true": 0,
                "not_provided_or_false": 0
            }
        }


async def fetch_agent_names():
    """
    Fetch all unique agent names from DynamoDB based on the Summary -> Sales_Agent -> Name field.
    If agent name is missing or null, include it as 'Not Provided'.
    """
    try:
        print("🔍 Fetching all agent names from DynamoDB...")
 
        response = table.scan()
        items = response.get("Items", [])
 
        names_set = set()
 
        for item in items:
            summary = item.get("summary") or item.get("Summary") or {}
            agent = summary.get("Sales_Agent", {})
 
            # Handle missing or null name
            name = agent.get("Name")
            clean_name = str(name).strip() if name is not None else ''
 
            if clean_name:
                names_set.add(clean_name)
            else:
                names_set.add("Not Provided")
 
        agent_names = sorted(list(names_set))
        print(f"✅ Found {len(agent_names)} unique agent names.")
        return agent_names
 
    except Exception as e:
        print(f"❗ DynamoDB Error fetching agent names: {e}")
        return ["Not Provided"]
    


def parse_updated_at(updated_at_value):
    try:
        if isinstance(updated_at_value, str):
            try:
                return datetime.fromisoformat(updated_at_value).date().isoformat()
            except ValueError:
                return datetime.strptime(updated_at_value, "%Y-%m-%d %H:%M:%S").date().isoformat()
        else:
            return datetime.strptime(updated_at_value, "%Y-%m-%d %H:%M:%S").date().isoformat()
    except Exception:
        return None

def get_calls_per_day_from_db():
    """
    Scans the table and returns count of items grouped by date.
    """
    date_counts = defaultdict(int)
    items = []

    response = table.scan(ProjectionExpression="CreatedOn")
    print("Initial scan response:", response)
    items.extend(response.get('Items', []))

    while 'LastEvaluatedKey' in response:
        response = table.scan(
            ProjectionExpression="CreatedOn",
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response.get('Items', []))

    for item in items:
        updated_at = item.get("CreatedOn")
        parsed_date = parse_updated_at(updated_at)
        if parsed_date:
            date_counts[parsed_date] += 1

    return dict(date_counts)



def get_sentiment_summary_from_dynamodb():
    try:
        # Scan the DynamoDB table to get all items
        response = table.scan()
        all_items = response.get('Items', [])
        print(f"Initial scan returned {len(all_items)} items.")
 
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            all_items.extend(response.get('Items', []))
 
        if not all_items:
            return {
                "avg_sentiment": 0.0,
                "total_calls": 0,
                "positive_calls": 0,
                "negative_calls": 0,
                "neutral_calls": 0
            }
 
        total_calls = 0
        positive_calls = 0
        negative_calls = 0
        neutral_calls = 0
 
        for item in all_items:
            # Check if 'Summary' and 'Sentiment_Scores' exist
            summary = item.get('Summary', {})
            sentiment_scores = summary.get('Sentiment_Scores', {})
 
            pos = int(sentiment_scores.get('Positive_Sentiment_Score', 0))
            neg = int(sentiment_scores.get('Negative_Sentiment_Score', 0))
            neu = int(sentiment_scores.get('Neutral_Sentiment_Score', 0))
 
            print(f"Processing Call ID: {item.get('call_id')}, Pos: {pos}, Neg: {neg}, Neu: {neu}")
 
            # Determine dominant sentiment
            if pos >= neg and pos >= neu:
                positive_calls += 1
            elif neg >= pos and neg >= neu:
                negative_calls += 1
            else:
                neutral_calls += 1
 
            total_calls += 1
 
        # Calculate weighted average
        weighted_score = (
            (positive_calls * 1.0) +
            (neutral_calls * 0.5) +
            (negative_calls * 0.0)
        ) / total_calls if total_calls > 0 else 0
 
        avg_sentiment = round(weighted_score * 100, 2)
 
        return {
            "avg_sentiment": avg_sentiment,
            "total_calls": total_calls,
            "positive_calls": positive_calls,
            "negative_calls": negative_calls,
            "neutral_calls": neutral_calls
        }
 
    except Exception as e:
        print(f"Error processing sentiment data: {str(e)}")
        raise e
 
   
 

def fetch_agent_score_rankings():
    try:
        # Scan the DynamoDB table
        response = table.scan()
        print(response)
        
        if not response.get('Items'):
            return {"top_5_agents": [], "bottom_5_agents": [], "message": "No call records found."}

        agent_metrics = defaultdict(lambda: {
            "scores": [],
            "professionalism": [],
            "product_knowledge": [],
            "communication_skills": [],
            "problem_solving": []
        })

        # Process each item in the response
        for item in response['Items']:
            # Check if Summary exists and has Sales_Agent_Score
            if 'Summary' not in item or 'Sales_Agent_Score' not in item['Summary']:
                continue
                
            # Get agent name - checking multiple possible locations
            agent_name = None
            if 'Sales_Agent' in item['Summary'] and 'Name' in item['Summary']['Sales_Agent']:
                agent_name = item['Summary']['Sales_Agent']['Name']
            elif 'agent_name' in item:
                agent_name = item['agent_name']
                
            if not agent_name:
                continue

            # Get scores from Sales_Agent_Score
            scores = item['Summary']['Sales_Agent_Score']
            
            try:
                # Handle score (check both Summary level and Sales_Agent_Score)
                if 'score' in item['Summary'] and item['Summary']['score'] is not None:
                    agent_metrics[agent_name]["scores"].append(float(item['Summary']['score']))
                
                # Handle other metrics
                if 'Professionalism' in scores and scores['Professionalism'] is not None:
                    agent_metrics[agent_name]["professionalism"].append(float(scores['Professionalism']))
                if 'Product_Knowledge' in scores and scores['Product_Knowledge'] is not None:
                    agent_metrics[agent_name]["product_knowledge"].append(float(scores['Product_Knowledge']))
                if 'Communication_Skills' in scores and scores['Communication_Skills'] is not None:
                    agent_metrics[agent_name]["communication_skills"].append(float(scores['Communication_Skills']))
                if 'Problem_Solving' in scores and scores['Problem_Solving'] is not None:
                    agent_metrics[agent_name]["problem_solving"].append(float(scores['Problem_Solving']))
            except (ValueError, TypeError):
                continue  # Skip non-numeric or invalid values

        # Calculate averages for each agent
        agent_avg_data = []
        for name, metrics in agent_metrics.items():
            total_calls = len(metrics["scores"])
            if total_calls == 0:
                continue
                
            avg_score = round(sum(metrics["scores"]) / total_calls, 2)
            avg_prof = round(sum(metrics["professionalism"]) / len(metrics["professionalism"]), 2) if metrics["professionalism"] else 0
            avg_pk = round(sum(metrics["product_knowledge"]) / len(metrics["product_knowledge"]), 2) if metrics["product_knowledge"] else 0
            avg_comm = round(sum(metrics["communication_skills"]) / len(metrics["communication_skills"]), 2) if metrics["communication_skills"] else 0
            avg_prob = round(sum(metrics["problem_solving"]) / len(metrics["problem_solving"]), 2) if metrics["problem_solving"] else 0
            
            agent_avg_data.append({
                "agent_name": name,
                "avg_score": avg_score,
                "avg_professionalism": avg_prof,
                "avg_product_knowledge": avg_pk,
                "avg_communication_skills": avg_comm,
                "avg_problem_solving": avg_prob,
                "total_calls": total_calls
            })

        # Sort and get top/bottom 5 agents
        top_5 = sorted(agent_avg_data, key=lambda x: x["avg_score"], reverse=True)[:5]
        bottom_5 = sorted(agent_avg_data, key=lambda x: x["avg_score"])[:5]
        
        return {
            "top_5_agents": top_5,
            "bottom_5_agents": bottom_5,
            "message": "Successfully fetched agent rankings"
        }
        
    except Exception as e:
        return {
            "top_5_agents": [],
            "bottom_5_agents": [],
            "message": f"Error fetching agent rankings: {str(e)}"
        }