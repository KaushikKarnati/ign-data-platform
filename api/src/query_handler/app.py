import json
import time
import re
import os
import boto3
from botocore.exceptions import ClientError

# Read config from environment variables (set in SAM template)
# os.getenv('KEY', 'default') — reads env var, falls back to default if not set
ATHENA_DATABASE  = os.getenv('ATHENA_DATABASE',  'nl_sql_db')
ATHENA_TABLE = os.getenv('ATHENA_TABLE', 'ign_videos')
RESULTS_BUCKET   = os.getenv('RESULTS_BUCKET',   '')
BEDROCK_MODEL_ID = os.getenv('BEDROCK_MODEL_ID', 'anthropic.claude-3-sonnet-20240229-v1:0')
BEDROCK_REGION   = os.getenv('BEDROCK_REGION',   'us-east-1')
 
# AWS clients
bedrock_client = boto3.client('bedrock-runtime', region_name=BEDROCK_REGION)
athena_client  = boto3.client('athena',          region_name='us-east-1')


bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-1')
athena_client = boto3.client('athena', region_name='us-east-1')

SCHEMA = """You have access to ONE table in Amazon Athena:
 
Table name: ign_videos
Database: nl_sql_db
 
Columns:
  - video_id          (string)  : unique YouTube video ID
  - title             (string)  : full video title
  - published_date    (string)  : date published in YYYY-MM-DD format
  - published_year    (bigint)  : year the video was published
  - published_month   (bigint)  : month number (1=January, 12=December)
  - duration_seconds  (bigint)  : video length in seconds
  - duration_category (string)  : Short (<60s), Medium (1-5min), Long (5-20min), Extra Long (20min+)
  - view_count        (bigint)  : total number of views
  - like_count        (bigint)  : total number of likes
  - comment_count     (bigint)  : total number of comments
  - engagement_rate   (double)  : ((likes + comments) / views) * 100
 
Sample values:
  - duration_category: Short, Medium, Long, Extra Long
  - published_year:    2025, 2026
  - published_month:   1 through 12
  - published_date:    ranges from 2025-02-13 to 2026-01-27
  - view_count:        ranges from 0 to several million
  - engagement_rate:   typically 1.0 to 20.0 (percentage)
"""


def generate_sql(question: str) -> str:
    """
    Takes a plain English question and returns a valid Athena SQL query.
    Uses AWS Bedrock (Claude 3 Sonnet) to do the conversion.
    """
    # Build the prompt
    prompt = f"""You are an expert SQL analyst. Your job is to convert natural language questions into valid Amazon Athena SQL queries.
 
Here is the database schema you must use:
{SCHEMA}
 
Rules you MUST follow:
1. Return ONLY the SQL query — no explanation, no markdown, no code fences
2. Always use the exact table name: sales
3. Always use the exact column names listed above
4. Use standard SQL that works in Amazon Athena (Presto-based)
5. For date filtering, use string comparison: WHERE order_date >= '2024-01-01'
6. Always include a LIMIT clause (default LIMIT 100 if not specified)
7. If the question is ambiguous, write the most useful query you can
 
Question: {question}
 
SQL query:"""
 
    # Call Bedrock
    response = bedrock_client.invoke_model(
        modelId='anthropic.claude-3-sonnet-20240229-v1:0',
        body=json.dumps({
            'anthropic_version': 'bedrock-2023-05-31',
            'max_tokens': 500,
            'temperature': 0.0,
            'messages': [
                {'role': 'user', 'content': prompt}
            ]
        })
    )
 
    # Parse the response
    response_body = json.loads(response['body'].read())
    raw_sql = response_body['content'][0]['text'].strip()
 
    # Clean up: remove markdown code fences if Bedrock added them
    # e.g. ```sql ... ``` -> just the SQL
    cleaned_sql = re.sub(r'```(?:sql)?\n?', '', raw_sql).strip()
    cleaned_sql = re.sub(r'```$', '', cleaned_sql).strip()
 
    return cleaned_sql

def run_athena_query(sql: str) -> list:
    """
    Submits SQL to Athena, waits for completion, returns results as
    a list of dicts. Each dict is one row: {column_name: value}.
    """
    # Step 1: Submit the query
    response = athena_client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            'Database': ATHENA_DATABASE   # The Glue database from Day 2
        },
        ResultConfiguration={
            'OutputLocation': f's3://{RESULTS_BUCKET}/'
        }
    )
    query_execution_id = response['QueryExecutionId']
 
    # Step 2: Poll until the query finishes
    max_attempts = 30   # Max 30 attempts * 2 seconds = 60 second timeout
    for attempt in range(max_attempts):
        status_response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        state = status_response['QueryExecution']['Status']['State']
 
        if state == 'SUCCEEDED':
            break   # Query finished — exit the loop
        elif state in ['FAILED', 'CANCELLED']:
            error = status_response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            raise Exception(f'Athena query {state}: {error}')
        else:
            # State is RUNNING or QUEUED — wait and try again
            time.sleep(2)
    else:
        raise Exception('Athena query timed out after 60 seconds')
 
    # Step 3: Fetch the results
    results_response = athena_client.get_query_results(
        QueryExecutionId=query_execution_id
    )
 
    # Step 4: Parse results into a list of dicts
    rows = results_response['ResultSet']['Rows']
 
    if len(rows) <= 1:
        return []   # Only header row, no data
 
    # First row is always the column headers
    headers = [col['VarCharValue'] for col in rows[0]['Data']]
 
    # Remaining rows are data
    results = []
    for row in rows[1:]:
        row_dict = {}
        for i, cell in enumerate(row['Data']):
            row_dict[headers[i]] = cell.get('VarCharValue', '')
        results.append(row_dict)
 
    return results

def health_handler(event, context):
    """Simple health check endpoint — returns 200 if Lambda is running."""
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({
            'status': 'healthy',
            'service': 'nl-to-sql-engine',
            'version': '1.0.0'
        })
    }


def lambda_handler(event, context):

    # Route to health check if it's a GET /health request
    http_method = event.get('httpMethod', '')
    path        = event.get('path', '')
 
    if http_method == 'GET' and path == '/health':
        return health_handler(event, context)


    """
    Main Lambda entry point. AWS calls this function when the Lambda is invoked.
 
    event:   dict containing the incoming request (API Gateway passes the HTTP request here)
    context: Lambda runtime info (function name, timeout remaining, etc.) — we don't use it
    """
    # --- 1. Parse the incoming request ---
    try:
        # API Gateway sends the request body as a JSON string inside event['body']
        body = json.loads(event.get('body', '{}'))
    except json.JSONDecodeError:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Request body must be valid JSON'})
        }
 
    question = body.get('question', '').strip()
 
    # --- 2. Validate input ---
    if not question:
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Missing required field: question'})
        }
 
    if len(question) > 500:
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Question too long. Maximum 500 characters.'})
        }
 
    # --- 3. Generate SQL using Bedrock ---
    try:
        sql = generate_sql(question)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        return {
            'statusCode': 503,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': f'Bedrock error: {error_code}',
                'message': 'Could not generate SQL. Please try again.'
            })
        }
 
    # --- 4. Run the SQL in Athena ---
    try:
        results = run_athena_query(sql)
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Athena query failed',
                'sql': sql,
                'message': str(e)
            })
        }
 
    # --- 5. Return success response ---
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'    # Allows browser apps to call this API
        },
        'body': json.dumps({
            'question': question,
            'sql':      sql,
            'results':  results,
            'count':    len(results)
        })
    }
