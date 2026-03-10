# config.py — central config for the NL-to-SQL engine
import os
 
# S3 Buckets
DATA_BUCKET    = os.getenv('DATA_BUCKET',    'nl-sql-data-867207177403')
RESULTS_BUCKET = os.getenv('RESULTS_BUCKET', 'nl-sql-athena-results-867207177403')
 
# Athena
ATHENA_DATABASE = os.getenv('ATHENA_DATABASE', 'nl_sql_db')
ATHENA_TABLE    = os.getenv('ATHENA_TABLE',    'sales')
ATHENA_REGION   = os.getenv('ATHENA_REGION',   'us-east-1')
 
# Bedrock
BEDROCK_MODEL_ID = os.getenv('BEDROCK_MODEL_ID', 'anthropic.claude-3-sonnet-20240229-v1:0')
BEDROCK_REGION   = os.getenv('BEDROCK_REGION',   'us-east-1')
