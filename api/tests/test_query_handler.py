import json
import pytest
import sys
import os

sys.path.insert(0,os.path.join(os.path.dirname(__file__), '..','src','query_handler'))
from unittest.mock import patch, MagicMock
from app import lambda_handler

def make_event(question):
    return {'body': json.dumps({'question': question}),'httpMethod': 'POST', 'path': '/query'}
# ── Test 1: Missing question returns 400 ──────────────────────
def test_missing_question_returns_400():
    event = {'body': json.dumps({}), 'httpMethod': 'POST', 'path': '/query'}
    result = lambda_handler(event, {})
    assert result['statusCode'] == 400
    body = json.loads(result['body'])
    assert 'error' in body
 
 
# ── Test 2: Empty question returns 400 ───────────────────────
def test_empty_question_returns_400():
    result = lambda_handler(make_event(''), {})
    assert result['statusCode'] == 400
 
 
# ── Test 3: Question too long returns 400 ────────────────────
def test_question_too_long_returns_400():
    long_question = 'a' * 501
    result = lambda_handler(make_event(long_question), {})
    assert result['statusCode'] == 400
 
 
# ── Test 4: Invalid JSON body returns 400 ────────────────────
def test_invalid_json_returns_400():
    event = {'body': 'this is not json', 'httpMethod': 'POST', 'path': '/query'}
    result = lambda_handler(event, {})
    assert result['statusCode'] == 400
 
 
# ── Test 5: Health check returns 200 ─────────────────────────
def test_health_check_returns_200():
    event = {'httpMethod': 'GET', 'path': '/health', 'body': None}
    result = lambda_handler(event, {})
    assert result['statusCode'] == 200
    body = json.loads(result['body'])
    assert body['status'] == 'healthy'
 
 
# ── Test 6: Successful question returns 200 with SQL ─────────
@patch('app.generate_sql', return_value='SELECT * FROM sales LIMIT 5')
@patch('app.run_athena_query', return_value=[{'product_name': 'Laptop', 'total_revenue': '999'}])
def test_valid_question_returns_200(mock_athena, mock_sql):
    result = lambda_handler(make_event('show me products'), {})
    assert result['statusCode'] == 200
    body = json.loads(result['body'])
    assert 'sql' in body
    assert 'results' in body
    assert 'count' in body
    assert body['count'] == 1
 
 
# ── Test 7: Response has correct CORS header ─────────────────
@patch('app.generate_sql', return_value='SELECT * FROM sales LIMIT 5')
@patch('app.run_athena_query', return_value=[])
def test_response_has_cors_header(mock_athena, mock_sql):
    result = lambda_handler(make_event('show me products'), {})
    assert 'Access-Control-Allow-Origin' in result['headers']
    assert result['headers']['Access-Control-Allow-Origin'] == '*'