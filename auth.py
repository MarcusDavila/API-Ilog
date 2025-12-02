import requests
import json
import os
from datetime import datetime, timedelta
from database import get_db_connection


API_LOGIN_URL = os.getenv("API_LOGIN_URL")
API_EMAIL = os.getenv("API_EMAIL")
API_PASSWORD = os.getenv("API_PASSWORD")

def _get_external_token():

    payload = json.dumps({
        "email": API_EMAIL,
        "password": API_PASSWORD
    })
    headers = {'Content-Type': 'application/json'}
    
    print("Autenticando na API externa...")
    response = requests.post(API_LOGIN_URL, headers=headers, data=payload)
    response.raise_for_status()
    
    token = response.text.strip()
    if token.startswith('"') and token.endswith('"'):
        token = token[1:-1]
    return token

def _save_token_to_db(token):

    conn = get_db_connection()
    cursor = conn.cursor()
    now = datetime.now()

    cursor.execute("SELECT id FROM pub_auth_token LIMIT 1")
    data = cursor.fetchone()

    if data:
        cursor.execute("UPDATE pub_auth_token SET token = %s, created_at = %s WHERE id = %s", (token, now, data[0]))
    else:
        cursor.execute("INSERT INTO pub_auth_token (token, created_at) VALUES (%s, %s)", (token, now))

    conn.commit()
    cursor.close()
    conn.close()

def get_valid_token():

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT token, created_at FROM pub_auth_token LIMIT 1")
    data = cursor.fetchone()
    cursor.close()
    conn.close()

    if data:
        token_salvo, data_criacao = data

        if (datetime.now() - data_criacao) < timedelta(hours=7, minutes=55):
            return token_salvo


    novo_token = _get_external_token()
    _save_token_to_db(novo_token)
    return novo_token


def refresh_token():

    novo_token = _get_external_token()
    _save_token_to_db(novo_token)
    return novo_token