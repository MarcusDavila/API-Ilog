import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():

    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_DATABASE"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def init_db():

    conn = get_db_connection()
    cursor = conn.cursor()


    cursor.execute('''
        CREATE TABLE IF NOT EXISTS public.pub_auth_token (
            id SERIAL PRIMARY KEY,
            token TEXT,
            created_at TIMESTAMP
        )
    ''')

    cursor.execute('''
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_class
                WHERE relkind = 'S'
                  AND relname = 'pub_seq_controle_despesa_ilog'
            ) THEN
                CREATE SEQUENCE public.pub_seq_controle_despesa_ilog
                INCREMENT 1
                START 1
                MINVALUE 1;
            END IF;
        END
        $$;
    ''')

    conn.commit()
    cursor.close()
    conn.close()


def obter_proxima_sequencia(cursor):

    try:

        cursor.execute("SELECT nextval('pub_seq_controle_despesa_ilog')")
        sequencia_id = cursor.fetchone()[0]
        return sequencia_id
    except Exception as e:
        print(f"Erro ao gerar sequência: {e}")
        raise e

def obter_multiplas_sequencias(cursor, quantidade):

    try:
        placeholders = ', '.join(['nextval(\'pub_seq_controle_despesa_ilog\')'] * quantidade)
        cursor.execute(f"SELECT {placeholders}")
        sequencias = cursor.fetchone()
        return list(sequencias)
    except Exception as e:
        print(f"Erro ao gerar múltiplas sequências: {e}")
        raise e


def resetar_sequencia_para_um():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("ALTER SEQUENCE pub_seq_controle_despesa_ilog RESTART WITH 1")
    conn.commit()
    cursor.close()
    conn.close()