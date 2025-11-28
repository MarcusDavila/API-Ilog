import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    """Cria e retorna a conexão com o banco PostgreSQL."""
    # (Mantenha o código de conexão igual ao anterior)
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_DATABASE"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def init_db():
    """
    Inicializa tabelas e SEQUENCES para controle de concorrência.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Tabela de Tokens (Já existia)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS auth_token (
            id SERIAL PRIMARY KEY,
            token TEXT,
            created_at TIMESTAMP
        )
    ''')
    
    # 2. NOVA: Criar a Sequence Nativa do PostgreSQL
    # Isso cria um contador atômico no banco.
    # START 1: Começa no 1.
    # CACHE 1: Garante a sequência exata sem pular muito em caso de crash (pode ser maior para performance).
    # Algumas versões do Postgres não suportam 'IF NOT EXISTS' para CREATE SEQUENCE.
    # Usamos um bloco DO que checa a existência da sequence em pg_class antes de criar.
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

# --- FUNÇÕES PARA CONTROLE DE SEQUÊNCIA ---
def obter_proxima_sequencia(cursor):
    """
    Retorna o próximo número da sequência de forma atômica.
    IMPORTANTE: Deve ser chamado dentro de uma transação ativa.
    O cursor deve estar em uma conexão aberta e em transação.
    A sequência só é confirmada quando a transação é comitada.
    """
    try:
        # 'nextval' é a função nativa do Postgres para avançar a sequence
        cursor.execute("SELECT nextval('pub_seq_controle_despesa_ilog')")
        sequencia_id = cursor.fetchone()[0]
        return sequencia_id
    except Exception as e:
        print(f"Erro ao gerar sequência: {e}")
        raise e

def obter_multiplas_sequencias(cursor, quantidade):
    """
    Retorna múltiplos números de sequência em uma única operação.
    Mais eficiente para inserções em lote.
    """
    try:
        placeholders = ', '.join(['nextval(\'pub_seq_controle_despesa_ilog\')'] * quantidade)
        cursor.execute(f"SELECT {placeholders}")
        sequencias = cursor.fetchone()
        return list(sequencias)
    except Exception as e:
        print(f"Erro ao gerar múltiplas sequências: {e}")
        raise e

# Função opcional: Se você quiser RESETAR a sequência para 1 manualmente
def resetar_sequencia_para_um():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("ALTER SEQUENCE pub_seq_controle_despesa_ilog RESTART WITH 1")
    conn.commit()
    cursor.close()
    conn.close()