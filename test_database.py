import pytest
from unittest.mock import patch, MagicMock, call
import os

# Importa as funções do seu arquivo database.py
from database import (
    get_db_connection, 
    init_db, 
    obter_proxima_sequencia, 
    obter_multiplas_sequencias, 
    resetar_sequencia_para_um
)

# ==========================================
# 1. Teste de Conexão (get_db_connection)
# ==========================================

@patch('database.psycopg2.connect')
@patch('database.os.getenv')
def test_get_db_connection(mock_getenv, mock_connect):
    """
    Testa se a função pega as variáveis de ambiente e chama o connect.
    """
    # Simulamos valores para as variáveis de ambiente
    def getenv_side_effect(key):
        envs = {
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
            "DB_DATABASE": "meu_banco",
            "DB_USER": "admin",
            "DB_PASSWORD": "123"
        }
        return envs.get(key)
    
    mock_getenv.side_effect = getenv_side_effect

    conn = get_db_connection()

    # Verifica se chamou o connect do psycopg2 com os parametros certos
    mock_connect.assert_called_with(
        host="localhost",
        port="5432",
        dbname="meu_banco",
        user="admin",
        password="123"
    )
    # Verifica se retornou o que o connect retornou
    assert conn == mock_connect.return_value


# ==========================================
# 2. Teste de Inicialização (init_db)
# ==========================================

@patch('database.get_db_connection')
def test_init_db_cria_tabela_e_sequencia(mock_get_db):
    """
    Testa se o init_db executa os SQLs de criação de tabela e sequence.
    """
    mock_conn = mock_get_db.return_value
    mock_cursor = mock_conn.cursor.return_value

    init_db()

    # Verifica se houve chamadas de execução de SQL
    assert mock_cursor.execute.call_count >= 2
    
    # Verifica Commit e Close
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()

    # Verificação opcional: Checar se o SQL contem palavras chaves
    # Pegamos todas as chamadas feitas ao cursor.execute
    sql_calls = [args[0] for args, _ in mock_cursor.execute.call_args_list]
    
    # Verifica criação da tabela
    assert any("CREATE TABLE IF NOT EXISTS public.pub_auth_token" in sql for sql in sql_calls)
    
    # Verifica criação da sequence
    assert any("CREATE SEQUENCE public.pub_seq_controle_despesa_ilog" in sql for sql in sql_calls)


# ==========================================
# 3. Teste de Obter Sequência Única
# ==========================================

def test_obter_proxima_sequencia_sucesso():
    """
    Testa se retorna o ID correto vindo do banco.
    """
    mock_cursor = MagicMock()
    # Simula retorno do banco: (101,)
    mock_cursor.fetchone.return_value = (101,)

    resultado = obter_proxima_sequencia(mock_cursor)

    assert resultado == 101
    mock_cursor.execute.assert_called_with("SELECT nextval('pub_seq_controle_despesa_ilog')")

def test_obter_proxima_sequencia_erro():
    """
    Testa se a função relança a exceção em caso de erro.
    """
    mock_cursor = MagicMock()
    # Simula erro ao executar SQL
    mock_cursor.execute.side_effect = Exception("Erro de Banco")

    with pytest.raises(Exception) as excinfo:
        obter_proxima_sequencia(mock_cursor)
    
    assert "Erro de Banco" in str(excinfo.value)


# ==========================================
# 4. Teste de Obter Múltiplas Sequências
# ==========================================

def test_obter_multiplas_sequencias_sucesso():
    """
    Testa a lógica de montar SQL dinâmico e retornar lista.
    """
    mock_cursor = MagicMock()
    # Simula pedir 3 IDs, o banco retorna (50, 51, 52)
    mock_cursor.fetchone.return_value = (50, 51, 52)

    quantidade = 3
    resultado = obter_multiplas_sequencias(mock_cursor, quantidade)

    assert resultado == [50, 51, 52]
    
    # Verifica se o SQL foi montado com 3 nextvals
    args, _ = mock_cursor.execute.call_args
    sql_executado = args[0]
    
    expected_part = "nextval('pub_seq_controle_despesa_ilog'), nextval('pub_seq_controle_despesa_ilog'), nextval('pub_seq_controle_despesa_ilog')"
    assert expected_part in sql_executado


# ==========================================
# 5. Teste de Resetar Sequência
# ==========================================

@patch('database.get_db_connection')
def test_resetar_sequencia(mock_get_db):
    """
    Testa se o comando RESTART é enviado.
    """
    mock_conn = mock_get_db.return_value
    mock_cursor = mock_conn.cursor.return_value

    resetar_sequencia_para_um()

    mock_cursor.execute.assert_called_with("ALTER SEQUENCE pub_seq_controle_despesa_ilog RESTART WITH 1")
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()