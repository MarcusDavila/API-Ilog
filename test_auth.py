import pytest
from unittest.mock import patch, Mock, MagicMock
from datetime import datetime, timedelta

# Importamos as funções do seu script
from auth import get_valid_token, refresh_token, _get_external_token, _save_token_to_db

# ==========================================
# 1. Testes de Integração com API (_get_external_token)
# ==========================================

@patch('auth.requests.post')
def test_get_external_token_sucesso(mock_post):
    """Testa se a função chama a API, limpa as aspas e retorna o token."""
    
    # Simulamos uma resposta da API com aspas extras, como no seu script
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = '"token-super-secreto-123"'
    mock_post.return_value = mock_response

    token = _get_external_token()

    # Verifica se removeu as aspas
    assert token == "token-super-secreto-123"
    
    # Verifica se chamou a URL correta (mesmo sendo None no ambiente de teste)
    mock_post.assert_called_once()

@patch('auth.requests.post')
def test_get_external_token_erro_api(mock_post):
    """Testa se o script quebra (raise_for_status) quando a API falha."""
    import requests
    
    # Simulamos um erro 401 ou 500
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Erro API")
    mock_post.return_value = mock_response

    # O teste deve garantir que a exceção foi lançada
    with pytest.raises(requests.exceptions.HTTPError):
        _get_external_token()


# ==========================================
# 2. Testes de Banco de Dados (_save_token_to_db)
# ==========================================

@patch('auth.get_db_connection')
def test_save_token_update(mock_get_db):
    """Testa o cenário onde JÁ existe um token (faz UPDATE)."""
    
    # Configuração do Mock do DB
    mock_conn = mock_get_db.return_value
    mock_cursor = mock_conn.cursor.return_value
    
    # Simulando que o banco retornou um ID (data exists)
    mock_cursor.fetchone.return_value = (1,) 

    _save_token_to_db("novo-token")

    # Verifica se chamou UPDATE
    # Nota: verificamos se a string de query contém "UPDATE"
    args, _ = mock_cursor.execute.call_args
    sql_query = args[0]
    assert "UPDATE pub_auth_token" in sql_query
    
    # Verifica commit e close
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()

@patch('auth.get_db_connection')
def test_save_token_insert(mock_get_db):
    """Testa o cenário onde NÃO existe token (faz INSERT)."""
    
    mock_conn = mock_get_db.return_value
    mock_cursor = mock_conn.cursor.return_value
    
    # Simulando que o banco retornou None (tabela vazia)
    mock_cursor.fetchone.return_value = None

    _save_token_to_db("novo-token")

    # Verifica se chamou INSERT
    args, _ = mock_cursor.execute.call_args
    sql_query = args[0]
    assert "INSERT INTO pub_auth_token" in sql_query


# ==========================================
# 3. Testes de Lógica Principal (get_valid_token)
# ==========================================

@patch('auth._get_external_token') # Mockamos para garantir que NÃO seja chamada
@patch('auth.get_db_connection')
def test_get_valid_token_usa_cache_valido(mock_get_db, mock_external_token):
    """
    Cenário: Token no banco tem apenas 1 hora de vida.
    Resultado esperado: Retorna token do banco, NÃO chama API externa.
    """
    mock_conn = mock_get_db.return_value
    mock_cursor = mock_conn.cursor.return_value

    # Token criado 1 hora atrás (limite é 7h55m)
    data_criacao = datetime.now() - timedelta(hours=1)
    mock_cursor.fetchone.return_value = ('token_do_banco', data_criacao)

    token = get_valid_token()

    assert token == 'token_do_banco'
    mock_external_token.assert_not_called() # Garante que economizamos a requisição


@patch('auth._save_token_to_db')   # Mockamos o save
@patch('auth._get_external_token') # Mockamos a API
@patch('auth.get_db_connection')
def test_get_valid_token_renova_se_expirado(mock_get_db, mock_external_token, mock_save):
    """
    Cenário: Token no banco tem 8 horas de vida (Expirado).
    Resultado esperado: Chama API externa e Salva no banco.
    """
    mock_conn = mock_get_db.return_value
    mock_cursor = mock_conn.cursor.return_value

    # Token criado 8 horas atrás (limite é 7h55m -> expirou)
    data_criacao = datetime.now() - timedelta(hours=8)
    mock_cursor.fetchone.return_value = ('token_velho', data_criacao)
    
    # Configura API externa para devolver token novo
    mock_external_token.return_value = 'token_novinho'

    token = get_valid_token()

    assert token == 'token_novinho'
    mock_external_token.assert_called_once() # Chamou a API
    mock_save.assert_called_with('token_novinho') # Salvou no banco


@patch('auth._save_token_to_db')
@patch('auth._get_external_token')
@patch('auth.get_db_connection')
def test_get_valid_token_banco_vazio(mock_get_db, mock_external_token, mock_save):
    """
    Cenário: Banco não retorna nada (primeira execução).
    Resultado esperado: Chama API externa.
    """
    mock_conn = mock_get_db.return_value
    mock_cursor = mock_conn.cursor.return_value

    # Banco retorna None
    mock_cursor.fetchone.return_value = None
    
    mock_external_token.return_value = 'token_primeira_vez'

    token = get_valid_token()

    assert token == 'token_primeira_vez'
    mock_external_token.assert_called_once()
    mock_save.assert_called_once()