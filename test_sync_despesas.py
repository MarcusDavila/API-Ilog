import pytest
from unittest.mock import patch, Mock, MagicMock
from datetime import datetime
from unittest.mock import patch, Mock, MagicMock, ANY
# Importamos a função principal do seu arquivo sync_ilog.py
from sync_ilog import sincronizar_despesas

# --- DADOS MOCK (Fictícios para o teste) ---

# Simulando uma linha retornada pelo SELECT do banco
# Colunas: grupo, empresa, filial, unidade, diferenciadorsequencia, sequencia, numero
PROCESSOS_MOCK = [
    ('GRP', 1, 1, 'UN', 0, 100, 'REF-TESTE-01'),
]

# Simulando resposta JSON da API com sucesso
API_RESPONSE_SUCCESS = {
    "success": True,
    "data": {
        "despesas": [
            {
                "valor_despesa": 200.50,
                "processoid": 99,
                "nome_despesa": "Frete Internacional",
                "beneficiario": "Maersk Line"
            }
        ]
    }
}

# ==========================================
# TESTES UNITÁRIOS
# ==========================================

@patch('sync_ilog.get_db_connection')          # 1. Mock do Banco
@patch('sync_ilog.get_valid_token')            # 2. Mock do Token
@patch('sync_ilog.requests.get')               # 3. Mock da API
@patch('sync_ilog.obter_multiplas_sequencias') # 4. Mock da função de sequencia
def test_fluxo_completo_sucesso(mock_seq, mock_req_get, mock_token, mock_db_conn):
    """
    Cenário Feliz: 
    - Banco retorna processos.
    - API retorna 200 OK com dados.
    - Script insere dados corretamente.
    """
    
    # -- CONFIGURAÇÃO DOS MOCKS --
    
    # Configurar Banco de Dados
    mock_conn = mock_db_conn.return_value
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.return_value = PROCESSOS_MOCK # Retorna 1 processo

    # Configurar Token
    mock_token.return_value = "TOKEN_VALIDO_123"

    # Configurar API (Requests)
    mock_response = Mock()
    mock_response.ok = True
    mock_response.status_code = 200
    mock_response.json.return_value = API_RESPONSE_SUCCESS
    mock_req_get.return_value = mock_response

    # Configurar Sequencia (simula que retornou o ID 5000 para a despesa)
    mock_seq.return_value = [5000]

    # -- EXECUÇÃO --
    sincronizar_despesas()

    # -- VERIFICAÇÕES (ASSERTS) --

    # 1. Ver se limpou a tabela geral no início
    mock_cursor.execute.assert_any_call("DELETE FROM public.pub_processoaduaneiro_despesa_ilog")

    # 2. Ver se chamou a API com o Token correto
    mock_req_get.assert_called_with(
        ANY, # <--- MUDANÇA AQUI: Aceita qualquer URL (seja None ou a da iData)
        headers={'Authorization': 'Bearer TOKEN_VALIDO_123'},
        params={'Referencia': 'REF-TESTE-01'}
    )

    # 3. Ver se deletou as despesas antigas ESPECÍFICAS deste processo
    # Verifica se existe um DELETE com WHERE grupo=%s ...
    calls_args = [args[0] for args, _ in mock_cursor.execute.call_args_list]
    delete_especifico = any("DELETE FROM public.pub_processoaduaneiro_despesa_ilog\n                WHERE grupo=%s" in cmd for cmd in calls_args)
    assert delete_especifico is True

    # 4. Ver se fez o INSERT dos dados novos
    # Pega a última chamada ao execute
    args_da_chamada = mock_cursor.execute.call_args[0] 
    
    sql_executado = args_da_chamada[0]     # O primeiro argumento é a string SQL
    params_executados = args_da_chamada[1] # O segundo argumento são os valores (%s, %s...)

    # Agora verificamos se a frase INSERT está contida no texto do SQL
    assert "INSERT INTO public.pub_processoaduaneiro_despesa_ilog" in sql_executado
    
    # Verifica se os valores inseridos batem com o Mock da API
    # Params: (... , sequenciadespesa, ..., valor, ..., nome, beneficiario)
    assert params_executados[6] == 5000         # Sequencia gerada
    assert params_executados[9] == 200.50       # Valor
    assert params_executados[11] == "Frete Internacional" # Nome
    
    # 5. Commit final
    mock_conn.commit.assert_called()


@patch('sync_ilog.get_db_connection')
@patch('sync_ilog.get_valid_token')
@patch('sync_ilog.refresh_token') # Precisamos mockar o refresh aqui
@patch('sync_ilog.requests.get')
def test_renovacao_token_401(mock_req_get, mock_refresh, mock_token, mock_db_conn):
    """
    Cenário: 
    - API retorna 401 (Unauthorized) na primeira tentativa.
    - Script deve chamar refresh_token().
    - Script deve tentar novamente com sucesso.
    """
    
    # Configurar Banco
    mock_conn = mock_db_conn.return_value
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.return_value = PROCESSOS_MOCK

    mock_token.return_value = "TOKEN_EXPIRADO"
    mock_refresh.return_value = "TOKEN_NOVO_RENOVADO"

    # -- TRUQUE DO SIDE EFFECT --
    # Primeira chamada retorna 401, Segunda retorna 200
    response_erro = Mock()
    response_erro.status_code = 401
    response_erro.ok = False

    response_sucesso = Mock()
    response_sucesso.status_code = 200
    response_sucesso.ok = True
    response_sucesso.json.return_value = {"success": True, "data": {"despesas": []}} # Sem despesas pra simplificar

    mock_req_get.side_effect = [response_erro, response_sucesso]

    # -- EXECUÇÃO --
    sincronizar_despesas()

    # -- VERIFICAÇÕES --
    
    # Garante que o refresh_token foi chamado
    mock_refresh.assert_called_once()
    
    # Garante que o requests.get foi chamado 2 vezes
    assert mock_req_get.call_count == 2
    
    # Verifica se a segunda chamada usou o token novo
    segunda_chamada_args = mock_req_get.call_args_list[1]
    headers_usados = segunda_chamada_args[1]['headers']
    assert headers_usados['Authorization'] == 'Bearer TOKEN_NOVO_RENOVADO'


@patch('sync_ilog.get_db_connection')
@patch('sync_ilog.get_valid_token')
@patch('sync_ilog.requests.get')
def test_api_retorna_erro_generico(mock_req_get, mock_token, mock_db_conn):
    """
    Cenário: API retorna erro 500 ou 404.
    O script NÃO deve quebrar, apenas pular o processo.
    """
    mock_conn = mock_db_conn.return_value
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.return_value = PROCESSOS_MOCK

    # API retorna 500 Internal Server Error
    mock_response = Mock()
    mock_response.ok = False
    mock_response.status_code = 500
    mock_response.text = "Internal Error"
    mock_req_get.return_value = mock_response

    # -- EXECUÇÃO --
    sincronizar_despesas()

    # -- VERIFICAÇÕES --
    # Não deve ter chamado INSERT (pois falhou a API)
    # Procuramos por chamadas de INSERT na lista de chamadas do cursor
    chamou_insert = any("INSERT INTO" in str(args) for args, _ in mock_cursor.execute.call_args_list)
    assert not chamou_insert, "Não deveria fazer INSERT se a API falhou"


@patch('sync_ilog.get_db_connection')
@patch('sync_ilog.get_valid_token')
@patch('sync_ilog.requests.get')
@patch('sync_ilog.obter_multiplas_sequencias')
def test_rollback_ao_falhar_banco(mock_seq, mock_req_get, mock_token, mock_db_conn):
    """
    Cenário: API funciona, mas ocorre erro ao inserir no banco.
    Deve executar ROLLBACK.
    """
    mock_conn = mock_db_conn.return_value
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.return_value = PROCESSOS_MOCK

    # API OK
    mock_response = Mock()
    mock_response.ok = True
    mock_response.status_code = 200
    mock_response.json.return_value = API_RESPONSE_SUCCESS
    mock_req_get.return_value = mock_response

    # Simula erro fatal no banco ao tentar pegar sequencia (ou no insert)
    mock_seq.side_effect = Exception("Erro Conexão Banco")

    # -- EXECUÇÃO --
    sincronizar_despesas()

    # -- VERIFICAÇÕES --
    # Verifica se rollback foi chamado
    mock_conn.rollback.assert_called()