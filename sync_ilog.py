import requests
from datetime import datetime
import os
from database import get_db_connection, obter_proxima_sequencia, obter_multiplas_sequencias
from auth import get_valid_token, refresh_token

URL_WEBHOOK = os.getenv("API_WEBHOOK_URL")

def sincronizar_despesas():
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        print("--- Iniciando Sincronização ---")

        # REMOVIDO: O delete global foi retirado daqui para não limpar a tabela inteira.
        
        sql_busca = """
            SELECT grupo, empresa, filial, unidade, diferenciadorsequencia, sequencia, numero 
            FROM processoaduaneiro 
            WHERE (dtfechamento IS NULL 
               OR dtfechamento >= NOW() - INTERVAL '3 days')
               AND dtemissao > '2025-01-01'
        """
        cursor.execute(sql_busca)
        processos = cursor.fetchall()
        print(f"Processos encontrados: {len(processos)}")

        # Obtém token uma vez e reutiliza durante todo o processamento.
        try:
            token = get_valid_token()
        except Exception as e:
            print(f"Aviso: falha ao obter token inicial: {e}. Tentaremos obter por processo.")
            token = None

        for row in processos:
            grupo, empresa, filial, unidade, dif_seq, seq, numero_ref = row


            try:
                # Usa token em memória (obtido antes). Se estiver vazio, tenta obter do DB.
                if not token:
                    token = get_valid_token()
                headers = {'Authorization': f'Bearer {token}'}

                response = requests.get(URL_WEBHOOK, headers=headers, params={'Referencia': numero_ref})

                if response.status_code == 401:
                    print(f"401 ao consultar API para {numero_ref}. Tentando renovar token e refazer.")
                    try:
                        token = refresh_token()
                        headers = {'Authorization': f'Bearer {token}'}
                        response = requests.get(URL_WEBHOOK, headers=headers, params={'Referencia': numero_ref})
                    except Exception as e:
                        print(f"Falha ao renovar token: {e}")

                if not response.ok:
                    body = None
                    try:
                        body = response.text
                    except Exception:
                        body = '<não foi possível ler body>'
                    print(f"Erro API ({numero_ref}): status={response.status_code}, body={body}")

                    if response.status_code == 401:
                        continue
                    continue

                dados_api = response.json()
            except Exception as e:
                print(f"Erro API ({numero_ref}): {e}")
                continue

            if not dados_api.get("success") or not dados_api.get("data"):
                continue

            lista_despesas = dados_api["data"].get("despesas", [])
            
            # Se não houver despesas na API, pulamos (não deleta nem insere nada)
            if not lista_despesas:
                continue

            try:
                # 1. Deleta SOMENTE os registros deste processo específico antes de inserir os novos
                cursor.execute("""
                    DELETE FROM public.pub_processoaduaneiro_despesa_ilog
                    WHERE grupo=%s AND empresa=%s AND filial=%s AND unidade=%s 
                      AND diferenciadorsequencia=%s AND sequencia=%s
                """, (grupo, empresa, filial, unidade, dif_seq, seq))

                # 2. Obtém sequências para os novos registros
                sequencias = obter_multiplas_sequencias(cursor, len(lista_despesas))

                data_hora_atual = datetime.now()

                sql_insert = """
                    INSERT INTO public.pub_processoaduaneiro_despesa_ilog (
                        grupo, empresa, filial, unidade, diferenciadorsequencia, sequencia,
                        sequenciadespesa, 
                        dtinc, dtalt,
                        valor_despesa, processoid, nome_despesa, beneficiario
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                for idx, despesa in enumerate(lista_despesas):
                    id_sequencia_unica = sequencias[idx]

                    valores = (
                        grupo, empresa, filial, unidade, dif_seq, seq,
                        id_sequencia_unica,
                        data_hora_atual,
                        data_hora_atual,
                        despesa.get("valor_despesa") or 0,
                        despesa.get("processoid"),
                        despesa.get("nome_despesa"),
                        despesa.get("beneficiario")
                    )
                    cursor.execute(sql_insert, valores)

                # Commit a cada processo para garantir que os dados sejam salvos
                # mesmo se o script parar no meio.
                conn.commit()
                print(f"Processo {numero_ref}: {len(lista_despesas)} despesas atualizadas.")

            except Exception as e:
                conn.rollback()
                print(f"Erro ao processar banco de dados para {numero_ref}: {e}")
                continue

    except Exception as e:
        conn.rollback()
        print(f"Erro Geral: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    sincronizar_despesas()