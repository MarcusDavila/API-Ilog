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

        # 1. Busca processos (sem alteração)
        sql_busca = """
            SELECT grupo, empresa, filial, unidade, diferenciadorsequencia, sequencia, numero 
            FROM processoaduaneiro 
            WHERE dtfechamento IS NULL 
               OR dtfechamento >= NOW() - INTERVAL '3 days'
        """
        cursor.execute(sql_busca)
        processos = cursor.fetchall()
        print(f"Processos encontrados: {len(processos)}")

        for row in processos:
            grupo, empresa, filial, unidade, dif_seq, seq, numero_ref = row
            
            # 2. API (sem alteração)
            try:
                token = get_valid_token()
                headers = {'Authorization': f'Bearer {token}'}

                response = requests.get(URL_WEBHOOK, headers=headers, params={'Referencia': numero_ref})

                # Se der 401, tenta renovar token uma vez e reexecutar
                if response.status_code == 401:
                    print(f"401 ao consultar API para {numero_ref}. Tentando renovar token e refazer.")
                    try:
                        token = refresh_token()
                        headers = {'Authorization': f'Bearer {token}'}
                        response = requests.get(URL_WEBHOOK, headers=headers, params={'Referencia': numero_ref})
                    except Exception as e:
                        print(f"Falha ao renovar token: {e}")

                if not response.ok:
                    # Log para ajudar debug (corpo da resposta pode explicar o 4xx/5xx)
                    body = None
                    try:
                        body = response.text
                    except Exception:
                        body = '<não foi possível ler body>'
                    print(f"Erro API ({numero_ref}): status={response.status_code}, body={body}")
                    # Se for 401 mesmo após refresh, pula este processo
                    if response.status_code == 401:
                        continue
                    # para outros códigos de erro, pula também
                    continue

                dados_api = response.json()
            except Exception as e:
                print(f"Erro API ({numero_ref}): {e}")
                continue

            if not dados_api.get("success") or not dados_api.get("data"):
                continue

            lista_despesas = dados_api["data"].get("despesas", [])
            if not lista_despesas:
                continue

            # 3. Limpeza (Opcional, mantenha se quiser evitar duplicar dados do mesmo processo)
            cursor.execute("""
                DELETE FROM public.pub_processoaduaneiro_despesa_ilog
                WHERE grupo=%s AND empresa=%s AND filial=%s AND unidade=%s 
                  AND diferenciadorsequencia=%s AND sequencia=%s
            """, (grupo, empresa, filial, unidade, dif_seq, seq))

            try:
                # 4. Obtém sequências em lote (mais eficiente)
                sequencias = obter_multiplas_sequencias(cursor, len(lista_despesas))
                
                # 5. Insere todas as despesas com as sequências
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
                
                # 6. Confirma transação do processo INTEIRO
                conn.commit()
                print(f"Processo {numero_ref}: {len(lista_despesas)} despesas salvas.")
                
            except Exception as e:
                # Rollback apenas desta iteração (processo)
                conn.rollback()
                print(f"Erro ao processar {numero_ref}: {e}")
                # Continua com próximo processo
                continue

    except Exception as e:
        conn.rollback()
        print(f"Erro Geral: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    sincronizar_despesas()