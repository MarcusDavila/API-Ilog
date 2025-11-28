from fastapi import FastAPI, BackgroundTasks, HTTPException
from database import init_db
from sync_ilog import sincronizar_despesas
from auth import get_valid_token
import sys
import threading

app = FastAPI()

@app.on_event("startup")
def ao_iniciar():
    init_db()
    try:
        
        t = threading.Thread(target=sincronizar_despesas, daemon=True)
        t.start()
        print("Sincronização inicial iniciada em background.")
    except Exception as e:
        print(f"Erro ao iniciar sincronização inicial: {e}")

@app.get("/")
def home():
    return {"status": "Sistema Online"}

@app.post("/sincronizar")
def iniciar_sincronizacao(background_tasks: BackgroundTasks):
    
    background_tasks.add_task(sincronizar_despesas)
    return {"mensagem": "Sincronização iniciada em background."}

def run_full_once():
  
    try:
        init_db()
        
        try:
            token = get_valid_token()
            print("Token obtido com sucesso.")
        except Exception as e:
            print(f"Falha ao obter token: {e}")
           
        sincronizar_despesas()
        print("Sincronização completa (execução única).")
    except Exception as e:
        print(f"Erro na execução completa: {e}")


if __name__ == "__main__":
  
    if "--serve" in sys.argv:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
      
        run_full_once()