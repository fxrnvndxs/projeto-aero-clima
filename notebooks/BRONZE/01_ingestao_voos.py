# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Bronze: Extracao de Trafego Aereo
# MAGIC **Projeto:** Aero Clima | **Modulo:** OpenSky API | **Versao:** 3.0 (Modular)
# MAGIC
# MAGIC Responsavel exclusivamente por autenticar via OAuth2, extrair vetores 
# MAGIC de estado da OpenSky Network e persistir no Volume.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuracao e Utilitarios

# COMMAND ----------

import requests
import json
from datetime import datetime

# Destino
BRONZE_PATH = "/Volumes/workspace/default/camada_bronze/"
BBOX_VOOS = "lamin=-32.0&lomin=-55.0&lamax=-22.0&lomax=-43.0"

# Credenciais
OPENSKY_CLIENT_ID = dbutils.secrets.get(scope="aero_clima_secrets", key="opensky_client_id")
OPENSKY_CLIENT_SECRET = dbutils.secrets.get(scope="aero_clima_secrets", key="opensky_client_secret")

def enviar_alerta_operacional(rotina, status, mensagem):
    """
    Mock: Simula o disparo de alertas via E-mail, Slack ou Microsoft Teams.
    No ambiente corporativo real, injetariamos um Webhook (requests.post) aqui.
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if status == "ERRO":
        print(f"[ALERTA CRITICO ENVIADO] {ts} | {rotina} | {mensagem}")
    elif status == "ALERTA":
        print(f"[ALERTA PARCIAL ENVIADO] {ts} | {rotina} | {mensagem}")
    else:
        print(f"[INFO ENVIADA] {ts} | {rotina} | {mensagem}")

def salvar_no_volume(dados, prefixo):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"{BRONZE_PATH}{prefixo}_{timestamp}.json"
    dbutils.fs.put(file_path, json.dumps(dados), overwrite=True)
    print(f"Log: Persistencia concluida -> {file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Autenticacao e Extracao

# COMMAND ----------

class TokenManager:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.url = "https://opensky-network.org/auth/realms/opensky/protocol/openid-connect/token"

    def get_token(self):
        payload = {'grant_type': 'client_credentials', 'client_id': self.client_id, 'client_secret': self.client_secret}
        try:
            # Timeout rigido de 10s para nao prender o cluster
            response = requests.post(self.url, data=payload, timeout=10)
            if response.status_code == 200:
                return response.json().get('access_token')
            else:
                enviar_alerta_operacional("Auth OpenSky", "ERRO", f"Status {response.status_code}")
                return None
        except Exception as e:
            enviar_alerta_operacional("Auth OpenSky", "ERRO", f"Timeout ou falha de rede: {e}")
            return None

auth_manager = TokenManager(OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET)
token = auth_manager.get_token()

if token:
    headers = {'Authorization': f'Bearer {token}'}
    url_voos = f"https://opensky-network.org/api/states/all?{BBOX_VOOS}"
    
    try:
        res_voos = requests.get(url_voos, headers=headers, timeout=15)
        if res_voos.status_code == 200:
            salvar_no_volume(res_voos.json(), "flights")
            enviar_alerta_operacional("Ingestao Voos", "SUCESSO", "Lote de voos processado com exito.")
        else:
            enviar_alerta_operacional("Ingestao Voos", "ERRO", f"API retornou status {res_voos.status_code}")
    except Exception as e:
        enviar_alerta_operacional("Ingestao Voos", "ERRO", f"Timeout de conexao: {e}")
else:
    print("Execucao abortada: Token nao disponivel.")