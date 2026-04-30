# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Bronze: Extracao Meteorologica
# MAGIC **Projeto:** Aero Clima | **Modulo:** OpenWeather API | **Versao:** 3.1 (Resiliente)
# MAGIC
# MAGIC Responsavel por coletar os dados climaticos com resiliencia (Exponential Backoff)
# MAGIC e persistir o payload bruto no Data Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuracao e Utilitarios

# COMMAND ----------

import requests
import json
import time
import logging
from datetime import datetime

# Configuracao de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BRONZE_PATH = "/Volumes/workspace/default/camada_bronze/"
OPENWEATHER_API_KEY = dbutils.secrets.get(scope="aero_clima_secrets", key="openweather_api_key")

AEROPORTOS = {
    "GRU": (-23.43, -46.47), 
    "CGH": (-23.62, -46.65), 
    "POA": (-29.99, -51.17)
}

def enviar_alerta_operacional(rotina, status, mensagem):
    """Mock de integracao com mensageria corporativa."""
    if status == "ERRO":
        logging.error(f"[ALERTA CRITICO ENVIADO] | {rotina} | {mensagem}")
    elif status == "ALERTA":
        logging.warning(f"[ALERTA PARCIAL ENVIADO] | {rotina} | {mensagem}")
    else:
        logging.info(f"[INFO ENVIADA] | {rotina} | {mensagem}")

def salvar_no_volume(dados, prefixo):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"{BRONZE_PATH}{prefixo}_{timestamp}.json"
    dbutils.fs.put(file_path, json.dumps(dados), overwrite=True)
    logging.info(f"Persistencia concluida -> {file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Motor de Requisicao com Exponential Backoff

# COMMAND ----------

def fetch_with_backoff(url, aero, max_retries=3, base_delay=2):
    """
    Executa chamadas HTTP com tentativas repetidas e intervalos crescentes.
    """
    for attempt in range(max_retries):
        try:
            res = requests.get(url, timeout=10)
            res.raise_for_status() # Levanta excecao para codigos HTTP 4xx e 5xx
            return res.json()
        except requests.exceptions.RequestException as e:
            logging.warning(f"Tentativa {attempt + 1}/{max_retries} falhou para o aeroporto {aero}: {e}")
            if attempt < max_retries - 1:
                sleep_time = base_delay * (2 ** attempt) # 2s, 4s...
                logging.info(f"Aguardando {sleep_time}s antes de tentar novamente...")
                time.sleep(sleep_time)
            else:
                logging.error(f"Todas as {max_retries} tentativas esgotadas para {aero}.")
                return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extracao OpenWeather

# COMMAND ----------

weather_payload = {}
falhas_registradas = 0

for aero, coords in AEROPORTOS.items():
    url_w = f"https://api.openweathermap.org/data/2.5/weather?lat={coords[0]}&lon={coords[1]}&appid={OPENWEATHER_API_KEY}&units=metric"
    
    # Substituicao do try/except simples pela funcao com backoff
    dados_clima = fetch_with_backoff(url_w, aero)
    
    if dados_clima:
        weather_payload[aero] = dados_clima
    else:
        enviar_alerta_operacional("Ingestao Clima", "ERRO", f"Falha definitiva na coleta para {aero}")
        falhas_registradas += 1

if weather_payload:
    salvar_no_volume(weather_payload, "weather")
    if falhas_registradas == 0:
        enviar_alerta_operacional("Ingestao Clima", "SUCESSO", "Clima atualizado para todos os aeroportos.")
    else:
        enviar_alerta_operacional("Ingestao Clima", "ALERTA", f"Clima salvo parcialmente. Falhas: {falhas_registradas}")
else:
    enviar_alerta_operacional("Ingestao Clima", "ERRO", "Nenhum dado climatico foi coletado. Volume nao alterado.")