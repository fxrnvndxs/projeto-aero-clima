# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Bronze: Extracao Meteorologica
# MAGIC **Projeto:** Aero Clima | **Modulo:** OpenWeather API | **Versao:** 3.0 (Modular)
# MAGIC
# MAGIC Responsavel exclusivamente por coletar os dados climaticos dos aeroportos
# MAGIC monitorados e gravar o payload bruto no Data Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuracao e Utilitarios

# COMMAND ----------

import requests
import json
from datetime import datetime

BRONZE_PATH = "/Volumes/workspace/default/camada_bronze/"
OPENWEATHER_API_KEY = dbutils.secrets.get(scope="aero_clima_secrets", key="openweather_api_key")

AEROPORTOS = {
    "GRU": (-23.43, -46.47), 
    "CGH": (-23.62, -46.65), 
    "POA": (-29.99, -51.17)
}

def enviar_alerta_operacional(rotina, status, mensagem):
    """Mock de integracao com mensageria corporativa."""
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
# MAGIC ## 2. Extracao OpenWeather

# COMMAND ----------

weather_payload = {}
falhas_registradas = 0

for aero, coords in AEROPORTOS.items():
    url_w = f"https://api.openweathermap.org/data/2.5/weather?lat={coords[0]}&lon={coords[1]}&appid={OPENWEATHER_API_KEY}&units=metric"
    try:
        # Timeout rigido de 10 segundos por aeroporto
        res_w = requests.get(url_w, timeout=10)
        if res_w.status_code == 200:
            weather_payload[aero] = res_w.json()
        else:
            enviar_alerta_operacional("Ingestao Clima", "ERRO", f"Falha no {aero} - Status: {res_w.status_code}")
            falhas_registradas += 1
    except Exception as e:
        enviar_alerta_operacional("Ingestao Clima", "ERRO", f"Timeout no {aero}: {e}")
        falhas_registradas += 1

if weather_payload:
    salvar_no_volume(weather_payload, "weather")
    if falhas_registradas == 0:
        enviar_alerta_operacional("Ingestao Clima", "SUCESSO", "Clima atualizado para todos os aeroportos.")
    else:
        enviar_alerta_operacional("Ingestao Clima", "ALERTA", f"Clima salvo parcialmente. Falhas: {falhas_registradas}")
else:
    enviar_alerta_operacional("Ingestao Clima", "ERRO", "Nenhum dado climatico foi coletado. Volume nao alterado.")