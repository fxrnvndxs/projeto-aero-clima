# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Gold: Tabela Fato e Data Contract
# MAGIC **Projeto:** Aero Clima | **Modulo:** Serving (Power BI) | **Versao:** 4.0 (Star Schema Raiz)
# MAGIC
# MAGIC Responsavel por estruturar a tabela Fato e aplicar a Validacao Final (Data Contract)
# MAGIC garantindo isolamento total de strings (Star Schema puro).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuracao e Dependencias

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

CATALOG  = "workspace"
DATABASE = "default"

TABLE_GOLD_MONITOR = f"{CATALOG}.{DATABASE}.monitor_operacional_gold"
TABLE_FACT_OPS     = f"{CATALOG}.{DATABASE}.fato_operacoes"

# NOVO CONTRATO: Apenas IDs e Metricas. Proibido o trafego de strings descritivas.
CONTRATO_COLUNAS_ESPERADAS = [
    "id_operacao", "fk_voo", "fk_aeroporto", "fk_risco", "fk_condicao", 
    "timestamp_evento", "velocidade_ms", "taxa_vertical_ms", "direcao_graus", 
    "em_solo", "temperatura_c", "umidade_pct", "vento_ms", "vento_direcao_graus", 
    "visibilidade_m"
]

def enviar_alerta_operacional(rotina, status, mensagem):
    import datetime
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if status == "ERRO":
        print(f"[ALERTA CRITICO ENVIADO] {ts} | {rotina} | {mensagem}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Construcao da Tabela Fato (Puramente Numerica/Chaves)

# COMMAND ----------

df_gold_temp = spark.read.table(TABLE_GOLD_MONITOR)

df_fato = (
    df_gold_temp
    .withColumn(
        "id_operacao",
        F.sha2(F.concat_ws("|", F.col("icao24"), F.col("timestamp_coleta").cast("string")), 256)
    )
    .select(
        F.col("id_operacao"),                                         
        F.trim(F.col("callsign")).alias("fk_voo"),                    
        F.col("aeroporto_proximo").alias("fk_aeroporto"),
        F.col("fk_risco"),
        F.col("fk_condicao"),
        F.col("timestamp_coleta").alias("timestamp_evento"), 
        F.col("velocity_ms").cast(FloatType()).alias("velocidade_ms"),
        F.col("vertical_rate_ms").cast(FloatType()).alias("taxa_vertical_ms"),
        F.col("true_track_deg").cast(FloatType()).alias("direcao_graus"),
        F.col("on_ground").alias("em_solo"),
        F.col("temperatura_c").cast(FloatType()),
        F.col("umidade_pct").cast(FloatType()),
        F.col("vento_velocidade_ms").cast(FloatType()).alias("vento_ms"),
        F.col("vento_direcao_graus").cast(FloatType()),
        F.col("visibilidade_m").cast(FloatType())
    )
    .dropDuplicates(["id_operacao"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Contract Validation

# COMMAND ----------

colunas_reais = df_fato.columns
colunas_faltantes = set(CONTRATO_COLUNAS_ESPERADAS) - set(colunas_reais)

if colunas_faltantes:
    mensagem_erro = f"Data Contract Violation. Colunas estruturais ausentes: {colunas_faltantes}"
    enviar_alerta_operacional("Validacao Fato", "ERRO", mensagem_erro)
    raise ValueError(mensagem_erro)

print("Check de Qualidade [PASSOU]: A tabela Fato e puramente dimensional e aderente ao contrato.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Carga Final de Consumo (Append Otimizado)

# COMMAND ----------

(
    df_fato.write
    .format("delta")
    .mode("append") 
    .saveAsTable(TABLE_FACT_OPS)
)

print(f"Log: Tabela Fato alimentada com sucesso no modo append. Dashboard atualizado.")