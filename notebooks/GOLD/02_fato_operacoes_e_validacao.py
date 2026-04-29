# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Gold: Tabela Fato e Data Contract
# MAGIC **Projeto:** Aero Clima | **Modulo:** Serving (Power BI) | **Versao:** 3.0 (Modular)
# MAGIC
# MAGIC Responsavel por estruturar a tabela Fato e aplicar a Validacao Final (Data Contract)
# MAGIC antes de anexar (Append) os dados otimizados para o Dashboard.

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

# O Contrato: Estas colunas DEVEM existir exatamente com estes nomes e formatos
CONTRATO_COLUNAS_ESPERADAS = [
    "id_operacao", "fk_voo", "fk_aeroporto", "timestamp_evento", 
    "velocidade_ms", "taxa_vertical_ms", "direcao_graus", "em_solo", 
    "temperatura_c", "umidade_pct", "vento_ms", "vento_direcao_graus", 
    "visibilidade_m", "nivel_risco", "status_sla", "alerta_sla"
]

def enviar_alerta_operacional(rotina, status, mensagem):
    """Mock de integracao com mensageria corporativa."""
    import datetime
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if status == "ERRO":
        print(f"[ALERTA CRITICO ENVIADO] {ts} | {rotina} | {mensagem}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Construcao da Tabela Fato (Geração de Hash)

# COMMAND ----------

df_gold_temp = spark.read.table(TABLE_GOLD_MONITOR)

df_fato = (
    df_gold_temp
    # Geracao de Surrogate Key deterministica para evitar duplicidade no Append
    .withColumn(
        "id_operacao",
        F.sha2(F.concat_ws("|", F.col("icao24"), F.col("timestamp_processamento").cast("string")), 256)
    )
    .select(
        F.col("id_operacao"),                                         
        F.trim(F.col("callsign")).alias("fk_voo"),                    
        F.col("aeroporto_proximo").alias("fk_aeroporto"),           
        F.col("timestamp_processamento").alias("timestamp_evento"),
        F.col("velocity_ms").cast(FloatType()).alias("velocidade_ms"),
        F.col("vertical_rate_ms").cast(FloatType()).alias("taxa_vertical_ms"),
        F.col("true_track_deg").cast(FloatType()).alias("direcao_graus"),
        F.col("on_ground").alias("em_solo"),
        F.col("temperatura_c").cast(FloatType()),
        F.col("umidade_pct").cast(FloatType()),
        F.col("vento_velocidade_ms").cast(FloatType()).alias("vento_ms"),
        F.col("vento_direcao_graus").cast(FloatType()),
        F.col("visibilidade_m").cast(FloatType()),
        F.col("nivel_risco"),
        F.col("status_sla"),
        F.col("alerta_sla"),
    )
    .dropDuplicates(["id_operacao"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Contract Validation (Segurança contra quebra de Dashboard)

# COMMAND ----------

colunas_reais = df_fato.columns
colunas_faltantes = set(CONTRATO_COLUNAS_ESPERADAS) - set(colunas_reais)

if colunas_faltantes:
    mensagem_erro = f"Data Contract Violation. Colunas exigidas pelo Power BI estao ausentes: {colunas_faltantes}"
    enviar_alerta_operacional("Validacao Fato", "ERRO", mensagem_erro)
    raise ValueError(mensagem_erro)

print("Check de Qualidade [PASSOU]: O schema gerado esta em conformidade com o Contrato de Dados do BI.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Carga Final de Consumo (Append Otimizado)

# COMMAND ----------

# Removido option mergeSchema. Garantimos o append estrito para performance maxima.
(
    df_fato.write
    .format("delta")
    .mode("append") 
    .saveAsTable(TABLE_FACT_OPS)
)

print(f"Log: Tabela Fato ({TABLE_FACT_OPS}) alimentada com sucesso. Dashboard pronto para consulta.")