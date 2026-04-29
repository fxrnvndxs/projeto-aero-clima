# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Gold: Motor de SLA e Dimensoes Estaticas
# MAGIC **Projeto:** Aero Clima | **Modulo:** Regras de Negocio | **Versao:** 3.0 (Modular)
# MAGIC
# MAGIC Responsavel por:
# MAGIC 1. Atualizar a tabela Dimensao de Aeroportos (Star Schema).
# MAGIC 2. Realizar o cruzamento espacial (Geofencing) entre voos e clima.
# MAGIC 3. Aplicar o motor de regras corporativas para definicao de Risco e SLA.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuracao e Parametros de Negocio

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType

CATALOG  = "workspace"
DATABASE = "default"

TABLE_VOOS_SILVER  = f"{CATALOG}.{DATABASE}.voos_silver"
TABLE_CLIMA_SILVER = f"{CATALOG}.{DATABASE}.clima_silver"
TABLE_GOLD_MONITOR = f"{CATALOG}.{DATABASE}.monitor_operacional_gold" # Tabela temporaria de processamento
TABLE_DIM_AIRPORTS = f"{CATALOG}.{DATABASE}.dim_aeroportos"

# Limites Operacionais (m/s)
WIND_LIMITS = {
    "CGH": {"alto": 9.0,  "medio": 6.0},
    "GRU": {"alto": 13.0, "medio": 9.0},
    "POA": {"alto": 11.0, "medio": 7.5},
}

HIGH_RISK_CONDITIONS = ["Thunderstorm", "Snow", "Tornado", "Squall"]
MED_RISK_CONDITIONS  = ["Rain", "Drizzle", "Fog", "Mist", "Haze"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carga da Dimensao Aeroportos (Overwrite)

# COMMAND ----------

AIRPORTS_SCHEMA = StructType([
    StructField("sigla", StringType(), nullable=False),
    StructField("nome_aeroporto", StringType(), nullable=False),
    StructField("cidade", StringType(), nullable=False),
    StructField("estado", StringType(), nullable=False),
    StructField("latitude_ref", DoubleType(), nullable=False),
    StructField("longitude_ref", DoubleType(), nullable=False),
    StructField("limite_vento_alto_ms", FloatType(), nullable=False),
    StructField("limite_vento_medio_ms", FloatType(), nullable=False),
])

airports_data = [
    ("CGH", "Congonhas", "Sao Paulo", "SP", -23.6261, -46.6564, 9.0, 6.0),
    ("GRU", "Guarulhos", "Guarulhos", "SP", -23.4356, -46.4731, 13.0, 9.0),
    ("POA", "Salgado Filho", "Porto Alegre", "RS", -29.9944, -51.1713, 11.0, 7.5),
]

df_dim_aeroportos = spark.createDataFrame(airports_data, schema=AIRPORTS_SCHEMA)

(
    df_dim_aeroportos.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TABLE_DIM_AIRPORTS)
)
print(f"Log: Dimensao {TABLE_DIM_AIRPORTS} atualizada com exito.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Integracao e Motor de Risco

# COMMAND ----------

try:
    df_voos  = spark.read.table(TABLE_VOOS_SILVER)
    df_clima = spark.read.table(TABLE_CLIMA_SILVER)
except Exception as e:
    print(f"Log: Tabelas Silver ainda nao existem ou estao inacessiveis: {e}")
    dbutils.notebook.exit("Sem dados na Silver.")

# 3.1 Geofencing
df_voos_geo = df_voos.withColumn(
    "aeroporto_proximo",
    F.when(F.col("latitude") < -28.0, "POA")
     .when((F.col("latitude") >= -28.0) & (F.col("latitude") < -23.55), "CGH")
     .otherwise("GRU")
)

# 3.2 Join Relacional (Left Join protege os voos caso o sensor de clima falhe)
df_joined = df_voos_geo.join(
    df_clima.select(
        F.col("aeroporto_sigla"),
        F.col("timestamp_coleta").alias("ts_clima"),
        F.col("temperatura_c"),
        F.col("umidade_pct"),
        F.col("vento_velocidade_ms"),
        F.col("vento_direcao_graus"),
        F.col("condicao_climatica"),
        F.col("descricao_climatica"),
        F.col("visibilidade_m"),
    ),
    on=df_voos_geo["aeroporto_proximo"] == F.col("aeroporto_sigla"),
    how="left",
)

# 3.3 Motor de Regras (Avaliacao Dinamica)
def build_risk_expression():
    expr = F.lit("NORMAL")

    for iata, limits in WIND_LIMITS.items():
        expr = F.when((F.col("aeroporto_proximo") == iata) & (F.col("vento_velocidade_ms") > limits["medio"]), "MEDIO").otherwise(expr)
    
    expr = F.when(F.col("condicao_climatica").isin(MED_RISK_CONDITIONS), "MEDIO").otherwise(expr)

    for iata, limits in WIND_LIMITS.items():
        expr = F.when((F.col("aeroporto_proximo") == iata) & (F.col("vento_velocidade_ms") > limits["alto"]), "ALTO").otherwise(expr)

    expr = F.when(F.col("condicao_climatica").isin(HIGH_RISK_CONDITIONS), "ALTO").otherwise(expr)
    return expr

df_gold = (
    df_joined
    .withColumn("nivel_risco", build_risk_expression())
    .withColumn(
        "status_sla",
        F.when(F.col("nivel_risco") == "ALTO",   "CRITICO")
         .when(F.col("nivel_risco") == "MEDIO",  "ATENCAO")
         .otherwise("OPERACIONAL")
    )
    .withColumn(
        "alerta_sla",
        F.when(F.col("nivel_risco") == "ALTO",  "INTERRUPCAO IMINENTE")
         .when(F.col("nivel_risco") == "MEDIO", "MONITORAR")
         .otherwise("OPERACAO NORMAL")
    )
    .withColumn("timestamp_processamento", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Persistencia da Visao Consolidada

# COMMAND ----------

# Grava uma tabela intermediaria (Overwrite) para que a proxima etapa faca a validacao limpa
(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true") 
    .saveAsTable(TABLE_GOLD_MONITOR)
)

print(f"Processamento concluido. Visao intermediaria gravada em {TABLE_GOLD_MONITOR}.")