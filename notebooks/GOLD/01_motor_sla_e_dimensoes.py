# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Gold: Motor de SLA e Dimensoes Estaticas
# MAGIC **Projeto:** Aero Clima | **Modulo:** Regras de Negocio | **Versao:** 4.0 (Star Schema Raiz)
# MAGIC
# MAGIC Responsavel por:
# MAGIC 1. Atualizar as Dimensoes (Aeroportos, Risco, Condicao Climatica).
# MAGIC 2. Realizar o cruzamento espacial (Geofencing).
# MAGIC 3. Aplicar o motor de regras corporativas gerando apenas FKs para a Fato.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuracao e Parametros de Negocio

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType
from pyspark.sql.window import Window

CATALOG  = "workspace"
DATABASE = "default"

TABLE_VOOS_SILVER  = f"{CATALOG}.{DATABASE}.voos_silver"
TABLE_CLIMA_SILVER = f"{CATALOG}.{DATABASE}.clima_silver"
TABLE_GOLD_MONITOR = f"{CATALOG}.{DATABASE}.monitor_operacional_gold" 
TABLE_DIM_AIRPORTS = f"{CATALOG}.{DATABASE}.dim_aeroportos"
TABLE_DIM_RISCO    = f"{CATALOG}.{DATABASE}.dim_risco"
TABLE_DIM_CONDICAO = f"{CATALOG}.{DATABASE}.dim_condicao"

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
# MAGIC ## 2. Carga das Dimensoes (Star Schema)

# COMMAND ----------

# 2.1 Dimensao Aeroportos
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

spark.createDataFrame(airports_data, schema=AIRPORTS_SCHEMA).write.format("delta").mode("overwrite").saveAsTable(TABLE_DIM_AIRPORTS)

# 2.2 Dimensao Risco (Exigencia do Mentor)
RISK_SCHEMA = StructType([
    StructField("id_risco", IntegerType(), nullable=False),
    StructField("nivel_risco", StringType(), nullable=False),
    StructField("status_sla", StringType(), nullable=False),
    StructField("alerta_sla", StringType(), nullable=False)
])

risk_data = [
    (1, "NORMAL", "OPERACIONAL", "OPERACAO NORMAL"),
    (2, "MEDIO", "ATENCAO", "MONITORAR"),
    (3, "ALTO", "CRITICO", "INTERRUPCAO IMINENTE")
]

spark.createDataFrame(risk_data, schema=RISK_SCHEMA).write.format("delta").mode("overwrite").saveAsTable(TABLE_DIM_RISCO)
print(f"Log: Dimensoes estaticas atualizadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Integracao e Motor de Risco (Gerando FKs)

# COMMAND ----------

df_voos_raw = spark.read.table(TABLE_VOOS_SILVER)
df_clima_raw = spark.read.table(TABLE_CLIMA_SILVER)

# Pega o dado mais recente
window_clima = Window.partitionBy("aeroporto_sigla").orderBy(F.col("timestamp_coleta").desc())
df_clima = df_clima_raw.withColumn("rn", F.row_number().over(window_clima)).filter(F.col("rn") == 1).drop("rn")

window_voos = Window.partitionBy("icao24").orderBy(F.col("timestamp_coleta").desc())
df_voos = df_voos_raw.withColumn("rn", F.row_number().over(window_voos)).filter(F.col("rn") == 1).drop("rn")

# 2.3 Dimensao Condicao Climatica Dinamica (Surrogate Key via Hash)
df_dim_condicao = (
    df_clima.select("condicao_climatica", "descricao_climatica")
    .dropDuplicates()
    .withColumn("id_condicao", F.sha2(F.concat_ws("|", F.col("condicao_climatica"), F.col("descricao_climatica")), 256))
)
df_dim_condicao.write.format("delta").mode("overwrite").saveAsTable(TABLE_DIM_CONDICAO)

# Geofencing
df_voos_geo = df_voos.withColumn(
    "aeroporto_proximo",
    F.when(F.col("latitude") < -28.0, "POA")
     .when((F.col("latitude") >= -28.0) & (F.col("latitude") < -23.55), "CGH")
     .otherwise("GRU")
)

# Join Relacional (CORREÇÃO APLICADA AQUI)
# Renomeamos a coluna do df_clima na hora do join para evitar duplicidade no Delta Lake
df_joined = df_voos_geo.join(
    df_clima.withColumnRenamed("timestamp_coleta", "timestamp_coleta_clima"),
    on=df_voos_geo["aeroporto_proximo"] == F.col("aeroporto_sigla"),
    how="left"
)

# Motor de Regras: Retornando apenas o id_risco (1=Normal, 2=Medio, 3=Alto)
def build_fk_risk_expression():
    expr = F.lit(1) # Default Normal
    for iata, limits in WIND_LIMITS.items():
        expr = F.when((F.col("aeroporto_proximo") == iata) & (F.col("vento_velocidade_ms") > limits["medio"]), 2).otherwise(expr)
    expr = F.when(F.col("condicao_climatica").isin(MED_RISK_CONDITIONS), 2).otherwise(expr)
    for iata, limits in WIND_LIMITS.items():
        expr = F.when((F.col("aeroporto_proximo") == iata) & (F.col("vento_velocidade_ms") > limits["alto"]), 3).otherwise(expr)
    expr = F.when(F.col("condicao_climatica").isin(HIGH_RISK_CONDITIONS), 3).otherwise(expr)
    return expr

df_gold = (
    df_joined
    .withColumn("fk_risco", build_fk_risk_expression())
    .withColumn("fk_condicao", F.sha2(F.concat_ws("|", F.col("condicao_climatica"), F.col("descricao_climatica")), 256))
    .withColumn("timestamp_processamento", F.current_timestamp())
    # Removendo as strings pesadas que sujavam a fato
    .drop("condicao_climatica", "descricao_climatica")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Persistencia da Visao Consolidada

# COMMAND ----------

(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true") 
    .saveAsTable(TABLE_GOLD_MONITOR)
)
print(f"Processamento concluido. Visao intermediaria gravada.")