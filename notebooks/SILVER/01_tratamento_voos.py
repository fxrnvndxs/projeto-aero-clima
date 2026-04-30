# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Silver: Processamento de Voos
# MAGIC **Projeto:** Aero Clima | **Modulo:** Tratamento OpenSky | **Versao:** 3.0 (Modular)
# MAGIC
# MAGIC Responsavel pela leitura, validacao de contrato (Schema), explode dos arrays 
# MAGIC e persistencia tabular otimizada (Append-only).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuracao e Dependencias

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, FloatType, BooleanType

BRONZE_PATH  = "/Volumes/workspace/default/camada_bronze"
FLIGHTS_GLOB = f"{BRONZE_PATH}/flights_*.json"

CATALOG  = "workspace"
DATABASE = "default"
TABLE_VOOS_SILVER = f"{CATALOG}.{DATABASE}.voos_silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingestao e Validacao de Contrato (Data Quality)

# COMMAND ----------

try:
    df_voos_raw = spark.read.option("multiline", "true").json(FLIGHTS_GLOB)
except Exception as e:
    print(f"Log: Nenhum arquivo de voos encontrado ou erro de leitura: {e}")
    dbutils.notebook.exit("Sem dados para processar.")

# Validacao estrutural de metadados
expected_cols = {"time", "states"}
actual_cols   = set(df_voos_raw.columns)
missing       = expected_cols - actual_cols

if missing:
    raise ValueError(f"FALHA DE CONTRATO: Schema inesperado nos dados de voos. Colunas ausentes: {missing}")

print("Validacao de contrato concluida: Estrutura JSON reconhecida.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transformacao e Tipagem Forte

# COMMAND ----------

df_voos_silver = (
    df_voos_raw
    .select(
        F.col("time").cast(LongType()).alias("timestamp_unix"),
        F.explode("states").alias("v")
    )
    .select(
        F.to_timestamp("timestamp_unix").alias("timestamp_coleta"),
        F.col("v")[0].alias("icao24"),
        F.trim(F.col("v")[1]).alias("callsign"),
        F.col("v")[2].alias("origin_country"),
        F.col("v")[5].cast(FloatType()).alias("longitude"),
        F.col("v")[6].cast(FloatType()).alias("latitude"),
        F.col("v")[8].cast(BooleanType()).alias("on_ground"),
        F.col("v")[9].cast(FloatType()).alias("velocity_ms"),
        F.col("v")[10].cast(FloatType()).alias("true_track_deg"),
        F.col("v")[11].cast(FloatType()).alias("vertical_rate_ms")
    )
    .filter(F.col("icao24").isNotNull())
    .dropDuplicates(["icao24", "timestamp_coleta"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Carga Otimizada (Append Estrito)

# COMMAND ----------

# Removido o option("mergeSchema", "true") para evitar recalculo de metadados historicos
(
    df_voos_silver.write
    .format("delta")
    .mode("append")
    .saveAsTable(TABLE_VOOS_SILVER)
)

print(f"Processamento concluido. Registros inseridos na tabela {TABLE_VOOS_SILVER}.")

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# Criamos um dataframe inicial
voo_fantasma = spark.createDataFrame([
    ("MOCK_POA_01", -29.99, -51.17, 15.0, datetime.now())
], ["icao24", "latitude", "longitude", "velocity", "timestamp_coleta"])

# Adicionamos as colunas faltantes e forçamos o cast para Float (FloatType)
voo_fantasma_completo = (
    voo_fantasma
    .withColumn("latitude", F.col("latitude").cast("float"))
    .withColumn("longitude", F.col("longitude").cast("float"))
    .withColumn("callsign", F.lit("TEST_POA"))
    .withColumn("origin_country", F.lit("Brazil"))
    .withColumn("on_ground", F.lit(False))
    .withColumn("velocity_ms", F.col("velocity").cast("float"))
    .withColumn("true_track_deg", F.lit(0.0).cast("float"))
    .withColumn("vertical_rate_ms", F.lit(0.0).cast("float"))
    .drop("velocity")
)

# Injetamos na tabela Silver
(
    voo_fantasma_completo.write
    .format("delta")
    .mode("append")
    .saveAsTable("workspace.default.voos_silver")
)

print("Voo fantasma inserido com sucesso em POA!")