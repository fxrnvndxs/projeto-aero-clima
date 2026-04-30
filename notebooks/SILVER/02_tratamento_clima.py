# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Silver: Processamento Meteorologico
# MAGIC **Projeto:** Aero Clima | **Modulo:** Tratamento OpenWeather | **Versao:** 3.0 (Modular)
# MAGIC
# MAGIC Responsavel por extrair metricas climaticas do JSON, validar os aeroportos 
# MAGIC mapeados e padronizar os tipos de dados para cruzamento relacional.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuracao e Dependencias

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, ArrayType, FloatType

BRONZE_PATH  = "/Volumes/workspace/default/camada_bronze"
WEATHER_GLOB = f"{BRONZE_PATH}/weather_*.json"

CATALOG  = "workspace"
DATABASE = "default"
TABLE_CLIMA_SILVER = f"{CATALOG}.{DATABASE}.clima_silver"

# Schema rigoroso para parsing dinamico
weather_schema = StructType([
    StructField("dt", LongType()),
    StructField("visibility", LongType()),
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("humidity", LongType())
    ])),
    StructField("wind", StructType([
        StructField("speed", DoubleType()),
        StructField("deg", LongType())
    ])),
    StructField("weather", ArrayType(
        StructType([
            StructField("main", StringType()),
            StructField("description", StringType())
        ])
    ))
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingestao e Validacao de Contrato

# COMMAND ----------

try:
    df_clima_raw = spark.read.option("multiline", "true").json(WEATHER_GLOB)
except Exception as e:
    print(f"Log: Nenhum arquivo de clima encontrado ou erro de leitura: {e}")
    dbutils.notebook.exit("Sem dados para processar.")

expected_airports = {"CGH", "GRU", "POA"}
actual_airports   = set(df_clima_raw.columns)
missing_airports  = expected_airports - actual_airports

if missing_airports:
    raise ValueError(f"FALHA DE CONTRATO: Aeroportos ausentes no payload: {missing_airports}")

print("Validacao de contrato concluida: Aeroportos esperados localizados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transformacao e Tipagem Forte

# COMMAND ----------

df_clima_normalizado = (
    df_clima_raw
    .select(
        F.explode(
            F.array(
                F.struct(F.lit("CGH").alias("aeroporto"), F.to_json("CGH").alias("payload")),
                F.struct(F.lit("GRU").alias("aeroporto"), F.to_json("GRU").alias("payload")),
                F.struct(F.lit("POA").alias("aeroporto"), F.to_json("POA").alias("payload"))
            )
        ).alias("dado")
    )
    .select(
        F.col("dado.aeroporto").alias("aeroporto_sigla"),
        F.from_json("dado.payload", weather_schema).alias("payload")
    )
)

df_clima_silver = (
    df_clima_normalizado
    .select(
        "aeroporto_sigla",
        F.to_timestamp("payload.dt").alias("timestamp_coleta"),
        F.col("payload.main.temp").cast(FloatType()).alias("temperatura_c"),
        F.col("payload.main.humidity").cast(FloatType()).alias("umidade_pct"),
        F.col("payload.wind.speed").cast(FloatType()).alias("vento_velocidade_ms"),
        F.col("payload.wind.deg").cast(FloatType()).alias("vento_direcao_graus"),
        F.col("payload.weather")[0]["main"].alias("condicao_climatica"),
        F.col("payload.weather")[0]["description"].alias("descricao_climatica"),
        F.col("payload.visibility").cast(FloatType()).alias("visibilidade_m")
    )
    .filter(
        F.col("aeroporto_sigla").isNotNull() & 
        F.col("timestamp_coleta").isNotNull()
    )
    .dropDuplicates(["aeroporto_sigla", "timestamp_coleta"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Carga Otimizada (Append Estrito)

# COMMAND ----------

# Removido o option("mergeSchema", "true") para evitar overhead de I/O
(
    df_clima_silver.write
    .format("delta")
    .mode("append")
    .saveAsTable(TABLE_CLIMA_SILVER)
)

print(f"Processamento concluido. Registros inseridos na tabela {TABLE_CLIMA_SILVER}.")