# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Silver: Processamento de Voos
# MAGIC **Projeto:** Aero Clima | **Modulo:** Tratamento OpenSky | **Versao:** 3.2 (Data Contract Integrado)
# MAGIC
# MAGIC Responsavel pela leitura, validacao de contrato orientada a objetos, 
# MAGIC explode dos arrays e persistencia tabular otimizada (Append-only).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuracao, Dependencias e Classes Base

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, FloatType, BooleanType
import logging

# Configuracao de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BRONZE_PATH  = "/Volumes/workspace/default/camada_bronze"
FLIGHTS_GLOB = f"{BRONZE_PATH}/flights_*.json"

CATALOG  = "workspace"
DATABASE = "default"
TABLE_VOOS_SILVER = f"{CATALOG}.{DATABASE}.voos_silver"

class ContractViolationError(Exception):
    """Excecao customizada para quebra de contrato de dados."""
    pass

class DataContractValidator:
    """
    Simula o comportamento de frameworks como Great Expectations, 
    isolando a logica de Data Quality do fluxo de transformacao.
    """
    def __init__(self, df, expected_schema, layer_name):
        self.df = df
        self.expected_schema = set(expected_schema)
        self.layer_name = layer_name
        self.missing_columns = set()

    def expect_all_columns_to_exist(self):
        actual_cols = set(self.df.columns)
        self.missing_columns = self.expected_schema - actual_cols
        return len(self.missing_columns) == 0

    def validate_or_fail(self):
        if not self.expect_all_columns_to_exist():
            error_msg = f"FALHA DE CONTRATO ({self.layer_name}): Colunas ausentes no payload: {self.missing_columns}"
            logging.error(error_msg)
            raise ContractViolationError(error_msg)
        
        logging.info(f"Data Contract [PASSOU]: Schema validado com sucesso para a camada {self.layer_name}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingestao e Validacao de Contrato (Data Quality)

# COMMAND ----------

try:
    df_voos_raw = spark.read.option("multiline", "true").json(FLIGHTS_GLOB)
except Exception as e:
    logging.warning(f"Nenhum arquivo de voos encontrado ou erro de leitura: {e}")
    dbutils.notebook.exit("Sem dados para processar.")

# Aplicacao do Contrato de Dados usando a classe instanciada
validator = DataContractValidator(
    df=df_voos_raw, 
    expected_schema=["time", "states"], 
    layer_name="Silver - Voos"
)

# Se falhar, o pipeline para aqui disparando o ContractViolationError
validator.validate_or_fail()

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