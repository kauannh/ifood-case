from pyspark.sql import SparkSession, DataFrame, functions as F

from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)

class RawToTrustedProcessor:

    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def _apply_schema(self, df: DataFrame, schema: dict[str, str]) -> DataFrame:
        df = df.select(*schema.keys())
        for column, target_type in schema.items():
            df = df.withColumn(column, F.col(column).cast(target_type))
        return df
    
    def process(self, source_table: str, target_table: str, schema: dict[str, str]) -> None:
        logger.info(f"Processando {source_table} para {target_table}")
        df = self.spark.table(source_table)
        df_casted = self._apply_schema(df, schema)
        # TODO: deixar generico o suficiente a nivel de parametrizar particoes
        (
            df_casted.write.format("delta")
            .mode("overwrite")
            .partitionBy("year", "month")
            .option("partitionOverwriteMode", "dynamic")
            .saveAsTable(target_table)
        )
        logger.info(f"Processamento concluído")