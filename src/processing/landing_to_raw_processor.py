from functools import reduce
from pyspark.sql import DataFrame, SparkSession, functions as F

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)

class LandingToRawProcessor:

    def __init__(self, spark: SparkSession, dbutils):
        self.spark = spark
        self.dbutils = dbutils

    def _list_partition_paths(self, landing_path: str) -> list[str]:
        return [
            f"{landing_path}/{y.name}{m.name}"
            for y in self.dbutils.fs.ls(landing_path)
            for m in self.dbutils.fs.ls(f"{landing_path}/{y.name}")
        ]

    def _force_cast_to_string(self, path: str, base_path: str) -> DataFrame:
        df = self.spark.read.option("basePath", base_path).parquet(path)
        return ( df.select([F.col(c).cast("string").alias(c) for c in df.columns])
                .withColumn("month", F.lpad(F.col("month"), 2, "0"))
            )

    def _read_all(self, landing_path: str) -> DataFrame:
        landing_paths = self._list_partition_paths(landing_path)
        if not landing_paths:
            raise ValueError(f"Nenhuma partição encontrada em {landing_path}")
        return reduce(
            lambda acc, nxt: acc.unionByName(nxt, allowMissingColumns=True),
            (self._force_cast_to_string(p, landing_path) for p in landing_paths),
        )

    def process(self, landing_path: str, target_table: str) -> None:
        logger.info(f"Processando dados do {landing_path} para {target_table}")
        df = self._read_all(landing_path)
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy("year", "month")
            .option("partitionOverwriteMode", "dynamic")
            .option("mergeSchema", "true")
            .saveAsTable(target_table)
        )
        logger.info(f"Processamento concluído")