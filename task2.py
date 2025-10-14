from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


def write_batch(output_dir: Path):
    def _write(df, batch_id: int) -> None:
        if df.rdd.isEmpty():
            return
        batch_path = output_dir / f"batch_{batch_id}"
        (
            df.orderBy(F.col("driver_id"))
            .write.mode("overwrite")
            .option("header", "true")
            .csv(str(batch_path))
        )

    return _write


def main() -> None:
    spark = SparkSession.builder.appName("RideSharingAnalyticsTask2").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("trip_id", StringType(), True),
        StructField("driver_id", IntegerType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("timestamp", StringType(), True),
    ])

    project_root = Path(__file__).resolve().parent
    output_dir = project_root / "outputs" / "task_2"
    checkpoint_dir = project_root / "checkpoints" / "task_2"

    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    raw_stream = (
        spark.readStream.format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()
    )

    parsed_stream = (
        raw_stream.select(F.from_json(F.col("value"), schema).alias("data"))
        .select("data.*")
        .dropna(subset=["driver_id", "trip_id"])
    )

    aggregated_stream = (
        parsed_stream.groupBy("driver_id")
        .agg(
            F.sum(F.col("fare_amount")).alias("total_fare"),
            F.avg(F.col("distance_km")).alias("avg_distance"),
        )
    )

    query = (
        aggregated_stream.writeStream.outputMode("update")
        .foreachBatch(write_batch(output_dir))
        .option("checkpointLocation", str(checkpoint_dir))
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
