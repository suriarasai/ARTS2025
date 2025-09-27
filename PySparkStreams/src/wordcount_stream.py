import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def main():
    master = os.getenv("SPARK_MASTER", "local[*]")
    app_name = "SocketWordCount"
    host = os.getenv("SOCKET_HOST", "socket")   # service name in compose
    port = int(os.getenv("SOCKET_PORT", "9999"))

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # Tune shuffle/partitions for local runs
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Read from socket
    lines = (
        spark.readStream
        .format("socket")
        .option("host", host)
        .option("port", port)
        .load()
    )

    words = lines.select(explode(split(col("value"), r"\s+")).alias("word")).where(col("word") != "")
    counts = words.groupBy("word").count().orderBy(col("count").desc())

    query = (
        counts.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 50)
        .start()
    )

    print(f"Streaming from {host}:{port} ... Type into that socket to see counts update.")
    query.awaitTermination()

if __name__ == "__main__":
    main()
