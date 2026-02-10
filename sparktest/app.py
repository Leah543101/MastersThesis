from pyspark.sql import SparkSession
import time 
import os
import argparse
from pathlib import Path

def arg_parser():
    DEFAULT_EVENT_LOG_DIR = str((Path(__file__).resolve().parent / "sparktestexperiments"))
    p = argparse.ArgumentParser(description="Spark Test Application")
    p.add_argument("--master",default='local[*]', help="Spark master URL e.g, local[4], yarn, spark://host:7077, k8s://https://...")
    p.add_argument("--app-name", default="Spark Test Application", help="Name of the Spark application")

    # Event log directory argument
    p.add_argument("--event-log-dir", default=DEFAULT_EVENT_LOG_DIR, help="Directory to store Spark event logs")
    p.add_argument("--event-log-codec", default="zstd", help="Codec for Spark event logs (e.g., lz4, snappy, uncompressed)")
    
    # Driver and executor memory arguments
    p.add_argument("--driver-memory", default="1.5g", help="Memory allocation for Spark driver (e.g., 2g, 4g)")
    p.add_argument("--executor-memory", default="2.4g", help="Memory allocation for Spark executor (e.g., 2g, 4g)")

    # Driver and executor cores arguments
    p.add_argument("--driver-cores", default=4, type=int, help="Number of CPU cores for Spark driver")
    p.add_argument("--executor-cores", default=4, type=int, help="Number of CPU cores for Spark executor") 

    # Number of executors argument
    p.add_argument("--num-executors", default=2, type=int, help="Number of Spark executors")
    #p.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    p.add_argument("--executor-memory-overhead", default="512m", help="Memory overhead for Spark executor (e.g., 512m, 1g)")

    return p.parse_args()


def build_spark(args):
    is_local = args.master.startswith("local")

     # Initialize SparkSession with configurations
    spark = (SparkSession.builder \
        .appName(args.app_name) \
        #.master(args.master) \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", args.event_log_dir)  
    )

    if args.event_log_codec and args.event_log_codec != "None":
        spark = spark.config("spark.eventLog.compression.codec", args.event_log_codec)

    if args.driver_memory:
        spark = spark.config("spark.driver.memory", args.driver_memory)
    if args.driver_cores is not None:
        spark = spark.config("spark.driver.cores", str(args.driver_cores))   
    

    if not is_local:
        if args.executor_memory:
            spark = spark.config("spark.executor.memory", args.executor_memory)
        if args.executor_cores is not None:
            spark = spark.config("spark.executor.cores", str(args.executor_cores))
#
        if args.num_executors is not None:
            spark = spark.config("spark.executor.instances", str(args.num_executors))
        if args.executor_memory_overhead:
            spark = spark.config("spark.executor.memoryOverhead", args.executor_memory_overhead)
    else:
        # For local mode, set executor memory and cores same as driver
        print("Running in local mode; setting executor memory and cores same as driver.")
        if args.executor_memory is None:
            spark = spark.config("spark.executor.memory", args.driver_memory)
        if args.executor_cores is None:
            spark = spark.config("spark.executor.cores", str(args.driver_cores))
    return spark.getOrCreate()

        
    
#def log(message):
#    spark.sparkContext.setLogLevel("INFO")
#    print(f"[LOG] {message}") 
#    return message      
#log("This is a log message at LOG level.")
#log("Starting the Spark application...")

def perform_computation():
    #log("Performing computation...")
    data = spark.range(0, 1000)
    df = data.withColumn("squared", data["id"] ** 2)
    agg = df.groupBy((df["id"] % 10).alias("bucket_mod10")).count()
    agg.write.mode("overwrite").parquet("./output/parquet_output")
    
    time.sleep(3)
    #log("Computation done.")    
    return df

#result_df = perform_computation()

#print(spark.sparkContext.getConf().get("spark.eventLog.dir"))

def main():
    args = arg_parser()
    global spark
    # create local directory for event logs if it doesn't exist
    if args.event_log_dir.startswith("file://"):
        os.makedirs(args.event_log_dir.replace("file://",""), exist_ok=True)

    spark = build_spark(args)
    sc = spark.sparkContext

    keys = [
        "spark.master",
        "spark.driver.memory", 
        "spark.driver.cores",
        "spark.executor.memory",
        "spark.executor.cores",
        "spark.eventLog.dir",
        "spark.eventLog.compression.codec",
        "spark.executor.instances",
        "spark.executor.memoryOverhead"]
    for key in keys:
        print(f"{key}: {sc.getConf().get(key, 'Not Set')}")
    print("Starting computation...")
    result_df = perform_computation()
    
    spark.stop()
    print("Spark application finished.")
if __name__ == "__main__":
    main()