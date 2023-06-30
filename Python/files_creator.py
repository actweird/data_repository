from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = (SparkSession
         .builder
         .appName("files_creator")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .config("hive.exec.dynamic.partition", "true")
         .config("hive.exec.dynamic.partition.mode", "nonstrict")
         .enableHiveSupport()
         .getOrCreate()
         )

df = spark.range(100000).cache()
df2 = df.withColumn("partitionCol", lit("p1"))
df2.repartition(200).write.partitionBy("partitionCol").saveAsTable("schema.table")
