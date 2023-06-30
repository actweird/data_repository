# data_repository

how to run hdfs_compaction.py with spark-submit:

spark3-submit --master yarn \
--deploy-mode cluster \
--conf spark.pyspark.virtualenv.enabled=true \
--conf spark.pyspark.virtualenv.type=native \
hdfs_compaction.py \
--table_name schema.table \
--dir_size 550505

you need 2 parameters - first is table name (table_name, you write it like schema.table), because that's how program gets the path to the table; second is directory size (dir_size, you write it in bytes) because with it you will get optimal number for repartition

if you want to test solution with small files, you can generate small files in HDFS with files_creator.py program from this repository.
