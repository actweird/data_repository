# data_repository

how to run hdfs_compaction.py with spark-submit:

spark3-submit --master yarn \
--deploy-mode cluster \
--driver-memory 1g \
--executor-memory 1g \
hdfs_compaction.py \
--table_name schema.table

You need set table name in parameters (table_name, you write it like schema.table), because that's how program gets the path to the table.

**If you want to test solution with small files, you can generate small files in HDFS with files_creator.py program from this repository.**
