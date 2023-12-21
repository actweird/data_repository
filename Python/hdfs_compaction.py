import math
import argparse
import subprocess
from datetime import datetime

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.shell import sc
from pyspark.sql import SparkSession


def current_date():
    """
    Get current date in YARN date format for logging
    """
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]


def delete_checkpoint_path(path):
    subprocess.call(["hadoop", "fs", "-rm", "-r", path])


def get_table_location(table_name):
    """
    Get location where table stored. Used for scanning number of files and its storage size
    """
    table_location = (spark
        .sql(f"describe formatted {table_name}")
        .filter("col_name='Location' and data_type like 'hdfs://%'")
        .select("data_type").collect()[0][0])
    return table_location


if __name__ == "__main__":

    spark = (SparkSession
             .builder
             .appName("hdfs_compactor")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("hive.exec.dynamic.partition", "true")
             .config("hive.exec.dynamic.partition.mode", "nonstrict")
             .enableHiveSupport()
             .getOrCreate()
             )
    spark.sparkContext.setLogLevel("WARN")

    sc = SparkContext.getOrCreate(SparkConf().setMaster("yarn"))

    parser = argparse.ArgumentParser(description="Spark application to to compact data on Hive")
    parser.add_argument("--table_name", type=str, required=True, help="Table name to compact")

    args = parser.parse_args()
    name_table = args.table_name

    table_directory = get_table_location(name_table)
    fs_uri = "your url, like hdfs://smthhere"

    def get_repartition_factor():
        """
        Divide folder size by block size and get optimal number for repartition
        """
        block_size = sc._jsc.hadoopConfiguration().get("dfs.blocksize")
        URI = spark.sparkContext._gateway.jvm.java.net.URI
        Path = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path
        FileSystem = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem
        Configuration = spark.sparkContext._gateway.jvm.org.apache.hadoop.conf.Configuration
        fs = FileSystem.get(URI(fs_uri), Configuration())
        dir_size = fs.getContentSummary(Path(table_directory)).getLength()
        return math.ceil(int(dir_size) / int(block_size))

    print(f'{current_date()} INFO: Started to backup {name_table}')

    # backup table to avoid unpleasant incidents
    df_for_bkp = spark.sql(f"select * from {name_table}")
    (
        df_for_bkp
        .write
        .saveAsTable(f'{name_table}_bkp')
    )

    bkp_df = spark.sql(f"select * from {name_table}_bkp")

    print(f'{current_date()} INFO: Backup SUCCESS. Backup table name: {name_table}_bkp')

    origin_table_count = df_for_bkp.count()
    backup_table_count = bkp_df.count()

    print(f'{current_date()} INFO: Origin table count is: {origin_table_count}')
    print(f'{current_date()} INFO: Backup table count is: {backup_table_count}')

    try:
        if origin_table_count == backup_table_count:
            print(f'{current_date()} INFO: Counts of {name_table} and {name_table}_bkp are equals.')
            print(f'{current_date()} INFO: Started to repartition {name_table}')

            sc.setCheckpointDir(f"{table_directory}_checkpoint")

            part_df = spark.sql(f"select * from {name_table}").checkpoint() # we need use checkpoint because we can't read table and write table at the same time

            # this is how we compact small files in HDFS. Just use repartition and see magic :
            (
                part_df
                .repartition(get_repartition_factor())
                .write
                .insertInto(name_table, overwrite=True)
            )

            print(f'{current_date()} INFO: MARKED AS SUCCESS. Repartition of {name_table} finished. Parquet has been recorded at {table_directory}')

            print(f'{current_date()} INFO: Deleting {table_directory}_checkpoint.')

            # delete checkpoint directory because we don't need it anymore
            delete_checkpoint_path(f"{table_directory}_checkpoint")
            print(f'{current_date()} INFO: {table_directory}_checkpoint deleted. Application marked as SUCCESS.')
        else:
            print(f'{current_date()} INFO: Counts of {name_table} and {name_table}_bkp are not equals.')

    except ValueError as err:
        print(f'Got error: {err}')
