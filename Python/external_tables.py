"""
This program can help get all external tables from your hive database.
In our case, external tables, that we copied with self-write ETL solution, broke.
We needed to fix them and prevent other such tables from breaking, so we found all the external tables.
Maybe this simple program will help anyone :)
P.S. Watch spark-submit application logs to see tables.
"""


from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark import SparkConf


if __name__ == "__main__":

    spark = (SparkSession
             .builder
             .appName("external_checker")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("hive.exec.dynamic.partition", "true")
             .config("hive.exec.dynamic.partition.mode", "nonstrict")
             .enableHiveSupport()
             .getOrCreate()
             )
    spark.sparkContext.setLogLevel("WARN")

    sc = SparkContext.getOrCreate(SparkConf().setMaster("yarn"))

    schema_name = [
        # write your schemas here
    ]

    for schema in schema_name:
        try:
            tables = spark.sql(f"show tables in {schema}")
            tables.show(truncate=False)
            df = tables.select('tableName').rdd.flatMap(list).collect()

            for table in df:
                try:
                    describe_tables = spark.sql(f'describe formatted {schema}.{table}')
                    is_extermal = describe_tables.filter(describe_tables['data_type'].contains("EXTERNAL"))
                    if is_extermal.count() == 0:
                        print(f'{schema}.{table} is managed or view')
                    else:
                        print(f'{schema}.{table} EXTERNAL')
                except:
                    print(f'{schema}.{table} not exist')

            print(f'Tables in schema {schema} over, go fix if needed')
        except:
            print(f'{schema} not exist')

    print('Process end')
