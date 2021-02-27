import os
import shutil
import time
from pathlib import Path

import pyspark.sql.functions as functions
from py4j.java_gateway import java_import
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame, WindowSpec, Window
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql import functions as f


def _create_spark():
    return SparkSession.builder \
        .appName("app1") \
        .enableHiveSupport() \
        .config("spark.driver.extraClassPath",
                r"C:\Spark\spark-3.0.1-bin-hadoop3.2\sql-spark-connector\spark-mssql-connector_2.12_3.0-1.0.0-alpha.jar;"
                r"C:\Spark\spark-3.0.1-bin-hadoop3.2\sql-spark-connector\mssql-jdbc-9.2.0.jre8.jar") \
        .getOrCreate()


def simple_example_with_dataframe_api(spark: SparkSession):
    path = Path(os.path.dirname(__file__)) / "data/input.txt"
    data_frame = spark.read.text(str(path))

    return data_frame \
        .select(functions.split(functions.col("value"), " ").alias("words")) \
        .select(functions.explode(functions.col("words")).alias("word")) \
        .groupBy("word") \
        .count() \
        .orderBy(functions.col("count").desc(), functions.col("word").asc())


def simple_example_with_sql_api(spark: SparkSession):
    path = Path(os.path.dirname(__file__)) / "data/input.txt"
    data_frame = spark.read.text(str(path))

    data_frame.createOrReplaceTempView("my_view")

    return spark.sql("""
        SELECT word, COUNT(*) AS count FROM (
        SELECT explode(split(value, ' ')) AS word 
        FROM my_view)
        GROUP BY word
        ORDER BY count DESC, word ASC""")


@f.udf(returnType=StringType())
def snake_case(value: str):
    result = ""
    for word in value.split(" "):
        result += word[0:1].lower() + word[1:len(word)] + "_"
    return result[:-1]  # remove trailing underscore


def udf_example(spark: SparkSession):
    schema = StructType([StructField("phrase", StringType(), nullable=False)])
    data = [
        ("Hello Word",),
        ("Hello NetCoreConf",),
    ]
    data_frame = spark.createDataFrame(schema=schema, data=data)

    data_frame.select(snake_case(data_frame["phrase"]).alias("snake_case_phrase")).show()

    spark.udf.register("snake_case", snake_case)
    data_frame.createOrReplaceTempView("my_view")
    spark.sql("SELECT snake_case(phrase) AS snake_case_phrase FROM my_view").show()


def calculate_stock_intervals_with_dataframe_api(daily_stock: DataFrame) -> DataFrame:
    group_window_spec: WindowSpec = Window \
        .partitionBy("StoreId", "ProductId", "Quantity") \
        .orderBy("Date")

    return daily_stock.select(
        (-(f.row_number().over(group_window_spec))).alias("group"),
        "StoreId",
        "ProductId",
        "Date",
        "Quantity"
    ) \
        .withColumn("group", f.expr("date_add(Date, group)")) \
        .groupBy("StoreId", "ProductId", "group", "Quantity") \
        .agg(
        f.min("Date").alias("StartDate"),
        f.max("Date").alias("EndDate")) \
        .select("StoreId", "ProductId", "StartDate", "EndDate", "Quantity")


def calculate_stock_intervals_with_sql_api(spark: SparkSession, daily_stock: DataFrame) -> DataFrame:
    daily_stock.createOrReplaceTempView("my_view")

    return spark.sql("""
WITH my_cte AS
  (SELECT ROW_NUMBER() OVER (
                             ORDER BY date) AS RowNumber,
                            DATE_ADD(Date, -ROW_NUMBER() OVER (PARTITION BY StoreId, ProductId, Quantity
                                                             ORDER BY Date)) AS group,
                            StoreId, 
                            ProductId, Date, Quantity
   FROM my_view)
SELECT StoreId, 
       ProductId,
       MIN(Date) AS StartDate,
       MAX(Date) AS EndDate,
       Quantity
FROM my_cte
GROUP BY StoreId,
         ProductId,
         group,
         Quantity""")


def start_thrift_server_from_python(spark: SparkSession):
    java_import(spark.sparkContext._gateway.jvm, "")
    # Start the Thrift Server using the jvm and passing the same spark session corresponding to pyspark session in the jvm side.
    spark.sparkContext._gateway.jvm.org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.startWithContext(
        spark._jwrapped)


def spark_mssql_connector_example(spark: SparkSession):
    server_name = "jdbc:sqlserver://localhost:1433"
    database_name = "pubs"
    url = f"{server_name};databaseName={database_name};"
    user = "sa"
    password = "P@ssw0rd"
    db_table = "authors"

    df = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("dbtable", db_table) \
        .option("user", user) \
        .option("password", password).load()

    df.show()

    df.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode("overwrite") \
        .option("url", url) \
        .option("dbtable", "authors2") \
        .option("user", user) \
        .option("password", password) \
        .save()


def catalog_example(spark: SparkSession, start_thrift_server=False):
    # region Remove hive directories and files
    shutil.rmtree("metastore_db", ignore_errors=True)
    shutil.rmtree("spark-warehouse", ignore_errors=True)
    if os.path.exists("derby.log"):
        os.remove("derby.log")
    # endregion

    spark.sql("create database pubs")  # metastore_db / spark-warehouse / derby.log
    spark.sql("show databases").show()
    spark.sql("describe database pubs").show(truncate=False)
    spark.sql("use pubs")
    spark.sql("select current_database()").show()

    spark.sql("create table student (id int, name string, age int)")  # pubs.db/student
    spark.sql("show tables").show()
    spark.sql("describe table student").show()
    spark.sql("describe table extended student").show(truncate=False)  # Provider hive
    spark.sql("drop table if exists student")

    spark.sql("create table student (id int, name string, age int)")
    spark.sql("insert into student values (1, 'Sergio', 45)")
    spark.sql("select * from student").show()


    spark.sql("create table student_copy using csv as select * from student")  # Provider csv

    spark.sql("drop table student")

    spark.sql("""
    create table if not exists student (id int, name string, age int) using parquet 
    comment 'a comment' 
    tblproperties('key1'='value1', 'key2'='value2')""")  # Provider parquet
    spark.sql("describe table extended student").show(truncate=False)
    spark.sql("drop table student")

    spark.sql(
        "create table student row format delimited fields terminated by ';' AS select name, age from student_copy")  # Provider hive
    spark.sql("drop table student")

    spark.sql("create table student (name string, age int)")
    spark.read.csv("data/student.csv", inferSchema=True, header=True).createOrReplaceTempView("temp_student")
    spark.sql("insert into student select * from temp_student")
    spark.sql("select * from student").show()
    spark.sql("drop table student")

    df = spark.read.csv("data/student.csv", inferSchema=True, header=True)
    df.write.saveAsTable("student", mode="append", format="hive")

    df = spark.table("student")
    df.show()
    spark.sql("drop table student")

    # create external table from existing file
    spark.sql("create external table if not exists student (name string, age int) "
              "row format delimited "
              "fields terminated by ',' "
              "stored as textfile "
              "location 'data/existing_student'")  # directory
    spark.sql("select * from student").show()
    spark.sql("describe table extended student").show(truncate=False)  # Type EXTERNAL
    spark.sql("drop table student")  # it does not remove the external data source

    spark.sql("""
    create table student (name string, age int) 
    row format delimited fields terminated by ','
    """)
    spark.sql("load data local inpath 'data/student2.csv' into table student")
    spark.sql("select * from student").show()

    if start_thrift_server:
        start_thrift_server_from_python(spark)

        sc: SparkContext = spark.sparkContext
        print(sc.uiWebUrl)

        while True:
            time.sleep(60)  # use pubs; show tables; select * from student;


def main():
    spark = _create_spark()
    sc: SparkContext = spark.sparkContext
    print(sc.uiWebUrl)

    simple_example_with_dataframe_api(spark).show()
    simple_example_with_sql_api(spark).show()

    udf_example(spark)

    daily_stock = spark.read.parquet(str(Path(os.path.dirname(__file__)) / "data/daily_stocks"))
    calculate_stock_intervals_with_dataframe_api(daily_stock).show()
    calculate_stock_intervals_with_sql_api(spark, daily_stock).show()

    spark_mssql_connector_example(spark)

    catalog_example(spark, start_thrift_server=False)


if __name__ == '__main__':
    main()
