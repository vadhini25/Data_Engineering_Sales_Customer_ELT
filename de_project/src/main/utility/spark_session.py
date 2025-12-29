from pyspark.sql import SparkSession
import os
os.environ.get("JAVA_HOME")
from pyspark import SparkConf

def spark_session():
    # Correct macOS JAR path (update this to your real path)
    mysql_jar_path = "/Users/vadhinijhaver/Downloads/mysql-connector-j-8.0.33.jar"

    if not os.path.exists(mysql_jar_path):
        raise FileNotFoundError(f"MySQL JAR not found at: {mysql_jar_path}")
    conf = SparkConf()
    conf.set("spark.driver.extraClassPath", mysql_jar_path)
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("de_project")
        .config(conf=conf)
        .getOrCreate()
    )
    print("spark session created")
    return spark