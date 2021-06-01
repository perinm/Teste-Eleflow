import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def df():
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    df = spark.read.csv("SEMANAL_MUNICIPIOS-2019.csv",
                    inferSchema=True,
                    header =True,
                    sep=",",
                    encoding = 'utf-8',
    )
    return df