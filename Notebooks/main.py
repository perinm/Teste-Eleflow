import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.window import Window


def convert_date_column(col, df):
    return df.withColumn(col, sf.to_date(col,"d/M/y"))

def iterate_and_convert_date_columns(date_col_list, df):
    for col in date_col_list:
        df = convert_date_column(col, df)
    return df

def main():
    # Creating o ambient variable and instantiate Spark session
    spark = SparkSession.builder.appName("Processa dados massivos").master("local[1]").getOrCreate()

    df = spark.read.csv("SEMANAL_MUNICIPIOS-2019.csv",
                    inferSchema=True,
                    header =True,
                    sep=",",
                    encoding = 'utf-8',
    )
    
    df = iterate_and_convert_date_columns(["DATA INICIAL","DATA FINAL"], df)
    print(df.schema[0].dataType)

if __name__ == "__main__":
    main()