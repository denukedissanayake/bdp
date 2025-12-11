# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, when, month, year, round as spark_round, to_date
)
from pyspark.sql.types import DoubleType


def create_spark_session():
    """Create and configure Spark session."""
    return (SparkSession.builder
        .appName("Task1_Shortwave_Radiation_Analysis")
        .getOrCreate())


def load_weather_data(spark, input_path):
    df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path))
    
    df = df.withColumnRenamed("shortwave_radiation_sum (MJ/m²)", "radiation")

    return df


def parse_dates(df):
    return (df.withColumn("date_parsed", to_date(col("date"), "M/d/yyyy"))
             .withColumn("year", year("date_parsed"))
             .withColumn("month", month("date_parsed")))


def calculate_radiation_percentage(df):
    """
    Calculate the percentage of radiation > 15 MJ/m² per month.
    Formula: (sum of radiation values > 15) / (total sum of radiation) * 100
    """
    df_clean = df.filter(col("radiation").isNotNull())
    
    result = df_clean.groupBy("year", "month").agg(
        spark_sum("radiation").alias("total_radiation"),
        spark_sum(
            when(col("radiation") > 15, col("radiation")).otherwise(0)
        ).alias("high_radiation_sum")
    )
    
    result = result.withColumn(
        "percentage_above_15",
        spark_round((col("high_radiation_sum") / col("total_radiation")) * 100, 2)
    )
    
    return (result.select("year", "month", "total_radiation", "high_radiation_sum", "percentage_above_15")
                 .orderBy("year", "month"))


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    input_path = "hdfs://namenode:9000/user/data/input/weatherData.csv"
    
    print("TASK 1: Percentage of Total Shortwave Radiation > 15 MJ/m² per Month")
    
    df = load_weather_data(spark, input_path)
    df = parse_dates(df)
    
    result = calculate_radiation_percentage(df)
    
    result.show(result.count(), truncate=False)
    
    result.describe("percentage_above_15").show()
    
    spark.stop()

if __name__ == "__main__":
    main()
