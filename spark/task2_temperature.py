# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max as spark_max, year, month, weekofyear, to_date, round as spark_round
)
from pyspark.sql.window import Window
from pyspark.sql import functions as F


def create_spark_session():
    """Create and configure Spark session."""
    return (SparkSession.builder
        .appName("Task2_Weekly_Max_Temperature_Analysis")
        .getOrCreate())


def load_weather_data(spark, input_path):
    df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path))
    
    df = df.withColumnRenamed("temperature_2m_max (°C)", "temp_max")
    
    return df


def parse_dates(df):
    return (df.withColumn("date_parsed", to_date(col("date"), "M/d/yyyy"))
             .withColumn("year", year("date_parsed"))
             .withColumn("month", month("date_parsed"))
             .withColumn("week", weekofyear("date_parsed")))


def find_hottest_months(df):
    """
    Find the hottest month for each year.
    Hottest month = month with highest average maximum temperature.
    """
    monthly_temps = (df.filter(col("temp_max").isNotNull())
                      .groupBy("year", "month")
                      .agg(avg("temp_max").alias("avg_max_temp")))
    
    window_spec = Window.partitionBy("year").orderBy(col("avg_max_temp").desc())
    
    hottest_months = (monthly_temps
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(col("rank") == 1)
        .select(
            col("year").alias("hottest_year"),
            col("month").alias("hottest_month"),
            spark_round("avg_max_temp", 2).alias("avg_temp")
        ))
    
    return hottest_months


def calculate_weekly_max_temps(df, hottest_months):
    df_with_hottest = df.join(
        hottest_months,
        (df["year"] == hottest_months["hottest_year"]) & 
        (df["month"] == hottest_months["hottest_month"]),
        "inner"
    )
    
    weekly_max = (df_with_hottest
        .groupBy("hottest_year", "hottest_month", "week")
        .agg(spark_max("temp_max").alias("weekly_max_temp"))
        .select(
            col("hottest_year").alias("year"),
            col("hottest_month").alias("month"),
            col("week"),
            spark_round("weekly_max_temp", 2).alias("max_temperature")
        )
        .orderBy("year", "week"))
    
    return weekly_max


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    input_path = "hdfs://namenode:9000/user/data/input/weatherData.csv"
    
    print("TASK 2: Weekly Maximum Temperatures for Hottest Months of Each Year")
    
    df = load_weather_data(spark, input_path)
    df = parse_dates(df)
    
    hottest_months = find_hottest_months(df)
    
    hottest_months.orderBy("hottest_year").show(20, truncate=False)
    
    weekly_max = calculate_weekly_max_temps(df, hottest_months)
    
    weekly_max.show(100, truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    main()
