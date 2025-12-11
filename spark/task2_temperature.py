# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max as sparkMax, year, month, weekofyear, to_date, round as sparkRound
)
from pyspark.sql.window import Window
from pyspark.sql import functions as F

def createSparkSession():
    return (SparkSession.builder
        .appName("Task2_Weekly_Max_Temperature_Analysis")
        .getOrCreate())

def loadWeatherData(spark, inputPath):  
    df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(inputPath))
    
    df = df.withColumnRenamed("temperature_2m_max (°C)", "tempMax")
    
    return df

def parseDates(df):
    return (df.withColumn("dateParsed", to_date(col("date"), "M/d/yyyy"))
             .withColumn("year", year("dateParsed"))
             .withColumn("month", month("dateParsed"))
             .withColumn("week", weekofyear("dateParsed")))

def findHottestMonths(df):
    """
    Find the hottest month for each year.
    Hottest month = month with highest average maximum temperature.
    """
    monthlyTemps = (df.filter(col("tempMax").isNotNull())
                      .groupBy("year", "month")
                      .agg(avg("tempMax").alias("avgMaxTemp")))
    
    windowSpec = Window.partitionBy("year").orderBy(col("avgMaxTemp").desc())
    
    hottestMonths = (monthlyTemps
        .withColumn("rank", F.row_number().over(windowSpec))
        .filter(col("rank") == 1)
        .select(
            col("year").alias("hottestYear"),
            col("month").alias("hottestMonth"),
            sparkRound("avgMaxTemp", 2).alias("avgTemp")
        ))
    
    return hottestMonths

def calculateWeeklyMaxTemps(df, hottestMonths):
    dfWithHottest = df.join(
        hottestMonths,
        (df["year"] == hottestMonths["hottestYear"]) & 
        (df["month"] == hottestMonths["hottestMonth"]),
        "inner"
    )
    
    weeklyMax = (dfWithHottest
        .groupBy("hottestYear", "hottestMonth", "week")
        .agg(sparkMax("tempMax").alias("weeklyMaxTemp"))
        .select(
            col("hottestYear").alias("year"),
            col("hottestMonth").alias("month"),
            col("week"),
            sparkRound("weeklyMaxTemp", 2).alias("maxTemperature")
        )
        .orderBy("year", "week"))
    
    return weeklyMax

def main():
    spark = createSparkSession()
    spark.sparkContext.setLogLevel("WARN")
    
    inputPath = "hdfs://namenode:9000/user/data/input/weatherData.csv"
    
    print("TASK 2: Weekly Maximum Temperatures for Hottest Months of Each Year")
    
    df = loadWeatherData(spark, inputPath)
    df = parseDates(df)
    
    hottestMonths = findHottestMonths(df)
    
    hottestMonths.orderBy("hottestYear").show(20, truncate=False)
    
    weeklyMax = calculateWeeklyMaxTemps(df, hottestMonths)
    
    weeklyMax.show(100, truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    main()
