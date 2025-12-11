# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as sparkSum, when, month, year, round as sparkRound, to_date
)

def createSparkSession():
    return (SparkSession.builder
        .appName("Task1_Shortwave_Radiation_Analysis")
        .getOrCreate())

def loadWeatherData(spark, inputPath):
    df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(inputPath))
    
    df = df.withColumnRenamed("shortwave_radiation_sum (MJ/m²)", "radiation")

    return df

def parseDates(df):
    return (df.withColumn("dateParsed", to_date(col("date"), "M/d/yyyy"))
             .withColumn("year", year("dateParsed"))
             .withColumn("month", month("dateParsed")))


def calculateRadiationPercentage(df):
    """
    Calculate the percentage of radiation > 15 MJ/m² per month.
    Formula: (sum of radiation values > 15) / (total sum of radiation) * 100
    """
    dfClean = df.filter(col("radiation").isNotNull())
    
    result = dfClean.groupBy("year", "month").agg(
        sparkSum("radiation").alias("totalRadiation"),
        sparkSum(
            when(col("radiation") > 15, col("radiation")).otherwise(0)
        ).alias("highRadiationSum")
    )
    
    result = result.withColumn(
        "percentageAbove15",
        sparkRound((col("highRadiationSum") / col("totalRadiation")) * 100, 2)
    )
    
    return (result.select("year", "month", "totalRadiation", "highRadiationSum", "percentageAbove15")
                 .orderBy("year", "month"))


def main():
    spark = createSparkSession()
    spark.sparkContext.setLogLevel("WARN")
    
    inputPath = "hdfs://namenode:9000/user/data/input/weatherData.csv"
    
    print("TASK 1: Percentage of Total Shortwave Radiation > 15 MJ/m² per Month")
    
    df = loadWeatherData(spark, inputPath)
    df = parseDates(df)
    
    result = calculateRadiationPercentage(df)
    
    result.show(200, truncate=False)
    
    result.describe("percentageAbove15").show()
    
    spark.stop()

if __name__ == "__main__":
    main()
