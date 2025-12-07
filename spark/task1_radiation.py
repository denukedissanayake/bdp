from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, when, month, year, round as spark_round, to_date
)
from pyspark.sql.types import DoubleType


def create_spark_session():
    """Create and configure Spark session."""
    return SparkSession.builder \
        .appName("Task1_Shortwave_Radiation_Analysis") \
        .getOrCreate()


def load_weather_data(spark, input_path):
    """Load weather data from HDFS and clean column names."""
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_path)
    
    # Rename columns with special characters for easier handling
    df = df.withColumnRenamed("shortwave_radiation_sum (MJ/m²)", "radiation")
    
    return df


def parse_dates(df):
    """Parse date column and extract year/month."""
    return df.withColumn("date_parsed", to_date(col("date"), "M/d/yyyy")) \
             .withColumn("year", year("date_parsed")) \
             .withColumn("month", month("date_parsed"))


def calculate_radiation_percentage(df):
    """
    Calculate the percentage of radiation > 15 MJ/m² per month.
    
    Formula: (sum of radiation values > 15) / (total sum of radiation) * 100
    """
    # Filter out null radiation values
    df_clean = df.filter(col("radiation").isNotNull())
    
    # Group by year and month, calculate totals
    result = df_clean.groupBy("year", "month").agg(
        spark_sum("radiation").alias("total_radiation"),
        spark_sum(
            when(col("radiation") > 15, col("radiation")).otherwise(0)
        ).alias("high_radiation_sum")
    )
    
    # Calculate percentage
    result = result.withColumn(
        "percentage_above_15",
        spark_round((col("high_radiation_sum") / col("total_radiation")) * 100, 2)
    )
    
    return result.select("year", "month", "total_radiation", "high_radiation_sum", "percentage_above_15") \
                 .orderBy("year", "month")


def main():
    """Main execution function."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # HDFS path - data should already be uploaded
    input_path = "hdfs://namenode:9000/user/test/input/weatherData.csv"
    
    print("=" * 70)
    print("TASK 1: Percentage of Total Shortwave Radiation > 15 MJ/m² per Month")
    print("=" * 70)
    
    # Load and process data
    df = load_weather_data(spark, input_path)
    df = parse_dates(df)
    
    # Calculate results
    result = calculate_radiation_percentage(df)
    
    # Show results
    print("\nResults (showing all months):")
    result.show(200, truncate=False)
    
    # Summary statistics
    print("\nSummary Statistics:")
    result.describe("percentage_above_15").show()
    
    spark.stop()
    print("\nTask 1 completed successfully!")


if __name__ == "__main__":
    main()
