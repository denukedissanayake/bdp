from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max as spark_max, year, month, weekofyear, to_date, round as spark_round
)
from pyspark.sql.window import Window
from pyspark.sql import functions as F


def create_spark_session():
    """Create and configure Spark session."""
    return SparkSession.builder \
        .appName("Task2_Weekly_Max_Temperature_Analysis") \
        .getOrCreate()


def load_weather_data(spark, input_path):
    """Load weather data from HDFS and clean column names."""
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_path)
    
    # Rename columns with special characters for easier handling
    df = df.withColumnRenamed("temperature_2m_max (°C)", "temp_max")
    
    return df


def parse_dates(df):
    """Parse date column and extract temporal features."""
    return df.withColumn("date_parsed", to_date(col("date"), "M/d/yyyy")) \
             .withColumn("year", year("date_parsed")) \
             .withColumn("month", month("date_parsed")) \
             .withColumn("week", weekofyear("date_parsed"))


def find_hottest_months(df):
    """
    Find the hottest month for each year.
    
    Hottest month = month with highest average maximum temperature.
    """
    # Calculate average max temp per year-month
    monthly_temps = df.filter(col("temp_max").isNotNull()) \
                      .groupBy("year", "month") \
                      .agg(avg("temp_max").alias("avg_max_temp"))
    
    # Rank months within each year by avg temp (descending)
    window_spec = Window.partitionBy("year").orderBy(col("avg_max_temp").desc())
    
    hottest_months = monthly_temps \
        .withColumn("rank", F.row_number().over(window_spec)) \
        .filter(col("rank") == 1) \
        .select(
            col("year").alias("hottest_year"),
            col("month").alias("hottest_month"),
            spark_round("avg_max_temp", 2).alias("avg_temp")
        )
    
    return hottest_months


def calculate_weekly_max_temps(df, hottest_months):
    """
    Calculate weekly maximum temperatures for the hottest months.
    """
    # Join original data with hottest months
    # Using aliases to avoid ambiguous column references
    df_with_hottest = df.join(
        hottest_months,
        (df["year"] == hottest_months["hottest_year"]) & 
        (df["month"] == hottest_months["hottest_month"]),
        "inner"
    )
    
    # Calculate weekly max temperatures
    weekly_max = df_with_hottest \
        .groupBy("hottest_year", "hottest_month", "week") \
        .agg(spark_max("temp_max").alias("weekly_max_temp")) \
        .select(
            col("hottest_year").alias("year"),
            col("hottest_month").alias("month"),
            col("week"),
            spark_round("weekly_max_temp", 2).alias("max_temperature")
        ) \
        .orderBy("year", "week")
    
    return weekly_max


def main():
    """Main execution function."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # HDFS path - data should already be uploaded
    input_path = "hdfs://namenode:9000/user/test/input/weatherData.csv"
    
    print("=" * 70)
    print("TASK 2: Weekly Maximum Temperatures for Hottest Months of Each Year")
    print("=" * 70)
    
    # Load and process data
    df = load_weather_data(spark, input_path)
    df = parse_dates(df)
    
    # Step 1: Find hottest month per year
    hottest_months = find_hottest_months(df)
    
    print("\n[Step 1] Hottest Month for Each Year (by Avg Max Temperature):")
    hottest_months.orderBy("hottest_year").show(20, truncate=False)
    
    # Step 2: Calculate weekly max temps for those months
    weekly_max = calculate_weekly_max_temps(df, hottest_months)
    
    print("\n[Step 2] Weekly Maximum Temperatures for Hottest Months:")
    weekly_max.show(100, truncate=False)
    
    spark.stop()
    print("\nTask 2 completed successfully!")


if __name__ == "__main__":
    main()
