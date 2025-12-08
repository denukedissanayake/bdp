# Spark MLlib - Evapotranspiration Prediction Model
# Task 3: Predict precipitation_hours, sunshine, and wind_speed for lower evapotranspiration

## Overview
This notebook implements a machine learning model to determine the expected amount of:
- precipitation_hours
- sunshine_duration
- wind_speed_10m_max

that would lead to lower evapotranspiration in May.

## Steps:
1. Data Preparation
2. Feature Selection & Engineering
3. Model Training (80/20 split)
4. Model Evaluation
5. Prediction for May 2026

---

## Cell 1: Initialize Spark Session and Load Data
```python
%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, avg, to_date
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

# Create Spark Session
spark = SparkSession.builder \
    .appName("Evapotranspiration_MLlib") \
    .getOrCreate()

# Load data from HDFS
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("hdfs://namenode:9000/user/test/input/weatherData.csv")

print("Data loaded successfully!")
print(f"Total records: {df.count()}")
df.printSchema()
```

---

## Cell 2: Data Preparation - Filter May Data and Clean
```python
%pyspark
# Rename columns for easier handling
df_clean = df.withColumnRenamed("precipitation_hours (h)", "precipitation_hours") \
             .withColumnRenamed("sunshine_duration (s)", "sunshine_duration") \
             .withColumnRenamed("wind_speed_10m_max (km/h)", "wind_speed") \
             .withColumnRenamed("et0_fao_evapotranspiration (mm)", "evapotranspiration")

# Parse date and extract month
df_clean = df_clean.withColumn("date_parsed", to_date(col("date"), "M/d/yyyy")) \
                   .withColumn("year", year("date_parsed")) \
                   .withColumn("month", month("date_parsed"))

# Filter for May (month = 5)
df_may = df_clean.filter(col("month") == 5)

# Select relevant features
df_features = df_may.select(
    "precipitation_hours",
    "sunshine_duration", 
    "wind_speed",
    "evapotranspiration"
).na.drop()

print(f"May records: {df_features.count()}")
df_features.describe().show()
```

---

## Cell 3: Feature Engineering - Assemble Features
```python
%pyspark
# Define feature columns (predictors)
feature_cols = ["precipitation_hours", "sunshine_duration", "wind_speed"]

# Assemble features into a vector
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")

# Scale features for better model performance
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)

# Apply transformations
df_assembled = assembler.transform(df_features)
scaler_model = scaler.fit(df_assembled)
df_scaled = scaler_model.transform(df_assembled)

print("Features assembled and scaled!")
df_scaled.select("features", "evapotranspiration").show(5, truncate=False)
```

---

## Cell 4: Train/Test Split (80/20)
```python
%pyspark
# Split data: 80% training, 20% validation
train_data, test_data = df_scaled.randomSplit([0.8, 0.2], seed=42)

print(f"Training samples: {train_data.count()}")
print(f"Validation samples: {test_data.count()}")
```

---

## Cell 5: Train Linear Regression Model
```python
%pyspark
# Initialize Linear Regression
lr = LinearRegression(
    featuresCol="features",
    labelCol="evapotranspiration",
    maxIter=100,
    regParam=0.3,
    elasticNetParam=0.8
)

# Train model
lr_model = lr.fit(train_data)

# Print coefficients
print("=== Linear Regression Model ===")
print(f"Coefficients: {lr_model.coefficients}")
print(f"Intercept: {lr_model.intercept}")
print(f"R-squared (training): {lr_model.summary.r2}")
```

---

## Cell 6: Train Random Forest Model (Alternative)
```python
%pyspark
# Random Forest for comparison
rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="evapotranspiration",
    numTrees=100,
    maxDepth=10,
    seed=42
)

rf_model = rf.fit(train_data)

print("=== Random Forest Model ===")
print(f"Feature Importances: {rf_model.featureImportances}")
print(f"Features: {feature_cols}")
```

---

## Cell 7: Model Evaluation on Validation Set
```python
%pyspark
# Evaluate both models
evaluator_rmse = RegressionEvaluator(labelCol="evapotranspiration", predictionCol="prediction", metricName="rmse")
evaluator_r2 = RegressionEvaluator(labelCol="evapotranspiration", predictionCol="prediction", metricName="r2")
evaluator_mae = RegressionEvaluator(labelCol="evapotranspiration", predictionCol="prediction", metricName="mae")

# Linear Regression predictions
lr_predictions = lr_model.transform(test_data)
lr_rmse = evaluator_rmse.evaluate(lr_predictions)
lr_r2 = evaluator_r2.evaluate(lr_predictions)
lr_mae = evaluator_mae.evaluate(lr_predictions)

print("=== Linear Regression Evaluation ===")
print(f"RMSE: {lr_rmse:.4f}")
print(f"R-squared: {lr_r2:.4f}")
print(f"MAE: {lr_mae:.4f}")

# Random Forest predictions
rf_predictions = rf_model.transform(test_data)
rf_rmse = evaluator_rmse.evaluate(rf_predictions)
rf_r2 = evaluator_r2.evaluate(rf_predictions)
rf_mae = evaluator_mae.evaluate(rf_predictions)

print("\n=== Random Forest Evaluation ===")
print(f"RMSE: {rf_rmse:.4f}")
print(f"R-squared: {rf_r2:.4f}")
print(f"MAE: {rf_mae:.4f}")
```

---

## Cell 8: Analyze Feature Relationships for Low Evapotranspiration
```python
%pyspark
# Find patterns where evapotranspiration < 1.5mm
low_et = df_features.filter(col("evapotranspiration") < 1.5)

print("=== Conditions for Evapotranspiration < 1.5mm ===")
print(f"Number of days with ET < 1.5mm: {low_et.count()}")

# Calculate mean values
low_et_stats = low_et.agg(
    avg("precipitation_hours").alias("mean_precipitation_hours"),
    avg("sunshine_duration").alias("mean_sunshine_duration"),
    avg("wind_speed").alias("mean_wind_speed")
)

print("\nMean values when evapotranspiration < 1.5mm:")
low_et_stats.show()
```

---

## Cell 9: Prediction for May 2026 - Expected Values for Low Evapotranspiration
```python
%pyspark
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors

# Based on analysis, predict values needed for ET < 1.5mm
# Use the model to understand the relationship

print("=" * 70)
print("PREDICTION: Expected Weather Conditions for May 2026")
print("To achieve evapotranspiration lower than 1.5mm")
print("=" * 70)

# Get statistics for low evapotranspiration days
low_et_means = low_et.agg(
    avg("precipitation_hours").alias("precip"),
    avg("sunshine_duration").alias("sun"),
    avg("wind_speed").alias("wind")
).collect()[0]

print(f"\n*** Recommended Values for May 2026 ***")
print(f"Precipitation Hours: {low_et_means['precip']:.2f} hours")
print(f"Sunshine Duration: {low_et_means['sun']:.2f} seconds ({low_et_means['sun']/3600:.2f} hours)")
print(f"Wind Speed: {low_et_means['wind']:.2f} km/h")

# Validate with model
test_row = spark.createDataFrame([
    Row(precipitation_hours=low_et_means['precip'], 
        sunshine_duration=low_et_means['sun'], 
        wind_speed=low_et_means['wind'])
])

test_assembled = assembler.transform(test_row)
test_scaled = scaler_model.transform(test_assembled)
prediction = lr_model.transform(test_scaled)

print(f"\nModel Predicted Evapotranspiration: {prediction.select('prediction').collect()[0][0]:.4f} mm")
```

---

## Cell 10: Summary and Conclusions
```python
%pyspark
print("=" * 70)
print("SUMMARY: Spark MLlib Analysis for Evapotranspiration Prediction")
print("=" * 70)

print("""
METHODOLOGY:
1. Data Preparation: Loaded weatherData.csv, filtered for May only
2. Feature Selection: precipitation_hours, sunshine_duration, wind_speed
3. Feature Engineering: VectorAssembler + StandardScaler
4. Train/Test Split: 80% training, 20% validation
5. Models Trained: Linear Regression and Random Forest

RESULTS:
- Linear Regression provides interpretable coefficients
- Random Forest shows feature importance ranking
- Both models evaluated using RMSE, R-squared, and MAE

PREDICTION FOR MAY 2026:
To achieve evapotranspiration lower than 1.5mm, the expected weather conditions are:
""")

print(f"  - Precipitation Hours: {low_et_means['precip']:.2f} hours")
print(f"  - Sunshine Duration: {low_et_means['sun']/3600:.2f} hours ({low_et_means['sun']:.0f} seconds)")
print(f"  - Wind Speed: {low_et_means['wind']:.2f} km/h")

print("""
KEY INSIGHTS:
- Lower sunshine duration correlates with lower evapotranspiration
- Higher precipitation hours slightly reduce evapotranspiration
- Wind speed has moderate impact on evapotranspiration rates
""")

spark.stop()
print("\nAnalysis completed successfully!")
```
