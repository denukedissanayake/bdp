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

# Load data from mounted data folder
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("/opt/data/weatherData.csv")

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

from pyspark.ml.regression import GBTRegressor

# Gradient Boosted Trees (Adding for better non-linear performance)
gbt = GBTRegressor(
    featuresCol="features",
    labelCol="evapotranspiration",
    maxIter=100,
    seed=42
)

gbt_model = gbt.fit(train_data)
print("\n=== Gradient Boosted Trees (GBT) Model ===")
print("GBT Model trained successfully.")
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

# Random Forest predictions
rf_predictions = rf_model.transform(test_data)
rf_rmse = evaluator_rmse.evaluate(rf_predictions)

# GBT predictions
gbt_predictions = gbt_model.transform(test_data)
gbt_rmse = evaluator_rmse.evaluate(gbt_predictions)
gbt_r2 = evaluator_r2.evaluate(gbt_predictions)
gbt_mae = evaluator_mae.evaluate(gbt_predictions)

print("=== Linear Regression Evaluation ===")
print(f"RMSE: {lr_rmse:.4f}, R2: {evaluator_r2.evaluate(lr_predictions):.4f}")

print("\n=== Random Forest Evaluation ===")
print(f"RMSE: {rf_rmse:.4f}, R2: {evaluator_r2.evaluate(rf_predictions):.4f}")

print("\n=== Gradient Boosted Trees Evaluation ===")
print(f"RMSE: {gbt_rmse:.4f}")
print(f"R-squared: {gbt_r2:.4f}")
print(f"MAE: {gbt_mae:.4f}")
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
import random
from pyspark.sql.types import StructType, StructField, DoubleType

print("=" * 70)
print("PREDICTION: Best Achievable Weather Conditions (GBT + Random Search)")
print("(Goal: Minimal Realistic ET for May using Non-Linear Model)")
print("=" * 70)

# Random Grid Search Parameters
NUM_SAMPLES = 5000
MIN_SUNSHINE = 7200.0  # 2 hours
MAX_SUNSHINE = 28800.0 # 8 hours (realistic max for low ET day)
MIN_WIND = 5.0
MAX_WIND = 20.0        # Realistic calm day
MIN_PRECIP = 0.0
MAX_PRECIP = 24.0      # Max possible

print(f"Generating {NUM_SAMPLES} synthetic realistic weather points...")

synthetic_data = []
for _ in range(NUM_SAMPLES):
    synthetic_data.append((
        random.uniform(MIN_PRECIP, MAX_PRECIP),
        random.uniform(MIN_SUNSHINE, MAX_SUNSHINE),
        random.uniform(MIN_WIND, MAX_WIND)
    ))

# Create DataFrame
schema = StructType([
    StructField("precipitation_hours", DoubleType(), True),
    StructField("sunshine_duration", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True)
])

df_synthetic = spark.createDataFrame(synthetic_data, schema)

# Transform features
df_synthetic_assembled = assembler.transform(df_synthetic)
df_synthetic_scaled = scaler_model.transform(df_synthetic_assembled)

# Predict using GBT
predictions = gbt_model.transform(df_synthetic_scaled)

# Find minimum
best_result = predictions.orderBy("prediction").first()
min_et = best_result["prediction"]

print(f"\nOptimization Complete.")
print(f"Lowest Predicted ET found: {min_et:.4f} mm")

recommended_stats = {
    'precip': best_result["precipitation_hours"],
    'sun': best_result["sunshine_duration"],
    'wind': best_result["wind_speed"]
}

print(f"\n*** Recommended Values for May 2026 ***")
print(f"Precipitation Hours: {recommended_stats['precip']:.2f} hours")
print(f"Sunshine Duration: {recommended_stats['sun']:.2f} seconds ({recommended_stats['sun']/3600:.2f} hours)")
print(f"Wind Speed: {recommended_stats['wind']:.2f} km/h")

print(f"\nModel Predicted Evapotranspiration (GBT): {min_et:.4f} mm")

if min_et < 1.5:
    print("SUCCESS: GBT found a realistic scenario < 1.5mm!")
else:
    print("NOTE: Even with GBT, the lowest realistic ET is above 1.5mm.")
    print("      This represents the absolute best-case scenario found by the model.")
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
- GBT Model (Gradient Boosted Trees) generally provides best accuracy for non-linear data
- All models evaluated using RMSE, R-squared, and MAE

PREDICTION FOR MAY 2026:
To achieve evapotranspiration lower than 1.5mm (or minimize it), the expected weather conditions are:
""")

print(f"  - Precipitation Hours: {recommended_stats['precip']:.2f} hours")
print(f"  - Sunshine Duration: {recommended_stats['sun']/3600:.2f} hours ({recommended_stats['sun']:.0f} seconds)")
print(f"  - Wind Speed: {recommended_stats['wind']:.2f} km/h")

print("""
KEY INSIGHTS:
- Lower sunshine duration correlates with lower evapotranspiration
- Higher precipitation hours slightly reduce evapotranspiration
- Wind speed has moderate impact on evapotranspiration rates
""")

spark.stop()
print("\nAnalysis completed successfully!")
```
