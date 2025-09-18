# ml_models.py - Machine Learning Models for Taxi System
import argparse
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression as MLLogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


class TaxiMLModelManager:
    """
    Machine Learning model manager for taxi booking system
    Handles fare prediction, demand forecasting, and customer segmentation
    """

    def __init__(self, app_name="TaxiMLModels"):
        # Configure environment and Spark for Windows compatibility
        import sys
        import os

        # Set environment variables to fix Python path issues
        python_exe = sys.executable
        os.environ['PYSPARK_PYTHON'] = python_exe
        os.environ['PYSPARK_DRIVER_PYTHON'] = python_exe

        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[1]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.pyspark.python", python_exe) \
            .config("spark.pyspark.driver.python", python_exe) \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.python.worker.reuse", "false") \
            .config("spark.default.parallelism", "1") \
            .config("spark.sql.shuffle.partitions", "1") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        self.models = {}
        self.scalers = {}

    def generate_training_data(self, num_samples=10000):
        """Generate comprehensive training data for ML models"""

        print(f"Generating {num_samples} training samples...")

        import random
        from datetime import datetime, timedelta

        # Districts with realistic coordinates (simplified)
        districts = {
            'Central': (1.2966, 103.8520),
            'Orchard': (1.3048, 103.8318),
            'Marina Bay': (1.2815, 103.8639),
            'Chinatown': (1.2833, 103.8449),
            'Jurong East': (1.3329, 103.7436),
            'Tampines': (1.3496, 103.9568),
            'Woodlands': (1.4382, 103.7890),
            'Changi Airport': (1.3644, 103.9915),
            'Sentosa': (1.2494, 103.8303)
        }

        taxi_types = ['Standard', 'Premier', 'Mini Cab', 'Maxi Cab', 'Limosine']
        membership_tiers = ['Standard', 'Silver', 'Gold']
        payment_methods = ['Cash', 'Card', 'Digital Wallet', 'Corporate']

        training_data = []

        for i in range(num_samples):
            # Random datetime in the past year
            base_date = datetime.now() - timedelta(days=365)
            random_date = base_date + timedelta(
                days=random.randint(0, 365),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )

            pickup_district = random.choice(list(districts.keys()))
            dropoff_district = random.choice(list(districts.keys()))
            while dropoff_district == pickup_district:
                dropoff_district = random.choice(list(districts.keys()))

            # Calculate realistic distance
            pickup_coord = districts[pickup_district]
            dropoff_coord = districts[dropoff_district]
            distance = self.calculate_distance(pickup_coord, dropoff_coord)
            distance += random.uniform(-2, 2)  # Add some noise
            distance = distance if distance >= 1.0 else 1.0  # Minimum distance

            hour = random_date.hour
            day_of_week = random_date.weekday()

            # Time-based surge calculation
            surge_multiplier = self.calculate_surge(hour, day_of_week, pickup_district)

            taxi_type = random.choice(taxi_types)
            membership = random.choice(membership_tiers)
            payment_method = random.choice(payment_methods)
            num_passengers = random.randint(1, 6)

            # Calculate fare with realistic formula
            fare = self.calculate_realistic_fare(
                distance, surge_multiplier, taxi_type, membership,
                hour, day_of_week, num_passengers
            )

            # Trip duration (minutes)
            speed = self.get_average_speed(hour, pickup_district, dropoff_district)
            duration = (distance / speed) * 60 + random.uniform(5, 15)  # Travel + pickup/dropoff time

            # Customer features
            customer_age = random.randint(18, 70)
            customer_gender = random.choice(['M', 'F'])
            historical_trips = random.randint(1, 500)
            historical_spent = random.uniform(100, 5000)

            # Cancellation probability (target for classification)
            cancellation_prob = self.calculate_cancellation_probability(
                surge_multiplier, fare, distance, hour, membership
            )
            is_cancelled = random.random() < cancellation_prob

            # Rating prediction (1-5 stars)
            rating = self.calculate_expected_rating(
                taxi_type, duration, fare, distance, membership
            )

            training_data.append({
                'trip_id': f"TRIP_{i:06d}",
                'datetime': random_date,
                'pickup_district': pickup_district,
                'dropoff_district': dropoff_district,
                'distance': float(f"{distance:.2f}"),
                'duration_minutes': float(f"{duration:.1f}"),
                'hour': hour,
                'day_of_week': day_of_week,
                'surge_multiplier': float(f"{surge_multiplier:.2f}"),
                'taxi_type': taxi_type,
                'membership': membership,
                'payment_method': payment_method,
                'num_passengers': num_passengers,
                'customer_age': customer_age,
                'customer_gender': customer_gender,
                'historical_trips': historical_trips,
                'historical_spent': float(f"{historical_spent:.2f}"),
                'fare': float(f"{fare:.2f}"),
                'is_cancelled': is_cancelled,
                'rating': float(f"{rating:.1f}")
            })

        # Convert to Spark DataFrame
        df = self.spark.createDataFrame(training_data)

        # Add derived features
        df = df.withColumn('is_weekend', when(col('day_of_week').isin([5, 6]), 1).otherwise(0)) \
            .withColumn('is_rush_hour',
                        when((col('hour').between(7, 9)) | (col('hour').between(17, 19)), 1).otherwise(0)) \
            .withColumn('is_night', when((col('hour') >= 22) | (col('hour') <= 6), 1).otherwise(0)) \
            .withColumn('fare_per_km', col('fare') / col('distance')) \
            .withColumn('is_airport_trip', when(
            (col('pickup_district') == 'Changi Airport') | (col('dropoff_district') == 'Changi Airport'), 1).otherwise(
            0))

        print("Training data generation completed")
        return df

    def calculate_distance(self, coord1, coord2):
        """Calculate approximate distance between two coordinates"""
        # Simplified distance calculation (Euclidean * conversion factor)
        lat_diff = coord1[0] - coord2[0]
        lon_diff = coord1[1] - coord2[1]
        distance = ((lat_diff ** 2 + lon_diff ** 2) ** 0.5) * 111  # Rough km conversion
        return distance if distance >= 1.0 else 1.0

    def calculate_surge(self, hour, day_of_week, district):
        """Calculate surge multiplier based on time and location"""
        base_surge = 1.0

        # Rush hour surge
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            base_surge = np.random.uniform(1.3, 2.5)
        # Night surge
        elif hour >= 22 or hour <= 6:
            base_surge = np.random.uniform(1.1, 1.8)
        # Weekend surge
        elif day_of_week in [4, 5]:  # Friday, Saturday
            base_surge = np.random.uniform(1.2, 2.0)
        else:
            base_surge = np.random.uniform(1.0, 1.4)

        # Location-based surge
        if district in ['Orchard', 'Marina Bay', 'Changi Airport']:
            base_surge *= np.random.uniform(1.1, 1.3)

        return base_surge

    def calculate_realistic_fare(self, distance, surge, taxi_type, membership, hour, day_of_week, passengers):
        """Calculate realistic fare with multiple factors"""
        base_fare = 3.5
        distance_rate = 2.2

        # Base calculation
        fare = base_fare + (distance * distance_rate)

        # Time surcharges
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            fare += 2.0  # Rush hour
        elif hour >= 22 or hour <= 6:
            fare += 1.5  # Night

        if day_of_week in [4, 5]:  # Weekend
            fare += 1.0

        # Taxi type multipliers
        type_multipliers = {
            'Standard': 1.0,
            'Premier': 1.5,
            'Mini Cab': 0.9,
            'Maxi Cab': 1.3,
            'Limosine': 2.0
        }
        fare *= type_multipliers.get(taxi_type, 1.0)

        # Surge
        fare *= surge

        # Membership discounts
        if membership == 'Gold':
            fare *= 0.85
        elif membership == 'Silver':
            fare *= 0.92

        # Group size
        if passengers > 4:
            fare += 2.0

        # Add some random noise
        fare += np.random.uniform(-2, 2)

        return fare if fare >= 3.5 else 3.5  # Minimum fare

    def get_average_speed(self, hour, pickup, dropoff):
        """Get average speed based on time and route"""
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            return np.random.uniform(12, 20)  # Rush hour
        elif hour >= 22 or hour <= 6:
            return np.random.uniform(35, 45)  # Night
        else:
            return np.random.uniform(25, 35)  # Regular

    def calculate_cancellation_probability(self, surge, fare, distance, hour, membership):
        """Calculate probability of trip cancellation"""
        prob = 0.05  # Base cancellation rate

        # High surge increases cancellation
        if surge > 2.0:
            prob += 0.15
        elif surge > 1.5:
            prob += 0.08

        # High fare increases cancellation
        if fare > 50:
            prob += 0.10
        elif fare > 30:
            prob += 0.05

        # Very short trips more likely to be cancelled
        if distance < 3:
            prob += 0.05

        # Membership reduces cancellation
        if membership == 'Gold':
            prob *= 0.5
        elif membership == 'Silver':
            prob *= 0.7

        return prob if prob <= 0.4 else 0.4  # Cap at 40%

    def calculate_expected_rating(self, taxi_type, duration, fare, distance, membership):
        """Calculate expected trip rating"""
        base_rating = 4.0

        # Taxi type affects rating
        type_ratings = {
            'Standard': 4.0,
            'Premier': 4.5,
            'Mini Cab': 3.8,
            'Maxi Cab': 4.2,
            'Limosine': 4.7
        }
        base_rating = type_ratings.get(taxi_type, 4.0)

        # Duration affects rating (longer than expected is bad)
        expected_duration = (distance / 25) * 60 + 10
        if duration > expected_duration * 1.3:
            base_rating -= 0.5
        elif duration < expected_duration * 0.8:
            base_rating += 0.2

        # High fare relative to distance
        fare_per_km = fare / distance
        if fare_per_km > 8:
            base_rating -= 0.3

        # Premium customers tend to rate higher
        if membership == 'Gold':
            base_rating += 0.2

        # Add noise
        base_rating += np.random.uniform(-0.5, 0.5)

        return np.clip(base_rating, 1.0, 5.0)

    def prepare_features(self, df, target_col=None):
        """Prepare features for ML models"""

        # Categorical columns to encode
        categorical_cols = ['pickup_district', 'dropoff_district', 'taxi_type', 'membership', 'payment_method',
                            'customer_gender']

        # Numerical columns
        numerical_cols = ['distance', 'hour', 'day_of_week', 'surge_multiplier', 'num_passengers',
                          'customer_age', 'historical_trips', 'historical_spent', 'is_weekend',
                          'is_rush_hour', 'is_night', 'is_airport_trip']

        # String indexers for categorical variables
        indexers = []
        for col_name in categorical_cols:
            indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_index", handleInvalid="keep")
            indexers.append(indexer)

        # One-hot encoders
        encoders = []
        for col_name in categorical_cols:
            encoder = OneHotEncoder(inputCol=f"{col_name}_index", outputCol=f"{col_name}_encoded")
            encoders.append(encoder)

        # Feature assembler
        feature_cols = numerical_cols + [f"{col}_encoded" for col in categorical_cols]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")

        # Scaler
        scaler = StandardScaler(inputCol="raw_features", outputCol="features")

        # Create pipeline
        stages = indexers + encoders + [assembler, scaler]

        feature_pipeline = Pipeline(stages=stages)

        return feature_pipeline, feature_cols

    def train_fare_prediction_model(self, df):
        """Train fare prediction models"""

        print("Training fare prediction models...")

        # Prepare features
        feature_pipeline, feature_cols = self.prepare_features(df, "fare")

        # Split data
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

        # Fit feature pipeline
        feature_model = feature_pipeline.fit(train_df)
        train_features = feature_model.transform(train_df)
        test_features = feature_model.transform(test_df)

        # Train multiple models
        models = {}

        # 1. Linear Regression
        lr = LinearRegression(featuresCol="features", labelCol="fare", predictionCol="lr_prediction")
        lr_model = lr.fit(train_features)
        models['linear_regression'] = lr_model

        # 2. Random Forest
        rf = RandomForestRegressor(featuresCol="features", labelCol="fare", predictionCol="rf_prediction", numTrees=50)
        rf_model = rf.fit(train_features)
        models['random_forest'] = rf_model

        # 3. Gradient Boosted Trees
        gbt = GBTRegressor(featuresCol="features", labelCol="fare", predictionCol="gbt_prediction", maxIter=20)
        gbt_model = gbt.fit(train_features)
        models['gradient_boosting'] = gbt_model

        # Evaluate models
        evaluator = RegressionEvaluator(labelCol="fare", metricName="rmse")

        print("\nFare Prediction Model Performance:")
        for name, model in models.items():
            predictions = model.transform(test_features)
            pred_col = f"{name.split('_')[0]}_prediction" if '_' in name else f"{name}_prediction"

            if name == 'linear_regression':
                pred_col = 'lr_prediction'
            elif name == 'random_forest':
                pred_col = 'rf_prediction'
            elif name == 'gradient_boosting':
                pred_col = 'gbt_prediction'

            rmse = evaluator.setPredictionCol(pred_col).evaluate(predictions)
            mae = RegressionEvaluator(labelCol="fare", predictionCol=pred_col, metricName="mae").evaluate(predictions)
            r2 = RegressionEvaluator(labelCol="fare", predictionCol=pred_col, metricName="r2").evaluate(predictions)

            print(f"{name.upper()}:")
            print(f"  RMSE: {rmse:.3f}")
            print(f"  MAE:  {mae:.3f}")
            print(f"  R²:   {r2:.3f}")

        # Save best model (Random Forest typically performs well)
        best_model = models['random_forest']

        # Create complete pipeline
        complete_pipeline = Pipeline(stages=feature_pipeline.getStages() + [best_model])
        complete_model = complete_pipeline.fit(train_df)

        # Save model
        model_path = "models/fare_prediction_model"
        complete_model.write().overwrite().save(model_path)
        print(f"Fare prediction model saved to {model_path}")

        self.models['fare_prediction'] = complete_model

        return complete_model, test_features

    def train_cancellation_prediction_model(self, df):
        """Train trip cancellation prediction model"""

        print("Training cancellation prediction model...")

        # Prepare features
        feature_pipeline, feature_cols = self.prepare_features(df, "is_cancelled")

        # Split data
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

        # Fit feature pipeline
        feature_model = feature_pipeline.fit(train_df)
        train_features = feature_model.transform(train_df)
        test_features = feature_model.transform(test_df)

        # Train models
        # Logistic Regression
        lr_classifier = MLLogisticRegression(featuresCol="features", labelCol="is_cancelled")
        lr_model = lr_classifier.fit(train_features)

        # Random Forest Classifier
        rf_classifier = RandomForestClassifier(featuresCol="features", labelCol="is_cancelled", numTrees=30)
        rf_model = rf_classifier.fit(train_features)

        # Evaluate models
        evaluator = BinaryClassificationEvaluator(labelCol="is_cancelled", metricName="areaUnderROC")

        lr_predictions = lr_model.transform(test_features)
        rf_predictions = rf_model.transform(test_features)

        lr_auc = evaluator.evaluate(lr_predictions)
        rf_auc = evaluator.evaluate(rf_predictions)

        print(f"\nCancellation Prediction Performance:")
        print(f"Logistic Regression AUC: {lr_auc:.3f}")
        print(f"Random Forest AUC: {rf_auc:.3f}")

        # Use best model
        best_model = rf_model if rf_auc > lr_auc else lr_model
        model_name = "Random Forest" if rf_auc > lr_auc else "Logistic Regression"
        print(f"Best model: {model_name}")

        # Create complete pipeline
        complete_pipeline = Pipeline(stages=feature_pipeline.getStages() + [best_model])
        complete_model = complete_pipeline.fit(train_df)

        # Save model
        model_path = "models/cancellation_prediction_model"
        complete_model.write().overwrite().save(model_path)
        print(f"Cancellation prediction model saved to {model_path}")

        self.models['cancellation_prediction'] = complete_model

        return complete_model, test_features

    def train_customer_segmentation_model(self, df):
        """Train customer segmentation using K-means clustering"""

        print("Training customer segmentation model...")

        # Prepare customer features
        customer_features = ['customer_age', 'historical_trips', 'historical_spent', 'fare_per_km']

        # Aggregate customer data
        customer_agg = df.groupBy('customer_age', 'customer_gender', 'membership').agg(
            count('*').alias('total_trips'),
            avg('fare').alias('avg_fare'),
            sum('fare').alias('total_spent'),
            avg('distance').alias('avg_distance'),
            avg('surge_multiplier').alias('avg_surge'),
            sum(when(col('is_cancelled'), 1).otherwise(0)).alias('cancellations'),
            avg('rating').alias('avg_rating')
        ).withColumn('cancellation_rate', col('cancellations') / col('total_trips'))

        # Prepare features for clustering
        feature_cols = ['customer_age', 'total_trips', 'total_spent', 'avg_fare', 'avg_distance', 'avg_surge',
                        'cancellation_rate', 'avg_rating']
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
        scaler = StandardScaler(inputCol="raw_features", outputCol="features")

        # Prepare data
        feature_data = assembler.transform(customer_agg)
        scaler_model = scaler.fit(feature_data)
        scaled_data = scaler_model.transform(feature_data)

        # Train K-means with different k values
        best_k = 4
        kmeans = KMeans(featuresCol="features", predictionCol="segment", k=best_k, seed=42)
        kmeans_model = kmeans.fit(scaled_data)

        # Get predictions
        segmented_customers = kmeans_model.transform(scaled_data)

        print(f"\nCustomer Segmentation Results (K={best_k}):")
        segment_summary = segmented_customers.groupBy('segment').agg(
            count('*').alias('count'),
            avg('customer_age').alias('avg_age'),
            avg('total_trips').alias('avg_trips'),
            avg('total_spent').alias('avg_spent'),
            avg('avg_rating').alias('avg_rating')
        ).orderBy('segment')

        segment_summary.show()

        # Save model
        clustering_pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        clustering_model = clustering_pipeline.fit(customer_agg)

        model_path = "models/customer_segmentation_model"
        clustering_model.write().overwrite().save(model_path)
        print(f"Customer segmentation model saved to {model_path}")

        self.models['customer_segmentation'] = clustering_model

        return clustering_model, segmented_customers

    def train_demand_forecasting_model(self, df):
        """Train demand forecasting model"""

        print("Training demand forecasting model...")

        # Aggregate demand by hour and district
        demand_data = df.groupBy('hour', 'day_of_week', 'pickup_district').agg(
            count('*').alias('demand'),
            avg('surge_multiplier').alias('avg_surge'),
            sum(when(col('is_cancelled'), 1).otherwise(0)).alias('cancellations')
        ).withColumn('cancellation_rate', col('cancellations') / col('demand'))

        # Add time-based features
        demand_data = demand_data.withColumn('is_weekend', when(col('day_of_week').isin([5, 6]), 1).otherwise(0)) \
            .withColumn('is_rush_hour',
                        when((col('hour').between(7, 9)) | (col('hour').between(17, 19)), 1).otherwise(0)) \
            .withColumn('is_night', when((col('hour') >= 22) | (col('hour') <= 6), 1).otherwise(0))

        # Prepare features
        feature_cols = ['hour', 'day_of_week', 'is_weekend', 'is_rush_hour', 'is_night', 'avg_surge',
                        'cancellation_rate']

        # String indexer for district
        district_indexer = StringIndexer(inputCol='pickup_district', outputCol='district_index')
        district_encoder = OneHotEncoder(inputCol='district_index', outputCol='district_encoded')

        # Feature assembler
        assembler = VectorAssembler(inputCols=feature_cols + ['district_encoded'], outputCol='features')

        # Model
        rf_demand = RandomForestRegressor(featuresCol="features", labelCol="demand", numTrees=50)

        # Pipeline
        demand_pipeline = Pipeline(stages=[district_indexer, district_encoder, assembler, rf_demand])

        # Split and train
        train_demand, test_demand = demand_data.randomSplit([0.8, 0.2], seed=42)
        demand_model = demand_pipeline.fit(train_demand)

        # Evaluate
        demand_predictions = demand_model.transform(test_demand)
        demand_evaluator = RegressionEvaluator(labelCol="demand", predictionCol="prediction", metricName="rmse")
        demand_rmse = demand_evaluator.evaluate(demand_predictions)

        print(f"Demand Forecasting RMSE: {demand_rmse:.3f}")

        # Save model
        model_path = "models/demand_forecasting_model"
        demand_model.write().overwrite().save(model_path)
        print(f"Demand forecasting model saved to {model_path}")

        self.models['demand_forecasting'] = demand_model

        return demand_model, demand_predictions

    def hyperparameter_tuning(self, df, model_type='fare_prediction'):
        """Perform hyperparameter tuning using cross-validation"""

        print(f"Performing hyperparameter tuning for {model_type}...")

        if model_type == 'fare_prediction':
            # Prepare features
            feature_pipeline, _ = self.prepare_features(df, "fare")
            train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
            feature_model = feature_pipeline.fit(train_df)
            train_features = feature_model.transform(train_df)

            # Random Forest with parameter grid
            rf = RandomForestRegressor(featuresCol="features", labelCol="fare")

            # Parameter grid
            paramGrid = ParamGridBuilder() \
                .addGrid(rf.numTrees, [20, 50, 100]) \
                .addGrid(rf.maxDepth, [5, 10, 15]) \
                .addGrid(rf.minInstancesPerNode, [1, 5, 10]) \
                .build()

            # Cross validator
            evaluator = RegressionEvaluator(labelCol="fare", predictionCol="prediction", metricName="rmse")
            crossval = CrossValidator(estimator=rf,
                                      estimatorParamMaps=paramGrid,
                                      evaluator=evaluator,
                                      numFolds=3)

            # Fit cross validator
            cv_model = crossval.fit(train_features)

            # Get best model
            best_model = cv_model.bestModel

            print(f"Best parameters:")
            print(f"  numTrees: {best_model.getNumTrees}")
            print(f"  maxDepth: {best_model.getMaxDepth}")
            print(f"  minInstancesPerNode: {best_model.getMinInstancesPerNode}")

            return best_model

        return None

    def load_models(self, model_dir="models"):
        """Load pre-trained models"""

        print("Loading pre-trained models...")

        model_types = ['fare_prediction', 'cancellation_prediction', 'customer_segmentation', 'demand_forecasting']

        for model_type in model_types:
            model_path = f"{model_dir}/{model_type}_model"
            try:
                if os.path.exists(model_path):
                    model = PipelineModel.load(model_path)
                    self.models[model_type] = model
                    print(f"Loaded {model_type} model")
                else:
                    print(f"Model not found: {model_path}")
            except Exception as e:
                print(f"Error loading {model_type} model: {e}")

    def predict_fare(self, booking_data):
        """Predict fare for new booking"""

        if 'fare_prediction' not in self.models:
            print("Fare prediction model not loaded")
            return None

        model = self.models['fare_prediction']
        prediction = model.transform(booking_data)
        return prediction.select('prediction').collect()[0]['prediction']

    def predict_cancellation_probability(self, booking_data):
        """Predict cancellation probability for booking"""

        if 'cancellation_prediction' not in self.models:
            print("Cancellation prediction model not loaded")
            return None

        model = self.models['cancellation_prediction']
        prediction = model.transform(booking_data)
        return prediction.select('probability').collect()[0]['probability'][1]  # Probability of cancellation

    def batch_predictions(self, df):
        """Run batch predictions on a dataset"""

        print("Running batch predictions...")

        results = df

        # Fare prediction
        if 'fare_prediction' in self.models:
            fare_model = self.models['fare_prediction']
            results = fare_model.transform(results)
            results = results.withColumnRenamed('prediction', 'predicted_fare')

        # Cancellation prediction
        if 'cancellation_prediction' in self.models:
            cancel_model = self.models['cancellation_prediction']
            cancel_predictions = cancel_model.transform(results)
            # Extract probability of cancellation (class 1)
            results = cancel_predictions.withColumn('cancellation_probability',
                                                    col('probability').getItem(1))

        return results

    def model_performance_report(self, test_df):
        """Generate comprehensive model performance report"""

        print("\n" + "=" * 50)
        print("MODEL PERFORMANCE REPORT")
        print("=" * 50)

        # Run predictions
        predictions = self.batch_predictions(test_df)

        if 'predicted_fare' in predictions.columns:
            print("\nFARE PREDICTION METRICS:")
            fare_evaluator = RegressionEvaluator(labelCol="fare", predictionCol="predicted_fare")

            rmse = fare_evaluator.setMetricName("rmse").evaluate(predictions)
            mae = fare_evaluator.setMetricName("mae").evaluate(predictions)
            r2 = fare_evaluator.setMetricName("r2").evaluate(predictions)

            print(f"  RMSE: ${rmse:.2f}")
            print(f"  MAE:  ${mae:.2f}")
            print(f"  R²:   {r2:.3f}")

            # Fare distribution
            print("\nFARE PREDICTION DISTRIBUTION:")
            predictions.select('fare', 'predicted_fare').describe().show()

        if 'cancellation_probability' in predictions.columns:
            print("\nCANCELLATION PREDICTION METRICS:")
            cancel_evaluator = BinaryClassificationEvaluator(labelCol="is_cancelled",
                                                             rawPredictionCol="rawPrediction")
            auc = cancel_evaluator.evaluate(predictions)
            print(f"  AUC-ROC: {auc:.3f}")

            # Confusion matrix data
            predictions.groupBy('is_cancelled').agg(
                avg('cancellation_probability').alias('avg_prob'),
                count('*').alias('count')
            ).show()

        print("=" * 50)


def main():
    """Main function for ML model training and evaluation"""

    parser = argparse.ArgumentParser(description="Taxi ML Model Manager")
    parser.add_argument("--action", choices=['train', 'evaluate', 'tune', 'predict'],
                        default='train', help="Action to perform")
    parser.add_argument("--model", choices=['fare', 'cancellation', 'segmentation', 'demand', 'all'],
                        default='all', help="Model to train")
    parser.add_argument("--samples", type=int, default=10000, help="Number of training samples")
    parser.add_argument("--load-data", help="Path to existing training data CSV")
    parser.add_argument("--model-dir", default="models", help="Directory to save/load models")
    parser.add_argument("--use-pandas", action="store_true",
                        help="Use pandas instead of Spark for Windows compatibility")

    args = parser.parse_args()

    try:
        # Create models directory
        os.makedirs(args.model_dir, exist_ok=True)

        if args.use_pandas:
            # Alternative implementation using pandas for Windows compatibility
            print("Using pandas-based implementation for Windows compatibility...")
            run_pandas_ml_training(args)
        else:
            # Original Spark implementation
            ml_manager = TaxiMLModelManager()

            if args.action == 'train':
                # Generate or load training data
                if args.load_data:
                    print(f"Loading training data from {args.load_data}")
                    df = ml_manager.spark.read.option("header", "true").option("inferSchema", "true").csv(
                        args.load_data)
                else:
                    df = ml_manager.generate_training_data(args.samples)

                print(f"\nTraining data shape: {df.count()} rows, {len(df.columns)} columns")
                df.printSchema()

                # Train models based on selection
                if args.model in ['fare', 'all']:
                    ml_manager.train_fare_prediction_model(df)

                if args.model in ['cancellation', 'all']:
                    ml_manager.train_cancellation_prediction_model(df)

                if args.model in ['segmentation', 'all']:
                    ml_manager.train_customer_segmentation_model(df)

                if args.model in ['demand', 'all']:
                    ml_manager.train_demand_forecasting_model(df)

                print(f"\nModel training completed. Models saved to {args.model_dir}/")

            elif args.action == 'evaluate':
                # Load models and evaluate
                ml_manager.load_models(args.model_dir)

                # Generate test data
                test_df = ml_manager.generate_training_data(2000)
                ml_manager.model_performance_report(test_df)

            elif args.action == 'tune':
                # Hyperparameter tuning
                df = ml_manager.generate_training_data(5000)  # Smaller dataset for tuning
                best_model = ml_manager.hyperparameter_tuning(df, 'fare_prediction')

            elif args.action == 'predict':
                # Load models and make sample predictions
                ml_manager.load_models(args.model_dir)

                # Create sample booking data
                sample_data = ml_manager.generate_training_data(10)
                predictions = ml_manager.batch_predictions(sample_data)

                print("\nSample Predictions:")
                predictions.select('pickup_district', 'dropoff_district', 'distance',
                                   'surge_multiplier', 'fare', 'predicted_fare',
                                   'cancellation_probability').show(truncate=False)

            ml_manager.spark.stop()

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


def run_pandas_ml_training(args):
    """Alternative pandas-based ML training for Windows compatibility"""
    import pandas as pd
    import numpy as np
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
    from sklearn.linear_model import LinearRegression, LogisticRegression
    from sklearn.preprocessing import StandardScaler, LabelEncoder
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score, accuracy_score, roc_auc_score
    import joblib

    print("Generating training data with pandas...")

    # Generate simple training data
    np.random.seed(42)
    n_samples = args.samples

    data = {
        'distance': np.random.uniform(1, 30, n_samples),
        'surge_multiplier': np.random.uniform(1.0, 3.0, n_samples),
        'taxi_type': np.random.choice(['Standard', 'Premier', 'Mini Cab', 'Maxi Cab'], n_samples),
        'membership': np.random.choice(['Standard', 'Silver', 'Gold'], n_samples),
        'hour': np.random.randint(0, 24, n_samples),
        'day_of_week': np.random.randint(0, 7, n_samples),
        'num_passengers': np.random.randint(1, 6, n_samples)
    }

    df = pd.DataFrame(data)

    # Calculate fare
    base_fare = 3.5
    df['fare'] = (base_fare + df['distance'] * 2.2) * df['surge_multiplier']

    # Add noise
    df['fare'] += np.random.normal(0, 2, n_samples)
    df['fare'] = np.maximum(df['fare'], 3.5)  # Minimum fare

    # Calculate cancellation probability
    df['is_cancelled'] = (df['surge_multiplier'] > 2.0) & (np.random.random(n_samples) < 0.2)

    print(f"Generated {len(df)} training samples")
    print(df.head())
    print(f"\nDataset info:")
    print(df.info())

    # Encode categorical variables
    le_taxi = LabelEncoder()
    le_membership = LabelEncoder()

    df['taxi_type_encoded'] = le_taxi.fit_transform(df['taxi_type'])
    df['membership_encoded'] = le_membership.fit_transform(df['membership'])

    # Features for ML
    feature_cols = ['distance', 'surge_multiplier', 'taxi_type_encoded', 'membership_encoded',
                    'hour', 'day_of_week', 'num_passengers']
    X = df[feature_cols]

    # Train fare prediction model
    if args.model in ['fare', 'all']:
        print("\nTraining fare prediction model...")
        y_fare = df['fare']
        X_train, X_test, y_train, y_test = train_test_split(X, y_fare, test_size=0.2, random_state=42)

        # Random Forest for fare prediction
        rf_fare = RandomForestRegressor(n_estimators=100, random_state=42)
        rf_fare.fit(X_train, y_train)

        # Evaluate
        y_pred = rf_fare.predict(X_test)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        print(f"Fare Prediction Results:")
        print(f"  RMSE: ${rmse:.2f}")
        print(f"  MAE:  ${mae:.2f}")
        print(f"  R²:   {r2:.3f}")

        # Save model
        joblib.dump(rf_fare, f"{args.model_dir}/fare_model.pkl")
        joblib.dump(le_taxi, f"{args.model_dir}/taxi_encoder.pkl")
        joblib.dump(le_membership, f"{args.model_dir}/membership_encoder.pkl")
        print(f"Fare model saved to {args.model_dir}/")

    # Train cancellation prediction model
    if args.model in ['cancellation', 'all']:
        print("\nTraining cancellation prediction model...")
        y_cancel = df['is_cancelled'].astype(int)
        X_train, X_test, y_train, y_test = train_test_split(X, y_cancel, test_size=0.2, random_state=42)

        # Random Forest for cancellation prediction
        rf_cancel = RandomForestClassifier(n_estimators=100, random_state=42)
        rf_cancel.fit(X_train, y_train)

        # Evaluate
        y_pred = rf_cancel.predict(X_test)
        y_pred_proba = rf_cancel.predict_proba(X_test)[:, 1]

        accuracy = accuracy_score(y_test, y_pred)
        auc = roc_auc_score(y_test, y_pred_proba)

        print(f"Cancellation Prediction Results:")
        print(f"  Accuracy: {accuracy:.3f}")
        print(f"  AUC-ROC:  {auc:.3f}")

        # Save model
        joblib.dump(rf_cancel, f"{args.model_dir}/cancellation_model.pkl")
        print(f"Cancellation model saved to {args.model_dir}/")

    print("\nPandas-based model training completed successfully!")
    print(f"To use models: --use-pandas flag for compatibility")
    print(f"Models saved in: {args.model_dir}/")


if __name__ == "__main__":
    main()

# ==================== USAGE EXAMPLES ====================
"""
USAGE EXAMPLES:

1. Train all models:
   python ml_models.py --action train --model all --samples 20000

2. Train specific model:
   python ml_models.py --action train --model fare --samples 15000

3. Evaluate trained models:
   python ml_models.py --action evaluate --model-dir models

4. Hyperparameter tuning:
   python ml_models.py --action tune --model fare

5. Make predictions:
   python ml_models.py --action predict --model-dir models

6. Use existing data:
   python ml_models.py --action train --load-data historical_trips.csv

FEATURES:
- Comprehensive fare prediction with multiple factors
- Cancellation probability prediction
- Customer segmentation using K-means clustering
- Demand forecasting by location and time
- Hyperparameter tuning with cross-validation
- Model performance evaluation and reporting
- Batch prediction capabilities
- Model persistence and loading

MODELS INCLUDED:
1. Fare Prediction: Linear Regression, Random Forest, Gradient Boosting
2. Cancellation Prediction: Logistic Regression, Random Forest Classifier
3. Customer Segmentation: K-means clustering
4. Demand Forecasting: Random Forest for demand prediction

OUTPUT:
- Trained models saved to models/ directory
- Performance metrics (RMSE, MAE, R², AUC-ROC)
- Model comparison reports
- Customer segment analysis
- Sample predictions and probabilities
"""