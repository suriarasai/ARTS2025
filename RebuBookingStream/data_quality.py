# data_quality.py - Data Quality and Validation Framework
import argparse
import os
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


class TaxiDataQualityChecker:
    """
    Comprehensive data quality checking framework for taxi booking system
    Validates data integrity, completeness, consistency, and business rules
    """

    def __init__(self, app_name="TaxiDataQuality"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

        # Data quality rules and thresholds
        self.quality_rules = {
            'completeness_threshold': 0.95,  # 95% non-null values required
            'uniqueness_threshold': 0.99,  # 99% unique values for ID fields
            'fare_min': 3.50,  # Minimum fare
            'fare_max': 200.00,  # Maximum reasonable fare
            'distance_min': 0.1,  # Minimum distance (km)
            'distance_max': 50.0,  # Maximum distance (km)
            'rating_min': 1.0,  # Minimum rating
            'rating_max': 5.0,  # Maximum rating
            'surge_min': 1.0,  # Minimum surge multiplier
            'surge_max': 5.0,  # Maximum surge multiplier
            'passenger_capacity_max': 10  # Maximum passenger capacity
        }

        self.quality_report = {
            'timestamp': datetime.now().isoformat(),
            'datasets': {},
            'summary': {
                'total_checks': 0,
                'passed_checks': 0,
                'failed_checks': 0,
                'critical_issues': 0,
                'warnings': 0
            }
        }

    def load_datasets(self, file_paths=None):
        """Load all taxi datasets"""

        print("Loading datasets for quality checks...")

        datasets = {}

        # Default file paths
        if file_paths is None:
            file_paths = {
                'taxis': './data/RebuTaxiCabs.json',
                'drivers': './data/RebuDrivers.csv',
                'passengers': './data/RebuPassengers.csv',
                'trips': './data/RebuTripData.csv'
            }

        # Load Taxis
        try:
            if os.path.exists(file_paths['taxis']):
                datasets['taxis'] = self.spark.read.option("multiline", "true").json(file_paths['taxis'])
                print(f"Loaded taxis: {datasets['taxis'].count()} records")
            else:
                print(f"Warning: Taxi file not found: {file_paths['taxis']}")
        except Exception as e:
            print(f"Error loading taxis: {e}")

        # Load Drivers
        try:
            if os.path.exists(file_paths['drivers']):
                datasets['drivers'] = self.spark.read.option("header", "true").option("inferSchema", "true").csv(
                    file_paths['drivers'])
                print(f"Loaded drivers: {datasets['drivers'].count()} records")
            else:
                print(f"Warning: Drivers file not found: {file_paths['drivers']}")
        except Exception as e:
            print(f"Error loading drivers: {e}")

        # Load Passengers
        try:
            if os.path.exists(file_paths['passengers']):
                datasets['passengers'] = self.spark.read.option("header", "true").option("inferSchema", "true").csv(
                    file_paths['passengers'])
                print(f"Loaded passengers: {datasets['passengers'].count()} records")
            else:
                print(f"Warning: Passengers file not found: {file_paths['passengers']}")
        except Exception as e:
            print(f"Error loading passengers: {e}")

        # Load Trips
        try:
            if os.path.exists(file_paths['trips']):
                datasets['trips'] = self.spark.read.option("header", "true").option("inferSchema", "true").csv(
                    file_paths['trips'])
                print(f"Loaded trips: {datasets['trips'].count()} records")
            else:
                print(f"Warning: Trips file not found: {file_paths['trips']}")
        except Exception as e:
            print(f"Error loading trips: {e}")

        return datasets

    def check_completeness(self, df, dataset_name, required_columns=None):
        """Check data completeness (null values)"""

        print(f"\n=== COMPLETENESS CHECK: {dataset_name.upper()} ===")

        total_rows = df.count()
        completeness_results = {}

        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            completeness_rate = (total_rows - null_count) / total_rows

            completeness_results[column] = {
                'null_count': null_count,
                'completeness_rate': completeness_rate,
                'status': 'PASS' if completeness_rate >= self.quality_rules['completeness_threshold'] else 'FAIL'
            }

            status_symbol = "✓" if completeness_rate >= self.quality_rules['completeness_threshold'] else "✗"
            print(f"{status_symbol} {column}: {completeness_rate:.1%} complete ({null_count} nulls)")

            # Check if it's a required column
            if required_columns and column in required_columns and completeness_rate < 1.0:
                self.quality_report['summary']['critical_issues'] += 1
                print(f"  CRITICAL: Required column {column} has null values!")

        # Store results
        self.quality_report['datasets'][dataset_name] = self.quality_report['datasets'].get(dataset_name, {})
        self.quality_report['datasets'][dataset_name]['completeness'] = completeness_results

        return completeness_results

    def check_uniqueness(self, df, dataset_name, unique_columns):
        """Check uniqueness constraints"""

        print(f"\n=== UNIQUENESS CHECK: {dataset_name.upper()} ===")

        total_rows = df.count()
        uniqueness_results = {}

        for column in unique_columns:
            if column in df.columns:
                distinct_count = df.select(column).distinct().count()
                uniqueness_rate = distinct_count / total_rows

                uniqueness_results[column] = {
                    'distinct_count': distinct_count,
                    'total_count': total_rows,
                    'uniqueness_rate': uniqueness_rate,
                    'duplicates': total_rows - distinct_count,
                    'status': 'PASS' if uniqueness_rate >= self.quality_rules['uniqueness_threshold'] else 'FAIL'
                }

                status_symbol = "✓" if uniqueness_rate >= self.quality_rules['uniqueness_threshold'] else "✗"
                print(
                    f"{status_symbol} {column}: {uniqueness_rate:.1%} unique ({total_rows - distinct_count} duplicates)")

                if uniqueness_rate < self.quality_rules['uniqueness_threshold']:
                    self.quality_report['summary']['critical_issues'] += 1

        # Store results
        if dataset_name not in self.quality_report['datasets']:
            self.quality_report['datasets'][dataset_name] = {}
        self.quality_report['datasets'][dataset_name]['uniqueness'] = uniqueness_results

        return uniqueness_results

    def check_data_ranges(self, df, dataset_name, range_checks):
        """Check if data values are within expected ranges"""

        print(f"\n=== RANGE CHECK: {dataset_name.upper()} ===")

        range_results = {}

        for column, (min_val, max_val) in range_checks.items():
            if column in df.columns:
                out_of_range = df.filter((col(column) < min_val) | (col(column) > max_val)).count()
                total_non_null = df.filter(col(column).isNotNull()).count()

                if total_non_null > 0:
                    validity_rate = (total_non_null - out_of_range) / total_non_null

                    range_results[column] = {
                        'min_expected': min_val,
                        'max_expected': max_val,
                        'out_of_range_count': out_of_range,
                        'validity_rate': validity_rate,
                        'status': 'PASS' if out_of_range == 0 else 'FAIL'
                    }

                    status_symbol = "✓" if out_of_range == 0 else "✗"
                    print(
                        f"{status_symbol} {column}: {validity_rate:.1%} within range [{min_val}, {max_val}] ({out_of_range} violations)")

                    if out_of_range > 0:
                        # Show examples of out-of-range values
                        examples = df.filter((col(column) < min_val) | (col(column) > max_val)) \
                            .select(column).distinct().limit(5).collect()
                        example_values = [row[column] for row in examples]
                        print(f"  Examples: {example_values}")

                        if out_of_range > total_non_null * 0.1:  # More than 10% out of range
                            self.quality_report['summary']['critical_issues'] += 1
                        else:
                            self.quality_report['summary']['warnings'] += 1

        # Store results
        if dataset_name not in self.quality_report['datasets']:
            self.quality_report['datasets'][dataset_name] = {}
        self.quality_report['datasets'][dataset_name]['ranges'] = range_results

        return range_results

    def check_business_rules(self, datasets):
        """Check business-specific validation rules"""

        print(f"\n=== BUSINESS RULES CHECK ===")

        business_results = {}

        # Rule 1: Driver ratings should be between 1 and 5
        if 'drivers' in datasets:
            drivers_df = datasets['drivers']
            invalid_ratings = drivers_df.filter((col('Rating') < 1) | (col('Rating') > 5)).count()

            business_results['driver_ratings'] = {
                'rule': 'Driver ratings must be between 1 and 5',
                'violations': invalid_ratings,
                'status': 'PASS' if invalid_ratings == 0 else 'FAIL'
            }

            status_symbol = "✓" if invalid_ratings == 0 else "✗"
            print(f"{status_symbol} Driver ratings in valid range: {invalid_ratings} violations")

        # Rule 2: Taxi capacity should match taxi type
        if 'taxis' in datasets:
            taxis_df = datasets['taxis']

            # Expected capacities by type
            capacity_rules = {
                'Mini Cab': (3, 4),
                'Standard': (4, 4),
                'Premier': (4, 5),
                'Maxi Cab': (6, 8),
                'Limosine': (4, 6)
            }

            capacity_violations = 0
            for taxi_type, (min_cap, max_cap) in capacity_rules.items():
                violations = taxis_df.filter(
                    (col('TaxiType') == taxi_type) &
                    ((col('TaxiPassengerCapacity') < min_cap) | (col('TaxiPassengerCapacity') > max_cap))
                ).count()
                capacity_violations += violations

            business_results['taxi_capacity'] = {
                'rule': 'Taxi capacity must match taxi type',
                'violations': capacity_violations,
                'status': 'PASS' if capacity_violations == 0 else 'FAIL'
            }

            status_symbol = "✓" if capacity_violations == 0 else "✗"
            print(f"{status_symbol} Taxi capacity matches type: {capacity_violations} violations")

        # Rule 3: Trip fare should be reasonable for distance
        if 'trips' in datasets:
            trips_df = datasets['trips']

            # Fare per km should be reasonable (between $1 and $15 per km)
            trips_with_fare_per_km = trips_df.withColumn('fare_per_km',
                                                         col('TripFare') / col('DistanceTravelled'))

            unreasonable_fares = trips_with_fare_per_km.filter(
                (col('fare_per_km') < 1.0) | (col('fare_per_km') > 15.0)
            ).count()

            business_results['fare_reasonableness'] = {
                'rule': 'Fare per km should be between $1-$15',
                'violations': unreasonable_fares,
                'status': 'PASS' if unreasonable_fares == 0 else 'FAIL'
            }

            status_symbol = "✓" if unreasonable_fares == 0 else "✗"
            print(f"{status_symbol} Reasonable fare per km: {unreasonable_fares} violations")

        # Rule 4: Trip duration should be reasonable for distance
        if 'trips' in datasets:
            trips_df = datasets['trips']

            # Duration per km should be reasonable (between 1-10 minutes per km)
            trips_with_duration_per_km = trips_df.withColumn('duration_per_km',
                                                             col('TripDurationInSeconds') / 60 / col(
                                                                 'DistanceTravelled'))

            unreasonable_durations = trips_with_duration_per_km.filter(
                (col('duration_per_km') < 1.0) | (col('duration_per_km') > 10.0)
            ).count()

            business_results['duration_reasonableness'] = {
                'rule': 'Duration per km should be between 1-10 minutes',
                'violations': unreasonable_durations,
                'status': 'PASS' if unreasonable_durations == 0 else 'FAIL'
            }

            status_symbol = "✓" if unreasonable_durations == 0 else "✗"
            print(f"{status_symbol} Reasonable duration per km: {unreasonable_durations} violations")

        # Rule 5: Passenger count should not exceed taxi capacity
        if 'trips' in datasets and 'taxis' in datasets:
            trips_df = datasets['trips']
            taxis_df = datasets['taxis']

            # Join trips with taxis to check capacity
            trips_with_capacity = trips_df.join(
                taxis_df.select('TaxiNumber', 'TaxiPassengerCapacity'),
                'TaxiNumber', 'left'
            )

            capacity_exceeded = trips_with_capacity.filter(
                col('NumberOfPassengers') > col('TaxiPassengerCapacity')
            ).count()

            business_results['passenger_capacity'] = {
                'rule': 'Passenger count must not exceed taxi capacity',
                'violations': capacity_exceeded,
                'status': 'PASS' if capacity_exceeded == 0 else 'FAIL'
            }

            status_symbol = "✓" if capacity_exceeded == 0 else "✗"
            print(f"{status_symbol} Passengers within capacity: {capacity_exceeded} violations")

        # Store results
        self.quality_report['business_rules'] = business_results

        return business_results

    def check_data_consistency(self, datasets):
        """Check data consistency across datasets"""

        print(f"\n=== CONSISTENCY CHECK ===")

        consistency_results = {}

        # Check 1: All trips should reference valid taxis
        if 'trips' in datasets and 'taxis' in datasets:
            trips_df = datasets['trips']
            taxis_df = datasets['taxis']

            valid_taxi_numbers = set([row['TaxiNumber'] for row in taxis_df.select('TaxiNumber').collect()])
            trip_taxi_numbers = set([row['TaxiNumber'] for row in trips_df.select('TaxiNumber').distinct().collect()])

            invalid_taxi_refs = len(trip_taxi_numbers - valid_taxi_numbers)

            consistency_results['taxi_references'] = {
                'rule': 'All trip taxi numbers must exist in taxi master data',
                'invalid_references': invalid_taxi_refs,
                'status': 'PASS' if invalid_taxi_refs == 0 else 'FAIL'
            }

            status_symbol = "✓" if invalid_taxi_refs == 0 else "✗"
            print(f"{status_symbol} Valid taxi references: {invalid_taxi_refs} invalid references")

            if invalid_taxi_refs > 0:
                invalid_numbers = trip_taxi_numbers - valid_taxi_numbers
                print(f"  Invalid taxi numbers: {list(invalid_numbers)[:10]}...")  # Show first 10

        # Check 2: All trips should reference valid passengers
        if 'trips' in datasets and 'passengers' in datasets:
            trips_df = datasets['trips']
            passengers_df = datasets['passengers']

            valid_passenger_ids = set([row['PassengerID'] for row in passengers_df.select('PassengerID').collect()])
            trip_passenger_ids = set(
                [row['PassengerID'] for row in trips_df.select('PassengerID').distinct().collect() if
                 row['PassengerID'] is not None])

            invalid_passenger_refs = len(trip_passenger_ids - valid_passenger_ids)

            consistency_results['passenger_references'] = {
                'rule': 'All trip passenger IDs must exist in passenger master data',
                'invalid_references': invalid_passenger_refs,
                'status': 'PASS' if invalid_passenger_refs == 0 else 'FAIL'
            }

            status_symbol = "✓" if invalid_passenger_refs == 0 else "✗"
            print(f"{status_symbol} Valid passenger references: {invalid_passenger_refs} invalid references")

        # Check 3: Driver-Taxi assignments should be consistent
        if 'drivers' in datasets and 'taxis' in datasets:
            drivers_df = datasets['drivers']
            taxis_df = datasets['taxis']

            # Check if all drivers are assigned to valid taxis
            valid_taxi_ids = set([row['TaxiID'] for row in taxis_df.select('TaxiID').collect()])
            driver_taxi_assignments = set(
                [row['TaxiIDDriving'] for row in drivers_df.select('TaxiIDDriving').collect() if
                 row['TaxiIDDriving'] is not None])

            invalid_assignments = len(driver_taxi_assignments - valid_taxi_ids)

            consistency_results['driver_taxi_assignments'] = {
                'rule': 'All driver taxi assignments must reference valid taxis',
                'invalid_assignments': invalid_assignments,
                'status': 'PASS' if invalid_assignments == 0 else 'FAIL'
            }

            status_symbol = "✓" if invalid_assignments == 0 else "✗"
            print(f"{status_symbol} Valid driver-taxi assignments: {invalid_assignments} invalid assignments")

        # Store results
        self.quality_report['consistency'] = consistency_results

        return consistency_results

    def check_data_distribution(self, df, dataset_name):
        """Check data distribution and identify outliers"""

        print(f"\n=== DISTRIBUTION CHECK: {dataset_name.upper()} ===")

        distribution_results = {}

        # Check numerical columns for outliers
        numerical_cols = [col_name for col_name, col_type in df.dtypes if
                          col_type in ['int', 'double', 'float', 'bigint']]

        for column in numerical_cols:
            if df.filter(col(column).isNotNull()).count() > 0:
                # Calculate quartiles and IQR
                quantiles = df.approxQuantile(column, [0.25, 0.5, 0.75], 0.01)
                if len(quantiles) == 3:
                    q1, median, q3 = quantiles
                    iqr = q3 - q1
                    lower_bound = q1 - 1.5 * iqr
                    upper_bound = q3 + 1.5 * iqr

                    # Count outliers
                    outliers = df.filter((col(column) < lower_bound) | (col(column) > upper_bound)).count()
                    total_records = df.filter(col(column).isNotNull()).count()
                    outlier_percentage = (outliers / total_records) * 100 if total_records > 0 else 0

                    distribution_results[column] = {
                        'q1': q1,
                        'median': median,
                        'q3': q3,
                        'iqr': iqr,
                        'outliers': outliers,
                        'outlier_percentage': outlier_percentage,
                        'status': 'PASS' if outlier_percentage < 5 else 'WARNING'  # Warning if >5% outliers
                    }

                    status_symbol = "✓" if outlier_percentage < 5 else "⚠"
                    print(f"{status_symbol} {column}: {outlier_percentage:.1f}% outliers (median: {median:.2f})")

                    if outlier_percentage > 10:
                        self.quality_report['summary']['warnings'] += 1

        # Store results
        if dataset_name not in self.quality_report['datasets']:
            self.quality_report['datasets'][dataset_name] = {}
        self.quality_report['datasets'][dataset_name]['distribution'] = distribution_results

        return distribution_results

    def generate_data_profiling_report(self, df, dataset_name):
        """Generate comprehensive data profiling report"""

        print(f"\n=== DATA PROFILING: {dataset_name.upper()} ===")

        total_rows = df.count()
        total_cols = len(df.columns)

        profiling_report = {
            'total_rows': total_rows,
            'total_columns': total_cols,
            'columns': {}
        }

        print(f"Dataset: {total_rows} rows, {total_cols} columns")
        print(f"Schema:")
        df.printSchema()

        # Analyze each column
        for column in df.columns:
            col_type = dict(df.dtypes)[column]
            null_count = df.filter(col(column).isNull()).count()
            distinct_count = df.select(column).distinct().count()

            column_profile = {
                'data_type': col_type,
                'null_count': null_count,
                'null_percentage': (null_count / total_rows) * 100,
                'distinct_count': distinct_count,
                'cardinality': distinct_count / total_rows if total_rows > 0 else 0
            }

            # Additional stats for numerical columns
            if col_type in ['int', 'double', 'float', 'bigint']:
                try:
                    stats = df.select(column).describe().collect()
                    stats_dict = {row['summary']: float(row[column]) for row in stats if row[column] is not None}
                    column_profile.update(stats_dict)
                except:
                    pass

            # Sample values for categorical columns
            elif col_type == 'string':
                try:
                    sample_values = df.select(column).distinct().limit(10).collect()
                    column_profile['sample_values'] = [row[column] for row in sample_values if row[column] is not None]
                except:
                    pass

            profiling_report['columns'][column] = column_profile

            print(
                f"  {column} ({col_type}): {null_count} nulls ({(null_count / total_rows) * 100:.1f}%), {distinct_count} distinct")

        # Store results
        if dataset_name not in self.quality_report['datasets']:
            self.quality_report['datasets'][dataset_name] = {}
        self.quality_report['datasets'][dataset_name]['profiling'] = profiling_report

        return profiling_report

    def run_comprehensive_quality_check(self, file_paths=None):
        """Run all data quality checks"""

        print("=" * 60)
        print("COMPREHENSIVE DATA QUALITY CHECK")
        print("=" * 60)

        # Load datasets
        datasets = self.load_datasets(file_paths)

        if not datasets:
            print("No datasets loaded. Exiting.")
            return

        # Define validation rules for each dataset
        validation_rules = {
            'taxis': {
                'required_columns': ['TaxiID', 'TaxiNumber', 'TaxiType'],
                'unique_columns': ['TaxiID', 'TaxiNumber'],
                'range_checks': {
                    'TaxiPassengerCapacity': (1, 10)
                }
            },
            'drivers': {
                'required_columns': ['DriverID', 'DriverName'],
                'unique_columns': ['DriverID'],
                'range_checks': {
                    'Rating': (1.0, 5.0)
                }
            },
            'passengers': {
                'required_columns': ['PassengerID', 'PassengerName'],
                'unique_columns': ['PassengerID'],
                'range_checks': {
                    'Age': (0, 120),
                    'AmountSpent': (0, 50000)
                }
            },
            'trips': {
                'required_columns': ['TaxiNumber', 'PassengerID'],
                'unique_columns': [],
                'range_checks': {
                    'DistanceTravelled': (0.1, 100),
                    'TripFare': (3.5, 500),
                    'TripDurationInSeconds': (60, 14400),  # 1 minute to 4 hours
                    'NumberOfPassengers': (1, 10)
                }
            }
        }

        # Run checks for each dataset
        for dataset_name, df in datasets.items():
            rules = validation_rules.get(dataset_name, {})

            # Data profiling
            self.generate_data_profiling_report(df, dataset_name)

            # Completeness check
            self.check_completeness(df, dataset_name, rules.get('required_columns'))

            # Uniqueness check
            if rules.get('unique_columns'):
                self.check_uniqueness(df, dataset_name, rules['unique_columns'])

            # Range checks
            if rules.get('range_checks'):
                self.check_data_ranges(df, dataset_name, rules['range_checks'])

            # Distribution check
            self.check_data_distribution(df, dataset_name)

        # Cross-dataset checks
        self.check_business_rules(datasets)
        self.check_data_consistency(datasets)

        # Generate summary
        self.generate_quality_summary()

        return self.quality_report

    def generate_quality_summary(self):
        """Generate overall quality summary"""

        print(f"\n" + "=" * 60)
        print("DATA QUALITY SUMMARY")
        print("=" * 60)

        # Count total checks and results
        total_checks = 0
        passed_checks = 0
        failed_checks = 0

        for dataset_name, dataset_results in self.quality_report['datasets'].items():
            for check_type, results in dataset_results.items():
                if check_type in ['completeness', 'uniqueness', 'ranges']:
                    for field, result in results.items():
                        total_checks += 1
                        if result.get('status') == 'PASS':
                            passed_checks += 1
                        else:
                            failed_checks += 1

        # Add business rules and consistency checks
        if 'business_rules' in self.quality_report:
            for rule, result in self.quality_report['business_rules'].items():
                total_checks += 1
                if result.get('status') == 'PASS':
                    passed_checks += 1
                else:
                    failed_checks += 1

        if 'consistency' in self.quality_report:
            for rule, result in self.quality_report['consistency'].items():
                total_checks += 1
                if result.get('status') == 'PASS':
                    passed_checks += 1
                else:
                    failed_checks += 1

        # Update summary
        self.quality_report['summary'].update({
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'failed_checks': failed_checks,
            'pass_rate': (passed_checks / total_checks) * 100 if total_checks > 0 else 0
        })

        summary = self.quality_report['summary']

        print(f"Total Checks: {summary['total_checks']}")
        print(f"Passed: {summary['passed_checks']} ({summary['pass_rate']:.1f}%)")
        print(f"Failed: {summary['failed_checks']}")
        print(f"Critical Issues: {summary['critical_issues']}")
        print(f"Warnings: {summary['warnings']}")

        # Overall assessment
        if summary['pass_rate'] >= 95:
            overall_status = "EXCELLENT"
        elif summary['pass_rate'] >= 90:
            overall_status = "GOOD"
        elif summary['pass_rate'] >= 80:
            overall_status = "ACCEPTABLE"
        else:
            overall_status = "POOR"

        print(f"\nOverall Data Quality: {overall_status}")

        # Recommendations
        print(f"\nRECOMMENDATIONS:")
        if summary['critical_issues'] > 0:
            print(f"- Address {summary['critical_issues']} critical issues immediately")
        if summary['failed_checks'] > 0:
            print(f"- Investigate and fix {summary['failed_checks']} failed quality checks")
        if summary['warnings'] > 0:
            print(f"- Review {summary['warnings']} warning conditions")
        if summary['pass_rate'] < 100:
            print(f"- Implement data validation rules to prevent future quality issues")

        print("=" * 60)

    def save_quality_report(self, output_path="data_quality_report.json"):
        """Save detailed quality report to JSON file"""

        try:
            with open(output_path, 'w') as f:
                json.dump(self.quality_report, f, indent=2, default=str)
            print(f"\nDetailed quality report saved to: {output_path}")
        except Exception as e:
            print(f"Error saving report: {e}")

    def get_quality_metrics_for_monitoring(self):
        """Get key quality metrics for real-time monitoring"""

        summary = self.quality_report['summary']

        metrics = {
            'data_quality_score': summary['pass_rate'],
            'critical_issues': summary['critical_issues'],
            'warnings': summary['warnings'],
            'total_checks': summary['total_checks'],
            'timestamp': self.quality_report['timestamp']
        }

        # Dataset-specific metrics
        for dataset_name, dataset_results in self.quality_report['datasets'].items():
            if 'profiling' in dataset_results:
                profiling = dataset_results['profiling']
                metrics[f'{dataset_name}_row_count'] = profiling['total_rows']
                metrics[f'{dataset_name}_column_count'] = profiling['total_columns']

        return metrics


def main():
    """Main function for running data quality checks"""

    parser = argparse.ArgumentParser(description="Taxi Data Quality Checker")
    parser.add_argument("--taxis", help="Path to taxis JSON file")
    parser.add_argument("--drivers", help="Path to drivers CSV file")
    parser.add_argument("--passengers", help="Path to passengers CSV file")
    parser.add_argument("--trips", help="Path to trips CSV file")
    parser.add_argument("--output", default="data_quality_report.json", help="Output report file")
    parser.add_argument("--monitoring", action="store_true", help="Output monitoring metrics only")

    args = parser.parse_args()

    # Prepare file paths
    file_paths = {}
    if args.taxis:
        file_paths['taxis'] = args.taxis
    if args.drivers:
        file_paths['drivers'] = args.drivers
    if args.passengers:
        file_paths['passengers'] = args.passengers
    if args.trips:
        file_paths['trips'] = args.trips

    # Use defaults if no paths specified
    if not file_paths:
        file_paths = None

    try:
        # Create quality checker
        quality_checker = TaxiDataQualityChecker()

        # Run comprehensive quality check
        quality_report = quality_checker.run_comprehensive_quality_check(file_paths)

        if args.monitoring:
            # Output monitoring metrics
            metrics = quality_checker.get_quality_metrics_for_monitoring()
            print("\nMONITORING METRICS:")
            for key, value in metrics.items():
                print(f"{key}: {value}")
        else:
            # Save detailed report
            quality_checker.save_quality_report(args.output)

    except Exception as e:
        print(f"Error running quality checks: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if 'quality_checker' in locals():
            quality_checker.spark.stop()


if __name__ == "__main__":
    main()

# ==================== USAGE EXAMPLES ====================
"""
USAGE EXAMPLES:

1. Run complete quality check with default files:
   python data_quality.py

2. Specify custom file paths:
   python data_quality.py --taxis my_taxis.json --drivers my_drivers.csv

3. Output monitoring metrics only:
   python data_quality.py --monitoring

4. Save report to custom location:
   python data_quality.py --output /reports/quality_report_20231218.json

5. Integration with monitoring systems:
   python data_quality.py --monitoring | grep "data_quality_score"

FEATURES:
- Comprehensive data profiling with schema analysis
- Completeness checking (null value detection)
- Uniqueness validation for key fields
- Range validation for numerical data
- Business rule validation (domain-specific checks)
- Cross-dataset consistency checking
- Statistical outlier detection
- Data distribution analysis
- Detailed reporting with JSON export
- Monitoring metrics for real-time systems

QUALITY CHECKS INCLUDED:
1. Data Completeness (missing values)
2. Data Uniqueness (duplicate detection)
3. Data Range Validation (min/max bounds)
4. Business Rule Compliance
5. Cross-Dataset Consistency
6. Data Distribution Analysis
7. Schema Validation
8. Statistical Outlier Detection

OUTPUT:
- Console summary with visual indicators
- Detailed JSON report for archival
- Monitoring metrics for dashboards
- Recommendations for data improvement
- Pass/fail status for each check
"""