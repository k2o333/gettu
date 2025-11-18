"""
Main Data Validation Orchestrator

Integrates all data validation components and provides a unified interface
for data quality checking and integrity validation.
"""
import polars as pl
from typing import Dict, List, Optional, Union
import logging
from pathlib import Path

from .price_anomaly_detector import PriceAnomalyDetector
from .logical_consistency_validator import LogicalConsistencyValidator
from .cross_dataset_integrity_checker import CrossDatasetIntegrityChecker
from .financial_revision_detector import FinancialRevisionDetector
from .data_quality_reporter import DataQualityReporter


class DataValidationOrchestrator:
    """Orchestrates the complete data validation and integrity checking process."""

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the data validation orchestrator.

        Args:
            config: Configuration dictionary for all validation components
        """
        self.config = config or {}

        # Initialize all validation components
        self.price_detector = PriceAnomalyDetector(
            self.config.get('price_anomaly', {})
        )
        self.consistency_validator = LogicalConsistencyValidator(
            self.config.get('logical_consistency', {})
        )
        self.integrity_checker = CrossDatasetIntegrityChecker(
            self.config.get('cross_dataset_integrity', {})
        )
        self.revision_detector = FinancialRevisionDetector(
            self.config.get('financial_revision', {})
        )
        self.reporter = DataQualityReporter(
            self.config.get('reporting', {})
        )

    def validate_dataset(self, df: pl.DataFrame,
                        dataset_type: str = 'stock',
                        dataset_name: str = 'unknown') -> Dict:
        """
        Perform complete validation on a single dataset.

        Args:
            df: Polars DataFrame to validate
            dataset_type: Type of dataset ('stock', 'financial', 'index', etc.)
            dataset_name: Name of the dataset for reporting

        Returns:
            Dictionary with complete validation results
        """
        validation_results = {
            'dataset_name': dataset_name,
            'dataset_type': dataset_type,
            'validation_timestamp': None,
            'overall_valid': True,
            'issues': []
        }

        try:
            # Perform price anomaly detection for appropriate datasets
            if dataset_type in ['stock', 'index'] and any(col in df.columns for col in ['close', 'open', 'high', 'low']):
                logging.info(f"Running price anomaly detection on {dataset_name}")
                anomaly_results = self.price_detector.detect_anomalies(df)

                # Count anomalies
                anomaly_count = anomaly_results.select(pl.col('is_anomaly').sum()).item()
                if anomaly_count > 0:
                    anomalous_records = anomaly_results.filter(pl.col('is_anomaly'))
                    validation_results['anomaly_detection'] = {
                        'total_anomalies': anomaly_count,
                        'anomaly_percentage': anomaly_count / len(df) * 100,
                        'sample_anomalies': anomalous_records.head(5).to_dicts()
                    }
                    validation_results['issues'].append({
                        'type': 'anomaly_detection',
                        'count': anomaly_count,
                        'description': f'Detected {anomaly_count} price anomalies'
                    })

            # Perform logical consistency validation
            logging.info(f"Running logical consistency validation on {dataset_name}")
            consistency_results = self.consistency_validator.validate_consistency(df, dataset_type)
            validation_results['logical_consistency'] = consistency_results

            if not consistency_results['overall_valid']:
                validation_results['overall_valid'] = False
                validation_results['issues'].extend(consistency_results['issues'])

            # Add validation timestamp
            from datetime import datetime
            validation_results['validation_timestamp'] = datetime.now().isoformat()

        except Exception as e:
            logging.error(f"Error during validation of {dataset_name}: {str(e)}")
            validation_results['error'] = str(e)
            validation_results['overall_valid'] = False

        return validation_results

    def validate_multiple_datasets(self, datasets: Dict[str, pl.DataFrame],
                                 dataset_types: Optional[Dict[str, str]] = None) -> Dict:
        """
        Validate multiple datasets and check cross-dataset integrity.

        Args:
            datasets: Dictionary mapping dataset names to DataFrames
            dataset_types: Dictionary mapping dataset names to types

        Returns:
            Dictionary with validation results for all datasets
        """
        if dataset_types is None:
            dataset_types = {name: 'unknown' for name in datasets.keys()}

        multi_validation_results = {
            'datasets': {},
            'cross_dataset_integrity': {},
            'overall_valid': True,
            'total_issues': 0
        }

        # Validate each dataset individually
        for dataset_name, df in datasets.items():
            dataset_type = dataset_types.get(dataset_name, 'unknown')
            dataset_results = self.validate_dataset(df, dataset_type, dataset_name)

            multi_validation_results['datasets'][dataset_name] = dataset_results

            if not dataset_results.get('overall_valid', True):
                multi_validation_results['overall_valid'] = False

            multi_validation_results['total_issues'] += len(dataset_results.get('issues', []))

        # Check cross-dataset integrity
        logging.info("Running cross-dataset integrity checks")
        cross_integrity_results = self.integrity_checker.validate_cross_dataset_integrity(datasets)
        multi_validation_results['cross_dataset_integrity'] = cross_integrity_results

        if not cross_integrity_results['overall_valid']:
            multi_validation_results['overall_valid'] = False
            multi_validation_results['total_issues'] += len(cross_integrity_results['issues'])

        return multi_validation_results

    def detect_and_handle_revisions(self, current_data: pl.DataFrame,
                                  historical_data: pl.DataFrame,
                                  key_fields: List[str] = ['ts_code', 'end_date'],
                                  data_type: str = 'financial') -> Dict:
        """
        Detect and handle data revisions.

        Args:
            current_data: Current data DataFrame
            historical_data: Historical data DataFrame
            key_fields: Fields to use as primary keys
            data_type: Type of data being processed

        Returns:
            Dictionary with revision detection and handling results
        """
        try:
            # Detect revisions
            revision_results = self.revision_detector.detect_revisions(
                current_data, historical_data, key_fields
            )

            # Handle revisions
            handling_results = self.revision_detector.handle_revisions(
                revision_results, data_type
            )

            return {
                'revision_detection': revision_results,
                'revision_handling': handling_results,
                'success': True
            }

        except Exception as e:
            logging.error(f"Error in revision detection/handling: {str(e)}")
            return {
                'error': str(e),
                'success': False
            }

    def generate_comprehensive_report(self, validation_results: Dict,
                                    output_formats: List[str] = None) -> Dict:
        """
        Generate a comprehensive data quality report.

        Args:
            validation_results: Results from validation processes
            output_formats: List of formats to generate

        Returns:
            Dictionary with report generation results
        """
        try:
            # Generate quality report
            report_results = self.reporter.generate_quality_report(
                validation_results,
                validation_results.get('dataset_name', 'unknown'),
                output_formats
            )

            # Save report to files
            save_results = self.reporter.save_report(report_results)

            return {
                'report_generation': report_results,
                'report_saving': save_results,
                'success': True
            }

        except Exception as e:
            logging.error(f"Error generating report: {str(e)}")
            return {
                'error': str(e),
                'success': False
            }

    def run_complete_validation_flow(self, datasets: Dict[str, pl.DataFrame],
                                   dataset_types: Optional[Dict[str, str]] = None,
                                   historical_data: Optional[pl.DataFrame] = None) -> Dict:
        """
        Run the complete data validation flow.

        Args:
            datasets: Dictionary of datasets to validate
            dataset_types: Dictionary of dataset types
            historical_data: Historical data for revision detection

        Returns:
            Dictionary with complete validation flow results
        """
        complete_results = {
            'validation_flow': 'complete',
            'timestamp': None,
            'datasets_validated': list(datasets.keys()),
            'overall_success': True
        }

        try:
            from datetime import datetime
            complete_results['timestamp'] = datetime.now().isoformat()

            # 1. Validate datasets
            logging.info("Starting complete validation flow")
            validation_results = self.validate_multiple_datasets(datasets, dataset_types)
            complete_results['validation_results'] = validation_results

            # 2. Check for revisions if historical data provided
            if historical_data and len(datasets) == 1:
                dataset_name = list(datasets.keys())[0]
                current_data = datasets[dataset_name]

                logging.info("Checking for data revisions")
                revision_results = self.detect_and_handle_revisions(
                    current_data, historical_data
                )
                complete_results['revision_results'] = revision_results

            # 3. Generate reports
            logging.info("Generating quality reports")
            report_results = self.generate_comprehensive_report(validation_results)
            complete_results['report_results'] = report_results

            # 4. Overall status
            complete_results['overall_success'] = validation_results['overall_valid']

        except Exception as e:
            logging.error(f"Error in complete validation flow: {str(e)}")
            complete_results['error'] = str(e)
            complete_results['overall_success'] = False

        return complete_results