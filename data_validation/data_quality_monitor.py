"""
Data Quality Monitoring Module

Tracks and monitors data quality metrics over time to detect degradation or improvements.
"""
import polars as pl
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime
import json
from pathlib import Path


class DataQualityMonitor:
    """Monitors data quality metrics and tracks changes over time."""

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the data quality monitor.

        Args:
            config: Configuration dictionary with monitoring parameters
        """
        self.config = config or {
            'metrics_history_file': './data_quality_metrics.json',
            'alert_thresholds': {
                'quality_score': 0.8,
                'anomaly_rate': 0.05,
                'consistency_issues': 10,
                'integrity_issues': 5
            },
            'metrics_retention_days': 30,
            'enable_persistence': True
        }

        # In-memory metrics storage
        self.metrics_history = []

        # Load historical metrics if persistence is enabled
        if self.config['enable_persistence']:
            self._load_metrics_history()

    def record_data_quality_metrics(self, dataset_name: str, validation_results: Dict,
                                  timestamp: Optional[datetime] = None) -> Dict:
        """
        Record data quality metrics from validation results.

        Args:
            dataset_name: Name of the dataset
            validation_results: Results from data validation
            timestamp: Timestamp for the metrics (uses current time if None)

        Returns:
            Dictionary with recorded metrics
        """
        if timestamp is None:
            timestamp = datetime.now()

        # Extract key metrics from validation results
        metrics = self._extract_metrics(validation_results)

        # Create metrics record
        metric_record = {
            'dataset_name': dataset_name,
            'timestamp': timestamp.isoformat(),
            'metrics': metrics,
            'quality_score': metrics.get('quality_score', 0.0),
            'issues_count': metrics.get('total_issues', 0)
        }

        # Add to history
        self.metrics_history.append(metric_record)

        # Persist metrics if enabled
        if self.config['enable_persistence']:
            self._save_metrics_history()

        # Check for alerts
        alerts = self._check_metrics_alerts(metric_record)

        return {
            'recorded_metrics': metric_record,
            'alerts': alerts,
            'success': True
        }

    def _extract_metrics(self, validation_results: Dict) -> Dict:
        """
        Extract key metrics from validation results.

        Args:
            validation_results: Results from data validation

        Returns:
            Dictionary with extracted metrics
        """
        metrics = {
            'total_issues': 0,
            'quality_score': 1.0,
            'anomaly_count': 0,
            'consistency_issues': 0,
            'integrity_issues': 0,
            'total_records': 0,
            'null_counts': {},
            'bounds_violations': 0
        }

        # Extract overall metrics
        if 'counts' in validation_results:
            counts = validation_results['counts']
            metrics['total_records'] = counts.get('total_records', 0)

        # Extract quality score
        if 'overall_quality_score' in validation_results:
            metrics['quality_score'] = validation_results['overall_quality_score']
        elif 'overall_valid' in validation_results:
            # Simple quality score based on validity
            metrics['quality_score'] = 1.0 if validation_results['overall_valid'] else 0.0

        # Extract issue counts from different validation types
        total_issues = 0

        # Field validation issues
        if 'field_validation' in validation_results:
            field_issues = validation_results['field_validation'].get('issues', [])
            total_issues += len(field_issues)

            # Count null values and bounds violations
            for issue in field_issues:
                check = issue.get('check', '')
                if 'null' in check.lower():
                    field = issue.get('field', 'unknown')
                    null_count = issue.get('count', 0)
                    metrics['null_counts'][field] = metrics['null_counts'].get(field, 0) + null_count
                elif 'bounds' in check.lower():
                    metrics['bounds_violations'] += issue.get('count', 0)
                elif 'anomaly' in check.lower():
                    metrics['anomaly_count'] += issue.get('count', 0)

        # Logical validation issues
        if 'logical_validation' in validation_results:
            logical_issues = validation_results['logical_validation'].get('issues', [])
            total_issues += len(logical_issues)
            metrics['consistency_issues'] = len(logical_issues)

        # Referential integrity issues
        if 'referential_integrity' in validation_results:
            integrity_issues = validation_results['referential_integrity'].get('issues', [])
            total_issues += len(integrity_issues)
            metrics['integrity_issues'] = len(integrity_issues)

        # Cross-dataset consistency issues
        if 'dataset_consistency' in validation_results:
            consistency_issues = validation_results['dataset_consistency'].get('issues', [])
            total_issues += len(consistency_issues)

        metrics['total_issues'] = total_issues

        return metrics

    def _check_metrics_alerts(self, metric_record: Dict) -> List[Dict]:
        """
        Check if metrics exceed configured alert thresholds.

        Args:
            metric_record: Metrics record to check

        Returns:
            List of alert dictionaries
        """
        alerts = []
        metrics = metric_record['metrics']
        thresholds = self.config['alert_thresholds']

        # Check quality score alert
        if metrics.get('quality_score', 1.0) < thresholds['quality_score']:
            alerts.append({
                'type': 'quality_score',
                'severity': 'high',
                'message': f"Quality score {metrics['quality_score']:.2f} below threshold {thresholds['quality_score']}",
                'metric': 'quality_score',
                'value': metrics['quality_score'],
                'threshold': thresholds['quality_score']
            })

        # Check anomaly rate alert
        total_records = metrics.get('total_records', 1)
        if total_records > 0:
            anomaly_rate = metrics.get('anomaly_count', 0) / total_records
            if anomaly_rate > thresholds['anomaly_rate']:
                alerts.append({
                    'type': 'anomaly_rate',
                    'severity': 'medium',
                    'message': f"Anomaly rate {anomaly_rate:.4f} above threshold {thresholds['anomaly_rate']}",
                    'metric': 'anomaly_rate',
                    'value': anomaly_rate,
                    'threshold': thresholds['anomaly_rate']
                })

        # Check consistency issues alert
        if metrics.get('consistency_issues', 0) > thresholds['consistency_issues']:
            alerts.append({
                'type': 'consistency_issues',
                'severity': 'medium',
                'message': f"Consistency issues {metrics['consistency_issues']} above threshold {thresholds['consistency_issues']}",
                'metric': 'consistency_issues',
                'value': metrics['consistency_issues'],
                'threshold': thresholds['consistency_issues']
            })

        # Check integrity issues alert
        if metrics.get('integrity_issues', 0) > thresholds['integrity_issues']:
            alerts.append({
                'type': 'integrity_issues',
                'severity': 'high',
                'message': f"Integrity issues {metrics['integrity_issues']} above threshold {thresholds['integrity_issues']}",
                'metric': 'integrity_issues',
                'value': metrics['integrity_issues'],
                'threshold': thresholds['integrity_issues']
            })

        return alerts

    def get_quality_trends(self, dataset_name: Optional[str] = None,
                          days: int = 30) -> Dict:
        """
        Get data quality trends over time.

        Args:
            dataset_name: Filter by specific dataset (None for all)
            days: Number of days to include in trend analysis

        Returns:
            Dictionary with trend analysis
        """
        from datetime import timedelta

        # Filter metrics by dataset and time period
        cutoff_time = datetime.now() - timedelta(days=days)
        filtered_metrics = []

        for record in self.metrics_history:
            record_time = datetime.fromisoformat(record['timestamp'])
            if record_time >= cutoff_time:
                if dataset_name is None or record['dataset_name'] == dataset_name:
                    filtered_metrics.append(record)

        if not filtered_metrics:
            return {
                'trends': {},
                'period_days': days,
                'dataset_filter': dataset_name,
                'total_records': 0
            }

        # Calculate trends
        trends = {
            'quality_score_trend': self._calculate_trend(
                [r['quality_score'] for r in filtered_metrics]
            ),
            'issues_count_trend': self._calculate_trend(
                [r['issues_count'] for r in filtered_metrics]
            ),
            'dataset_coverage': len(set(r['dataset_name'] for r in filtered_metrics))
        }

        # Add per-dataset trends
        dataset_trends = {}
        datasets = set(r['dataset_name'] for r in filtered_metrics)

        for dataset in datasets:
            dataset_metrics = [r for r in filtered_metrics if r['dataset_name'] == dataset]
            dataset_trends[dataset] = {
                'quality_score_trend': self._calculate_trend(
                    [r['quality_score'] for r in dataset_metrics]
                ),
                'issues_count_trend': self._calculate_trend(
                    [r['issues_count'] for r in dataset_metrics]
                ),
                'record_count': len(dataset_metrics)
            }

        trends['per_dataset'] = dataset_trends

        return {
            'trends': trends,
            'period_days': days,
            'dataset_filter': dataset_name,
            'total_records': len(filtered_metrics)
        }

    def _calculate_trend(self, values: List[float]) -> Dict:
        """
        Calculate trend from a list of values.

        Args:
            values: List of numeric values

        Returns:
            Dictionary with trend information
        """
        if not values:
            return {'trend': 'none', 'change': 0.0, 'start_value': 0.0, 'end_value': 0.0}

        start_value = values[0]
        end_value = values[-1]
        change = end_value - start_value

        if len(values) < 2:
            trend = 'insufficient_data'
        elif change > 0.01:  # Positive change (improving)
            trend = 'improving'
        elif change < -0.01:  # Negative change (degrading)
            trend = 'degrading'
        else:  # Little to no change
            trend = 'stable'

        return {
            'trend': trend,
            'change': change,
            'start_value': start_value,
            'end_value': end_value,
            'average': sum(values) / len(values) if values else 0.0
        }

    def _load_metrics_history(self):
        """Load metrics history from persistent storage."""
        try:
            if Path(self.config['metrics_history_file']).exists():
                with open(self.config['metrics_history_file'], 'r') as f:
                    self.metrics_history = json.load(f)
                logging.info(f"Loaded {len(self.metrics_history)} historical metrics records")
        except Exception as e:
            logging.warning(f"Failed to load metrics history: {str(e)}")
            self.metrics_history = []

    def _save_metrics_history(self):
        """Save metrics history to persistent storage."""
        try:
            # Clean up old records based on retention policy
            self._cleanup_old_metrics()

            # Save to file
            with open(self.config['metrics_history_file'], 'w') as f:
                json.dump(self.metrics_history, f, indent=2)
        except Exception as e:
            logging.warning(f"Failed to save metrics history: {str(e)}")

    def _cleanup_old_metrics(self):
        """Remove old metrics based on retention policy."""
        from datetime import timedelta

        retention_days = self.config.get('metrics_retention_days', 30)
        cutoff_time = datetime.now() - timedelta(days=retention_days)

        # Filter out old records
        old_count = len(self.metrics_history)
        self.metrics_history = [
            record for record in self.metrics_history
            if datetime.fromisoformat(record['timestamp']) >= cutoff_time
        ]

        if len(self.metrics_history) < old_count:
            logging.info(f"Cleaned up {old_count - len(self.metrics_history)} old metrics records")

    def get_dataset_summary(self, dataset_name: str) -> Dict:
        """
        Get quality summary for a specific dataset.

        Args:
            dataset_name: Name of the dataset

        Returns:
            Dictionary with dataset quality summary
        """
        dataset_metrics = [
            record for record in self.metrics_history
            if record['dataset_name'] == dataset_name
        ]

        if not dataset_metrics:
            return {
                'dataset_name': dataset_name,
                'available': False,
                'message': 'No quality metrics available for this dataset'
            }

        # Get latest metrics
        latest_record = dataset_metrics[-1]
        metrics = latest_record['metrics']

        # Calculate averages
        quality_scores = [r['quality_score'] for r in dataset_metrics]
        issues_counts = [r['issues_count'] for r in dataset_metrics]

        summary = {
            'dataset_name': dataset_name,
            'available': True,
            'latest_metrics': {
                'timestamp': latest_record['timestamp'],
                'quality_score': latest_record['quality_score'],
                'issues_count': latest_record['issues_count'],
                'total_records': metrics.get('total_records', 0)
            },
            'historical_stats': {
                'average_quality_score': sum(quality_scores) / len(quality_scores),
                'max_issues': max(issues_counts) if issues_counts else 0,
                'min_issues': min(issues_counts) if issues_counts else 0,
                'total_records_processed': sum(
                    m['metrics'].get('total_records', 0) for m in dataset_metrics
                )
            },
            'trend': self._calculate_trend(quality_scores)
        }

        return summary