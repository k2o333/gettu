"""
Automatic Anomaly Repair Module

Automatically repairs common data quality issues and anomalies.
"""
import polars as pl
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import numpy as np


class AnomalyRepairEngine:
    """Automatically detects and repairs data anomalies."""

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the anomaly repair engine.

        Args:
            config: Configuration dictionary with repair parameters
        """
        self.config = config or {
            'auto_repair_enabled': True,
            'repair_strategies': {
                'null_values': 'forward_fill',
                'outliers': 'winsorize',
                'duplicates': 'remove',
                'inconsistencies': 'correct',
                'gaps': 'interpolate'
            },
            'confidence_threshold': 0.8,
            'backup_before_repair': True,
            'max_repair_attempts': 3
        }

    def repair_dataset(self, df: pl.DataFrame, repair_rules: Optional[Dict] = None) -> Dict:
        """
        Automatically repair anomalies in a dataset.

        Args:
            df: DataFrame to repair
            repair_rules: Custom repair rules (optional)

        Returns:
            Dictionary with repair results
        """
        repair_results = {
            'original_shape': df.shape,
            'repaired_shape': df.shape,
            'repairs_performed': [],
            'issues_detected': [],
            'confidence_score': 1.0,
            'backup_created': False,
            'success': True
        }

        # Create backup if configured
        original_df = None
        if self.config['backup_before_repair']:
            original_df = df.clone()
            repair_results['backup_created'] = True

        try:
            # Detect issues first
            issues = self._detect_issues(df)
            repair_results['issues_detected'] = issues

            # Perform repairs based on detected issues
            repaired_df = df
            for issue in issues:
                repair_result = self._perform_repair(repaired_df, issue, repair_rules)
                if repair_result['success']:
                    repaired_df = repair_result['repaired_df']
                    repair_results['repairs_performed'].append(repair_result['repair_info'])

            # Update results
            repair_results['repaired_df'] = repaired_df
            repair_results['repaired_shape'] = repaired_df.shape
            repair_results['confidence_score'] = self._calculate_repair_confidence(issues, repair_results['repairs_performed'])

        except Exception as e:
            logging.error(f"Error during automatic repair: {str(e)}")
            repair_results['success'] = False
            repair_results['error'] = str(e)

            # Restore from backup if available
            if original_df is not None:
                repair_results['repaired_df'] = original_df
                logging.info("Restored dataset from backup due to repair error")

        return repair_results

    def _detect_issues(self, df: pl.DataFrame) -> List[Dict]:
        """
        Detect various types of data issues.

        Args:
            df: DataFrame to analyze

        Returns:
            List of detected issues
        """
        issues = []

        # Detect null values
        null_issues = self._detect_null_issues(df)
        issues.extend(null_issues)

        # Detect outliers
        outlier_issues = self._detect_outlier_issues(df)
        issues.extend(outlier_issues)

        # Detect duplicates
        duplicate_issues = self._detect_duplicate_issues(df)
        issues.extend(duplicate_issues)

        # Detect inconsistencies
        inconsistency_issues = self._detect_inconsistency_issues(df)
        issues.extend(inconsistency_issues)

        # Detect data gaps
        gap_issues = self._detect_gap_issues(df)
        issues.extend(gap_issues)

        return issues

    def _detect_null_issues(self, df: pl.DataFrame) -> List[Dict]:
        """Detect null value issues."""
        issues = []

        for col in df.columns:
            null_count = df.select(pl.col(col).is_null().sum()).item()
            if null_count > 0:
                null_percentage = null_count / len(df) * 100
                issues.append({
                    'type': 'null_values',
                    'column': col,
                    'count': null_count,
                    'percentage': null_percentage,
                    'severity': 'high' if null_percentage > 10 else 'medium' if null_percentage > 1 else 'low'
                })

        return issues

    def _detect_outlier_issues(self, df: pl.DataFrame) -> List[Dict]:
        """Detect outlier issues using IQR method."""
        issues = []

        # Check numeric columns for outliers
        numeric_cols = [col for col in df.columns if df.schema[col] in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]]

        for col in numeric_cols:
            # Calculate IQR
            q1 = df.select(pl.col(col).quantile(0.25)).item()
            q3 = df.select(pl.col(col).quantile(0.75)).item()
            iqr = q3 - q1

            if iqr > 0:  # Only check if there's variance
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr

                # Count outliers
                outlier_count = df.select(
                    (pl.col(col) < lower_bound) | (pl.col(col) > upper_bound)
                ).sum().item()

                if outlier_count > 0:
                    outlier_percentage = outlier_count / len(df) * 100
                    issues.append({
                        'type': 'outliers',
                        'column': col,
                        'count': outlier_count,
                        'percentage': outlier_percentage,
                        'bounds': (lower_bound, upper_bound),
                        'severity': 'high' if outlier_percentage > 5 else 'medium' if outlier_percentage > 1 else 'low'
                    })

        return issues

    def _detect_duplicate_issues(self, df: pl.DataFrame) -> List[Dict]:
        """Detect duplicate row issues."""
        # Check for duplicate rows
        duplicate_count = len(df) - df.unique().height

        if duplicate_count > 0:
            duplicate_percentage = duplicate_count / len(df) * 100
            return [{
                'type': 'duplicates',
                'count': duplicate_count,
                'percentage': duplicate_percentage,
                'severity': 'high' if duplicate_percentage > 5 else 'medium' if duplicate_percentage > 1 else 'low'
            }]

        return []

    def _detect_inconsistency_issues(self, df: pl.DataFrame) -> List[Dict]:
        """Detect logical inconsistency issues."""
        issues = []

        # Check OHLC consistency for stock data
        ohlc_cols = ['open', 'high', 'low', 'close']
        available_ohlc = [col for col in ohlc_cols if col in df.columns]

        if len(available_ohlc) >= 4:
            # Check if high >= max(open, close) and low <= min(open, close)
            inconsistencies = df.select(
                ((pl.col('high') < pl.coalesce([pl.col('open'), pl.col('close')]).max_horizontal()) |
                 (pl.col('low') > pl.coalesce([pl.col('open'), pl.col('close')]).min_horizontal()))
                & pl.col('high').is_not_null()
                & pl.col('low').is_not_null()
                & pl.col('open').is_not_null()
                & pl.col('close').is_not_null()
            ).sum().item()

            if inconsistencies > 0:
                inconsistency_percentage = inconsistencies / len(df) * 100
                issues.append({
                    'type': 'ohlc_inconsistency',
                    'count': inconsistencies,
                    'percentage': inconsistency_percentage,
                    'severity': 'high' if inconsistency_percentage > 2 else 'medium'
                })

        return issues

    def _detect_gap_issues(self, df: pl.DataFrame) -> List[Dict]:
        """Detect date/time gap issues."""
        issues = []

        # Look for date columns
        date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]

        for date_col in date_cols:
            if df.schema[date_col] in [pl.Date, pl.Datetime]:
                # Sort by date and check for gaps
                sorted_df = df.sort(date_col)
                date_series = sorted_df[date_col].to_list()

                if len(date_series) > 1:
                    # Calculate expected frequency (simplified)
                    gaps = 0
                    for i in range(1, len(date_series)):
                        # Simple gap detection - could be enhanced
                        if (date_series[i] - date_series[i-1]).days > 7:  # More than a week gap
                            gaps += 1

                    if gaps > 0:
                        gap_percentage = gaps / (len(date_series) - 1) * 100
                        issues.append({
                            'type': 'date_gaps',
                            'column': date_col,
                            'count': gaps,
                            'percentage': gap_percentage,
                            'severity': 'medium' if gap_percentage > 5 else 'low'
                        })

        return issues

    def _perform_repair(self, df: pl.DataFrame, issue: Dict, repair_rules: Optional[Dict] = None) -> Dict:
        """
        Perform repair for a specific issue.

        Args:
            df: DataFrame to repair
            issue: Issue to repair
            repair_rules: Custom repair rules

        Returns:
            Dictionary with repair result
        """
        repair_result = {
            'success': False,
            'repaired_df': df,
            'repair_info': {},
            'confidence': 0.0
        }

        issue_type = issue['type']
        strategy = self.config['repair_strategies'].get(issue_type, 'skip')

        try:
            if strategy == 'forward_fill' and issue_type == 'null_values':
                repaired_df, confidence = self._repair_null_values(df, issue)
                repair_result['repaired_df'] = repaired_df
                repair_result['success'] = True
                repair_result['confidence'] = confidence

            elif strategy == 'winsorize' and issue_type == 'outliers':
                repaired_df, confidence = self._repair_outliers(df, issue)
                repair_result['repaired_df'] = repaired_df
                repair_result['success'] = True
                repair_result['confidence'] = confidence

            elif strategy == 'remove' and issue_type == 'duplicates':
                repaired_df, confidence = self._repair_duplicates(df, issue)
                repair_result['repaired_df'] = repaired_df
                repair_result['success'] = True
                repair_result['confidence'] = confidence

            elif strategy == 'correct' and issue_type == 'ohlc_inconsistency':
                repaired_df, confidence = self._repair_inconsistencies(df, issue)
                repair_result['repaired_df'] = repaired_df
                repair_result['success'] = True
                repair_result['confidence'] = confidence

            elif strategy == 'interpolate' and issue_type == 'date_gaps':
                repaired_df, confidence = self._repair_gaps(df, issue)
                repair_result['repaired_df'] = repaired_df
                repair_result['success'] = True
                repair_result['confidence'] = confidence

            # Record repair info
            repair_result['repair_info'] = {
                'issue_type': issue_type,
                'strategy': strategy,
                'column': issue.get('column', 'N/A'),
                'records_affected': issue.get('count', 0),
                'confidence': repair_result['confidence'],
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            logging.error(f"Error repairing {issue_type}: {str(e)}")
            repair_result['error'] = str(e)

        return repair_result

    def _repair_null_values(self, df: pl.DataFrame, issue: Dict) -> Tuple[pl.DataFrame, float]:
        """Repair null values using forward fill strategy."""
        column = issue['column']

        # Forward fill null values
        repaired_df = df.with_columns([
            pl.col(column).forward_fill().alias(column)
        ])

        # Calculate confidence based on percentage of nulls
        confidence = 1.0 - (issue['percentage'] / 100.0)
        return repaired_df, confidence

    def _repair_outliers(self, df: pl.DataFrame, issue: Dict) -> Tuple[pl.DataFrame, float]:
        """Repair outliers using winsorization."""
        column = issue['column']
        lower_bound, upper_bound = issue['bounds']

        # Winsorize outliers (cap them at bounds)
        repaired_df = df.with_columns([
            pl.when(pl.col(column) < lower_bound)
            .then(lower_bound)
            .when(pl.col(column) > upper_bound)
            .then(upper_bound)
            .otherwise(pl.col(column))
            .alias(column)
        ])

        # Confidence based on outlier percentage
        confidence = 1.0 - (issue['percentage'] / 100.0 * 0.5)  # Reduced confidence for outlier repairs
        return repaired_df, confidence

    def _repair_duplicates(self, df: pl.DataFrame, issue: Dict) -> Tuple[pl.DataFrame, float]:
        """Remove duplicate rows."""
        # Remove duplicates (keep first occurrence)
        repaired_df = df.unique(keep='first')

        # High confidence for duplicate removal
        confidence = 0.95
        return repaired_df, confidence

    def _repair_inconsistencies(self, df: pl.DataFrame, issue: Dict) -> Tuple[pl.DataFrame, float]:
        """Repair logical inconsistencies (OHLC)."""
        # Correct OHLC inconsistencies
        repaired_df = df.with_columns([
            # Fix high values
            pl.when(pl.col('high') < pl.coalesce([pl.col('open'), pl.col('close')]).max_horizontal())
            .then(pl.coalesce([pl.col('open'), pl.col('close')]).max_horizontal())
            .otherwise(pl.col('high'))
            .alias('high'),

            # Fix low values
            pl.when(pl.col('low') > pl.coalesce([pl.col('open'), pl.col('close')]).min_horizontal())
            .then(pl.coalesce([pl.col('open'), pl.col('close')]).min_horizontal())
            .otherwise(pl.col('low'))
            .alias('low')
        ])

        # Medium confidence for logical corrections
        confidence = 0.8
        return repaired_df, confidence

    def _repair_gaps(self, df: pl.DataFrame, issue: Dict) -> Tuple[pl.DataFrame, float]:
        """Attempt to fill date gaps (simplified)."""
        # For now, we'll just note the gaps - interpolation would require more complex logic
        # This is a placeholder for more sophisticated gap filling
        confidence = 0.6  # Lower confidence as this is just a placeholder
        return df, confidence

    def _calculate_repair_confidence(self, detected_issues: List[Dict],
                                   performed_repairs: List[Dict]) -> float:
        """
        Calculate overall confidence in repair process.

        Args:
            detected_issues: List of detected issues
            performed_repairs: List of performed repairs

        Returns:
            Overall confidence score (0.0 to 1.0)
        """
        if not detected_issues:
            return 1.0  # No issues detected, high confidence

        if not performed_repairs:
            # Issues detected but no repairs performed
            total_severity = sum(
                3 if issue.get('severity') == 'high'
                else 2 if issue.get('severity') == 'medium'
                else 1
                for issue in detected_issues
            )
            max_severity = len(detected_issues) * 3
            return 1.0 - (total_severity / max_severity) if max_severity > 0 else 0.5

        # Calculate weighted confidence based on repairs performed
        total_confidence = sum(repair.get('confidence', 0.0) for repair in performed_repairs)
        return total_confidence / len(performed_repairs) if performed_repairs else 0.5

    def generate_repair_report(self, repair_results: Dict) -> str:
        """
        Generate a human-readable repair report.

        Args:
            repair_results: Results from repair_dataset method

        Returns:
            Formatted repair report string
        """
        if not repair_results['success']:
            return f"Repair failed: {repair_results.get('error', 'Unknown error')}"

        report_lines = []
        report_lines.append("DATA QUALITY REPAIR REPORT")
        report_lines.append("=" * 40)
        report_lines.append(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Original shape: {repair_results['original_shape']}")
        report_lines.append(f"Repaired shape: {repair_results['repaired_shape']}")
        report_lines.append(f"Overall confidence: {repair_results['confidence_score']:.2%}")
        report_lines.append(f"Backup created: {repair_results['backup_created']}")
        report_lines.append("")

        # Issues detected
        if repair_results['issues_detected']:
            report_lines.append("ISSUES DETECTED:")
            report_lines.append("-" * 20)
            for issue in repair_results['issues_detected']:
                report_lines.append(
                    f"  {issue['type']} in {issue.get('column', 'N/A')}: "
                    f"{issue['count']} ({issue['percentage']:.2f}%) - {issue['severity']}"
                )
            report_lines.append("")

        # Repairs performed
        if repair_results['repairs_performed']:
            report_lines.append("REPAIRS PERFORMED:")
            report_lines.append("-" * 20)
            for repair in repair_results['repairs_performed']:
                report_lines.append(
                    f"  {repair['issue_type']} ({repair['strategy']}): "
                    f"{repair['records_affected']} records affected "
                    f"(confidence: {repair['confidence']:.2%})"
                )
        else:
            report_lines.append("No repairs performed.")

        report_lines.append("")
        report_lines.append("=" * 40)
        report_lines.append("End of Report")

        return "\n".join(report_lines)