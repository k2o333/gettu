"""
Cross-Dataset Referential Integrity Checker

Validates referential integrity across different datasets and ensures consistency
between financial statements, market data alignment, and temporal consistency.
"""
import polars as pl
from typing import Dict, List, Tuple, Optional
import logging
from pathlib import Path


class CrossDatasetIntegrityChecker:
    """Validates referential integrity across different datasets."""

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the cross-dataset integrity checker.

        Args:
            config: Configuration dictionary with integrity check parameters
        """
        self.config = config or {
            'check_stock_code_consistency': True,
            'check_date_alignment': True,
            'check_financial_statement_consistency': True,
            'check_temporal_consistency': True,
            'max_date_diff_days': 1  # Maximum allowed date difference for alignment
        }

    def validate_cross_dataset_integrity(self, datasets: Dict[str, pl.DataFrame]) -> Dict:
        """
        Validate referential integrity across multiple datasets.

        Args:
            datasets: Dictionary mapping dataset names to DataFrames

        Returns:
            Dictionary with integrity validation results
        """
        integrity_results = {
            'overall_valid': True,
            'issues': [],
            'dataset_consistency': {},
            'referential_integrity': {},
            'counts': {
                'total_datasets': len(datasets),
                'total_issues': 0
            }
        }

        # Check dataset consistency
        integrity_results['dataset_consistency'] = self._check_dataset_consistency(datasets)

        # Check referential integrity between datasets
        integrity_results['referential_integrity'] = self._check_referential_integrity(datasets)

        # Aggregate issues
        all_issues = []
        all_issues.extend(integrity_results['dataset_consistency']['issues'])
        all_issues.extend(integrity_results['referential_integrity']['issues'])

        integrity_results['issues'] = all_issues
        integrity_results['counts']['total_issues'] = len(all_issues)
        integrity_results['overall_valid'] = len(all_issues) == 0

        return integrity_results

    def _check_dataset_consistency(self, datasets: Dict[str, pl.DataFrame]) -> Dict:
        """
        Check for consistency within and across datasets.

        Args:
            datasets: Dictionary mapping dataset names to DataFrames

        Returns:
            Dictionary with dataset consistency validation results
        """
        issues = []
        dataset_consistency = {
            'issues': [],
            'checks_performed': []
        }

        # Check for common fields across datasets
        all_columns = {}
        for dataset_name, df in datasets.items():
            all_columns[dataset_name] = set(df.columns)

        # Find common fields
        common_fields = set()
        if all_columns:
            common_fields = set.intersection(*all_columns.values()) if all_columns.values() else set()

        # Check stock code consistency across datasets
        if self.config['check_stock_code_consistency'] and 'ts_code' in common_fields:
            stock_consistency_issues = self._check_stock_code_consistency(datasets)
            issues.extend(stock_consistency_issues)

        # Check date alignment if date fields are common
        date_fields = [field for field in common_fields if 'date' in field.lower()]
        if self.config['check_date_alignment'] and date_fields:
            date_alignment_issues = self._check_date_alignment(datasets, date_fields[0])
            issues.extend(date_alignment_issues)

        dataset_consistency['issues'] = issues
        dataset_consistency['checks_performed'].append('dataset_consistency')
        return dataset_consistency

    def _check_referential_integrity(self, datasets: Dict[str, pl.DataFrame]) -> Dict:
        """
        Check referential integrity between related datasets.

        Args:
            datasets: Dictionary mapping dataset names to DataFrames

        Returns:
            Dictionary with referential integrity validation results
        """
        issues = []
        referential_integrity = {
            'issues': [],
            'checks_performed': []
        }

        # Check financial statement consistency if applicable
        if self.config['check_financial_statement_consistency']:
            financial_consistency_issues = self._check_financial_statement_consistency(datasets)
            issues.extend(financial_consistency_issues)

        # Check temporal consistency
        if self.config['check_temporal_consistency']:
            temporal_consistency_issues = self._check_temporal_consistency(datasets)
            issues.extend(temporal_consistency_issues)

        referential_integrity['issues'] = issues
        referential_integrity['checks_performed'].append('referential_integrity')
        return referential_integrity

    def _check_stock_code_consistency(self, datasets: Dict[str, pl.DataFrame]) -> List[Dict]:
        """
        Check for stock code consistency across datasets.

        Args:
            datasets: Dictionary mapping dataset names to DataFrames

        Returns:
            List of stock code consistency issues
        """
        issues = []

        # Get all unique stock codes from each dataset
        stock_codes_by_dataset = {}
        for dataset_name, df in datasets.items():
            if 'ts_code' in df.columns:
                stock_codes = df.select(pl.col('ts_code').unique()).to_series().to_list()
                stock_codes_by_dataset[dataset_name] = set(stock_codes)

        if len(stock_codes_by_dataset) < 2:
            return issues  # Need at least 2 datasets to compare

        # Find common stock codes
        all_stock_codes = set()
        for stock_codes in stock_codes_by_dataset.values():
            all_stock_codes.update(stock_codes)

        # Check for missing stock codes in each dataset
        for dataset_name, expected_stock_codes in stock_codes_by_dataset.items():
            for stock_code in all_stock_codes:
                if stock_code not in expected_stock_codes:
                    issues.append({
                        'dataset': dataset_name,
                        'check': 'missing_stock_code',
                        'stock_code': stock_code,
                        'description': f'Stock code {stock_code} is missing in dataset {dataset_name}'
                    })

        return issues

    def _check_date_alignment(self, datasets: Dict[str, pl.DataFrame], date_field: str) -> List[Dict]:
        """
        Check for date alignment across datasets.

        Args:
            datasets: Dictionary mapping dataset names to DataFrames
            date_field: Common date field to check

        Returns:
            List of date alignment issues
        """
        issues = []

        # Get date ranges for each dataset
        date_ranges = {}
        for dataset_name, df in datasets.items():
            if date_field in df.columns and len(df) > 0:
                # Only process non-empty datasets
                date_range = df.select([
                    pl.col(date_field).min().alias('min_date'),
                    pl.col(date_field).max().alias('max_date')
                ]).row(0)
                # Only add to date_ranges if we got valid values
                if date_range[0] is not None and date_range[1] is not None:
                    date_ranges[dataset_name] = date_range

        # Check for date alignment issues
        for dataset_name, (min_date, max_date) in date_ranges.items():
            for other_dataset, (other_min, other_max) in date_ranges.items():
                if dataset_name != other_dataset:
                    # Check if date ranges overlap significantly but not completely
                    if min_date is not None and max_date is not None and other_min is not None and other_max is not None:
                        # Check if there are significant gaps
                        max_start = max(min_date, other_min)
                        min_end = min(max_date, other_max)

                        if max_start > min_end:  # No overlap
                            issues.append({
                                'datasets': [dataset_name, other_dataset],
                                'check': 'date_range_no_overlap',
                                'description': f'No overlap between {dataset_name} ({min_date} to {max_date}) and {other_dataset} ({other_min} to {other_max})'
                            })

        return issues

    def _check_financial_statement_consistency(self, datasets: Dict[str, pl.DataFrame]) -> List[Dict]:
        """
        Check for consistency in financial statements across periods.

        Args:
            datasets: Dictionary mapping dataset names to DataFrames

        Returns:
            List of financial statement consistency issues
        """
        issues = []

        # Look for financial statement related datasets (balance sheet, income statement, cash flow)
        financial_datasets = []
        for dataset_name in datasets.keys():
            if any(keyword in dataset_name.lower() for keyword in ['balance', 'income', 'cash', 'financial']):
                financial_datasets.append(dataset_name)

        # For each financial dataset, check for consistency within periods
        for dataset_name in financial_datasets:
            df = datasets[dataset_name]
            if 'ts_code' in df.columns and 'end_date' in df.columns:
                # Check for duplicate entries for the same stock and end date
                duplicates = df.group_by(['ts_code', 'end_date']).agg([
                    pl.len().alias('count')
                ]).filter(pl.col('count') > 1)

                if len(duplicates) > 0:
                    for row in duplicates.iter_rows():
                        issues.append({
                            'dataset': dataset_name,
                            'check': 'duplicate_financial_entry',
                            'stock_code': row[0],
                            'end_date': row[1],
                            'count': row[2],
                            'description': f'Duplicate financial entries for {row[0]} on {row[1]}: {row[2]} entries'
                        })

        return issues

    def _check_temporal_consistency(self, datasets: Dict[str, pl.DataFrame]) -> List[Dict]:
        """
        Check for temporal consistency across datasets.

        Args:
            datasets: Dictionary mapping dataset names to DataFrames

        Returns:
            List of temporal consistency issues
        """
        issues = []

        # Check for common date patterns across datasets
        for dataset_name, df in datasets.items():
            date_fields = [col for col in df.columns if 'date' in col.lower()]

            for date_field in date_fields:
                # Check for future dates
                from datetime import datetime
                now = datetime.now()

                # Try to parse string dates to datetime for comparison
                try:
                    future_date_count = df.select(
                        pl.when(
                            pl.col(date_field).is_not_null()
                        )
                        .then(
                            pl.col(date_field).str.strptime(pl.Date, '%Y%m%d').dt.timestamp() > now.timestamp()
                        )
                        .otherwise(False)
                    ).sum().item()
                except:
                    # If parsing fails, skip future date check for this field
                    future_date_count = 0

                if future_date_count > 0:
                    issues.append({
                        'dataset': dataset_name,
                        'field': date_field,
                        'check': 'future_date',
                        'count': future_date_count,
                        'description': f'Dataset {dataset_name} field {date_field} has {future_date_count} future dates'
                    })

                # Check for dates too far in the past
                try:
                    old_date_count = df.select(
                        pl.when(
                            pl.col(date_field).is_not_null()
                        )
                        .then(
                            pl.col(date_field).str.strptime(pl.Date, '%Y%m%d').dt.timestamp() < datetime(1900, 1, 1).timestamp()
                        )
                        .otherwise(False)
                    ).sum().item()
                except:
                    # If parsing fails, skip old date check for this field
                    old_date_count = 0

                if old_date_count > 0:
                    issues.append({
                        'dataset': dataset_name,
                        'field': date_field,
                        'check': 'old_date',
                        'count': old_date_count,
                        'description': f'Dataset {dataset_name} field {date_field} has {old_date_count} dates before 1900-01-01'
                    })

        return issues

    def validate_stock_code_references(self, main_dataset: pl.DataFrame,
                                     reference_dataset: pl.DataFrame,
                                     main_key: str = 'ts_code',
                                     ref_key: str = 'ts_code') -> Dict:
        """
        Validate that all stock codes in main dataset exist in reference dataset.

        Args:
            main_dataset: Main dataset to validate
            reference_dataset: Reference dataset to check against
            main_key: Column name in main dataset
            ref_key: Column name in reference dataset

        Returns:
            Dictionary with validation results
        """
        validation_result = {
            'valid': True,
            'missing_references': [],
            'stats': {}
        }

        if main_key not in main_dataset.columns or ref_key not in reference_dataset.columns:
            validation_result['valid'] = False
            validation_result['error'] = f'Missing required columns: {main_key} or {ref_key}'
            return validation_result

        # Get unique values from both datasets
        main_values = set(main_dataset.select(pl.col(main_key).unique()).to_series().to_list())
        ref_values = set(reference_dataset.select(pl.col(ref_key).unique()).to_series().to_list())

        # Find missing references
        missing_values = main_values - ref_values
        validation_result['missing_references'] = list(missing_values)
        validation_result['valid'] = len(missing_values) == 0
        validation_result['stats'] = {
            'main_count': len(main_values),
            'ref_count': len(ref_values),
            'missing_count': len(missing_values)
        }

        return validation_result