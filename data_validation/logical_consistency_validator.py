"""
Logical Consistency Validator

Validates logical relationships and constraints in financial data.
"""
import polars as pl
from typing import Dict, List, Tuple, Optional
import logging


class LogicalConsistencyValidator:
    """Validates logical consistency in financial data based on business rules."""

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the logical consistency validator.

        Args:
            config: Configuration dictionary with validation parameters
        """
        self.config = config or {
            'allow_negative_prices': False,
            'price_bounds': (0, 100000),  # (min, max) reasonable price bounds
            'volume_bounds': (0, 1e9),    # (min, max) reasonable volume bounds
            'max_price_change_pct': 0.20,  # Max 20% price change per day
            'required_fields_check': True,
            'date_consistency_check': True
        }

    def validate_consistency(self, df: pl.DataFrame, data_type: str = 'stock') -> Dict:
        """
        Validate logical consistency of data.

        Args:
            df: Polars DataFrame with data to validate
            data_type: Type of data ('stock', 'financial', 'index', etc.)

        Returns:
            Dictionary with validation results
        """
        validation_results = {
            'overall_valid': True,
            'issues': [],
            'field_validation': {},
            'logical_validation': {},
            'counts': {
                'total_records': len(df),
                'failed_records': 0
            }
        }

        # Run field-level validations
        validation_results['field_validation'] = self._validate_fields(df, data_type)

        # Run logical consistency validations
        validation_results['logical_validation'] = self._validate_logical_consistency(df, data_type)

        # Count total issues
        all_issues = []
        all_issues.extend(validation_results['field_validation']['issues'])
        all_issues.extend(validation_results['logical_validation']['issues'])

        validation_results['issues'] = all_issues
        validation_results['counts']['failed_records'] = len(all_issues)
        validation_results['overall_valid'] = len(all_issues) == 0

        return validation_results

    def _validate_fields(self, df: pl.DataFrame, data_type: str) -> Dict:
        """
        Validate individual fields for data type consistency and constraints.

        Args:
            df: DataFrame to validate
            data_type: Type of data being validated

        Returns:
            Dictionary with field validation results
        """
        issues = []
        field_validation = {
            'issues': [],
            'checks_performed': []
        }

        # Check for null values in required fields
        if self.config['required_fields_check']:
            required_fields = self._get_required_fields(data_type)
            for field in required_fields:
                if field in df.columns:
                    null_count = df.select(pl.col(field).is_null().sum()).item()
                    if null_count > 0:
                        issues.append({
                            'field': field,
                            'check': 'required_field_null',
                            'count': null_count,
                            'description': f'Required field {field} has {null_count} null values'
                        })

        # Validate field types and ranges
        for col in df.columns:
            if col in ['open', 'close', 'high', 'low', 'pre_close']:
                # Validate price fields
                price_issues = self._validate_price_field(df, col)
                issues.extend(price_issues)
            elif col == 'volume':
                # Validate volume field
                volume_issues = self._validate_volume_field(df, col)
                issues.extend(volume_issues)
            elif 'date' in col.lower():
                # Validate date fields
                date_issues = self._validate_date_field(df, col)
                issues.extend(date_issues)

        field_validation['issues'] = issues
        field_validation['checks_performed'].append('field_validation')
        return field_validation

    def _validate_logical_consistency(self, df: pl.DataFrame, data_type: str) -> Dict:
        """
        Validate logical consistency based on business rules.

        Args:
            df: DataFrame to validate
            data_type: Type of data being validated

        Returns:
            Dictionary with logical validation results
        """
        issues = []
        logical_validation = {
            'issues': [],
            'checks_performed': []
        }

        # Validate OHLC relationships
        if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
            ohlc_issues = self._validate_ohlc_consistency(df)
            issues.extend(ohlc_issues)

        # Validate price changes
        if 'close' in df.columns:
            price_change_issues = self._validate_price_changes(df)
            issues.extend(price_change_issues)

        # Validate date consistency (if applicable)
        if self.config['date_consistency_check']:
            date_consistency_issues = self._validate_date_consistency(df, data_type)
            issues.extend(date_consistency_issues)

        logical_validation['issues'] = issues
        logical_validation['checks_performed'].append('logical_validation')
        return logical_validation

    def _validate_price_field(self, df: pl.DataFrame, field: str) -> List[Dict]:
        """
        Validate price fields for reasonable values.

        Args:
            df: DataFrame to validate
            field: Field name to validate

        Returns:
            List of issues found
        """
        issues = []

        # Check for negative prices if not allowed
        if not self.config['allow_negative_prices']:
            negative_count = df.select(
                pl.col(field).is_not_null() & (pl.col(field) < 0)
            ).sum().item()

            if negative_count > 0:
                issues.append({
                    'field': field,
                    'check': 'negative_price',
                    'count': negative_count,
                    'description': f'Field {field} has {negative_count} negative values'
                })

        # Check price bounds
        min_bound, max_bound = self.config['price_bounds']
        out_of_bounds_count = df.select(
            pl.col(field).is_not_null() &
            ((pl.col(field) < min_bound) | (pl.col(field) > max_bound))
        ).sum().item()

        if out_of_bounds_count > 0:
            issues.append({
                'field': field,
                'check': 'price_bounds',
                'count': out_of_bounds_count,
                'description': f'Field {field} has {out_of_bounds_count} values outside bounds [{min_bound}, {max_bound}]'
            })

        return issues

    def _validate_volume_field(self, df: pl.DataFrame, field: str) -> List[Dict]:
        """
        Validate volume field for reasonable values.

        Args:
            df: DataFrame to validate
            field: Field name to validate

        Returns:
            List of issues found
        """
        issues = []

        # Check for negative volumes
        negative_count = df.select(
            pl.col(field).is_not_null() & (pl.col(field) < 0)
        ).sum().item()

        if negative_count > 0:
            issues.append({
                'field': field,
                'check': 'negative_volume',
                'count': negative_count,
                'description': f'Field {field} has {negative_count} negative values'
            })

        # Check volume bounds
        min_bound, max_bound = self.config['volume_bounds']
        out_of_bounds_count = df.select(
            pl.col(field).is_not_null() &
            ((pl.col(field) < min_bound) | (pl.col(field) > max_bound))
        ).sum().item()

        if out_of_bounds_count > 0:
            issues.append({
                'field': field,
                'check': 'volume_bounds',
                'count': out_of_bounds_count,
                'description': f'Field {field} has {out_of_bounds_count} values outside bounds [{min_bound}, {max_bound}]'
            })

        return issues

    def _validate_date_field(self, df: pl.DataFrame, field: str) -> List[Dict]:
        """
        Validate date field format and consistency.

        Args:
            df: DataFrame to validate
            field: Field name to validate

        Returns:
            List of issues found
        """
        issues = []

        # Check for null dates
        null_count = df.select(pl.col(field).is_null()).sum().item()
        if null_count > 0:
            issues.append({
                'field': field,
                'check': 'null_date',
                'count': null_count,
                'description': f'Field {field} has {null_count} null values'
            })

        return issues

    def _validate_ohlc_consistency(self, df: pl.DataFrame) -> List[Dict]:
        """
        Validate that OHLC values follow logical relationships.
        High >= Max(open, close)
        Low <= Min(open, close)

        Args:
            df: DataFrame with OHLC fields

        Returns:
            List of OHLC consistency issues
        """
        issues = []

        # Check that high is >= max(open, close)
        high_issues_count = df.select(
            pl.when(
                pl.col('high').is_not_null() &
                pl.col('open').is_not_null() &
                pl.col('close').is_not_null()
            )
            .then(
                pl.col('high') < pl.max_horizontal(pl.col('open'), pl.col('close'))
            )
            .otherwise(False)
        ).sum().item()

        if high_issues_count > 0:
            issues.append({
                'field': 'high',
                'check': 'ohlc_high',
                'count': high_issues_count,
                'description': f'High price is less than open/close in {high_issues_count} records'
            })

        # Check that low is <= min(open, close)
        low_issues_count = df.select(
            pl.when(
                pl.col('low').is_not_null() &
                pl.col('open').is_not_null() &
                pl.col('close').is_not_null()
            )
            .then(
                pl.col('low') > pl.min_horizontal(pl.col('open'), pl.col('close'))
            )
            .otherwise(False)
        ).sum().item()

        if low_issues_count > 0:
            issues.append({
                'field': 'low',
                'check': 'ohlc_low',
                'count': low_issues_count,
                'description': f'Low price is greater than open/close in {low_issues_count} records'
            })

        return issues

    def _validate_price_changes(self, df: pl.DataFrame) -> List[Dict]:
        """
        Validate price changes are within reasonable bounds.

        Args:
            df: DataFrame with close prices

        Returns:
            List of price change issues
        """
        issues = []

        max_change = self.config['max_price_change_pct']

        # Calculate percentage changes
        pct_change_expr = (pl.col('close') - pl.col('close').shift(1)) / pl.col('close').shift(1)

        unusual_changes_count = df.with_columns([
            pct_change_expr.alias('pct_change')
        ]).select(
            pl.when(
                pl.col('pct_change').is_not_null()
            )
            .then(
                pl.col('pct_change').abs() > max_change
            )
            .otherwise(False)
        ).sum().item()

        if unusual_changes_count > 0:
            issues.append({
                'field': 'close',
                'check': 'price_change',
                'count': unusual_changes_count,
                'description': f'Close price changes exceed {max_change*100}% in {unusual_changes_count} records'
            })

        return issues

    def _validate_date_consistency(self, df: pl.DataFrame, data_type: str) -> List[Dict]:
        """
        Validate date consistency based on data type.

        Args:
            df: DataFrame to validate
            data_type: Type of data being validated

        Returns:
            List of date consistency issues
        """
        issues = []

        # Look for common date field names
        date_fields = [col for col in df.columns if 'date' in col.lower()]
        if not date_fields:
            return issues

        date_field = date_fields[0]  # Use the first date field found

        # Check for future dates
        from datetime import datetime
        now = datetime.now()

        # Try to handle different date formats
        try:
            # If the column is already datetime type
            future_date_count = df.select(
                pl.when(
                    pl.col(date_field).is_not_null()
                )
                .then(
                    pl.col(date_field) > pl.lit(now)
                )
                .otherwise(False)
            ).sum().item()
        except:
            # If the column is string type, try to parse it
            try:
                future_date_count = df.select(
                    pl.when(
                        pl.col(date_field).is_not_null()
                    )
                    .then(
                        pl.col(date_field).str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S') > pl.lit(now)
                    )
                    .otherwise(False)
                ).sum().item()
            except:
                # Try another common format
                try:
                    future_date_count = df.select(
                        pl.when(
                            pl.col(date_field).is_not_null()
                        )
                        .then(
                            pl.col(date_field).str.strptime(pl.Datetime, '%Y-%m-%d') > pl.lit(now)
                        )
                        .otherwise(False)
                    ).sum().item()
                except:
                    # If all parsing fails, skip this check
                    future_date_count = 0

        if future_date_count > 0:
            issues.append({
                'field': date_field,
                'check': 'future_date',
                'count': future_date_count,
                'description': f'Field {date_field} has {future_date_count} future dates'
            })

        # Check for past dates too far in the past (e.g., before 1900)
        old_date = datetime(1900, 1, 1)
        try:
            # If the column is already datetime type
            old_date_count = df.select(
                pl.when(
                    pl.col(date_field).is_not_null()
                )
                .then(
                    pl.col(date_field) < pl.lit(old_date)
                )
                .otherwise(False)
            ).sum().item()
        except:
            # If the column is string type, try to parse it
            try:
                old_date_count = df.select(
                    pl.when(
                        pl.col(date_field).is_not_null()
                    )
                    .then(
                        pl.col(date_field).str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S') < pl.lit(old_date)
                    )
                    .otherwise(False)
                ).sum().item()
            except:
                # Try another common format
                try:
                    old_date_count = df.select(
                        pl.when(
                            pl.col(date_field).is_not_null()
                        )
                        .then(
                            pl.col(date_field).str.strptime(pl.Datetime, '%Y-%m-%d') < pl.lit(old_date)
                        )
                        .otherwise(False)
                    ).sum().item()
                except:
                    # If all parsing fails, skip this check
                    old_date_count = 0

        if old_date_count > 0:
            issues.append({
                'field': date_field,
                'check': 'old_date',
                'count': old_date_count,
                'description': f'Field {date_field} has {old_date_count} dates before 1900-01-01'
            })

        return issues

    def _get_required_fields(self, data_type: str) -> List[str]:
        """
        Get required fields based on data type.

        Args:
            data_type: Type of data

        Returns:
            List of required field names
        """
        if data_type == 'stock':
            return ['ts_code', 'trade_date', 'close']
        elif data_type == 'financial':
            return ['ts_code', 'ann_date', 'end_date', 'f_ann_date']
        elif data_type == 'index':
            return ['ts_code', 'trade_date', 'close']
        else:
            return ['ts_code', 'trade_date']  # Default required fields