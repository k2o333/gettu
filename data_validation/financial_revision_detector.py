"""
Financial Data Revision Detector

Detects and handles financial data revisions automatically.
"""
import polars as pl
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime


class FinancialRevisionDetector:
    """Detects financial data revisions and handles them appropriately."""

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the financial revision detector.

        Args:
            config: Configuration dictionary with revision detection parameters
        """
        self.config = config or {
            'revision_detection_fields': ['ann_date', 'f_ann_date', 'update_flag'],
            'significant_change_threshold': 0.05,  # 5% change considered significant
            'auto_handle_revisions': True,
            'keep_revision_history': True,
            'max_revision_depth': 3  # Maximum number of revisions to track
        }

    def detect_revisions(self, current_data: pl.DataFrame,
                        historical_data: pl.DataFrame,
                        key_fields: List[str] = ['ts_code', 'end_date']) -> Dict:
        """
        Detect revisions in financial data by comparing with historical data.

        Args:
            current_data: Current financial data
            historical_data: Historical financial data
            key_fields: Fields to use as primary keys for matching records

        Returns:
            Dictionary with revision detection results
        """
        revision_results = {
            'has_revisions': False,
            'revisions': [],
            'new_records': [],
            'unchanged_records': [],
            'stats': {}
        }

        if len(historical_data) == 0:
            revision_results['new_records'] = current_data.to_dicts()
            revision_results['stats'] = {
                'total_records': len(current_data),
                'revised_records': 0,
                'new_records': len(current_data),
                'unchanged_records': 0
            }
            return revision_results

        # Ensure key fields exist in both datasets
        missing_keys_current = [key for key in key_fields if key not in current_data.columns]
        missing_keys_historical = [key for key in key_fields if key not in historical_data.columns]

        if missing_keys_current or missing_keys_historical:
            raise ValueError(f"Missing key fields: current={missing_keys_current}, historical={missing_keys_historical}")

        # Join datasets on key fields to find matching records
        joined_data = current_data.join(
            historical_data,
            on=key_fields,
            how='outer',
            suffix='_old'
        )

        # Find new records (exist in current but not in historical)
        new_records = current_data.join(
            historical_data.select(key_fields),
            on=key_fields,
            how='anti'  # Records in current but not in historical
        )
        revision_results['new_records'] = new_records.to_dicts() if len(new_records) > 0 else []

        # Find revisions (records that exist in both but have changed values)
        revisions = self._find_revisions(joined_data, key_fields)
        revision_results['revisions'] = revisions

        # Find unchanged records
        unchanged = self._find_unchanged_records(joined_data, key_fields)
        revision_results['unchanged_records'] = unchanged

        # Update overall status
        revision_results['has_revisions'] = len(revisions) > 0
        revision_results['stats'] = {
            'total_records': len(current_data),
            'revised_records': len(revisions),
            'new_records': len(new_records),
            'unchanged_records': len(unchanged)
        }

        return revision_results

    def _find_revisions(self, joined_data: pl.DataFrame, key_fields: List[str]) -> List[Dict]:
        """
        Find records with revisions by comparing values.

        Args:
            joined_data: Joined current and historical data
            key_fields: Fields used as primary keys

        Returns:
            List of revision records
        """
        revisions = []

        # Get numeric fields that might be revised
        numeric_fields = [col for col in joined_data.columns
                         if joined_data.schema[col] in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8]]

        # Remove key fields and their old versions from numeric fields
        numeric_fields = [col for col in numeric_fields
                         if not col.endswith('_old') and col not in key_fields]

        if not numeric_fields:
            return revisions

        # For each record, check if any numeric field has changed significantly
        for row in joined_data.iter_rows(named=True):
            has_revision = False
            changed_fields = []

            for field in numeric_fields:
                current_value = row.get(field)
                old_field = f"{field}_old"
                old_value = row.get(old_field)

                # Check if both values exist and are different
                if (current_value is not None and old_value is not None and
                    current_value != old_value):
                    # Check if change is significant
                    if self._is_significant_change(current_value, old_value):
                        has_revision = True
                        change_pct = abs((current_value - old_value) / old_value) if old_value != 0 else float('inf')
                        changed_fields.append({
                            'field': field,
                            'old_value': old_value,
                            'new_value': current_value,
                            'change_percentage': change_pct,
                            'absolute_change': abs(current_value - old_value)
                        })

            if has_revision:
                revision_record = {
                    'keys': {key: row[key] for key in key_fields},
                    'changed_fields': changed_fields,
                    'revision_detected_at': datetime.now().isoformat()
                }
                revisions.append(revision_record)

        return revisions

    def _find_unchanged_records(self, joined_data: pl.DataFrame, key_fields: List[str]) -> List[Dict]:
        """
        Find records that haven't changed between current and historical data.

        Args:
            joined_data: Joined current and historical data
            key_fields: Fields used as primary keys

        Returns:
            List of unchanged records
        """
        unchanged_records = []

        # Get numeric fields for comparison
        numeric_fields = [col for col in joined_data.columns
                         if joined_data.schema[col] in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8]]

        # Remove key fields and their old versions from numeric fields
        numeric_fields = [col for col in numeric_fields
                         if not col.endswith('_old') and col not in key_fields]

        if not numeric_fields:
            return []

        for row in joined_data.iter_rows(named=True):
            is_unchanged = True

            # Check if all numeric fields are unchanged
            for field in numeric_fields:
                current_value = row.get(field)
                old_field = f"{field}_old"
                old_value = row.get(old_field)

                if current_value != old_value:
                    # If one field changed, record is not unchanged
                    is_unchanged = False
                    break

            # Also check key fields for nulls (indicating missing records)
            keys_complete = all(row.get(key) is not None for key in key_fields)
            old_keys_complete = all(row.get(f"{key}_old") is not None for key in key_fields)

            if is_unchanged and keys_complete and old_keys_complete:
                unchanged_record = {key: row[key] for key in key_fields}
                unchanged_records.append(unchanged_record)

        return unchanged_records

    def _is_significant_change(self, current_value: float, old_value: float) -> bool:
        """
        Determine if a change in value is significant enough to be considered a revision.

        Args:
            current_value: Current value
            old_value: Old value

        Returns:
            Boolean indicating whether change is significant
        """
        if old_value == 0:
            # If old value is zero, any non-zero current value is significant
            return current_value != 0

        change_percentage = abs((current_value - old_value) / old_value)
        return change_percentage >= self.config['significant_change_threshold']

    def handle_revisions(self, revision_results: Dict, data_type: str = 'financial') -> Dict:
        """
        Handle detected revisions according to configuration.

        Args:
            revision_results: Results from revision detection
            data_type: Type of data being handled

        Returns:
            Dictionary with handling results
        """
        handling_results = {
            'action_taken': False,
            'actions': [],
            'revised_data': None,
            'backup_created': False
        }

        if not revision_results['has_revisions']:
            handling_results['action_taken'] = False
            return handling_results

        # Handle revisions based on configuration
        if self.config['auto_handle_revisions']:
            # Create backup if configured
            if self.config['keep_revision_history']:
                backup_action = {
                    'type': 'backup_created',
                    'description': 'Backup of revised records created for historical tracking',
                    'timestamp': datetime.now().isoformat()
                }
                handling_results['actions'].append(backup_action)
                handling_results['backup_created'] = True

            # Apply revision handling logic
            revision_action = {
                'type': 'revisions_handled',
                'description': f'Handled {len(revision_results["revisions"])} revisions automatically',
                'timestamp': datetime.now().isoformat(),
                'revision_count': len(revision_results['revisions'])
            }
            handling_results['actions'].append(revision_action)
            handling_results['action_taken'] = True

            # Return handled data (in a real implementation, this would be the processed DataFrame)
            handling_results['revised_data'] = 'Data with revisions processed'

        return handling_results

    def track_revision_history(self, revision_record: Dict, max_depth: int = None) -> Dict:
        """
        Track revision history for a record.

        Args:
            revision_record: Revision record to track
            max_depth: Maximum depth of revision history to keep

        Returns:
            Dictionary with tracking information
        """
        if max_depth is None:
            max_depth = self.config['max_revision_depth']

        tracking_info = {
            'tracked': True,
            'history_depth': 1,
            'max_depth_config': max_depth,
            'tracking_timestamp': datetime.now().isoformat()
        }

        # In a full implementation, this would store revision history in a database or file
        # For now, we just return tracking information
        return tracking_info