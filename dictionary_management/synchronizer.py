"""
Dictionary Synchronizer Module
=============================

This module provides synchronization functionality to ensure dictionary consistency
across different modules and systems.
"""

import logging
from typing import Dict, Any, Optional
import polars as pl
from config import DICT_DIR

from .dictionary_manager import DictionaryManager


class DictionarySynchronizer:
    """
    Ensures dictionary consistency across different modules and systems.
    """

    def __init__(self, dictionary_manager: DictionaryManager):
        self.dictionary_manager = dictionary_manager
        self._initialized = False

    def initialize(self):
        """
        Initialize the dictionary synchronizer.
        """
        if self._initialized:
            return

        # Ensure dictionary manager is initialized
        self.dictionary_manager.initialize()
        self._initialized = True

    def sync_with_concurrent_downloader(self, downloader_data: pl.DataFrame) -> pl.DataFrame:
        """
        Synchronize dictionary with concurrent downloader data by adding internal IDs.

        Args:
            downloader_data: Data from concurrent downloader

        Returns:
            Data with internal IDs added
        """
        if not self._initialized:
            self.initialize()

        try:
            # Add internal IDs to stock codes if present
            if 'ts_code' in downloader_data.columns:
                # Create a mapping from ts_code to internal ID
                ts_code_to_id = {}
                for ts_code in downloader_data['ts_code'].unique().to_list():
                    internal_id = self.dictionary_manager.get_stock_id(ts_code)
                    if internal_id is not None:
                        ts_code_to_id[ts_code] = internal_id
                    else:
                        # Add new stock to dictionary
                        internal_id = self.dictionary_manager.add_stock(ts_code)
                        ts_code_to_id[ts_code] = internal_id

                # Add internal ID column to data
                downloader_data = downloader_data.with_columns([
                    pl.col('ts_code').map_elements(
                        lambda x: ts_code_to_id.get(x, None),
                        return_dtype=pl.Int64
                    ).alias('ts_code_id')
                ])

            logging.debug(f"Synchronized downloader data with {len(downloader_data)} records")
            return downloader_data

        except Exception as e:
            logging.error(f"Error synchronizing with concurrent downloader: {e}")
            return downloader_data

    def sync_with_etl_runtime(self, etl_data: pl.DataFrame) -> pl.DataFrame:
        """
        Synchronize dictionary with ETL runtime data by adding internal IDs.

        Args:
            etl_data: Data from ETL runtime

        Returns:
            Data with internal IDs added
        """
        if not self._initialized:
            self.initialize()

        try:
            # Add internal IDs to stock codes if present
            if 'ts_code' in etl_data.columns:
                # Create a mapping from ts_code to internal ID
                ts_code_to_id = {}
                for ts_code in etl_data['ts_code'].unique().to_list():
                    internal_id = self.dictionary_manager.get_stock_id(ts_code)
                    if internal_id is not None:
                        ts_code_to_id[ts_code] = internal_id
                    else:
                        # Add new stock to dictionary
                        internal_id = self.dictionary_manager.add_stock(ts_code)
                        ts_code_to_id[ts_code] = internal_id

                # Add internal ID column to data and remove external code
                etl_data = etl_data.with_columns([
                    pl.col('ts_code').map_elements(
                        lambda x: ts_code_to_id.get(x, None),
                        return_dtype=pl.Int64
                    ).alias('ts_code_id')
                ]).drop('ts_code')

            logging.debug(f"Synchronized ETL data with {len(etl_data)} records")
            return etl_data

        except Exception as e:
            logging.error(f"Error synchronizing with ETL runtime: {e}")
            return etl_data

    def sync_with_storage(self, storage_data: pl.DataFrame) -> pl.DataFrame:
        """
        Synchronize dictionary with storage data by ensuring ID consistency.

        Args:
            storage_data: Data from storage

        Returns:
            Data with consistent IDs
        """
        if not self._initialized:
            self.initialize()

        try:
            # Validate existing IDs if present
            if 'ts_code_id' in storage_data.columns:
                # Validate that all IDs are within the correct range
                invalid_ids = storage_data.filter(
                    ~pl.col('ts_code_id').is_between(1000000, 1999999, closed='both')
                )
                if len(invalid_ids) > 0:
                    logging.warning(f"Found {len(invalid_ids)} records with invalid stock IDs")

            logging.debug(f"Synchronized storage data with {len(storage_data)} records")
            return storage_data

        except Exception as e:
            logging.error(f"Error synchronizing with storage: {e}")
            return storage_data

    def validate_cross_module_consistency(self) -> Dict[str, Any]:
        """
        Validate ID consistency across all modules.

        Returns:
            Validation results
        """
        if not self._initialized:
            self.initialize()

        results = {
            'valid': True,
            'issues': [],
            'stats': {}
        }

        try:
            # Load stock dictionary
            stock_path = DICT_DIR / 'stock_basic_dict.parquet'
            if stock_path.exists():
                stock_df = pl.read_parquet(stock_path)
                results['stats']['total_stocks'] = len(stock_df)

                # Validate ID ranges
                if 'ts_code_id' in stock_df.columns:
                    invalid_ids = stock_df.filter(
                        ~pl.col('ts_code_id').is_between(1000000, 1999999, closed='both')
                    )
                    if len(invalid_ids) > 0:
                        results['valid'] = False
                        results['issues'].append(f"Found {len(invalid_ids)} stocks with invalid IDs")
                        logging.warning(f"Dictionary validation found {len(invalid_ids)} invalid stock IDs")

            # Validate industry dictionary
            industry_path = DICT_DIR / 'industry_dict.parquet'
            if industry_path.exists():
                industry_df = pl.read_parquet(industry_path)
                results['stats']['total_industries'] = len(industry_df)

                # Validate ID ranges
                if 'industry_id' in industry_df.columns:
                    invalid_ids = industry_df.filter(
                        ~pl.col('industry_id').is_between(2000000, 2999999, closed='both')
                    )
                    if len(invalid_ids) > 0:
                        results['valid'] = False
                        results['issues'].append(f"Found {len(invalid_ids)} industries with invalid IDs")
                        logging.warning(f"Dictionary validation found {len(invalid_ids)} invalid industry IDs")

            # Validate region dictionary
            region_path = DICT_DIR / 'area_dict.parquet'
            if region_path.exists():
                region_df = pl.read_parquet(region_path)
                results['stats']['total_regions'] = len(region_df)

                # Validate ID ranges
                if 'area_id' in region_df.columns:
                    invalid_ids = region_df.filter(
                        ~pl.col('area_id').is_between(3000000, 3999999, closed='both')
                    )
                    if len(invalid_ids) > 0:
                        results['valid'] = False
                        results['issues'].append(f"Found {len(invalid_ids)} regions with invalid IDs")
                        logging.warning(f"Dictionary validation found {len(invalid_ids)} invalid region IDs")

            logging.info(f"Cross-module consistency validation completed: {results}")
            return results

        except Exception as e:
            logging.error(f"Error validating cross-module consistency: {e}")
            results['valid'] = False
            results['issues'].append(f"Validation error: {str(e)}")
            return results

    def get_sync_status(self) -> Dict[str, Any]:
        """
        Get the current synchronization status.

        Returns:
            Sync status information
        """
        if not self._initialized:
            self.initialize()

        return {
            'initialized': self._initialized,
            'dictionary_manager_initialized': self.dictionary_manager._initialized,
            'allocated_stock_count': self.dictionary_manager.get_allocated_count('stock'),
            'allocated_industry_count': self.dictionary_manager.get_allocated_count('industry'),
            'allocated_region_count': self.dictionary_manager.get_allocated_count('region')
        }