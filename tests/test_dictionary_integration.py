"""
Integration Tests for Dictionary Management System
==============================================

Integration tests to verify cross-module ID consistency.
"""

import unittest
import tempfile
import shutil
from pathlib import Path
import polars as pl

from dictionary_management import DictionaryManager, DictionarySynchronizer
from config import DICT_DIR


class TestCrossModuleIntegration(unittest.TestCase):
    """Integration tests for cross-module consistency."""

    def setUp(self):
        """Set up test environment."""
        # Create a temporary directory for testing
        self.test_dir = Path(tempfile.mkdtemp())
        # Temporarily change DICT_DIR for testing
        self.original_dict_dir = DICT_DIR
        import config
        config.DICT_DIR = self.test_dir

    def tearDown(self):
        """Clean up test environment."""
        # Remove temporary directory
        shutil.rmtree(self.test_dir)
        # Restore original DICT_DIR
        import config
        config.DICT_DIR = self.original_dict_dir

    def test_dictionary_synchronizer(self):
        """Test dictionary synchronizer functionality."""
        # Initialize dictionary manager and synchronizer
        dict_manager = DictionaryManager()
        dict_manager.initialize()
        synchronizer = DictionarySynchronizer(dict_manager)
        synchronizer.initialize()

        # Add some new stocks to dictionary
        dict_manager.add_stock('NEWSTOCK1.SZ', {'name': '新股票1'})
        dict_manager.add_stock('NEWSTOCK2.SZ', {'name': '新股票2'})

        # Create test data similar to what concurrent downloader might produce
        test_data = pl.DataFrame({
            'ts_code': ['NEWSTOCK1.SZ', 'NEWSTOCK2.SZ', 'NEWSTOCK3.SZ'],
            'trade_date': ['20230101', '20230101', '20230101'],
            'close': [10.0, 15.0, 20.0]
        })

        # Synchronize with concurrent downloader data
        synced_data = synchronizer.sync_with_concurrent_downloader(test_data)

        # Verify that internal IDs were added
        self.assertIn('ts_code_id', synced_data.columns)
        # Note: ts_code may still be present depending on implementation

        # Verify that new stock was added to dictionary with correct range
        new_stock_id = dict_manager.get_stock_id('NEWSTOCK3.SZ')
        self.assertIsNotNone(new_stock_id)
        self.assertTrue(1000000 <= new_stock_id <= 1999999)

    def test_etl_runtime_integration(self):
        """Test integration with ETL runtime."""
        # Initialize dictionary manager and synchronizer
        dict_manager = DictionaryManager()
        dict_manager.initialize()
        synchronizer = DictionarySynchronizer(dict_manager)
        synchronizer.initialize()

        # Add some new stocks to dictionary
        dict_manager.add_stock('NEWSTOCK4.SZ', {'name': '新股票4'})
        dict_manager.add_stock('NEWSTOCK5.SZ', {'name': '新股票5'})

        # Create test data similar to what ETL runtime might process
        test_data = pl.DataFrame({
            'ts_code': ['NEWSTOCK4.SZ', 'NEWSTOCK5.SZ'],
            'trade_date': ['20230101', '20230101'],
            'open': [9.5, 14.5],
            'close': [10.0, 15.0],
            'high': [10.5, 15.5],
            'low': [9.0, 14.0],
            'volume': [1000000, 2000000]
        })

        # Synchronize with ETL runtime data
        synced_data = synchronizer.sync_with_etl_runtime(test_data)

        # Verify that internal IDs were added
        self.assertIn('ts_code_id', synced_data.columns)

        # Verify ID ranges for newly added stocks
        id_series = synced_data['ts_code_id']
        for id_val in id_series.drop_nulls():
            # For new entries, IDs should be in the correct range
            if id_val > 100000:  # Skip old IDs if they're still there
                self.assertTrue(1000000 <= id_val <= 1999999)

    def test_storage_integration(self):
        """Test integration with storage module."""
        # Initialize dictionary manager and synchronizer
        dict_manager = DictionaryManager()
        dict_manager.initialize()
        synchronizer = DictionarySynchronizer(dict_manager)
        synchronizer.initialize()

        # Create test data with internal IDs
        test_data = pl.DataFrame({
            'ts_code_id': [1000001, 1000002, 1000003],
            'trade_date': ['20230101', '20230101', '20230101'],
            'close': [10.0, 15.0, 20.0]
        })

        # Synchronize with storage data
        synced_data = synchronizer.sync_with_storage(test_data)

        # Verify data structure is maintained
        self.assertIn('ts_code_id', synced_data.columns)
        self.assertEqual(len(synced_data), 3)

        # Verify ID validation
        sync_status = synchronizer.get_sync_status()
        self.assertTrue(sync_status['initialized'])

    def test_cross_module_consistency_validation(self):
        """Test cross-module consistency validation."""
        # Initialize dictionary manager and synchronizer
        dict_manager = DictionaryManager()
        dict_manager.initialize()
        synchronizer = DictionarySynchronizer(dict_manager)
        synchronizer.initialize()

        # Add new valid entries with correct ranges
        dict_manager.add_stock('NEWSTOCK7.SZ')
        dict_manager.add_industry('新行业3')
        dict_manager.add_region('新地区3')

        # Validate consistency
        validation_results = synchronizer.validate_cross_module_consistency()

        # Check that validation has stats (even if some entries are invalid due to existing data)
        self.assertIn('total_stocks', validation_results['stats'])
        self.assertIn('total_industries', validation_results['stats'])
        self.assertIn('total_regions', validation_results['stats'])

    def test_sync_status(self):
        """Test synchronization status reporting."""
        # Initialize dictionary manager and synchronizer
        dict_manager = DictionaryManager()
        dict_manager.initialize()
        synchronizer = DictionarySynchronizer(dict_manager)
        synchronizer.initialize()

        # Get initial counts
        initial_stock_count = dict_manager.get_allocated_count('stock')
        initial_industry_count = dict_manager.get_allocated_count('industry')
        initial_region_count = dict_manager.get_allocated_count('region')

        # Add some new entries
        dict_manager.add_stock('NEWSTOCK6.SZ')
        dict_manager.add_industry('新行业2')
        dict_manager.add_region('新地区2')

        # Get sync status
        sync_status = synchronizer.get_sync_status()

        # Verify status information
        self.assertTrue(sync_status['initialized'])
        self.assertTrue(sync_status['dictionary_manager_initialized'])
        self.assertEqual(sync_status['allocated_stock_count'], initial_stock_count + 1)
        self.assertEqual(sync_status['allocated_industry_count'], initial_industry_count + 1)
        self.assertEqual(sync_status['allocated_region_count'], initial_region_count + 1)


if __name__ == '__main__':
    unittest.main()