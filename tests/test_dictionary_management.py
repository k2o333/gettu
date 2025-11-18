"""
Test Dictionary Management System
===============================

Unit tests for the dictionary management system implementation.
"""

import unittest
import tempfile
import shutil
from pathlib import Path
import polars as pl

from dictionary_management import DictionaryManager
from config import DICT_DIR


class TestDictionaryManagement(unittest.TestCase):
    """Test cases for the dictionary management system."""

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

    def test_id_allocation_ranges(self):
        """Test that new IDs are allocated within correct ranges."""
        dict_manager = DictionaryManager()
        dict_manager.initialize()

        # Test stock ID allocation for new entries
        stock_id = dict_manager.add_stock('NEWSTOCK.SZ')
        self.assertTrue(1000000 <= stock_id <= 1999999)
        self.assertTrue(dict_manager.validate_id('stock', stock_id))

        # Test industry ID allocation for new entries
        industry_id = dict_manager.add_industry('新行业', 'New Industry')
        self.assertTrue(2000000 <= industry_id <= 2999999)
        self.assertTrue(dict_manager.validate_id('industry', industry_id))

        # Test region ID allocation for new entries
        region_id = dict_manager.add_region('新地区', 'New Region')
        self.assertTrue(3000000 <= region_id <= 3999999)
        self.assertTrue(dict_manager.validate_id('region', region_id))

    def test_bidirectional_mapping(self):
        """Test bidirectional mapping functionality."""
        dict_manager = DictionaryManager()
        dict_manager.initialize()

        # Test stock mapping
        original_code = '000001.SZ'
        stock_id = dict_manager.add_stock(original_code)

        retrieved_code = dict_manager.get_stock_code(stock_id)
        retrieved_id = dict_manager.get_stock_id(original_code)

        self.assertEqual(original_code, retrieved_code)
        self.assertEqual(stock_id, retrieved_id)

        # Test industry mapping
        original_industry = '金融'
        industry_id = dict_manager.add_industry(original_industry)

        retrieved_industry = dict_manager.get_industry_code(industry_id)
        retrieved_industry_id = dict_manager.get_industry_id(original_industry)

        self.assertEqual(original_industry, retrieved_industry)
        self.assertEqual(industry_id, retrieved_industry_id)

        # Test region mapping
        original_region = '北京'
        region_id = dict_manager.add_region(original_region)

        retrieved_region = dict_manager.get_region_code(region_id)
        retrieved_region_id = dict_manager.get_region_id(original_region)

        self.assertEqual(original_region, retrieved_region)
        self.assertEqual(region_id, retrieved_region_id)

    def test_persistence(self):
        """Test that allocations persist across sessions."""
        dict_manager = DictionaryManager()
        dict_manager.initialize()

        # Add some entries
        stock_id = dict_manager.add_stock('000001.SZ', {'name': '平安银行'})
        industry_id = dict_manager.add_industry('金融', 'Finance')
        region_id = dict_manager.add_region('深圳', 'Shenzhen')

        # Create a new instance to test persistence
        dict_manager2 = DictionaryManager()
        dict_manager2.initialize()

        # Check that the same IDs are returned
        retrieved_stock_id = dict_manager2.get_stock_id('000001.SZ')
        retrieved_industry_id = dict_manager2.get_industry_id('金融')
        retrieved_region_id = dict_manager2.get_region_id('深圳')

        self.assertEqual(stock_id, retrieved_stock_id)
        self.assertEqual(industry_id, retrieved_industry_id)
        self.assertEqual(region_id, retrieved_region_id)

    def test_allocated_counts(self):
        """Test allocated count functionality."""
        dict_manager = DictionaryManager()
        dict_manager.initialize()

        # Check initial counts (should include existing data)
        initial_stock_count = dict_manager.get_allocated_count('stock')
        initial_industry_count = dict_manager.get_allocated_count('industry')
        initial_region_count = dict_manager.get_allocated_count('region')

        # Add some entries
        dict_manager.add_stock('NEWSTOCK.SZ')
        dict_manager.add_stock('NEWSTOCK2.SZ')
        dict_manager.add_industry('新行业')
        dict_manager.add_region('新地区')

        # Check counts after adding
        self.assertEqual(dict_manager.get_allocated_count('stock'), initial_stock_count + 2)
        self.assertEqual(dict_manager.get_allocated_count('industry'), initial_industry_count + 1)
        self.assertEqual(dict_manager.get_allocated_count('region'), initial_region_count + 1)

    def test_next_available_id(self):
        """Test next available ID functionality."""
        dict_manager = DictionaryManager()
        dict_manager.initialize()

        # Get initial next available IDs
        initial_stock_next = dict_manager.get_next_available_id('stock')

        # Add an entry and check next ID increments
        # Use a new stock code to ensure we get a new ID
        dict_manager.add_stock('NEWSTOCK3.SZ')
        new_stock_next = dict_manager.get_next_available_id('stock')

        # The new ID should be one more than the allocated ID
        allocated_id = dict_manager.get_stock_id('NEWSTOCK3.SZ')
        self.assertEqual(new_stock_next, allocated_id + 1)

    def test_version_control(self):
        """Test version control functionality."""
        dict_manager = DictionaryManager()
        dict_manager.initialize()

        # Get initial version count
        initial_history = dict_manager.get_version_history('stock')
        initial_count = len(initial_history)

        # Create a version
        version_id = dict_manager.create_dictionary_version('stock', 'test content', {'test': True})

        # Check current version
        current_version = dict_manager.get_current_version('stock')
        self.assertIsNotNone(current_version)
        self.assertEqual(current_version['version_id'], version_id)

        # Check version history
        history = dict_manager.get_version_history('stock')
        self.assertEqual(len(history), initial_count + 1)
        self.assertEqual(history[-1]['version_id'], version_id)


if __name__ == '__main__':
    unittest.main()