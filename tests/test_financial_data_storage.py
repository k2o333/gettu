import unittest
import tempfile
import os
from pathlib import Path
import sys
import shutil

# Add the app2 directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Mock polars for testing
import pandas as pd
import polars as pl

from storage import atomic_unpartitioned_sink

class TestFinancialDataStorage(unittest.TestCase):
    """Test cases for financial data no-partition storage"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.test_data = pl.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ', '600000.SH'],
            'ann_date': ['20230101', '20230102', '20230103'],
            'end_date': ['20221231', '20221231', '20221231'],
            'revenue': [1000000.0, 2000000.0, 3000000.0],
            'profit': [100000.0, 200000.0, 300000.0]
        })

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def test_financial_data_single_file_storage(self):
        """Test financial data single file storage functionality"""
        output_path = self.temp_dir / "financial_data.parquet"

        # Test atomic unpartitioned sink
        lazy_frame = self.test_data.lazy()
        atomic_unpartitioned_sink(lazy_frame, str(output_path))

        # Verify file was created
        self.assertTrue(output_path.exists())

        # Verify data integrity
        loaded_data = pl.read_parquet(output_path)
        self.assertEqual(len(loaded_data), len(self.test_data))

        # Check a few key values
        self.assertEqual(loaded_data['ts_code'][0], self.test_data['ts_code'][0])
        self.assertEqual(loaded_data['revenue'][0], self.test_data['revenue'][0])

    def test_large_file_handling_performance(self):
        """Test large file handling performance"""
        # Create larger test dataset
        large_data = pl.DataFrame({
            'ts_code': [f'{i:06d}.SZ' for i in range(10000)],
            'ann_date': ['20230101'] * 10000,
            'end_date': ['20221231'] * 10000,
            'revenue': [float(i * 1000) for i in range(10000)],
            'profit': [float(i * 100) for i in range(10000)]
        })

        output_path = self.temp_dir / "large_financial_data.parquet"
        lazy_frame = large_data.lazy()

        # This should not raise any exceptions
        atomic_unpartitioned_sink(lazy_frame, str(output_path))

        # Verify file was created
        self.assertTrue(output_path.exists())

        # Verify data count
        loaded_data = pl.read_parquet(output_path)
        self.assertEqual(len(loaded_data), 10000)

    def test_file_integrity_verification(self):
        """Test file integrity verification"""
        output_path = self.temp_dir / "financial_data.parquet"

        # Store data
        lazy_frame = self.test_data.lazy()
        atomic_unpartitioned_sink(lazy_frame, str(output_path))

        # Verify file can be read without errors
        try:
            loaded_data = pl.read_parquet(output_path)
            # Basic integrity checks
            self.assertGreater(len(loaded_data), 0)
            self.assertIn('ts_code', loaded_data.columns)
            self.assertIn('revenue', loaded_data.columns)
        except Exception as e:
            self.fail(f"File integrity check failed: {str(e)}")

if __name__ == '__main__':
    unittest.main()