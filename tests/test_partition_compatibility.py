import unittest
import tempfile
import os
from pathlib import Path
import sys
import shutil

# Add the app2 directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Mock polars for testing
import polars as pl

from storage import (
    atomic_partitioned_sink, atomic_unpartitioned_sink,
    enhanced_yearly_partitioned_sink, enhanced_monthly_partitioned_sink
)

class TestPartitionCompatibility(unittest.TestCase):
    """Test cases for partition compatibility"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = Path(tempfile.mkdtemp())

        # Create test data for compatibility testing
        self.test_data = pl.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ', '600000.SH'],
            'trade_date': ['20230101', '20230102', '20230103'],
            'trade_date_int': [20230101, 20230102, 20230103],
            'year': [2023, 2023, 2023],
            'year_month': [202301, 202301, 202301],
            'value': [100.0, 200.0, 300.0]
        })

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def test_backward_compatibility_old_data_format(self):
        """Test backward compatibility with old data format"""
        # Test that old partitioned data can still be read
        # Create old-style partitioned data
        old_partition_dir = self.temp_dir / "year=2023"
        old_partition_dir.mkdir()
        old_data_file = old_partition_dir / "data.parquet"
        self.test_data.write_parquet(old_data_file)

        # Verify that new functions can read old data
        loaded_data = pl.read_parquet(old_data_file)
        self.assertEqual(len(loaded_data), len(self.test_data))

    def test_forward_compatibility_new_data_format(self):
        """Test forward compatibility with new data format"""
        # Test that new partitioned data can be processed by old systems
        # This is a conceptual test since we don't have old systems to test against
        # but we verify that the data structure is compatible

        # Create new-style partitioned data
        lazy_frame = self.test_data.lazy()
        enhanced_yearly_partitioned_sink(lazy_frame, self.temp_dir, partition_field='year')

        # Verify that the partition structure is standard
        partition_dir = self.temp_dir / "year=2023"
        self.assertTrue(partition_dir.exists())

        data_file = partition_dir / "data.parquet"
        self.assertTrue(data_file.exists())

    def test_mixed_partition_strategy_compatibility(self):
        """Test compatibility with mixed partition strategies"""
        # Test that different partition strategies can coexist
        # Create yearly partitioned data
        yearly_data = pl.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ'],
            'trade_date': ['20230101', '20230102'],
            'year': [2023, 2023],
            'value': [100.0, 200.0]
        })

        lazy_frame_yearly = yearly_data.lazy()
        enhanced_yearly_partitioned_sink(lazy_frame_yearly, self.temp_dir / "yearly_data", partition_field='year')

        # Create monthly partitioned data in a different location
        monthly_data = pl.DataFrame({
            'ts_code': ['600000.SH', '600001.SH'],
            'trade_date': ['20230101', '20230201'],
            'year_month': [202301, 202302],
            'value': [300.0, 400.0]
        })

        lazy_frame_monthly = monthly_data.lazy()
        enhanced_monthly_partitioned_sink(lazy_frame_monthly, self.temp_dir / "monthly_data", partition_field='year_month')

        # Verify both partition structures exist
        self.assertTrue((self.temp_dir / "yearly_data" / "year=2023").exists())
        self.assertTrue((self.temp_dir / "monthly_data" / "year_month=202301").exists())
        self.assertTrue((self.temp_dir / "monthly_data" / "year_month=202302").exists())

if __name__ == '__main__':
    unittest.main()