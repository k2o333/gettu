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
    enhanced_yearly_partitioned_sink, enhanced_monthly_partitioned_sink,
    optimize_partition_storage, adjust_partition_strategy,
    manage_partition_metadata, manage_partition_lifecycle
)

class TestStorageIntegration(unittest.TestCase):
    """Test cases for storage module partition functionality integration"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = Path(tempfile.mkdtemp())

        # Create test data for integration testing
        self.test_data = pl.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ', '600000.SH', '000001.SZ'],
            'trade_date': ['20230101', '20230102', '20230201', '20240101'],
            'trade_date_int': [20230101, 20230102, 20230201, 20240101],
            'year': [2023, 2023, 2023, 2024],
            'year_month': [202301, 202301, 202302, 202401],
            'value': [100.0, 200.0, 300.0, 400.0]
        })

        self.financial_data = pl.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ'],
            'ann_date': ['20230101', '20230102'],
            'end_date': ['20221231', '20221231'],
            'revenue': [1000000.0, 2000000.0],
            'profit': [100000.0, 200000.0]
        })

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def test_storage_module_interface_compatibility(self):
        """Test storage module interface compatibility"""
        # Test atomic partitioned sink
        lazy_frame = self.test_data.lazy()
        atomic_partitioned_sink(lazy_frame, self.temp_dir, partition_by=['year'])

        # Verify partitions were created
        self.assertTrue((self.temp_dir / "year=2023").exists())
        self.assertTrue((self.temp_dir / "year=2024").exists())

    def test_partition_functionality_end_to_end_integration(self):
        """Test partition functionality end-to-end integration"""
        # Test yearly partitioning
        lazy_frame = self.test_data.lazy()
        enhanced_yearly_partitioned_sink(lazy_frame, self.temp_dir, partition_field='year')

        # Verify yearly partitions
        self.assertTrue((self.temp_dir / "year=2023").exists())
        self.assertTrue((self.temp_dir / "year=2024").exists())

        # Test monthly partitioning on a different directory
        monthly_dir = self.temp_dir / "monthly"
        enhanced_monthly_partitioned_sink(lazy_frame, monthly_dir, partition_field='year_month')

        # Verify monthly partitions
        self.assertTrue((monthly_dir / "year_month=202301").exists())
        self.assertTrue((monthly_dir / "year_month=202302").exists())
        self.assertTrue((monthly_dir / "year_month=202401").exists())

    def test_error_handling_and_degradation_mechanism(self):
        """Test error handling and degradation mechanism"""
        # Test with invalid partition field
        invalid_data = pl.DataFrame({
            'ts_code': ['000001.SZ'],
            'invalid_field': ['value']
        })

        lazy_frame = invalid_data.lazy()

        # This should raise an exception due to missing partition field
        with self.assertRaises(Exception):
            atomic_partitioned_sink(lazy_frame, self.temp_dir, partition_by=['year'])

    def test_partition_functionality_performance_optimization(self):
        """Test partition functionality performance optimization"""
        # Create larger dataset for performance testing
        large_data = pl.DataFrame({
            'ts_code': [f'{i:06d}.SZ' for i in range(1000)],
            'trade_date': [f'202301{i%30+1:02d}' for i in range(1000)],
            'year': [2023 for _ in range(1000)],
            'value': [float(i * 10) for i in range(1000)]
        })

        # Test partitioned storage
        lazy_frame = large_data.lazy()
        atomic_partitioned_sink(lazy_frame, self.temp_dir, partition_by=['year'])

        # Test optimization
        optimize_partition_storage(self.temp_dir, optimization_strategy="auto")

        # Verify optimization completed
        self.assertTrue((self.temp_dir / "year=2023").exists())

if __name__ == '__main__':
    unittest.main()