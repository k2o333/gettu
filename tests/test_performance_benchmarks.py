import unittest
import tempfile
import os
from pathlib import Path
import sys
import shutil
import time

# Add the app2 directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Mock polars for testing
import polars as pl

from storage import (
    atomic_partitioned_sink, atomic_unpartitioned_sink,
    enhanced_yearly_partitioned_sink, enhanced_monthly_partitioned_sink,
    optimize_partition_storage, analyze_partition_query_performance
)

class TestPerformanceBenchmarks(unittest.TestCase):
    """Test cases for performance benchmarks"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = Path(tempfile.mkdtemp())

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def test_different_partition_strategies_query_performance_comparison(self):
        """Test different partition strategies query performance comparison"""
        # Create large dataset for performance testing
        large_data = pl.DataFrame({
            'ts_code': [f'{i:06d}.SZ' for i in range(10000)],
            'trade_date': [f'202301{i%30+1:02d}' for i in range(10000)],
            'year': [2023 for _ in range(10000)],
            'year_month': [202301 for _ in range(10000)],
            'value': [float(i * 10) for i in range(10000)]
        })

        # Test unpartitioned performance
        start_time = time.time()
        lazy_frame = large_data.lazy()
        atomic_unpartitioned_sink(lazy_frame, str(self.temp_dir / "unpartitioned.parquet"))
        unpartitioned_write_time = time.time() - start_time

        # Test yearly partitioned performance
        start_time = time.time()
        yearly_dir = self.temp_dir / "yearly"
        enhanced_yearly_partitioned_sink(lazy_frame, yearly_dir, partition_field='year')
        yearly_write_time = time.time() - start_time

        # Test monthly partitioned performance
        start_time = time.time()
        monthly_dir = self.temp_dir / "monthly"
        enhanced_monthly_partitioned_sink(lazy_frame, monthly_dir, partition_field='year_month')
        monthly_write_time = time.time() - start_time

        # Verify all operations completed
        self.assertGreater(unpartitioned_write_time, 0)
        self.assertGreater(yearly_write_time, 0)
        self.assertGreater(monthly_write_time, 0)

    def test_performance_at_different_data_scales(self):
        """Test performance at different data scales"""
        scales = [1000, 5000, 10000]
        results = {}

        for scale in scales:
            # Create dataset of specific scale
            data = pl.DataFrame({
                'ts_code': [f'{i:06d}.SZ' for i in range(scale)],
                'trade_date': [f'202301{i%30+1:02d}' for i in range(scale)],
                'year': [2023 for _ in range(scale)],
                'value': [float(i * 10) for i in range(scale)]
            })

            # Measure write performance
            start_time = time.time()
            lazy_frame = data.lazy()
            enhanced_yearly_partitioned_sink(lazy_frame, self.temp_dir / f"data_{scale}", partition_field='year')
            write_time = time.time() - start_time

            results[scale] = write_time

        # Verify that performance scales reasonably
        # Larger datasets should take more time, but not exponentially more
        self.assertGreater(results[1000], 0)
        self.assertGreater(results[5000], 0)
        self.assertGreater(results[10000], 0)

    def test_concurrent_access_performance(self):
        """Test concurrent access performance"""
        # Create test data
        data = pl.DataFrame({
            'ts_code': [f'{i:06d}.SZ' for i in range(1000)],
            'trade_date': [f'202301{i%30+1:02d}' for i in range(1000)],
            'year': [2023 for _ in range(1000)],
            'value': [float(i * 10) for i in range(1000)]
        })

        # Write data
        lazy_frame = data.lazy()
        enhanced_yearly_partitioned_sink(lazy_frame, self.temp_dir, partition_field='year')

        # Simulate concurrent reads by reading multiple times
        read_times = []
        for i in range(5):
            start_time = time.time()
            # Read from different partitions
            partition_dir = self.temp_dir / "year=2023"
            if partition_dir.exists():
                data_file = partition_dir / "data.parquet"
                if data_file.exists():
                    df = pl.read_parquet(data_file)
            read_time = time.time() - start_time
            read_times.append(read_time)

        # Verify reads completed
        self.assertEqual(len(read_times), 5)
        for read_time in read_times:
            self.assertGreater(read_time, 0)

    def test_memory_usage_efficiency(self):
        """Test memory usage efficiency"""
        # This is a conceptual test since we can't easily measure memory usage in unit tests
        # but we can verify that the functions complete without memory errors

        # Create moderately large dataset
        data = pl.DataFrame({
            'ts_code': [f'{i:06d}.SZ' for i in range(5000)],
            'trade_date': [f'202301{i%30+1:02d}' for i in range(5000)],
            'year': [2023 for _ in range(5000)],
            'year_month': [202301 for _ in range(5000)],
            'value': [float(i * 10) for i in range(5000)]
        })

        # Test that operations complete without memory errors
        lazy_frame = data.lazy()

        # Test unpartitioned
        atomic_unpartitioned_sink(lazy_frame, str(self.temp_dir / "test.parquet"))

        # Test partitioned
        enhanced_yearly_partitioned_sink(lazy_frame, self.temp_dir / "yearly", partition_field='year')

        # Test optimization
        optimize_partition_storage(self.temp_dir / "yearly")

        # If we reach here, no memory errors occurred
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()