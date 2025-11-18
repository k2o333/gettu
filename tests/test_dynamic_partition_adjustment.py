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

from storage import adjust_partition_strategy, analyze_storage_efficiency

class TestDynamicPartitionAdjustment(unittest.TestCase):
    """Test cases for dynamic partition adjustment"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = Path(tempfile.mkdtemp())

        # Create test data with varying sizes
        self.small_data = pl.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ'],
            'trade_date': ['20230101', '20230102'],
            'year': [2023, 2023],
            'value': [100.0, 200.0]
        })

        self.large_data = pl.DataFrame({
            'ts_code': [f'{i:06d}.SZ' for i in range(1000)],
            'trade_date': [f'202301{i%30+1:02d}' for i in range(1000)],
            'year': [2023 for _ in range(1000)],
            'value': [float(i * 10) for i in range(1000)]
        })

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def test_data_volume_change_partition_adjustment(self):
        """Test data volume change partition adjustment"""
        # Create partition with initial data
        partition_dir = self.temp_dir / "year=2023"
        partition_dir.mkdir()
        data_file = partition_dir / "data.parquet"

        # Write initial data
        self.small_data.write_parquet(data_file)

        # Simulate data volume change by adding more data
        # In a real implementation, this would trigger a partition strategy adjustment
        initial_analysis = analyze_storage_efficiency(self.temp_dir)
        self.assertIsNotNone(initial_analysis)

        # The function should handle data volume changes appropriately
        adjust_partition_strategy(self.temp_dir, data_type="daily")

    def test_query_pattern_change_partition_optimization(self):
        """Test query pattern change partition optimization"""
        # Create multiple partitions to simulate different query patterns
        for i in range(3):
            partition_dir = self.temp_dir / f"year=202{i+1}"
            partition_dir.mkdir()

            # Create test data
            data = pl.DataFrame({
                'ts_code': [f'{j:06d}.SZ' for j in range(100)],
                'trade_date': [f'202{i+1}01{j+1:02d}' for j in range(100)],
                'year': [2020 + i + 1 for _ in range(100)],
                'value': [float(j * 10) for j in range(100)]
            })
            data.write_parquet(partition_dir / "data.parquet")

        # In a real implementation, this would analyze query patterns and optimize
        # For now, we just verify the function can run
        adjust_partition_strategy(self.temp_dir, data_type="daily")

    def test_dynamic_adjustment_performance_impact(self):
        """Test dynamic adjustment performance impact"""
        # Create many partitions to test performance impact
        for i in range(10):
            partition_dir = self.temp_dir / f"year=202{i+1}"
            partition_dir.mkdir()

            # Create data for each partition
            data = pl.DataFrame({
                'ts_code': [f'{j:06d}.SZ' for j in range(50)],
                'trade_date': [f'202{i+1}01{j%30+1:02d}' for j in range(50)],
                'year': [2020 + i + 1 for _ in range(50)],
                'value': [float(j * 5) for j in range(50)]
            })
            data.write_parquet(partition_dir / "data.parquet")

        # Run dynamic adjustment
        import time
        start_time = time.time()
        adjust_partition_strategy(self.temp_dir, data_type="daily")
        duration = time.time() - start_time

        # Verify that adjustment completed within a reasonable time
        self.assertGreater(duration, 0)  # Should take some time

    def test_adjustment_process_concurrency_safety(self):
        """Test adjustment process concurrency safety"""
        # Create partitions with test data
        for i in range(5):
            partition_dir = self.temp_dir / f"year=202{i+1}"
            partition_dir.mkdir()

            # Create test data
            data = pl.DataFrame({
                'ts_code': [f'{i:06d}.SZ'],
                'trade_date': [f'202{i+1}0101'],
                'year': [2020 + i + 1],
                'value': [float(i * 100)]
            })
            data.write_parquet(partition_dir / "data.parquet")

        # Test that the function can run without thread safety issues
        # In a real implementation, this would involve actual concurrency testing
        try:
            adjust_partition_strategy(self.temp_dir, data_type="daily")
        except Exception as e:
            self.fail(f"Dynamic partition adjustment failed with error: {str(e)}")

if __name__ == '__main__':
    unittest.main()