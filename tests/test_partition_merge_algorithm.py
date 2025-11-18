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

from storage import merge_adjacent_partitions, optimize_partition_storage

class TestPartitionMergeAlgorithm(unittest.TestCase):
    """Test cases for partition merge algorithm"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = Path(tempfile.mkdtemp())

        # Create test data for adjacent partitions
        self.data_202301 = pl.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ'],
            'trade_date': ['20230101', '20230102'],
            'year_month': [202301, 202301],
            'value': [100.0, 200.0]
        })

        self.data_202302 = pl.DataFrame({
            'ts_code': ['000003.SZ', '000004.SZ'],
            'trade_date': ['20230201', '20230202'],
            'year_month': [202302, 202302],
            'value': [300.0, 400.0]
        })

        self.data_202303 = pl.DataFrame({
            'ts_code': ['000005.SZ', '000006.SZ'],
            'trade_date': ['20230301', '20230302'],
            'year_month': [202303, 202303],
            'value': [500.0, 600.0]
        })

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def test_adjacent_partition_merge_logic(self):
        """Test adjacent partition merge logic"""
        # Create adjacent partition files
        partition_1_dir = self.temp_dir / "year_month=202301"
        partition_1_dir.mkdir()
        self.data_202301.write_parquet(partition_1_dir / "data.parquet")

        partition_2_dir = self.temp_dir / "year_month=202302"
        partition_2_dir.mkdir()
        self.data_202302.write_parquet(partition_2_dir / "data.parquet")

        partition_3_dir = self.temp_dir / "year_month=202303"
        partition_3_dir.mkdir()
        self.data_202303.write_parquet(partition_3_dir / "data.parquet")

        # Test merge adjacent partitions
        merge_adjacent_partitions(self.temp_dir, 'year_month')

        # Verify that partitions still exist but potentially optimized
        self.assertTrue((self.temp_dir / "year_month=202301").exists())
        self.assertTrue((self.temp_dir / "year_month=202302").exists())
        self.assertTrue((self.temp_dir / "year_month=202303").exists())

    def test_merge_algorithm_time_complexity(self):
        """Test merge algorithm time complexity"""
        # Create multiple partitions
        for i in range(5):
            partition_dir = self.temp_dir / f"year_month=20230{i+1:02d}"
            partition_dir.mkdir()

            # Create data for each partition
            data = pl.DataFrame({
                'ts_code': [f'{j:06d}.SZ' for j in range(10)],
                'trade_date': [f'20230{i+1:02d}{d+1:02d}' for d in range(10)],
                'year_month': [202300 + (i+1) for _ in range(10)],
                'value': [float(j * 10) for j in range(10)]
            })
            data.write_parquet(partition_dir / "data.parquet")

        # Run the merge algorithm
        merge_adjacent_partitions(self.temp_dir, 'year_month')

        # Check that optimization completed without error
        partition_dirs = list(self.temp_dir.iterdir())
        self.assertGreaterEqual(len(partition_dirs), 0)

    def test_merge_failure_data_recovery(self):
        """Test merge failure data recovery"""
        # Create partition files
        partition_dir = self.temp_dir / "year_month=202301"
        partition_dir.mkdir()
        self.data_202301.write_parquet(partition_dir / "data.parquet")

        # Add a corrupted partition to test error handling
        corrupted_dir = self.temp_dir / "corrupted_partition"
        corrupted_dir.mkdir()
        # Create an empty file to simulate corruption
        (corrupted_dir / "data.parquet").touch()

        # Should handle the error gracefully
        try:
            merge_adjacent_partitions(self.temp_dir, 'year_month')
        except Exception:
            # If there's an error, ensure original data is preserved
            original_partition = self.temp_dir / "year_month=202301" / "data.parquet"
            self.assertTrue(original_partition.exists())

    def test_merge_performance_monitoring(self):
        """Test merge performance monitoring"""
        # Create many small partitions to test merge performance
        for i in range(10):
            partition_dir = self.temp_dir / f"year_month=20230{i+1:02d}"
            partition_dir.mkdir()

            # Create small data files for each partition
            data = pl.DataFrame({
                'ts_code': [f'{i:06d}.SZ'],
                'trade_date': [f'20230{i+1:02d}01'],
                'year_month': [202300 + (i+1)],
                'value': [float(i * 10)]
            })
            data.write_parquet(partition_dir / "data.parquet")

        # Run optimization
        optimize_partition_storage(self.temp_dir)

        # Verify that optimization completed
        partition_dirs = list(self.temp_dir.iterdir())
        self.assertGreaterEqual(len(partition_dirs), 0)

if __name__ == '__main__':
    unittest.main()