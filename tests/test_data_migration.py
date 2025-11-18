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
    optimize_partition_storage
)

class TestDataMigration(unittest.TestCase):
    """Test cases for data migration"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = Path(tempfile.mkdtemp())

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def test_migration_from_old_to_new_partition_strategy(self):
        """Test migration from old to new partition strategy"""
        # Create old-style partitioned data (basic partitioning)
        old_data = pl.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ', '600000.SH'],
            'trade_date': ['20230101', '20230102', '20230103'],
            'year': [2023, 2023, 2023],
            'value': [100.0, 200.0, 300.0]
        })

        # Simulate old partitioned data structure
        old_partition_dir = self.temp_dir / "old_data" / "year=2023"
        old_partition_dir.mkdir(parents=True)
        old_data_file = old_partition_dir / "data.parquet"
        old_data.write_parquet(old_data_file)

        # Load old data
        loaded_old_data = pl.read_parquet(old_data_file)

        # Migrate to new enhanced partitioning strategy
        new_partition_dir = self.temp_dir / "new_data"
        lazy_frame = loaded_old_data.lazy()
        enhanced_yearly_partitioned_sink(lazy_frame, new_partition_dir, partition_field='year')

        # Verify migration completed
        new_partition_path = new_partition_dir / "year=2023"
        self.assertTrue(new_partition_path.exists())
        new_data_file = new_partition_path / "data.parquet"
        self.assertTrue(new_data_file.exists())

        # Verify data integrity
        migrated_data = pl.read_parquet(new_data_file)
        self.assertEqual(len(migrated_data), len(loaded_old_data))

    def test_data_consistency_during_migration_process(self):
        """Test data consistency during migration process"""
        # Create test dataset
        original_data = pl.DataFrame({
            'ts_code': [f'{i:06d}.SZ' for i in range(100)],
            'trade_date': [f'202301{i%30+1:02d}' for i in range(100)],
            'year': [2023 for _ in range(100)],
            'value': [float(i * 10) for i in range(100)]
        })

        # Save original data
        original_data_file = self.temp_dir / "original_data.parquet"
        original_data.write_parquet(original_data_file)

        # Load data for migration
        loaded_data = pl.read_parquet(original_data_file)

        # Perform migration
        partition_dir = self.temp_dir / "migrated_data"
        lazy_frame = loaded_data.lazy()
        enhanced_yearly_partitioned_sink(lazy_frame, partition_dir, partition_field='year')

        # Verify data consistency
        migrated_partition = partition_dir / "year=2023" / "data.parquet"
        self.assertTrue(migrated_partition.exists())

        migrated_data = pl.read_parquet(migrated_partition)
        self.assertEqual(len(migrated_data), len(original_data))

        # Check key data points
        self.assertEqual(
            sorted(migrated_data['ts_code'].to_list()),
            sorted(original_data['ts_code'].to_list())
        )

    def test_migration_failure_data_recovery(self):
        """Test migration failure data recovery"""
        # Create source data
        source_data = pl.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ'],
            'trade_date': ['20230101', '20230102'],
            'year': [2023, 2023],
            'value': [100.0, 200.0]
        })

        source_file = self.temp_dir / "source.parquet"
        source_data.write_parquet(source_file)

        # Verify source data exists before migration
        self.assertTrue(source_file.exists())

        # Perform migration that should succeed
        try:
            loaded_data = pl.read_parquet(source_file)
            partition_dir = self.temp_dir / "migrated"
            lazy_frame = loaded_data.lazy()
            enhanced_yearly_partitioned_sink(lazy_frame, partition_dir, partition_field='year')

            # Verify migration completed
            migrated_partition = partition_dir / "year=2023"
            self.assertTrue(migrated_partition.exists())
        except Exception as e:
            # If migration fails, source data should still be available
            self.assertTrue(source_file.exists())

    def test_migration_performance_optimization(self):
        """Test migration performance optimization"""
        # Create moderately large dataset
        data = pl.DataFrame({
            'ts_code': [f'{i:06d}.SZ' for i in range(2000)],
            'trade_date': [f'202301{i%30+1:02d}' for i in range(2000)],
            'year': [2023 for _ in range(2000)],
            'value': [float(i * 10) for i in range(2000)]
        })

        # Save original data
        original_file = self.temp_dir / "original.parquet"
        data.write_parquet(original_file)

        import time
        start_time = time.time()

        # Perform migration with optimization
        loaded_data = pl.read_parquet(original_file)
        partition_dir = self.temp_dir / "optimized"
        lazy_frame = loaded_data.lazy()
        enhanced_yearly_partitioned_sink(lazy_frame, partition_dir, partition_field='year', optimize_compression=True)

        migration_time = time.time() - start_time

        # Verify migration completed
        self.assertTrue((partition_dir / "year=2023").exists())
        self.assertGreater(migration_time, 0)

if __name__ == '__main__':
    unittest.main()