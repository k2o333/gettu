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

from storage import manage_partition_metadata, manage_partition_lifecycle, manage_partition_access_control

class TestPartitionLevelManagement(unittest.TestCase):
    """Test cases for partition-level data management"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = Path(tempfile.mkdtemp())

        # Create test data for partition management
        self.test_data = pl.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ', '600000.SH'],
            'trade_date': ['20230101', '20230102', '20230103'],
            'year': [2023, 2023, 2023],
            'value': [100.0, 200.0, 300.0]
        })

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def test_partition_level_metadata_management(self):
        """Test partition level metadata management"""
        # Create partition with data
        partition_dir = self.temp_dir / "year=2023"
        partition_dir.mkdir()
        data_file = partition_dir / "data.parquet"
        self.test_data.write_parquet(data_file)

        # Test metadata management
        metadata = manage_partition_metadata(partition_dir, action="create")
        self.assertIsNotNone(metadata)

        # Test metadata retrieval
        retrieved_metadata = manage_partition_metadata(partition_dir, action="get")
        self.assertIsNotNone(retrieved_metadata)

    def test_partition_level_data_lifecycle_management(self):
        """Test partition level data lifecycle management"""
        # Create multiple partitions
        for i in range(3):
            partition_dir = self.temp_dir / f"year=202{i+1}"
            partition_dir.mkdir()

            # Create test data
            data = pl.DataFrame({
                'ts_code': [f'{j:06d}.SZ' for j in range(10)],
                'trade_date': [f'202{i+1}01{j+1:02d}' for j in range(10)],
                'year': [2020 + i + 1 for _ in range(10)],
                'value': [float(j * 10) for j in range(10)]
            })
            data.write_parquet(partition_dir / "data.parquet")

        # Test lifecycle management (archive old partitions)
        lifecycle_result = manage_partition_lifecycle(self.temp_dir, retention_days=30)
        self.assertIsNotNone(lifecycle_result)

    def test_partition_level_access_control(self):
        """Test partition level access control"""
        # Create partition with data
        partition_dir = self.temp_dir / "year=2023"
        partition_dir.mkdir()
        data_file = partition_dir / "data.parquet"
        self.test_data.write_parquet(data_file)

        # Test access control management
        access_control_result = manage_partition_access_control(partition_dir, user="test_user", permission="read")
        self.assertTrue(access_control_result)

        # Test access control verification
        access_check = manage_partition_access_control(partition_dir, user="test_user", permission="read", action="check")
        self.assertTrue(access_check)

    def test_partition_level_backup_and_recovery(self):
        """Test partition level backup and recovery"""
        # Create partition with data
        partition_dir = self.temp_dir / "year=2023"
        partition_dir.mkdir()
        data_file = partition_dir / "data.parquet"
        self.test_data.write_parquet(data_file)

        # Test backup
        backup_result = manage_partition_lifecycle(partition_dir, action="backup")
        self.assertIsNotNone(backup_result)

        # Test recovery would require a backup to exist, which we've created above
        # In a real implementation, this would test the recovery process

if __name__ == '__main__':
    unittest.main()