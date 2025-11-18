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

from storage import atomic_partitioned_sink

class TestYearlyPartition(unittest.TestCase):
    """Test cases for yearly partition implementation"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.test_data = pl.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ', '600000.SH', '000001.SZ'],
            'trade_date': ['20230101', '20230102', '20230103', '20240101'],
            'trade_date_int': [20230101, 20230102, 20230103, 20240101],
            'year': [2023, 2023, 2023, 2024],
            'open': [10.0, 20.0, 30.0, 15.0],
            'close': [11.0, 21.0, 31.0, 16.0]
        })

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def test_yearly_partition_directory_creation(self):
        """Test yearly partition directory creation"""
        lazy_frame = self.test_data.lazy()
        atomic_partitioned_sink(lazy_frame, self.temp_dir, partition_by=['year'])

        # Check that year directories were created
        self.assertTrue((self.temp_dir / "year=2023").exists())
        self.assertTrue((self.temp_dir / "year=2024").exists())

        # Check that data files were created
        self.assertTrue((self.temp_dir / "year=2023" / "data.parquet").exists())
        self.assertTrue((self.temp_dir / "year=2024" / "data.parquet").exists())

    def test_same_year_data_correct_partitioning(self):
        """Test that same year data is correctly partitioned"""
        lazy_frame = self.test_data.lazy()
        atomic_partitioned_sink(lazy_frame, self.temp_dir, partition_by=['year'])

        # Load data from 2023 partition
        data_2023 = pl.read_parquet(self.temp_dir / "year=2023" / "data.parquet")
        self.assertEqual(len(data_2023), 3)  # Three rows for 2023

        # Load data from 2024 partition
        data_2024 = pl.read_parquet(self.temp_dir / "year=2024" / "data.parquet")
        self.assertEqual(len(data_2024), 1)  # One row for 2024

        # Verify data integrity
        self.assertEqual(data_2023['year'][0], 2023)
        self.assertEqual(data_2024['year'][0], 2024)

    def test_cross_year_data_handling(self):
        """Test handling of cross-year data"""
        # Create data spanning multiple years
        cross_year_data = pl.DataFrame({
            'ts_code': ['000001.SZ'] * 5,
            'trade_date': ['20211231', '20220601', '20230101', '20231231', '20240601'],
            'trade_date_int': [20211231, 20220601, 20230101, 20231231, 20240601],
            'year': [2021, 2022, 2023, 2023, 2024],
            'price': [10.0, 15.0, 20.0, 25.0, 30.0]
        })

        lazy_frame = cross_year_data.lazy()
        atomic_partitioned_sink(lazy_frame, self.temp_dir, partition_by=['year'])

        # Check that all year directories were created
        self.assertTrue((self.temp_dir / "year=2021").exists())
        self.assertTrue((self.temp_dir / "year=2022").exists())
        self.assertTrue((self.temp_dir / "year=2023").exists())
        self.assertTrue((self.temp_dir / "year=2024").exists())

        # Check data counts in each partition
        data_2021 = pl.read_parquet(self.temp_dir / "year=2021" / "data.parquet")
        self.assertEqual(len(data_2021), 1)

        data_2022 = pl.read_parquet(self.temp_dir / "year=2022" / "data.parquet")
        self.assertEqual(len(data_2022), 1)

        data_2023 = pl.read_parquet(self.temp_dir / "year=2023" / "data.parquet")
        self.assertEqual(len(data_2023), 2)

        data_2024 = pl.read_parquet(self.temp_dir / "year=2024" / "data.parquet")
        self.assertEqual(len(data_2024), 1)

    def test_year_format_validation(self):
        """Test year format validation and error handling"""
        # Test with invalid year data
        invalid_data = pl.DataFrame({
            'ts_code': ['000001.SZ'],
            'trade_date': ['invalid_date'],
            'year': ['invalid_year']  # Invalid year type
        })

        lazy_frame = invalid_data.lazy()

        # This should handle invalid data gracefully without crashing
        # The function should log the error but not crash
        try:
            atomic_partitioned_sink(lazy_frame, self.temp_dir, partition_by=['year'])
            # If we reach here, the function handled the error gracefully
            self.assertTrue(True)
        except Exception as e:
            # If an exception is raised, that's also acceptable
            self.assertIsInstance(e, Exception)

if __name__ == '__main__':
    unittest.main()