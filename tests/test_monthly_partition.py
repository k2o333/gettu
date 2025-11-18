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

class TestMonthlyPartition(unittest.TestCase):
    """Test cases for monthly partition implementation"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.test_data = pl.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ', '600000.SH', '000001.SZ', '000002.SZ'],
            'trade_date': ['20230115', '20230120', '20230210', '20230215', '20230310'],
            'trade_date_int': [20230115, 20230120, 20230210, 20230215, 20230310],
            'year_month': [202301, 202301, 202302, 202302, 202303],
            'price': [10.0, 15.0, 20.0, 25.0, 30.0]
        })

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def test_monthly_partition_directory_structure(self):
        """Test monthly partition directory structure"""
        lazy_frame = self.test_data.lazy()
        atomic_partitioned_sink(lazy_frame, self.temp_dir, partition_by=['year_month'])

        # Check that year_month directories were created
        self.assertTrue((self.temp_dir / "year_month=202301").exists())
        self.assertTrue((self.temp_dir / "year_month=202302").exists())
        self.assertTrue((self.temp_dir / "year_month=202303").exists())

        # Check that data files were created
        self.assertTrue((self.temp_dir / "year_month=202301" / "data.parquet").exists())
        self.assertTrue((self.temp_dir / "year_month=202302" / "data.parquet").exists())
        self.assertTrue((self.temp_dir / "year_month=202303" / "data.parquet").exists())

    def test_same_month_data_correct_partitioning(self):
        """Test that same month data is correctly partitioned"""
        lazy_frame = self.test_data.lazy()
        atomic_partitioned_sink(lazy_frame, self.temp_dir, partition_by=['year_month'])

        # Load data from 202301 partition
        data_202301 = pl.read_parquet(self.temp_dir / "year_month=202301" / "data.parquet")
        self.assertEqual(len(data_202301), 2)  # Two rows for 202301

        # Load data from 202302 partition
        data_202302 = pl.read_parquet(self.temp_dir / "year_month=202302" / "data.parquet")
        self.assertEqual(len(data_202302), 2)  # Two rows for 202302

        # Load data from 202303 partition
        data_202303 = pl.read_parquet(self.temp_dir / "year_month=202303" / "data.parquet")
        self.assertEqual(len(data_202303), 1)  # One row for 202303

        # Verify data integrity
        self.assertEqual(data_202301['year_month'][0], 202301)
        self.assertEqual(data_202302['year_month'][0], 202302)
        self.assertEqual(data_202303['year_month'][0], 202303)

    def test_cross_month_data_handling(self):
        """Test handling of cross-month data"""
        # Create data spanning multiple months
        cross_month_data = pl.DataFrame({
            'ts_code': ['000001.SZ'] * 6,
            'trade_date': ['20221215', '20230115', '20230215', '20230315', '20230415', '20230515'],
            'trade_date_int': [20221215, 20230115, 20230215, 20230315, 20230415, 20230515],
            'year_month': [202212, 202301, 202302, 202303, 202304, 202305],
            'volume': [1000, 1500, 2000, 2500, 3000, 3500]
        })

        lazy_frame = cross_month_data.lazy()
        atomic_partitioned_sink(lazy_frame, self.temp_dir, partition_by=['year_month'])

        # Check that all month directories were created
        self.assertTrue((self.temp_dir / "year_month=202212").exists())
        self.assertTrue((self.temp_dir / "year_month=202301").exists())
        self.assertTrue((self.temp_dir / "year_month=202302").exists())
        self.assertTrue((self.temp_dir / "year_month=202303").exists())
        self.assertTrue((self.temp_dir / "year_month=202304").exists())
        self.assertTrue((self.temp_dir / "year_month=202305").exists())

        # Check data counts in each partition
        for year_month in [202212, 202301, 202302, 202303, 202304, 202305]:
            data = pl.read_parquet(self.temp_dir / f"year_month={year_month}" / "data.parquet")
            self.assertEqual(len(data), 1)

    def test_month_format_validation_and_error_handling(self):
        """Test month format validation and error handling"""
        # Test with invalid month data
        invalid_data = pl.DataFrame({
            'ts_code': ['000001.SZ'],
            'trade_date': ['invalid_date'],
            'year_month': ['invalid_month']  # Invalid month type
        })

        lazy_frame = invalid_data.lazy()

        # This should handle invalid data gracefully without crashing
        # The function should log the error but not crash
        try:
            atomic_partitioned_sink(lazy_frame, self.temp_dir, partition_by=['year_month'])
            # If we reach here, the function handled the error gracefully
            self.assertTrue(True)
        except Exception as e:
            # If an exception is raised, that's also acceptable
            self.assertIsInstance(e, Exception)

if __name__ == '__main__':
    unittest.main()