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

from storage import monitor_partition_performance, analyze_partition_query_performance, setup_performance_alerts

class TestPartitionPerformanceMonitoring(unittest.TestCase):
    """Test cases for partition performance monitoring"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = Path(tempfile.mkdtemp())

        # Create test data for performance monitoring
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

    def test_partition_query_performance_monitoring(self):
        """Test partition query performance monitoring"""
        # Create partition with data
        partition_dir = self.temp_dir / "year=2023"
        partition_dir.mkdir()
        data_file = partition_dir / "data.parquet"
        self.test_data.write_parquet(data_file)

        # Test performance monitoring
        performance_metrics = monitor_partition_performance(partition_dir)
        self.assertIsNotNone(performance_metrics)
        self.assertIn("query_time_ms", performance_metrics)
        self.assertIn("data_size_mb", performance_metrics)

    def test_partition_storage_efficiency_monitoring(self):
        """Test partition storage efficiency monitoring"""
        # Create multiple partitions with different sizes
        for i in range(3):
            partition_dir = self.temp_dir / f"year=202{i+1}"
            partition_dir.mkdir()

            # Create test data with varying sizes
            data_size = 10 * (i + 1)  # Increasing size
            data = pl.DataFrame({
                'ts_code': [f'{j:06d}.SZ' for j in range(data_size)],
                'trade_date': [f'202{i+1}01{j+1:02d}' for j in range(data_size)],
                'year': [2020 + i + 1 for _ in range(data_size)],
                'value': [float(j * 10) for j in range(data_size)]
            })
            data.write_parquet(partition_dir / "data.parquet")

        # Test storage efficiency analysis
        efficiency_analysis = analyze_partition_query_performance(self.temp_dir)
        self.assertIsNotNone(efficiency_analysis)
        self.assertIn("total_partitions", efficiency_analysis)
        self.assertIn("avg_query_time_ms", efficiency_analysis)

    def test_partition_access_frequency_statistics(self):
        """Test partition access frequency statistics"""
        # Create partition with data
        partition_dir = self.temp_dir / "year=2023"
        partition_dir.mkdir()
        data_file = partition_dir / "data.parquet"
        self.test_data.write_parquet(data_file)

        # Test access frequency tracking (simulated)
        # In a real implementation, this would track actual access patterns
        performance_metrics = monitor_partition_performance(partition_dir)
        self.assertIsNotNone(performance_metrics)

    def test_performance_anomaly_alert_mechanism(self):
        """Test performance anomaly alert mechanism"""
        # Create partitions
        for i in range(3):
            partition_dir = self.temp_dir / f"year=202{i+1}"
            partition_dir.mkdir()
            data = pl.DataFrame({
                'ts_code': [f'{j:06d}.SZ' for j in range(50)],
                'trade_date': [f'202{i+1}01{j+1:02d}' for j in range(50)],
                'year': [2020 + i + 1 for _ in range(50)],
                'value': [float(j * 10) for j in range(50)]
            })
            data.write_parquet(partition_dir / "data.parquet")

        # Test alert setup
        alert_result = setup_performance_alerts(self.temp_dir, threshold_ms=1000)
        self.assertTrue(alert_result)

        # Test performance monitoring with alerts
        performance_analysis = analyze_partition_query_performance(self.temp_dir)
        self.assertIsNotNone(performance_analysis)

if __name__ == '__main__':
    unittest.main()