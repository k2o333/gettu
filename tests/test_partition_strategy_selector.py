import unittest
from pathlib import Path
import sys
import os

# Add the app2 directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from partition_strategy_selector import PartitionStrategySelector, PartitionStrategy

class TestPartitionStrategySelector(unittest.TestCase):
    """Test cases for the PartitionStrategySelector class"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.selector = PartitionStrategySelector()

    def test_financial_data_strategy(self):
        """Test that financial data uses no partition strategy"""
        data_type = "income"  # Financial data type from config
        strategy = self.selector.select_strategy(data_type)
        self.assertEqual(strategy, PartitionStrategy.NONE)

    def test_daily_market_data_strategy(self):
        """Test that daily market data uses yearly partition strategy"""
        data_type = "daily"  # Daily market data type from config
        strategy = self.selector.select_strategy(data_type)
        self.assertEqual(strategy, PartitionStrategy.YEAR)

    def test_high_frequency_event_data_strategy(self):
        """Test that high frequency event data uses monthly partition strategy"""
        data_type = "block_trade"  # High frequency event data type from config
        strategy = self.selector.select_strategy(data_type)
        self.assertEqual(strategy, PartitionStrategy.YEAR_MONTH)

    def test_low_frequency_event_data_strategy(self):
        """Test that low frequency event data uses no partition strategy"""
        data_type = "forecast"  # Low frequency event data type from config
        strategy = self.selector.select_strategy(data_type)
        self.assertEqual(strategy, PartitionStrategy.NONE)

    def test_invalid_data_type(self):
        """Test handling of invalid data types"""
        data_type = "invalid_data_type"
        strategy = self.selector.select_strategy(data_type)
        # Should default to NONE for unknown data types
        self.assertEqual(strategy, PartitionStrategy.NONE)

if __name__ == '__main__':
    unittest.main()