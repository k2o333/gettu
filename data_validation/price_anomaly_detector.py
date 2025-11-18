"""
Price Anomaly Detection Module

Implements advanced algorithms for detecting price anomalies in financial time series data.
"""
import polars as pl
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
from scipy import stats


class PriceAnomalyDetector:
    """Detects anomalies in price time series data using multiple statistical methods."""

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the price anomaly detector.

        Args:
            config: Configuration dictionary with detection parameters
        """
        self.config = config or {
            'iqr_multiplier': 1.5,
            'zscore_threshold': 3.0,
            'volatility_window': 20,
            'volatility_threshold': 2.0,
            'volume_confirmation_window': 5
        }

    def detect_anomalies(self, df: pl.DataFrame, price_col: str = 'close',
                        volume_col: str = 'volume', date_col: str = 'trade_date') -> pl.DataFrame:
        """
        Detect price anomalies using multiple methods.

        Args:
            df: Polars DataFrame with price data
            price_col: Column name for price data
            volume_col: Column name for volume data
            date_col: Column name for date data

        Returns:
            DataFrame with anomaly detection results
        """
        # Ensure required columns exist
        required_cols = [price_col, volume_col, date_col]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Sort by date to ensure proper time series analysis
        df = df.sort(date_col)

        # Calculate various anomaly detection metrics
        result_df = df.with_columns([
            # IQR-based outlier detection
            self._iqr_outlier_detection(price_col).alias('iqr_anomaly'),

            # Z-score based anomaly detection
            self._zscore_anomaly_detection(price_col).alias('zscore_anomaly'),

            # Volatility-based anomaly detection
            self._volatility_anomaly_detection(price_col).alias('volatility_anomaly'),

            # Price gap detection
            self._price_gap_detection(price_col).alias('price_gap_anomaly'),

            # Volume confirmation for anomalies
            self._volume_confirmation(price_col, volume_col).alias('volume_confirmed')
        ])

        # Combine all anomaly detections
        result_df = result_df.with_columns([
            (pl.col('iqr_anomaly') | pl.col('zscore_anomaly') |
             pl.col('volatility_anomaly') | pl.col('price_gap_anomaly')).alias('is_anomaly')
        ])

        # Add anomaly scores
        result_df = result_df.with_columns([
            self._calculate_anomaly_score().alias('anomaly_score')
        ])

        return result_df

    def _iqr_outlier_detection(self, price_col: str) -> pl.Expr:
        """
        Detect outliers using Interquartile Range (IQR) method.

        Args:
            price_col: Column name for price data

        Returns:
            Boolean expression indicating IQR outliers
        """
        multiplier = self.config['iqr_multiplier']

        q1_expr = pl.col(price_col).quantile(0.25)
        q3_expr = pl.col(price_col).quantile(0.75)
        iqr_expr = q3_expr - q1_expr

        lower_bound = q1_expr - multiplier * iqr_expr
        upper_bound = q3_expr + multiplier * iqr_expr

        return (
            pl.when(
                pl.col(price_col).is_not_null()
            )
            .then(
                (pl.col(price_col) < lower_bound) | (pl.col(price_col) > upper_bound)
            )
            .otherwise(False)
        )

    def _zscore_anomaly_detection(self, price_col: str) -> pl.Expr:
        """
        Detect anomalies using Z-score method.

        Args:
            price_col: Column name for price data

        Returns:
            Boolean expression indicating Z-score anomalies
        """
        threshold = self.config['zscore_threshold']
        # Calculate Z-score and compare to threshold
        mean_price = pl.col(price_col).mean()
        std_price = pl.col(price_col).std()
        zscore = (pl.col(price_col) - mean_price) / std_price
        return zscore.abs() > threshold

    def _volatility_anomaly_detection(self, price_col: str) -> pl.Expr:
        """
        Detect anomalies based on volatility spikes.

        Args:
            price_col: Column name for price data

        Returns:
            Boolean expression indicating volatility anomalies
        """
        window = self.config['volatility_window']
        threshold = self.config['volatility_threshold']

        # Calculate returns
        returns = pl.col(price_col).pct_change()

        # Calculate rolling volatility with minimum valid data - use a minimum window of 5
        rolling_vol = returns.rolling_std(window_size=max(window, 5))  # Make sure window is at least 5

        # Calculate overall volatility for comparison
        overall_std = returns.drop_nulls().std()

        # Use a default if std is null or nan or if no valid data was processed
        return (
            pl.when(
                rolling_vol.is_not_null() &
                (overall_std.is_not_null()) &
                (rolling_vol > 0) &
                (rolling_vol > (overall_std * (1 + threshold)))
            )
            .then(True)
            .otherwise(False)
        )

    def _price_gap_detection(self, price_col: str) -> pl.Expr:
        """
        Detect price gaps (large jumps between consecutive periods).

        Args:
            price_col: Column name for price data

        Returns:
            Boolean expression indicating price gaps
        """
        # Calculate price changes
        price_change = pl.col(price_col) - pl.col(price_col).shift(1)
        abs_change = price_change.abs()

        # Detect gaps larger than 2 standard deviations
        mean_change = abs_change.mean()
        std_change = abs_change.std()

        return abs_change > (mean_change + 2 * std_change)

    def _volume_confirmation(self, price_col: str, volume_col: str) -> pl.Expr:
        """
        Confirm anomalies with volume data.

        Args:
            price_col: Column name for price data
            volume_col: Column name for volume data

        Returns:
            Boolean expression indicating volume confirmation
        """
        window = self.config['volume_confirmation_window']

        # Calculate rolling average volume
        avg_volume = pl.col(volume_col).rolling_mean(window_size=window)

        # Anomaly is confirmed if volume is above average
        return pl.col(volume_col) > avg_volume

    def _calculate_anomaly_score(self) -> pl.Expr:
        """
        Calculate composite anomaly score based on multiple detection methods.

        Returns:
            Expression for anomaly score (0-1 scale)
        """
        # Count how many methods detected an anomaly (handle nulls)
        anomaly_count = (
            pl.col('iqr_anomaly').fill_null(False).cast(pl.Int32) +
            pl.col('zscore_anomaly').fill_null(False).cast(pl.Int32) +
            pl.col('volatility_anomaly').fill_null(False).cast(pl.Int32) +
            pl.col('price_gap_anomaly').fill_null(False).cast(pl.Int32)
        )

        # Normalize to 0-1 scale
        return anomaly_count.cast(pl.Float64) / 4.0