"""
Data Quality Alerting System

Generates and manages alerts for data quality issues.
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from enum import Enum


class AlertSeverity(Enum):
    """Enumeration of alert severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class DataQualityAlert:
    """Represents a data quality alert."""

    def __init__(self, alert_type: str, severity: AlertSeverity, message: str,
                 dataset_name: str, timestamp: Optional[datetime] = None,
                 metadata: Optional[Dict] = None):
        """
        Initialize a data quality alert.

        Args:
            alert_type: Type of alert
            severity: Severity level
            message: Alert message
            dataset_name: Name of affected dataset
            timestamp: When the alert was generated
            metadata: Additional alert metadata
        """
        self.alert_type = alert_type
        self.severity = severity
        self.message = message
        self.dataset_name = dataset_name
        self.timestamp = timestamp or datetime.now()
        self.metadata = metadata or {}
        self.alert_id = f"{alert_type}_{dataset_name}_{self.timestamp.isoformat()}"

    def to_dict(self) -> Dict:
        """Convert alert to dictionary representation."""
        return {
            'alert_id': self.alert_id,
            'alert_type': self.alert_type,
            'severity': self.severity.value,
            'message': self.message,
            'dataset_name': self.dataset_name,
            'timestamp': self.timestamp.isoformat(),
            'metadata': self.metadata
        }

    def __str__(self) -> str:
        """String representation of the alert."""
        return f"[{self.severity.value.upper()}] {self.dataset_name}: {self.message}"


class DataQualityAlertManager:
    """Manages data quality alerts and notifications."""

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the alert manager.

        Args:
            config: Configuration dictionary
        """
        self.config = config or {
            'alert_retention_hours': 24,
            'max_alerts_per_dataset': 100,
            'enable_notifications': True,
            'notification_channels': ['log'],  # log, email, webhook, etc.
            'suppression_rules': {}
        }

        self.active_alerts = []
        self.alert_history = []
        self.suppression_rules = self.config.get('suppression_rules', {})

    def create_alert(self, alert_type: str, severity: AlertSeverity, message: str,
                    dataset_name: str, metadata: Optional[Dict] = None) -> DataQualityAlert:
        """
        Create and manage a new data quality alert.

        Args:
            alert_type: Type of alert
            severity: Severity level
            message: Alert message
            dataset_name: Name of affected dataset
            metadata: Additional alert metadata

        Returns:
            Created alert object
        """
        # Check if alert should be suppressed
        if self._should_suppress_alert(alert_type, dataset_name, severity):
            logging.debug(f"Alert suppressed: {alert_type} for {dataset_name}")
            return None

        # Create alert
        alert = DataQualityAlert(
            alert_type=alert_type,
            severity=severity,
            message=message,
            dataset_name=dataset_name,
            metadata=metadata
        )

        # Add to active alerts
        self.active_alerts.append(alert)

        # Add to history
        self.alert_history.append(alert)

        # Clean up old alerts
        self._cleanup_old_alerts()

        # Send notifications
        if self.config['enable_notifications']:
            self._send_alert_notification(alert)

        return alert

    def _should_suppress_alert(self, alert_type: str, dataset_name: str,
                              severity: AlertSeverity) -> bool:
        """
        Check if an alert should be suppressed based on rules.

        Args:
            alert_type: Type of alert
            dataset_name: Name of affected dataset
            severity: Severity level

        Returns:
            Boolean indicating if alert should be suppressed
        """
        # Check for duplicate alerts within a short time window
        suppression_window = timedelta(minutes=30)
        current_time = datetime.now()

        for existing_alert in self.active_alerts:
            time_diff = current_time - existing_alert.timestamp
            if (existing_alert.alert_type == alert_type and
                existing_alert.dataset_name == dataset_name and
                existing_alert.severity == severity and
                time_diff < suppression_window):
                return True

        # Check custom suppression rules
        alert_key = f"{alert_type}_{dataset_name}"
        if alert_key in self.suppression_rules:
            rule = self.suppression_rules[alert_key]
            # Simple time-based suppression for now
            return rule.get('suppress', False)

        return False

    def _send_alert_notification(self, alert: DataQualityAlert):
        """
        Send alert notification through configured channels.

        Args:
            alert: Alert to send notification for
        """
        channels = self.config.get('notification_channels', ['log'])

        for channel in channels:
            if channel == 'log':
                self._send_log_notification(alert)
            elif channel == 'email':
                self._send_email_notification(alert)
            elif channel == 'webhook':
                self._send_webhook_notification(alert)

    def _send_log_notification(self, alert: DataQualityAlert):
        """Send alert notification to logs."""
        log_level = logging.WARNING
        if alert.severity == AlertSeverity.HIGH:
            log_level = logging.ERROR
        elif alert.severity == AlertSeverity.CRITICAL:
            log_level = logging.CRITICAL

        logging.log(log_level, f"DATA QUALITY ALERT: {alert}")

    def _send_email_notification(self, alert: DataQualityAlert):
        """Send alert notification via email (placeholder)."""
        # This would require email configuration and implementation
        logging.debug(f"Email notification would be sent for: {alert}")

    def _send_webhook_notification(self, alert: DataQualityAlert):
        """Send alert notification via webhook (placeholder)."""
        # This would require webhook configuration and implementation
        logging.debug(f"Webhook notification would be sent for: {alert}")

    def _cleanup_old_alerts(self):
        """Remove old alerts based on retention policy."""
        retention_hours = self.config.get('alert_retention_hours', 24)
        cutoff_time = datetime.now() - timedelta(hours=retention_hours)

        # Remove from active alerts
        self.active_alerts = [
            alert for alert in self.active_alerts
            if alert.timestamp >= cutoff_time
        ]

        # Limit alert history size
        max_history = 1000
        if len(self.alert_history) > max_history:
            self.alert_history = self.alert_history[-max_history:]

    def get_active_alerts(self, dataset_name: Optional[str] = None,
                         severity: Optional[AlertSeverity] = None) -> List[DataQualityAlert]:
        """
        Get active alerts, optionally filtered by dataset or severity.

        Args:
            dataset_name: Filter by dataset name
            severity: Filter by severity level

        Returns:
            List of active alerts
        """
        filtered_alerts = self.active_alerts

        if dataset_name:
            filtered_alerts = [
                alert for alert in filtered_alerts
                if alert.dataset_name == dataset_name
            ]

        if severity:
            filtered_alerts = [
                alert for alert in filtered_alerts
                if alert.severity == severity
            ]

        return filtered_alerts

    def clear_alerts_for_dataset(self, dataset_name: str) -> int:
        """
        Clear all active alerts for a specific dataset.

        Args:
            dataset_name: Name of dataset to clear alerts for

        Returns:
            Number of alerts cleared
        """
        initial_count = len(self.active_alerts)
        self.active_alerts = [
            alert for alert in self.active_alerts
            if alert.dataset_name != dataset_name
        ]
        cleared_count = initial_count - len(self.active_alerts)
        logging.info(f"Cleared {cleared_count} alerts for dataset {dataset_name}")
        return cleared_count

    def get_alert_summary(self) -> Dict:
        """
        Get summary of all active alerts.

        Returns:
            Dictionary with alert summary
        """
        if not self.active_alerts:
            return {
                'total_alerts': 0,
                'by_severity': {},
                'by_dataset': {},
                'most_recent': None
            }

        # Group by severity
        severity_counts = {}
        for alert in self.active_alerts:
            severity = alert.severity.value
            severity_counts[severity] = severity_counts.get(severity, 0) + 1

        # Group by dataset
        dataset_counts = {}
        for alert in self.active_alerts:
            dataset = alert.dataset_name
            dataset_counts[dataset] = dataset_counts.get(dataset, 0) + 1

        # Find most recent alert
        most_recent = max(self.active_alerts, key=lambda x: x.timestamp)

        return {
            'total_alerts': len(self.active_alerts),
            'by_severity': severity_counts,
            'by_dataset': dataset_counts,
            'most_recent': most_recent.to_dict()
        }

    def acknowledge_alert(self, alert_id: str) -> bool:
        """
        Acknowledge and remove a specific alert.

        Args:
            alert_id: ID of alert to acknowledge

        Returns:
            Boolean indicating if alert was found and acknowledged
        """
        for i, alert in enumerate(self.active_alerts):
            if alert.alert_id == alert_id:
                acknowledged_alert = self.active_alerts.pop(i)
                logging.info(f"Acknowledged alert: {acknowledged_alert}")
                return True
        return False