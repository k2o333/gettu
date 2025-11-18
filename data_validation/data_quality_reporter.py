"""
Data Quality Report Generator

Generates comprehensive data quality reports based on validation results.
"""
import polars as pl
import json
from typing import Dict, List, Optional
import logging
from datetime import datetime
from pathlib import Path


class DataQualityReporter:
    """Generates comprehensive data quality reports."""

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the data quality reporter.

        Args:
            config: Configuration dictionary with report generation parameters
        """
        self.config = config or {
            'report_formats': ['json', 'text'],
            'include_detailed_stats': True,
            'include_visualizations': False,
            'output_directory': './reports',
            'report_retention_days': 30
        }

    def generate_quality_report(self, validation_results: Dict,
                              dataset_name: str = 'unknown',
                              output_formats: List[str] = None) -> Dict:
        """
        Generate a comprehensive data quality report.

        Args:
            validation_results: Results from various validation checks
            dataset_name: Name of the dataset being validated
            output_formats: List of formats to generate (json, text, html)

        Returns:
            Dictionary with report generation results
        """
        if output_formats is None:
            output_formats = self.config['report_formats']

        report_results = {
            'dataset_name': dataset_name,
            'generated_at': datetime.now().isoformat(),
            'overall_quality_score': 0.0,
            'reports': {},
            'summary': {}
        }

        # Calculate overall quality score
        quality_score = self._calculate_quality_score(validation_results)
        report_results['overall_quality_score'] = quality_score

        # Generate summary
        report_results['summary'] = self._generate_summary(validation_results)

        # Generate reports in specified formats
        for format_type in output_formats:
            if format_type == 'json':
                report_results['reports']['json'] = self._generate_json_report(
                    validation_results, dataset_name, quality_score, report_results['summary']
                )
            elif format_type == 'text':
                report_results['reports']['text'] = self._generate_text_report(
                    validation_results, dataset_name, quality_score, report_results['summary']
                )
            elif format_type == 'html':
                report_results['reports']['html'] = self._generate_html_report(
                    validation_results, dataset_name, quality_score, report_results['summary']
                )

        return report_results

    def _calculate_quality_score(self, validation_results: Dict) -> float:
        """
        Calculate an overall quality score based on validation results.

        Args:
            validation_results: Results from various validation checks

        Returns:
            Quality score between 0.0 and 1.0
        """
        total_issues = 0
        total_records = 0

        # Count issues from different validation types
        if 'field_validation' in validation_results:
            field_issues = validation_results['field_validation'].get('issues', [])
            total_issues += len(field_issues)

        if 'logical_validation' in validation_results:
            logical_issues = validation_results['logical_validation'].get('issues', [])
            total_issues += len(logical_issues)

        if 'referential_integrity' in validation_results:
            integrity_issues = validation_results['referential_integrity'].get('issues', [])
            total_issues += len(integrity_issues)

        # Get total records if available
        if 'counts' in validation_results:
            total_records = validation_results['counts'].get('total_records', 0)
        elif 'field_validation' in validation_results and 'counts' in validation_results['field_validation']:
            total_records = validation_results['field_validation']['counts'].get('total_records', 0)

        # Calculate quality score (higher is better)
        if total_records == 0:
            return 1.0 if total_issues == 0 else 0.0

        # Score based on issues per record
        issues_per_record = total_issues / total_records
        quality_score = max(0.0, 1.0 - issues_per_record)

        # Cap at 1.0
        return min(1.0, quality_score)

    def _generate_summary(self, validation_results: Dict) -> Dict:
        """
        Generate a summary of validation results.

        Args:
            validation_results: Results from various validation checks

        Returns:
            Dictionary with summary information
        """
        summary = {
            'total_issues': 0,
            'issue_severity_breakdown': {
                'critical': 0,
                'high': 0,
                'medium': 0,
                'low': 0
            },
            'validation_types': [],
            'data_coverage': {}
        }

        # Count issues by type and severity
        all_issues = []

        # Field validation issues
        if 'field_validation' in validation_results:
            field_issues = validation_results['field_validation'].get('issues', [])
            all_issues.extend(field_issues)
            summary['validation_types'].append('field_validation')

        # Logical validation issues
        if 'logical_validation' in validation_results:
            logical_issues = validation_results['logical_validation'].get('issues', [])
            all_issues.extend(logical_issues)
            summary['validation_types'].append('logical_validation')

        # Referential integrity issues
        if 'referential_integrity' in validation_results:
            integrity_issues = validation_results['referential_integrity'].get('issues', [])
            all_issues.extend(integrity_issues)
            summary['validation_types'].append('referential_integrity')

        # Cross-dataset issues
        if 'dataset_consistency' in validation_results:
            consistency_issues = validation_results['dataset_consistency'].get('issues', [])
            all_issues.extend(consistency_issues)
            summary['validation_types'].append('dataset_consistency')

        summary['total_issues'] = len(all_issues)

        # Categorize issues by severity (simplified)
        for issue in all_issues:
            # Determine severity based on issue type or description
            severity = self._determine_issue_severity(issue)
            summary['issue_severity_breakdown'][severity] += 1

        # Add data coverage information if available
        if 'counts' in validation_results:
            summary['data_coverage'] = validation_results['counts']

        return summary

    def _determine_issue_severity(self, issue: Dict) -> str:
        """
        Determine the severity of an issue.

        Args:
            issue: Issue dictionary

        Returns:
            Severity level ('critical', 'high', 'medium', 'low')
        """
        # Simplified severity determination
        issue_check = issue.get('check', '').lower()

        # Critical issues
        critical_patterns = ['missing', 'null', 'required', 'invalid']
        if any(pattern in issue_check for pattern in critical_patterns):
            return 'critical'

        # High issues
        high_patterns = ['anomaly', 'inconsistency', 'revision']
        if any(pattern in issue_check for pattern in high_patterns):
            return 'high'

        # Medium issues
        medium_patterns = ['bounds', 'range', 'format']
        if any(pattern in issue_check for pattern in medium_patterns):
            return 'medium'

        # Low issues (default)
        return 'low'

    def _generate_json_report(self, validation_results: Dict, dataset_name: str,
                            quality_score: float, summary: Dict) -> str:
        """
        Generate a JSON format report.

        Args:
            validation_results: Validation results
            dataset_name: Dataset name
            quality_score: Calculated quality score
            summary: Summary information

        Returns:
            JSON string report
        """
        report_data = {
            'report_metadata': {
                'dataset_name': dataset_name,
                'generated_at': datetime.now().isoformat(),
                'quality_score': quality_score
            },
            'summary': summary,
            'detailed_results': validation_results
        }

        return json.dumps(report_data, indent=2, default=str)

    def _generate_text_report(self, validation_results: Dict, dataset_name: str,
                            quality_score: float, summary: Dict) -> str:
        """
        Generate a text format report.

        Args:
            validation_results: Validation results
            dataset_name: Dataset name
            quality_score: Calculated quality score
            summary: Summary information

        Returns:
            Text string report
        """
        report_lines = []

        # Header
        report_lines.append("=" * 60)
        report_lines.append(f"DATA QUALITY REPORT - {dataset_name}")
        report_lines.append("=" * 60)
        report_lines.append(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Overall Quality Score: {quality_score:.2%}")
        report_lines.append("")

        # Summary section
        report_lines.append("SUMMARY")
        report_lines.append("-" * 20)
        report_lines.append(f"Total Issues: {summary['total_issues']}")

        # Severity breakdown
        report_lines.append("Issue Severity Breakdown:")
        for severity, count in summary['issue_severity_breakdown'].items():
            if count > 0:
                report_lines.append(f"  {severity.capitalize()}: {count}")

        # Validation types performed
        if summary['validation_types']:
            report_lines.append(f"Validation Types: {', '.join(summary['validation_types'])}")

        # Data coverage
        if 'data_coverage' in summary and summary['data_coverage']:
            report_lines.append("Data Coverage:")
            for key, value in summary['data_coverage'].items():
                report_lines.append(f"  {key}: {value}")
        report_lines.append("")

        # Detailed issues section
        report_lines.append("DETAILED ISSUES")
        report_lines.append("-" * 20)

        all_issues = []

        # Collect issues from different validation types
        if 'field_validation' in validation_results:
            field_issues = validation_results['field_validation'].get('issues', [])
            if field_issues:
                report_lines.append("Field Validation Issues:")
                for issue in field_issues:
                    desc = issue.get('description', 'No description')
                    count = issue.get('count', 'N/A')
                    report_lines.append(f"  - {desc} (Count: {count})")
                report_lines.append("")

        if 'logical_validation' in validation_results:
            logical_issues = validation_results['logical_validation'].get('issues', [])
            if logical_issues:
                report_lines.append("Logical Validation Issues:")
                for issue in logical_issues:
                    desc = issue.get('description', 'No description')
                    count = issue.get('count', 'N/A')
                    report_lines.append(f"  - {desc} (Count: {count})")
                report_lines.append("")

        if 'referential_integrity' in validation_results:
            integrity_issues = validation_results['referential_integrity'].get('issues', [])
            if integrity_issues:
                report_lines.append("Referential Integrity Issues:")
                for issue in integrity_issues:
                    desc = issue.get('description', 'No description')
                    report_lines.append(f"  - {desc}")
                report_lines.append("")

        if 'dataset_consistency' in validation_results:
            consistency_issues = validation_results['dataset_consistency'].get('issues', [])
            if consistency_issues:
                report_lines.append("Dataset Consistency Issues:")
                for issue in consistency_issues:
                    desc = issue.get('description', 'No description')
                    report_lines.append(f"  - {desc}")
                report_lines.append("")

        if not any([
            validation_results.get('field_validation', {}).get('issues'),
            validation_results.get('logical_validation', {}).get('issues'),
            validation_results.get('referential_integrity', {}).get('issues'),
            validation_results.get('dataset_consistency', {}).get('issues')
        ]):
            report_lines.append("No issues detected. Data quality is good!")
            report_lines.append("")

        # Footer
        report_lines.append("=" * 60)
        report_lines.append("End of Report")
        report_lines.append("=" * 60)

        return "\n".join(report_lines)

    def _generate_html_report(self, validation_results: Dict, dataset_name: str,
                            quality_score: float, summary: Dict) -> str:
        """
        Generate an HTML format report.

        Args:
            validation_results: Validation results
            dataset_name: Dataset name
            quality_score: Calculated quality score
            summary: Summary information

        Returns:
            HTML string report
        """
        # Basic HTML template
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Data Quality Report - {dataset_name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; }}
        .summary {{ background-color: #e8f4f8; padding: 15px; margin: 10px 0; border-radius: 5px; }}
        .issues {{ margin: 10px 0; }}
        .issue-section {{ margin: 15px 0; }}
        .quality-score {{ font-size: 24px; font-weight: bold; color: {'green' if quality_score > 0.8 else 'orange' if quality_score > 0.6 else 'red'}; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Data Quality Report - {dataset_name}</h1>
        <p>Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>Overall Quality Score: <span class="quality-score">{quality_score:.2%}</span></p>
    </div>

    <div class="summary">
        <h2>Summary</h2>
        <p><strong>Total Issues:</strong> {summary['total_issues']}</p>
        <h3>Issue Severity Breakdown:</h3>
        <ul>
"""

        # Add severity breakdown
        for severity, count in summary['issue_severity_breakdown'].items():
            if count > 0:
                html_content += f"            <li>{severity.capitalize()}: {count}</li>\n"

        html_content += f"""
        </ul>
        <p><strong>Validation Types:</strong> {', '.join(summary['validation_types'])}</p>
"""

        # Add data coverage if available
        if 'data_coverage' in summary and summary['data_coverage']:
            html_content += "        <h3>Data Coverage:</h3>\n        <ul>\n"
            for key, value in summary['data_coverage'].items():
                html_content += f"            <li>{key}: {value}</li>\n"
            html_content += "        </ul>\n"

        html_content += """
    </div>

    <div class="issues">
        <h2>Detailed Issues</h2>
"""

        # Add detailed issues sections
        if 'field_validation' in validation_results:
            field_issues = validation_results['field_validation'].get('issues', [])
            if field_issues:
                html_content += """
        <div class="issue-section">
            <h3>Field Validation Issues</h3>
            <table>
                <tr><th>Description</th><th>Count</th></tr>
"""
                for issue in field_issues:
                    desc = issue.get('description', 'No description')
                    count = issue.get('count', 'N/A')
                    html_content += f"                <tr><td>{desc}</td><td>{count}</td></tr>\n"
                html_content += "            </table>\n        </div>\n"

        if 'logical_validation' in validation_results:
            logical_issues = validation_results['logical_validation'].get('issues', [])
            if logical_issues:
                html_content += """
        <div class="issue-section">
            <h3>Logical Validation Issues</h3>
            <table>
                <tr><th>Description</th><th>Count</th></tr>
"""
                for issue in logical_issues:
                    desc = issue.get('description', 'No description')
                    count = issue.get('count', 'N/A')
                    html_content += f"                <tr><td>{desc}</td><td>{count}</td></tr>\n"
                html_content += "            </table>\n        </div>\n"

        if 'referential_integrity' in validation_results:
            integrity_issues = validation_results['referential_integrity'].get('issues', [])
            if integrity_issues:
                html_content += """
        <div class="issue-section">
            <h3>Referential Integrity Issues</h3>
            <table>
                <tr><th>Description</th></tr>
"""
                for issue in integrity_issues:
                    desc = issue.get('description', 'No description')
                    html_content += f"                <tr><td>{desc}</td></tr>\n"
                html_content += "            </table>\n        </div>\n"

        if 'dataset_consistency' in validation_results:
            consistency_issues = validation_results['dataset_consistency'].get('issues', [])
            if consistency_issues:
                html_content += """
        <div class="issue-section">
            <h3>Dataset Consistency Issues</h3>
            <table>
                <tr><th>Description</th></tr>
"""
                for issue in consistency_issues:
                    desc = issue.get('description', 'No description')
                    html_content += f"                <tr><td>{desc}</td></tr>\n"
                html_content += "            </table>\n        </div>\n"

        # Close HTML
        html_content += """
    </div>
</body>
</html>
"""

        return html_content

    def save_report(self, report_results: Dict, output_dir: str = None) -> Dict:
        """
        Save generated reports to files.

        Args:
            report_results: Results from generate_quality_report
            output_dir: Directory to save reports (uses config default if None)

        Returns:
            Dictionary with save results
        """
        if output_dir is None:
            output_dir = self.config['output_directory']

        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        save_results = {
            'saved_files': [],
            'errors': []
        }

        dataset_name = report_results.get('dataset_name', 'unknown')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Save each report format
        for format_type, report_content in report_results['reports'].items():
            filename = f"{dataset_name}_quality_report_{timestamp}.{format_type}"
            filepath = Path(output_dir) / filename

            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(report_content)
                save_results['saved_files'].append(str(filepath))
            except Exception as e:
                save_results['errors'].append(f"Failed to save {format_type} report: {str(e)}")

        return save_results