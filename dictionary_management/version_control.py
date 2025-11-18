"""
Version Control Module
=====================

This module provides version control functionality for dictionaries,
enabling historical tracking and rollback capabilities.
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional
import json
import hashlib
from pathlib import Path
from config import DICT_DIR


class DictionaryVersionControl:
    """
    Manages version control for dictionaries with historical tracking and rollback.
    """

    def __init__(self):
        self.version_history: Dict[str, list] = {}
        self.current_versions: Dict[str, Dict[str, Any]] = {}
        self._initialized = False

    def initialize(self):
        """
        Initialize the version control system by loading existing version data.
        """
        if self._initialized:
            return

        # Load existing version data from persisted files
        self._load_existing_versions()
        self._initialized = True

    def _load_existing_versions(self):
        """
        Load existing version data from persisted files.
        """
        version_file = DICT_DIR / 'dictionary_versions.json'
        if version_file.exists():
            try:
                with open(version_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.version_history = data.get('version_history', {})
                    self.current_versions = data.get('current_versions', {})
            except Exception as e:
                logging.warning(f"Error loading dictionary version data: {e}")

    def _save_versions(self):
        """
        Save version data to persistent storage.
        """
        try:
            version_file = DICT_DIR / 'dictionary_versions.json'
            data = {
                'version_history': self.version_history,
                'current_versions': self.current_versions
            }
            with open(version_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.warning(f"Error saving dictionary version data: {e}")

    def create_version(self, dictionary_type: str, content_hash: str,
                      metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Create a new version for the given dictionary type.

        Args:
            dictionary_type: Type of dictionary ('stock', 'industry', 'region')
            content_hash: Hash of the dictionary content
            metadata: Optional metadata about the version

        Returns:
            Version identifier
        """
        if not self._initialized:
            self.initialize()

        # Create version timestamp
        timestamp = datetime.now().isoformat()
        version_id = f"{dictionary_type}_{timestamp}"

        # Create version record
        version_record = {
            'version_id': version_id,
            'dictionary_type': dictionary_type,
            'content_hash': content_hash,
            'timestamp': timestamp,
            'metadata': metadata or {}
        }

        # Add to version history
        if dictionary_type not in self.version_history:
            self.version_history[dictionary_type] = []
        self.version_history[dictionary_type].append(version_record)

        # Update current version
        self.current_versions[dictionary_type] = version_record

        # Save to persistent storage
        self._save_versions()

        logging.info(f"Created new version {version_id} for {dictionary_type} dictionary")
        return version_id

    def get_current_version(self, dictionary_type: str) -> Optional[Dict[str, Any]]:
        """
        Get the current version for the given dictionary type.

        Args:
            dictionary_type: Type of dictionary ('stock', 'industry', 'region')

        Returns:
            Current version record or None if not found
        """
        if not self._initialized:
            self.initialize()

        return self.current_versions.get(dictionary_type)

    def get_version_history(self, dictionary_type: str) -> list:
        """
        Get the version history for the given dictionary type.

        Args:
            dictionary_type: Type of dictionary ('stock', 'industry', 'region')

        Returns:
            List of version records
        """
        if not self._initialized:
            self.initialize()

        return self.version_history.get(dictionary_type, [])

    def rollback_to_version(self, dictionary_type: str, target_version_id: str) -> bool:
        """
        Rollback to a specific version of the dictionary.

        Args:
            dictionary_type: Type of dictionary ('stock', 'industry', 'region')
            target_version_id: Target version identifier

        Returns:
            True if rollback successful, False otherwise
        """
        if not self._initialized:
            self.initialize()

        # Find the target version in history
        history = self.version_history.get(dictionary_type, [])
        target_version = None
        for version in history:
            if version['version_id'] == target_version_id:
                target_version = version
                break

        if not target_version:
            logging.warning(f"Target version {target_version_id} not found for {dictionary_type}")
            return False

        # Update current version
        self.current_versions[dictionary_type] = target_version

        # Save to persistent storage
        self._save_versions()

        logging.info(f"Rolled back {dictionary_type} dictionary to version {target_version_id}")
        return True

    def calculate_content_hash(self, content: str) -> str:
        """
        Calculate hash of content for version tracking.

        Args:
            content: Content to hash

        Returns:
            SHA256 hash of the content
        """
        return hashlib.sha256(content.encode('utf-8')).hexdigest()

    def get_version_count(self, dictionary_type: str) -> int:
        """
        Get the number of versions for the given dictionary type.

        Args:
            dictionary_type: Type of dictionary ('stock', 'industry', 'region')

        Returns:
            Number of versions
        """
        if not self._initialized:
            self.initialize()

        return len(self.version_history.get(dictionary_type, []))