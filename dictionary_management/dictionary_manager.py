"""
Dictionary Manager Module
========================

This module provides the main dictionary management system that coordinates
all dictionary components and provides a unified interface for dictionary operations.
"""

import logging
from typing import Optional, Dict, Any
import polars as pl
from config import DICT_DIR

from .id_allocator import IDAllocator
from .mapping_manager import MappingManager
from .version_control import DictionaryVersionControl
from .dictionaries import StockDictionary, IndustryDictionary, RegionDictionary


class DictionaryManager:
    """
    Main dictionary management system that coordinates all dictionary components.
    """

    def __init__(self):
        # Initialize core components
        self.id_allocator = IDAllocator()
        self.mapping_manager = MappingManager()
        self.version_control = DictionaryVersionControl()

        # Initialize specialized dictionaries
        self.stock_dict = StockDictionary(self.id_allocator, self.mapping_manager, self.version_control)
        self.industry_dict = IndustryDictionary(self.id_allocator, self.mapping_manager, self.version_control)
        self.region_dict = RegionDictionary(self.id_allocator, self.mapping_manager, self.version_control)

        self._initialized = False

    def initialize(self):
        """
        Initialize the entire dictionary management system.
        """
        if self._initialized:
            return

        # Initialize all components
        self.id_allocator.initialize()
        self.mapping_manager.initialize()
        self.version_control.initialize()
        self.stock_dict.initialize()
        self.industry_dict.initialize()
        self.region_dict.initialize()

        self._initialized = True
        logging.info("Dictionary management system initialized")

    def get_stock_id(self, ts_code: str) -> Optional[int]:
        """
        Get the internal ID for the given stock code.

        Args:
            ts_code: Stock code (e.g., '000001.SZ')

        Returns:
            Internal ID if exists, None otherwise
        """
        if not self._initialized:
            self.initialize()

        return self.stock_dict.get_stock_id(ts_code)

    def get_stock_code(self, stock_id: int) -> Optional[str]:
        """
        Get the stock code for the given internal ID.

        Args:
            stock_id: Internal stock ID

        Returns:
            Stock code if exists, None otherwise
        """
        if not self._initialized:
            self.initialize()

        return self.stock_dict.get_stock_code(stock_id)

    def add_stock(self, ts_code: str, metadata: Dict[str, Any] = None) -> int:
        """
        Add a stock to the dictionary and assign a permanent ID.

        Args:
            ts_code: Stock code (e.g., '000001.SZ')
            metadata: Optional metadata for the stock

        Returns:
            Assigned permanent ID
        """
        if not self._initialized:
            self.initialize()

        return self.stock_dict.add_stock(ts_code, metadata)

    def get_industry_id(self, industry_code: str) -> Optional[int]:
        """
        Get the internal ID for the given industry code.

        Args:
            industry_code: Industry code

        Returns:
            Internal ID if exists, None otherwise
        """
        if not self._initialized:
            self.initialize()

        return self.industry_dict.get_industry_id(industry_code)

    def get_industry_code(self, industry_id: int) -> Optional[str]:
        """
        Get the industry code for the given internal ID.

        Args:
            industry_id: Internal industry ID

        Returns:
            Industry code if exists, None otherwise
        """
        if not self._initialized:
            self.initialize()

        return self.industry_dict.get_industry_code(industry_id)

    def add_industry(self, industry_code: str, industry_name: str = None) -> int:
        """
        Add an industry to the dictionary and assign a permanent ID.

        Args:
            industry_code: Industry code
            industry_name: Optional industry name

        Returns:
            Assigned permanent ID
        """
        if not self._initialized:
            self.initialize()

        return self.industry_dict.add_industry(industry_code, industry_name)

    def get_region_id(self, region_code: str) -> Optional[int]:
        """
        Get the internal ID for the given region code.

        Args:
            region_code: Region code

        Returns:
            Internal ID if exists, None otherwise
        """
        if not self._initialized:
            self.initialize()

        return self.region_dict.get_region_id(region_code)

    def get_region_code(self, region_id: int) -> Optional[str]:
        """
        Get the region code for the given internal ID.

        Args:
            region_id: Internal region ID

        Returns:
            Region code if exists, None otherwise
        """
        if not self._initialized:
            self.initialize()

        return self.region_dict.get_region_code(region_id)

    def add_region(self, region_code: str, region_name: str = None) -> int:
        """
        Add a region to the dictionary and assign a permanent ID.

        Args:
            region_code: Region code
            region_name: Optional region name

        Returns:
            Assigned permanent ID
        """
        if not self._initialized:
            self.initialize()

        return self.region_dict.add_region(region_code, region_name)

    def validate_id(self, entity_type: str, id_val: int) -> bool:
        """
        Validate that the given ID is valid for the specified entity type.

        Args:
            entity_type: Type of entity ('stock', 'industry', 'region')
            id_val: Internal ID value

        Returns:
            True if valid, False otherwise
        """
        if not self._initialized:
            self.initialize()

        if entity_type == 'stock':
            return self.stock_dict.validate_stock_id(id_val)
        elif entity_type == 'industry':
            return self.industry_dict.validate_industry_id(id_val)
        elif entity_type == 'region':
            return self.region_dict.validate_region_id(id_val)
        else:
            return False

    def get_all_stocks(self) -> list:
        """
        Get all stock codes in the dictionary.

        Returns:
            List of stock codes
        """
        if not self._initialized:
            self.initialize()

        return self.stock_dict.get_all_stocks()

    def create_dictionary_version(self, dictionary_type: str, content: str,
                                metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Create a new version for the given dictionary type.

        Args:
            dictionary_type: Type of dictionary ('stock', 'industry', 'region')
            content: Content to hash for version tracking
            metadata: Optional metadata about the version

        Returns:
            Version identifier
        """
        if not self._initialized:
            self.initialize()

        content_hash = self.version_control.calculate_content_hash(content)
        return self.version_control.create_version(dictionary_type, content_hash, metadata)

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

        return self.version_control.get_current_version(dictionary_type)

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

        return self.version_control.get_version_history(dictionary_type)

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

        return self.version_control.rollback_to_version(dictionary_type, target_version_id)

    def get_allocated_count(self, entity_type: str) -> int:
        """
        Get the count of allocated IDs for the given entity type.

        Args:
            entity_type: Type of entity ('stock', 'industry', 'region')

        Returns:
            Number of allocated IDs
        """
        if not self._initialized:
            self.initialize()

        return self.id_allocator.get_allocated_count(entity_type)

    def get_next_available_id(self, entity_type: str) -> int:
        """
        Get the next available ID for the given entity type.

        Args:
            entity_type: Type of entity ('stock', 'industry', 'region')

        Returns:
            Next available ID
        """
        if not self._initialized:
            self.initialize()

        return self.id_allocator.get_next_available_id(entity_type)