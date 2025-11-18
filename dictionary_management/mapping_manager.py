"""
Mapping Manager Module
======================

This module manages bidirectional mappings between external codes and internal IDs,
ensuring efficient lookup in both directions with O(1) performance.
"""

import logging
from typing import Dict, Optional, Union
import polars as pl
from config import DICT_DIR


class MappingManager:
    """
    Manages bidirectional mappings between external codes and internal IDs.
    """

    def __init__(self):
        self._mappings: Dict[str, Dict[str, Union[int, str]]] = {}
        self._reverse_mappings: Dict[str, Dict[int, str]] = {}
        self._initialized = False

    def initialize(self):
        """
        Initialize the mapping manager by loading existing mappings from storage.
        """
        if self._initialized:
            return

        # Load existing mappings from persisted dictionary files
        self._load_existing_mappings()
        self._initialized = True

    def _load_existing_mappings(self):
        """
        Load existing mappings from persisted dictionary files.
        """
        # Load stock dictionary if it exists
        stock_path = DICT_DIR / 'stock_basic_dict.parquet'
        if stock_path.exists():
            try:
                df = pl.read_parquet(stock_path)
                if 'ts_code' in df.columns and 'ts_code_id' in df.columns:
                    self._mappings['stock'] = {}
                    self._reverse_mappings['stock'] = {}
                    for row in df.iter_rows(named=True):
                        code = row['ts_code']
                        id_val = row['ts_code_id']
                        if id_val is not None and code is not None:
                            self._mappings['stock'][code] = id_val
                            self._reverse_mappings['stock'][id_val] = code
            except Exception as e:
                logging.warning(f"Error loading stock dictionary for mapping: {e}")

        # Load industry dictionary if it exists
        industry_path = DICT_DIR / 'industry_dict.parquet'
        if industry_path.exists():
            try:
                df = pl.read_parquet(industry_path)
                if 'industry' in df.columns and 'industry_id' in df.columns:
                    self._mappings['industry'] = {}
                    self._reverse_mappings['industry'] = {}
                    for row in df.iter_rows(named=True):
                        code = row['industry']
                        id_val = row['industry_id']
                        if id_val is not None and code is not None:
                            self._mappings['industry'][code] = id_val
                            self._reverse_mappings['industry'][id_val] = code
            except Exception as e:
                logging.warning(f"Error loading industry dictionary for mapping: {e}")

        # Load region dictionary if it exists
        region_path = DICT_DIR / 'area_dict.parquet'  # Using 'area' consistent with existing code
        if region_path.exists():
            try:
                df = pl.read_parquet(region_path)
                if 'area' in df.columns and 'area_id' in df.columns:
                    self._mappings['region'] = {}
                    self._reverse_mappings['region'] = {}
                    for row in df.iter_rows(named=True):
                        code = row['area']
                        id_val = row['area_id']
                        if id_val is not None and code is not None:
                            self._mappings['region'][code] = id_val
                            self._reverse_mappings['region'][id_val] = code
            except Exception as e:
                logging.warning(f"Error loading region dictionary for mapping: {e}")

    def add_mapping(self, entity_type: str, external_code: str, internal_id: int):
        """
        Add a bidirectional mapping between external code and internal ID.

        Args:
            entity_type: Type of entity ('stock', 'industry', 'region')
            external_code: External identifier code
            internal_id: Internal ID value
        """
        if not self._initialized:
            self.initialize()

        if entity_type not in self._mappings:
            self._mappings[entity_type] = {}
            self._reverse_mappings[entity_type] = {}

        self._mappings[entity_type][external_code] = internal_id
        self._reverse_mappings[entity_type][internal_id] = external_code

    def get_id(self, entity_type: str, external_code: str) -> Optional[int]:
        """
        Get the internal ID for the given external code.

        Args:
            entity_type: Type of entity ('stock', 'industry', 'region')
            external_code: External identifier code

        Returns:
            Internal ID if exists, None otherwise
        """
        if not self._initialized:
            self.initialize()

        return self._mappings.get(entity_type, {}).get(external_code)

    def get_code(self, entity_type: str, internal_id: int) -> Optional[str]:
        """
        Get the external code for the given internal ID.

        Args:
            entity_type: Type of entity ('stock', 'industry', 'region')
            internal_id: Internal ID value

        Returns:
            External code if exists, None otherwise
        """
        if not self._initialized:
            self.initialize()

        return self._reverse_mappings.get(entity_type, {}).get(internal_id)

    def has_mapping(self, entity_type: str, external_code: str) -> bool:
        """
        Check if a mapping exists for the given external code.

        Args:
            entity_type: Type of entity ('stock', 'industry', 'region')
            external_code: External identifier code

        Returns:
            True if mapping exists, False otherwise
        """
        if not self._initialized:
            self.initialize()

        return external_code in self._mappings.get(entity_type, {})

    def remove_mapping(self, entity_type: str, external_code: str):
        """
        Remove a mapping for the given external code.

        Args:
            entity_type: Type of entity ('stock', 'industry', 'region')
            external_code: External identifier code
        """
        if not self._initialized:
            self.initialize()

        if entity_type in self._mappings and external_code in self._mappings[entity_type]:
            internal_id = self._mappings[entity_type][external_code]
            del self._mappings[entity_type][external_code]
            if entity_type in self._reverse_mappings and internal_id in self._reverse_mappings[entity_type]:
                del self._reverse_mappings[entity_type][internal_id]

    def get_all_mappings(self, entity_type: str) -> Dict[str, int]:
        """
        Get all mappings for the given entity type.

        Args:
            entity_type: Type of entity ('stock', 'industry', 'region')

        Returns:
            Dictionary of external code to internal ID mappings
        """
        if not self._initialized:
            self.initialize()

        return self._mappings.get(entity_type, {})

    def get_all_reverse_mappings(self, entity_type: str) -> Dict[int, str]:
        """
        Get all reverse mappings for the given entity type.

        Args:
            entity_type: Type of entity ('stock', 'industry', 'region')

        Returns:
            Dictionary of internal ID to external code mappings
        """
        if not self._initialized:
            self.initialize()

        return self._reverse_mappings.get(entity_type, {})