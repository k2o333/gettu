"""
ID Allocator Module
===================

This module provides permanent integer ID allocation functionality that ensures IDs
remain immutable once assigned. The system uses a range-based allocation where:
- Stock IDs: 1000000-1999999
- Industry IDs: 2000000-2999999
- Region IDs: 3000000-3999999
"""

import logging
from pathlib import Path
import polars as pl
from typing import Dict, Optional, Set
from config import DICT_DIR


class IDAllocator:
    """
    Manages permanent integer ID allocation ensuring immutability and range-based assignment.
    """

    # ID ranges for different entity types
    ID_RANGES = {
        'stock': (1000000, 1999999),
        'industry': (2000000, 2999999),
        'region': (3000000, 3999999)
    }

    def __init__(self):
        self.allocated_ids: Dict[str, Set[int]] = {}
        self.id_to_code: Dict[str, Dict[int, str]] = {}
        self.code_to_id: Dict[str, Dict[str, int]] = {}
        self.next_available_id: Dict[str, int] = {}
        self._initialized = False

    def initialize(self):
        """
        Initialize the ID allocator by loading existing allocations from storage.
        """
        if self._initialized:
            return

        # Load existing allocations from persisted dictionary files
        self._load_existing_allocations()
        self._initialized = True

    def _load_existing_allocations(self):
        """
        Load existing ID allocations from persisted dictionary files.
        """
        # Initialize empty sets for each type
        for entity_type in self.ID_RANGES:
            self.allocated_ids[entity_type] = set()
            self.id_to_code[entity_type] = {}
            self.code_to_id[entity_type] = {}
            self.next_available_id[entity_type] = self.ID_RANGES[entity_type][0]

        # Load stock dictionary if it exists
        stock_path = DICT_DIR / 'stock_basic_dict.parquet'
        if stock_path.exists():
            try:
                df = pl.read_parquet(stock_path)
                if 'ts_code' in df.columns and 'ts_code_id' in df.columns:
                    for row in df.iter_rows(named=True):
                        code = row['ts_code']
                        id_val = row['ts_code_id']
                        if id_val is not None and code is not None:
                            self.allocated_ids['stock'].add(id_val)
                            self.id_to_code['stock'][id_val] = code
                            self.code_to_id['stock'][code] = id_val
                            # Update next available ID if necessary, but ensure it stays within range
                            if id_val >= self.next_available_id['stock'] and self.ID_RANGES['stock'][0] <= id_val <= self.ID_RANGES['stock'][1]:
                                self.next_available_id['stock'] = id_val + 1
                                # Ensure we don't exceed the range
                                if self.next_available_id['stock'] > self.ID_RANGES['stock'][1]:
                                    self.next_available_id['stock'] = self.ID_RANGES['stock'][1] + 1
            except Exception as e:
                logging.warning(f"Error loading stock dictionary for ID allocation: {e}")

        # Load industry dictionary if it exists
        industry_path = DICT_DIR / 'industry_dict.parquet'
        if industry_path.exists():
            try:
                df = pl.read_parquet(industry_path)
                if 'industry' in df.columns and 'industry_id' in df.columns:
                    for row in df.iter_rows(named=True):
                        code = row['industry']
                        id_val = row['industry_id']
                        if id_val is not None and code is not None:
                            self.allocated_ids['industry'].add(id_val)
                            self.id_to_code['industry'][id_val] = code
                            self.code_to_id['industry'][code] = id_val
                            # Update next available ID if necessary, but ensure it stays within range
                            if id_val >= self.next_available_id['industry'] and self.ID_RANGES['industry'][0] <= id_val <= self.ID_RANGES['industry'][1]:
                                self.next_available_id['industry'] = id_val + 1
                                # Ensure we don't exceed the range
                                if self.next_available_id['industry'] > self.ID_RANGES['industry'][1]:
                                    self.next_available_id['industry'] = self.ID_RANGES['industry'][1] + 1
            except Exception as e:
                logging.warning(f"Error loading industry dictionary for ID allocation: {e}")

        # Load region dictionary if it exists
        region_path = DICT_DIR / 'area_dict.parquet'  # Using 'area' consistent with existing code
        if region_path.exists():
            try:
                df = pl.read_parquet(region_path)
                if 'area' in df.columns and 'area_id' in df.columns:
                    for row in df.iter_rows(named=True):
                        code = row['area']
                        id_val = row['area_id']
                        if id_val is not None and code is not None:
                            self.allocated_ids['region'].add(id_val)
                            self.id_to_code['region'][id_val] = code
                            self.code_to_id['region'][code] = id_val
                            # Update next available ID if necessary, but ensure it stays within range
                            if id_val >= self.next_available_id['region'] and self.ID_RANGES['region'][0] <= id_val <= self.ID_RANGES['region'][1]:
                                self.next_available_id['region'] = id_val + 1
                                # Ensure we don't exceed the range
                                if self.next_available_id['region'] > self.ID_RANGES['region'][1]:
                                    self.next_available_id['region'] = self.ID_RANGES['region'][1] + 1
            except Exception as e:
                logging.warning(f"Error loading region dictionary for ID allocation: {e}")

    def allocate_id(self, entity_type: str, external_code: str) -> int:
        """
        Allocate a permanent ID for the given entity type and external code.

        Args:
            entity_type: Type of entity ('stock', 'industry', 'region')
            external_code: External identifier code

        Returns:
            Allocated permanent ID
        """
        if entity_type not in self.ID_RANGES:
            raise ValueError(f"Unsupported entity type: {entity_type}")

        if not self._initialized:
            self.initialize()

        # Check if ID already exists for this code
        if external_code in self.code_to_id[entity_type]:
            return self.code_to_id[entity_type][external_code]

        # For new codes, allocate IDs in the proper range
        min_id, max_id = self.ID_RANGES[entity_type]

        # If next available ID is outside the range, check if we can reset to start of range
        if self.next_available_id[entity_type] < min_id or self.next_available_id[entity_type] > max_id:
            # Check if the minimum ID is available
            if min_id not in self.allocated_ids[entity_type]:
                self.next_available_id[entity_type] = min_id
            else:
                # Find the first available ID in the range
                candidate = min_id
                while candidate in self.allocated_ids[entity_type] and candidate <= max_id:
                    candidate += 1
                if candidate > max_id:
                    raise ValueError(f"No more available IDs for {entity_type}")
                self.next_available_id[entity_type] = candidate

        # If we're still outside valid range, allocate appropriately
        if self.next_available_id[entity_type] < min_id:
            self.next_available_id[entity_type] = min_id
        elif self.next_available_id[entity_type] > max_id:
            raise ValueError(f"No more available IDs for {entity_type}")

        # Allocate new ID
        new_id = self.next_available_id[entity_type]

        # Update tracking structures
        self.allocated_ids[entity_type].add(new_id)
        self.id_to_code[entity_type][new_id] = external_code
        self.code_to_id[entity_type][external_code] = new_id

        # Increment for next allocation
        self.next_available_id[entity_type] = new_id + 1

        # Ensure we're within bounds
        if self.next_available_id[entity_type] > max_id:
            raise ValueError(f"Exceeded ID range for {entity_type}")

        logging.debug(f"Allocated {entity_type} ID {new_id} for code '{external_code}'")
        return new_id

    def get_id(self, entity_type: str, external_code: str) -> Optional[int]:
        """
        Get the ID for the given external code, if it exists.

        Args:
            entity_type: Type of entity ('stock', 'industry', 'region')
            external_code: External identifier code

        Returns:
            ID if exists, None otherwise
        """
        if not self._initialized:
            self.initialize()

        return self.code_to_id.get(entity_type, {}).get(external_code)

    def get_code(self, entity_type: str, id_val: int) -> Optional[str]:
        """
        Get the external code for the given ID, if it exists.

        Args:
            entity_type: Type of entity ('stock', 'industry', 'region')
            id_val: Internal ID value

        Returns:
            External code if exists, None otherwise
        """
        if not self._initialized:
            self.initialize()

        return self.id_to_code.get(entity_type, {}).get(id_val)

    def validate_id(self, entity_type: str, id_val: int) -> bool:
        """
        Validate that the given ID is within the correct range for the entity type.

        Args:
            entity_type: Type of entity ('stock', 'industry', 'region')
            id_val: Internal ID value

        Returns:
            True if valid, False otherwise
        """
        if not self._initialized:
            self.initialize()

        if entity_type not in self.ID_RANGES:
            return False

        min_id, max_id = self.ID_RANGES[entity_type]
        return min_id <= id_val <= max_id and id_val in self.allocated_ids[entity_type]

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

        return len(self.allocated_ids.get(entity_type, set()))

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

        return self.next_available_id.get(entity_type, self.ID_RANGES[entity_type][0])