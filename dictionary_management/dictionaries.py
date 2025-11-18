"""
Dictionary Classes
=================

This module provides specialized dictionary implementations for different entity types:
- Stock dictionaries
- Industry dictionaries
- Region dictionaries
"""

import logging
from typing import Dict, Optional, List
import polars as pl
from config import DICT_DIR
from .id_allocator import IDAllocator
from .mapping_manager import MappingManager
from .version_control import DictionaryVersionControl


class StockDictionary:
    """
    Manages stock code to internal ID mappings with metadata.
    """

    def __init__(self, id_allocator: IDAllocator, mapping_manager: MappingManager,
                 version_control: DictionaryVersionControl):
        self.id_allocator = id_allocator
        self.mapping_manager = mapping_manager
        self.version_control = version_control
        self._data: Optional[pl.DataFrame] = None
        self._initialized = False

    def initialize(self):
        """
        Initialize the stock dictionary by loading existing data.
        """
        if self._initialized:
            return

        # Load existing stock data
        self._load_existing_data()
        self._initialized = True

    def _load_existing_data(self):
        """
        Load existing stock data from persisted files.
        """
        stock_path = DICT_DIR / 'stock_basic_dict.parquet'
        if stock_path.exists():
            try:
                self._data = pl.read_parquet(stock_path)
                # Ensure ID mappings are loaded
                if 'ts_code' in self._data.columns and 'ts_code_id' in self._data.columns:
                    for row in self._data.iter_rows(named=True):
                        code = row['ts_code']
                        id_val = row['ts_code_id']
                        if id_val is not None and code is not None:
                            self.mapping_manager.add_mapping('stock', code, id_val)
            except Exception as e:
                logging.warning(f"Error loading stock dictionary data: {e}")

    def add_stock(self, ts_code: str, metadata: Dict[str, any] = None) -> int:
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

        # Allocate permanent ID
        stock_id = self.id_allocator.allocate_id('stock', ts_code)

        # Add mapping
        self.mapping_manager.add_mapping('stock', ts_code, stock_id)

        # Update data if it exists
        if self._data is not None:
            # Check if stock already exists
            existing = self._data.filter(pl.col('ts_code') == ts_code)
            if len(existing) == 0:
                # Add new row
                # Create a dictionary matching the column structure of existing data
                new_row_data = {}
                # Add columns in the same order as existing data
                for col in self._data.columns:
                    if col == 'ts_code':
                        new_row_data[col] = [ts_code]
                    elif col == 'ts_code_id':
                        new_row_data[col] = [stock_id]
                    else:
                        # Use the same data type as in the existing data
                        new_row_data[col] = [None]  # Initialize with None

                new_row = pl.DataFrame(new_row_data)

                # Ensure the columns have the same data types as in existing data
                for col in new_row.columns:
                    if col in self._data.schema:
                        target_type = self._data.schema[col]
                        # Handle categorical types specially
                        if str(target_type) == 'Categorical':
                            # Only cast if the value is not None
                            new_row = new_row.with_columns([
                                pl.when(pl.col(col).is_not_null())
                                .then(pl.col(col).cast(pl.Utf8).cast(pl.Categorical))
                                .otherwise(pl.lit(None))
                                .alias(col)
                            ])
                        elif target_type != pl.Null:
                            new_row = new_row.cast({col: target_type})

                # Add metadata columns if provided
                if metadata:
                    for key, value in metadata.items():
                        if key in new_row.columns:
                            # Update existing column
                            new_row = new_row.with_columns([pl.lit(value).alias(key)])
                        else:
                            # Add as new column
                            new_row = new_row.with_columns([pl.lit(value).alias(key)])

                self._data = pl.concat([self._data, new_row])
            else:
                # Update existing row ID if needed
                self._data = self._data.with_columns([
                    pl.when(pl.col('ts_code') == ts_code)
                    .then(pl.lit(stock_id))
                    .otherwise(pl.col('ts_code_id'))
                    .alias('ts_code_id')
                ])
        else:
            # Create new data frame
            data_dict = {'ts_code': [ts_code], 'ts_code_id': [stock_id]}
            # Add metadata columns if provided
            if metadata:
                for key, value in metadata.items():
                    data_dict[key] = [value]
            self._data = pl.DataFrame(data_dict)

        # Save to persistent storage
        self._save_data()

        logging.debug(f"Added stock {ts_code} with ID {stock_id}")
        return stock_id

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

        return self.mapping_manager.get_id('stock', ts_code)

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

        return self.mapping_manager.get_code('stock', stock_id)

    def get_stock_data(self, ts_code: str) -> Optional[pl.DataFrame]:
        """
        Get the complete data for the given stock code.

        Args:
            ts_code: Stock code (e.g., '000001.SZ')

        Returns:
            Data frame with stock data if exists, None otherwise
        """
        if not self._initialized:
            self.initialize()

        if self._data is not None:
            result = self._data.filter(pl.col('ts_code') == ts_code)
            return result if len(result) > 0 else None
        return None

    def get_all_stocks(self) -> List[str]:
        """
        Get all stock codes in the dictionary.

        Returns:
            List of stock codes
        """
        if not self._initialized:
            self.initialize()

        if self._data is not None:
            return self._data['ts_code'].to_list()
        return []

    def _save_data(self):
        """
        Save stock data to persistent storage.
        """
        try:
            if self._data is not None:
                DICT_DIR.mkdir(parents=True, exist_ok=True)
                stock_path = DICT_DIR / 'stock_basic_dict.parquet'
                self._data.write_parquet(stock_path)
        except Exception as e:
            logging.warning(f"Error saving stock dictionary data: {e}")

    def validate_stock_id(self, stock_id: int) -> bool:
        """
        Validate that the given stock ID is valid.

        Args:
            stock_id: Internal stock ID

        Returns:
            True if valid, False otherwise
        """
        if not self._initialized:
            self.initialize()

        return self.id_allocator.validate_id('stock', stock_id)


class IndustryDictionary:
    """
    Manages industry classification dictionary.
    """

    def __init__(self, id_allocator: IDAllocator, mapping_manager: MappingManager,
                 version_control: DictionaryVersionControl):
        self.id_allocator = id_allocator
        self.mapping_manager = mapping_manager
        self.version_control = version_control
        self._data: Optional[pl.DataFrame] = None
        self._initialized = False

    def initialize(self):
        """
        Initialize the industry dictionary by loading existing data.
        """
        if self._initialized:
            return

        # Load existing industry data
        self._load_existing_data()
        self._initialized = True

    def _load_existing_data(self):
        """
        Load existing industry data from persisted files.
        """
        industry_path = DICT_DIR / 'industry_dict.parquet'
        if industry_path.exists():
            try:
                self._data = pl.read_parquet(industry_path)
                # Ensure ID mappings are loaded
                if 'industry' in self._data.columns and 'industry_id' in self._data.columns:
                    for row in self._data.iter_rows(named=True):
                        code = row['industry']
                        id_val = row['industry_id']
                        if id_val is not None and code is not None:
                            self.mapping_manager.add_mapping('industry', code, id_val)
            except Exception as e:
                logging.warning(f"Error loading industry dictionary data: {e}")

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

        # Allocate permanent ID
        industry_id = self.id_allocator.allocate_id('industry', industry_code)

        # Add mapping
        self.mapping_manager.add_mapping('industry', industry_code, industry_id)

        # Update data if it exists
        if self._data is not None:
            # Check if industry already exists
            existing = self._data.filter(pl.col('industry') == industry_code)
            if len(existing) == 0:
                # Add new row
                # Match the existing column structure and data types
                data_dict = {'industry_id': [industry_id], 'industry': [industry_code]}
                # Add industry_cat column if it exists in the existing data
                if 'industry_cat' in self._data.columns:
                    data_dict['industry_cat'] = [industry_code]
                new_row = pl.DataFrame(data_dict)

                # Ensure the columns have the same data types as in existing data
                for col in new_row.columns:
                    if col in self._data.schema:
                        target_type = self._data.schema[col]
                        # Handle categorical types specially
                        if str(target_type) == 'Categorical':
                            new_row = new_row.with_columns([
                                pl.col(col).cast(pl.Utf8).cast(pl.Categorical).alias(col)
                            ])
                        else:
                            new_row = new_row.cast({col: target_type})

                self._data = pl.concat([self._data, new_row])
            else:
                # Update existing row ID if needed
                self._data = self._data.with_columns([
                    pl.when(pl.col('industry') == industry_code)
                    .then(pl.lit(industry_id))
                    .otherwise(pl.col('industry_id'))
                    .alias('industry_id')
                ])
        else:
            # Create new data frame
            # Match the existing column structure
            data_dict = {'industry_id': [industry_id], 'industry': [industry_code]}
            # Add industry_cat column for consistency
            data_dict['industry_cat'] = [industry_code]
            self._data = pl.DataFrame(data_dict)

        # Save to persistent storage
        self._save_data()

        logging.debug(f"Added industry {industry_code} with ID {industry_id}")
        return industry_id

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

        return self.mapping_manager.get_id('industry', industry_code)

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

        return self.mapping_manager.get_code('industry', industry_id)

    def validate_industry_id(self, industry_id: int) -> bool:
        """
        Validate that the given industry ID is valid.

        Args:
            industry_id: Internal industry ID

        Returns:
            True if valid, False otherwise
        """
        if not self._initialized:
            self.initialize()

        return self.id_allocator.validate_id('industry', industry_id)

    def _save_data(self):
        """
        Save industry data to persistent storage.
        """
        try:
            if self._data is not None:
                DICT_DIR.mkdir(parents=True, exist_ok=True)
                industry_path = DICT_DIR / 'industry_dict.parquet'
                self._data.write_parquet(industry_path)
        except Exception as e:
            logging.warning(f"Error saving industry dictionary data: {e}")


class RegionDictionary:
    """
    Manages region classification dictionary.
    """

    def __init__(self, id_allocator: IDAllocator, mapping_manager: MappingManager,
                 version_control: DictionaryVersionControl):
        self.id_allocator = id_allocator
        self.mapping_manager = mapping_manager
        self.version_control = version_control
        self._data: Optional[pl.DataFrame] = None
        self._initialized = False

    def initialize(self):
        """
        Initialize the region dictionary by loading existing data.
        """
        if self._initialized:
            return

        # Load existing region data
        self._load_existing_data()
        self._initialized = True

    def _load_existing_data(self):
        """
        Load existing region data from persisted files.
        """
        region_path = DICT_DIR / 'area_dict.parquet'  # Using 'area' consistent with existing code
        if region_path.exists():
            try:
                self._data = pl.read_parquet(region_path)
                # Ensure ID mappings are loaded
                if 'area' in self._data.columns and 'area_id' in self._data.columns:
                    for row in self._data.iter_rows(named=True):
                        code = row['area']
                        id_val = row['area_id']
                        if id_val is not None and code is not None:
                            self.mapping_manager.add_mapping('region', code, id_val)
            except Exception as e:
                logging.warning(f"Error loading region dictionary data: {e}")

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

        # Allocate permanent ID
        region_id = self.id_allocator.allocate_id('region', region_code)

        # Add mapping
        self.mapping_manager.add_mapping('region', region_code, region_id)

        # Update data if it exists
        if self._data is not None:
            # Check if region already exists
            existing = self._data.filter(pl.col('area') == region_code)
            if len(existing) == 0:
                # Add new row
                # Match the existing column structure and data types
                data_dict = {'area_id': [region_id], 'area': [region_code]}
                # Add area_cat column if it exists in the existing data
                if 'area_cat' in self._data.columns:
                    data_dict['area_cat'] = [region_code]
                new_row = pl.DataFrame(data_dict)

                # Ensure the columns have the same data types as in existing data
                for col in new_row.columns:
                    if col in self._data.schema:
                        target_type = self._data.schema[col]
                        # Handle categorical types specially
                        if str(target_type) == 'Categorical':
                            new_row = new_row.with_columns([
                                pl.col(col).cast(pl.Utf8).cast(pl.Categorical).alias(col)
                            ])
                        else:
                            new_row = new_row.cast({col: target_type})

                self._data = pl.concat([self._data, new_row])
            else:
                # Update existing row ID if needed
                self._data = self._data.with_columns([
                    pl.when(pl.col('area') == region_code)
                    .then(pl.lit(region_id))
                    .otherwise(pl.col('area_id'))
                    .alias('area_id')
                ])
        else:
            # Create new data frame
            # Match the existing column structure
            data_dict = {'area_id': [region_id], 'area': [region_code]}
            # Add area_cat column for consistency
            data_dict['area_cat'] = [region_code]
            self._data = pl.DataFrame(data_dict)

        # Save to persistent storage
        self._save_data()

        logging.debug(f"Added region {region_code} with ID {region_id}")
        return region_id

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

        return self.mapping_manager.get_id('region', region_code)

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

        return self.mapping_manager.get_code('region', region_id)

    def validate_region_id(self, region_id: int) -> bool:
        """
        Validate that the given region ID is valid.

        Args:
            region_id: Internal region ID

        Returns:
            True if valid, False otherwise
        """
        if not self._initialized:
            self.initialize()

        return self.id_allocator.validate_id('region', region_id)

    def _save_data(self):
        """
        Save region data to persistent storage.
        """
        try:
            if self._data is not None:
                DICT_DIR.mkdir(parents=True, exist_ok=True)
                region_path = DICT_DIR / 'area_dict.parquet'
                self._data.write_parquet(region_path)
        except Exception as e:
            logging.warning(f"Error saving region dictionary data: {e}")