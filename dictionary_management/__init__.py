"""
Dictionary Management System
============================

This module provides a comprehensive dictionary management system for:
- Permanent ID allocation and management
- Bidirectional mapping between external codes and internal IDs
- Industry and region classification dictionaries
- Dictionary version control and historical tracking
- Cross-module ID consistency guarantees

The system ensures stable IDs throughout the system lifecycle and provides
efficient lookup mechanisms for all dictionary operations.
"""

# Core dictionary management classes
from .dictionary_manager import DictionaryManager
from .id_allocator import IDAllocator
from .mapping_manager import MappingManager
from .version_control import DictionaryVersionControl

# Dictionary types
from .dictionaries import (
    StockDictionary,
    IndustryDictionary,
    RegionDictionary
)

# Synchronization and integration
from .synchronizer import DictionarySynchronizer

__all__ = [
    'DictionaryManager',
    'IDAllocator',
    'MappingManager',
    'DictionaryVersionControl',
    'StockDictionary',
    'IndustryDictionary',
    'RegionDictionary',
    'DictionarySynchronizer'
]