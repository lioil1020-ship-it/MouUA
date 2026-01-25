"""Controllers package - Configuration management for ModUA application.

This package provides:
- AppController: Main application controller for config management
- DataBroker: Thread-safe data cache for polled values
- Validation utilities: Data normalization and validation
- Configuration builders: Driver and parameter normalization
- Serialization: Project import/export functionality
"""

# Expose main classes for backward compatibility
from .base_controller import AppController
from .data_manager import DataBroker
from .validators import (
    SIZE_MAP,
    to_numeric_flag,
    normalize_dict_flags,
    is_tcp_like_driver,
    parse_adapter_string,
    format_adapter_with_ip,
)
from .config_builder import (
    detect_interface_for_ip,
    detect_outbound_ip,
    normalize_communication_params,
    normalize_opcua_network_adapter,
    build_device_timing_for_driver,
)
from .serializers import (
    export_tags_to_csv,
    is_array_tag,
    normalize_address_number,
)

__all__ = [
    # Main classes
    "AppController",
    "DataBroker",
    # Validators
    "SIZE_MAP",
    "to_numeric_flag",
    "normalize_dict_flags",
    "is_tcp_like_driver",
    "parse_adapter_string",
    "format_adapter_with_ip",
    # Config builders
    "detect_interface_for_ip",
    "detect_outbound_ip",
    "normalize_communication_params",
    "normalize_opcua_network_adapter",
    "build_device_timing_for_driver",
    # Serializers
    "export_tags_to_csv",
    "is_array_tag",
    "normalize_address_number",
]
