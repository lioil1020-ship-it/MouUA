"""Modbus and application-wide constants.

Centralized definitions for Modbus address formats, prefixes, and other
configuration values used across the project.
"""

# ============================================================================
# Modbus 6-Digit Address Format Constants (IEC 61131)
# ============================================================================

# Modbus function code / address type prefixes (first digit)
MODBUS_COIL_PREFIX = "0"              # Coils: 0XXXXX (1-65536)
MODBUS_DISCRETE_PREFIX = "1"          # Discrete Inputs: 1XXXXX (100001-165536)
MODBUS_INPUT_REG_PREFIX = "3"         # Input Registers: 3XXXXX (300001-365536)
MODBUS_HOLDING_REG_PREFIX = "4"       # Holding Registers: 4XXXXX (400001-465536)

# Address offset value used in address extraction and reconstruction
# Used to extract sequence number: address % MODBUS_ADDRESS_OFFSET
MODBUS_ADDRESS_OFFSET = 100000

# Sequence number width in 6-digit format (e.g., "424576" has 5-digit sequence)
MODBUS_SEQUENCE_WIDTH = 5

# Modbus IEC 61131 standard address ranges (6-digit notation)
# Each range: (min_address, max_address, offset, type_name)
MODBUS_ADDRESS_RANGES = [
    # Holding Registers: 400001-465536 (offset: 400000)
    {"type": "holding_register", "min": 400001, "max": 465536, "offset": 400000},
    # Input Registers: 300001-365536 (offset: 300000)
    {"type": "input_register", "min": 300001, "max": 365536, "offset": 300000},
    # Discrete Inputs: 100001-165536 (offset: 100000)
    {"type": "discrete_input", "min": 100001, "max": 165536, "offset": 100000},
    # Coils: 1-65536 (offset: 0)
    {"type": "coil", "min": 1, "max": 65536, "offset": 0},
]

# ============================================================================
# Modbus Default Values
# ============================================================================

# Default scan rate (milliseconds)
MODBUS_DEFAULT_SCAN_RATE = 1000

# Default baud rate for serial connections
MODBUS_DEFAULT_BAUD_RATE = 9600

# Default timeout (seconds)
MODBUS_DEFAULT_TIMEOUT = 5

# ============================================================================
# Data Buffer Constants
# ============================================================================

# Maximum number of values to buffer before flushing
DATA_BUFFER_MAX_SIZE = 2000

# Maximum time (seconds) to wait before flushing buffer
DATA_BUFFER_MAX_AGE = 120

# ============================================================================
# OPC UA Constants
# ============================================================================

# Default OPC UA server endpoint
OPC_UA_DEFAULT_ENDPOINT = "opc.tcp://localhost:4840"

# Default OPC UA namespace
OPC_UA_DEFAULT_NAMESPACE = "ModUA"

# Default OPC UA port
OPC_UA_DEFAULT_PORT = 4840

# ============================================================================
# Hierarchy and Structure Constants
# ============================================================================

# Group/Tag hierarchy separator character
# Used for creating multi-level group structures (e.g., "Data.Point.01")
GROUP_SEPARATOR = "."

# ============================================================================
# CSV Export/Import Constants
# ============================================================================

# CSV field names for device tag export
CSV_FIELD_NAMES = [
    "Name",
    "Description",
    "Address",
    "Data Type",
    "Access",
    "Scan Rate",
    "Scaling Type",
    "Raw Low",
    "Raw High",
    "Scaled Type",
    "Scaled Low",
    "Scaled High",
    "Clamp Low",
    "Clamp High",
    "Negate",
    "Units",
]
