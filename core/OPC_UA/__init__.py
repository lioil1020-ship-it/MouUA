# -*- coding: utf-8 -*-
"""OPC UA module

This module exposes the in-package OPC UA server implementation.
Provides a clean, dynamic OPC UA server that maps all project tags
without hardcoding.
"""

from .opcua_server import OPCUAServer, OPCServer

__all__ = ["OPCUAServer", "OPCServer"]
