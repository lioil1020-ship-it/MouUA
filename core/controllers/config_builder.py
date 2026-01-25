"""Configuration building and driver/parameter normalization.

This module handles the complex logic for normalizing driver configurations,
network adapter settings, and device timing parameters.
"""

import socket
from typing import Any, Dict, Optional, Tuple
from .validators import is_tcp_like_driver, parse_adapter_string, format_adapter_with_ip

try:
    import psutil
except ImportError:
    psutil = None


def detect_interface_for_ip(ip_addr: str) -> Optional[str]:
    """Try to find the system network interface that has the given IP address."""
    if not psutil:
        return None
    
    try:
        for ifn, addrs in psutil.net_if_addrs().items():
            for addr_info in addrs:
                try:
                    family = getattr(addr_info, 'family', None)
                    address = getattr(addr_info, 'address', None)
                    # Check for IPv4 (socket.AF_INET is 2)
                    if family == socket.AF_INET and address == str(ip_addr):
                        return ifn
                except Exception:
                    continue
        return None
    except Exception:
        return None


def detect_outbound_ip() -> str:
    """Detect the local IP that would be used for outbound connections.
    
    Creates a dummy connection to 8.8.8.8:80 and reads the source IP.
    Falls back to 127.0.0.1 if detection fails.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.2)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return '127.0.0.1'


def normalize_communication_params(params: Dict[str, Any], driver_type: Optional[str] = None) -> Dict[str, Any]:
    """Normalize communication parameters for a channel.
    
    For TCP-like drivers, ensures network_adapter and network_adapter_ip are set.
    Converts various adapter format variations to standard form.
    
    Args:
        params: Communication parameters dict
        driver_type: Optional driver type to determine TCP-likeness
        
    Returns:
        Normalized params dict
    """
    if not isinstance(params, dict):
        return params or {}
    
    out = dict(params)
    
    if not is_tcp_like_driver(driver_type):
        return out
    
    # Try to establish network_adapter and network_adapter_ip
    adapter_raw = out.get('adapter') or out.get('adapter_name') or out.get('adapter_ip')
    
    if adapter_raw:
        # Parse existing adapter string
        name, ip = parse_adapter_string(str(adapter_raw))
        if name:
            out['network_adapter'] = name
        if ip:
            out['network_adapter_ip'] = ip
    else:
        # No adapter info; try to map from IP
        ip_val = out.get('ip')
        if ip_val and 'network_adapter' not in out:
            # Try to find interface with this IP
            iface = detect_interface_for_ip(str(ip_val))
            if iface:
                out['network_adapter'] = f"{iface} ({ip_val})"
                out['network_adapter_ip'] = str(ip_val)
            else:
                # Default to "Auto" with detected outbound IP
                detected_ip = detect_outbound_ip()
                out['network_adapter'] = f"Auto ({detected_ip})"
                out['network_adapter_ip'] = detected_ip
    
    return out


def normalize_opcua_network_adapter(opc_settings: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize OPC UA settings to ensure network_adapter/network_adapter_ip are canonical.
    
    Args:
        opc_settings: OPC UA configuration dict
        
    Returns:
        Normalized OPC UA settings dict
    """
    try:
        from copy import deepcopy
        out = deepcopy(opc_settings) if opc_settings else {}
    except Exception:
        out = dict(opc_settings or {})
    
    if not isinstance(out, dict):
        return out
    
    # Get general section or use top-level dict
    gen = out.get('general', {}) if isinstance(out.get('general'), dict) else out
    if not isinstance(gen, dict):
        gen = out
    
    na = gen.get('network_adapter')
    nip = gen.get('network_adapter_ip')
    
    # Parse existing network_adapter format
    if na and isinstance(na, str):
        parsed_name, parsed_ip = parse_adapter_string(na)
        if parsed_name:
            gen['network_adapter'] = parsed_name
        if parsed_ip:
            gen['network_adapter_ip'] = parsed_ip
    
    # If we still don't have IP, try to resolve from interface name
    if not gen.get('network_adapter_ip') and gen.get('network_adapter') and psutil:
        try:
            ifname = gen.get('network_adapter')
            infos = psutil.net_if_addrs()
            for ifn, addrs in infos.items():
                if ifn == ifname or ifname in ifn:
                    for addr_info in addrs:
                        try:
                            family = getattr(addr_info, 'family', None)
                            address = getattr(addr_info, 'address', None)
                            # Check for IPv4
                            if family == socket.AF_INET and address and not str(address).startswith('127.'):
                                gen['network_adapter_ip'] = address
                                break
                        except Exception:
                            continue
                    if gen.get('network_adapter_ip'):
                        break
        except Exception:
            pass
    
    # If still missing IP, use outbound detection
    if not gen.get('network_adapter_ip'):
        gen['network_adapter_ip'] = detect_outbound_ip()
    
    # Write back
    if isinstance(out, dict):
        if isinstance(out.get('general'), dict):
            out['general'].update(gen)
        else:
            out.update(gen)
    
    return out


def build_device_timing_for_driver(driver_type: Optional[str]) -> Dict[str, int]:
    """Build default timing parameters dict based on driver type.
    
    Different driver types require different timing parameters.
    RTU over TCP needs connect_timeout, while RTU Serial may not.
    
    Args:
        driver_type: The driver type string
        
    Returns:
        Dict with appropriate timing keys and default values
    """
    defaults = {
        'connect_timeout': 3,
        'connect_attempts': 1,
        'request_timeout': 1000,
        'attempts_before_timeout': 1,
        'inter_request_delay': 0,
    }
    
    drv_low = str(driver_type or '').lower()
    
    if 'over tcp' in drv_low or 'ethernet' in drv_low:
        # RTU over TCP
        desired = ['connect_timeout', 'connect_attempts', 'request_timeout', 
                   'attempts_before_timeout', 'inter_request_delay']
    elif 'tcp' in drv_low:
        # TCP Ethernet
        desired = ['connect_timeout', 'request_timeout', 
                   'attempts_before_timeout', 'inter_request_delay']
    else:
        # Serial RTU
        desired = ['request_timeout', 'attempts_before_timeout', 'inter_request_delay']
    
    return {k: defaults[k] for k in desired}


__all__ = [
    "detect_interface_for_ip",
    "detect_outbound_ip",
    "normalize_communication_params",
    "normalize_opcua_network_adapter",
    "build_device_timing_for_driver",
]
