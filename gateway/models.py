from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from enum import Enum

class ModbusProtocol(Enum):
    RTU = "rtu"
    TCP = "tcp"

class RegisterType(Enum):
    HOLDING = "HOLDING"
    INPUT = "INPUT"
    COIL = "COIL"
    DISCRETE_INPUT = "DISCRETE_INPUT"

class DataType(Enum):
    INT16 = "INT16"
    UINT16 = "UINT16"
    INT32 = "INT32"
    UINT32 = "UINT32"
    FLOAT32 = "FLOAT32"
    STRING = "STRING"
    BITMASK = "BITMASK"
    ENUM = "ENUM"
    VERSION = "VERSION"
    MAC_ADDRESS = "MAC_ADDRESS"

class BackendType(Enum):
    WEBSOCKET = "websocket"
    MQTT = "mqtt"
    HTTP = "http"

@dataclass
class AppConfig:
    """Application configuration."""
    name: str = "Modbus Gateway"
    version: str = "1.0.0"
    description: str = "Industrial Modbus to Cloud Gateway"
    log_level: str = "INFO"

@dataclass
class RegisterConfig:
    """Modbus register configuration."""
    name: str
    address: int
    type: RegisterType
    data_type: DataType
    scale: float = 1.0
    unit: str = ""
    byte_order: str = "ABCD"
    register_count: Optional[int] = None
    bits: Optional[Dict[str, int]] = None
    description: str = ""

@dataclass
class DeviceConfig:
    """Device configuration with nested registers."""
    table_id: int
    protocol: ModbusProtocol
    slave_id: int
    timeout: float = 3.0
    port: Optional[str] = None  # RTU port
    baudrate: int = 9600
    bytesize: int = 8
    parity: str = "N"
    stopbits: int = 1
    host: Optional[str] = None  # TCP host
    port_tcp: int = 502
    description: str = ""
    registers: List[RegisterConfig] = field(default_factory=list)

@dataclass
class BackendConfig:
    """Backend connection configuration."""
    type: BackendType
    url: str
    subprotocol: Optional[str] = None
    reconnect_interval: int = 5
    timeout: int = 10
    tls: bool = False

@dataclass
class SettingsConfig:
    """Gateway settings."""
    read_interval: int = 5
    heartbeat_interval: int = 30
    retry_count: int = 3
    retry_delay: int = 2
    max_recovery_attempts: int = 3
    recovery_interval: int = 5

@dataclass
class GatewayConfig:
    """Complete gateway configuration."""
    app: AppConfig
    device: DeviceConfig
    backend: BackendConfig
    settings: SettingsConfig