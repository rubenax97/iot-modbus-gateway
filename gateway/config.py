import json
import logging
from typing import Optional
from pathlib import Path

from .models import (
    GatewayConfig, AppConfig, DeviceConfig, RegisterConfig, BackendConfig, SettingsConfig,
    ModbusProtocol, RegisterType, DataType, BackendType
)

logger = logging.getLogger(__name__)

class ConfigLoader:
    """Load and validate configuration from JSON files."""
    
    @staticmethod
    def load_config(config_path: str) -> GatewayConfig:
        """Load gateway configuration from JSON file."""
        try:
            with open(config_path, 'r') as f:
                data = json.load(f)
            
            # Load app configuration
            app_data = data.get('app', {})
            app_config = AppConfig(
                name=app_data.get('name', 'Modbus Gateway'),
                version=app_data.get('version', '1.0.0'),
                description=app_data.get('description', 'Industrial Modbus to Cloud Gateway'),
                log_level=app_data.get('log_level', 'INFO')
            )
            
            # Load device configuration
            device_data = data.get('device', {})
            
            # Load register configurations (nested inside device)
            register_configs = []
            for reg_data in device_data.get('registers', []):
                register_configs.append(
                    RegisterConfig(
                        name=reg_data['name'],
                        address=reg_data['address'],
                        type=RegisterType(reg_data.get('type', 'INPUT')),
                        data_type=DataType(reg_data.get('data_type', 'UINT16')),
                        scale=reg_data.get('scale', 1.0),
                        unit=reg_data.get('unit', ''),
                        byte_order=reg_data.get('byte_order', 'ABCD'),
                        register_count=reg_data.get('register_count'),
                        bits=reg_data.get('bits'),
                        description=reg_data.get('description', '')
                    )
                )
            
            device_config = DeviceConfig(
                table_id=device_data.get('table_id', 1),
                protocol=ModbusProtocol(device_data.get('protocol', 'rtu')),
                slave_id=device_data.get('slave_id', 1),
                timeout=device_data.get('timeout', 3.0),
                port=device_data.get('port'),  # RTU
                baudrate=device_data.get('baudrate', 9600),
                bytesize=device_data.get('bytesize', 8),
                parity=device_data.get('parity', 'N'),
                stopbits=device_data.get('stopbits', 1),
                host=device_data.get('host'),  # TCP
                port_tcp=device_data.get('port', 502),  # TCP port (uses 'port' field)
                description=device_data.get('description', ''),
                registers=register_configs
            )
            
            # Load backend configuration
            backend_data = data.get('backend', {})
            backend_config = BackendConfig(
                type=BackendType(backend_data.get('type', 'websocket')),
                url=backend_data.get('url', 'ws://localhost:8765'),
                subprotocol=backend_data.get('subprotocol', 'modbus.gateway'),
                reconnect_interval=backend_data.get('reconnect_interval', 5),
                timeout=backend_data.get('timeout', 10),
                tls=backend_data.get('tls', False)
            )
            
            # Load settings
            settings_data = data.get('settings', {})
            settings_config = SettingsConfig(
                read_interval=settings_data.get('read_interval', 5),
                heartbeat_interval=settings_data.get('heartbeat_interval', 30),
                retry_count=settings_data.get('retry_count', 3),
                retry_delay=settings_data.get('retry_delay', 2),
                max_recovery_attempts=settings_data.get('max_recovery_attempts', 3),
                recovery_interval=settings_data.get('recovery_interval', 5)
            )
            
            return GatewayConfig(
                app=app_config,
                device=device_config,
                backend=backend_config,
                settings=settings_config
            )
            
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file: {e}")
            raise
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise