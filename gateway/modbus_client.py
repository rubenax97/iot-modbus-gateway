import asyncio
import struct
import logging
from typing import Optional, Dict, List, Any
from datetime import datetime

from pymodbus.client import AsyncModbusSerialClient, AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException, ConnectionException
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

from .models import DeviceConfig, RegisterConfig, RegisterType, DataType

logger = logging.getLogger(__name__)

class AsyncModbusDevice:
    """Async Modbus device client with retry logic and error handling."""
    
    def __init__(self, config: DeviceConfig):
        self.config = config
        self.client = None
        self._lock = asyncio.Lock()
        self.is_connected = False
        self._connection_attempts = 0
        self.last_connection_attempt = None
        
    async def connect(self) -> bool:
        """Establish connection to the Modbus device."""
        try:
            if self.client is None:
                if self.config.protocol.value == "rtu":
                    self.client = AsyncModbusSerialClient(
                        port=self.config.port,
                        baudrate=self.config.baudrate,
                        bytesize=self.config.bytesize,
                        parity=self.config.parity,
                        stopbits=self.config.stopbits,
                        timeout=self.config.timeout
                    )
                else:  # TCP
                    self.client = AsyncModbusTcpClient(
                        host=self.config.host,
                        port=self.config.port_tcp,
                        timeout=self.config.timeout
                    )
            
            self._connection_attempts += 1
            self.last_connection_attempt = datetime.now()
            
            # Try to connect
            connect_result = await self.client.connect()
            
            # For TCP clients, connect() may return True even if connection fails
            # We need to verify the connection actually works
            if connect_result:
                # Verify connection by trying to read a register
                try:
                    test_result = await self.client.read_holding_registers(
                        0, count=1, device_id=self.config.slave_id
                    )
                    # If we get here, connection is working
                    self.is_connected = True
                    device_name = self.config.description or f"Slave {self.config.slave_id}"
                    logger.info(f"Connected to Modbus device: {device_name}")
                    return True
                except Exception as e:
                    # Connection test failed
                    self.is_connected = False
                    logger.warning(f"Modbus connection test failed: {e}")
                    return False
            else:
                self.is_connected = False
                logger.error(f"Failed to connect to Modbus device")
                return False
            
        except ConnectionException as e:
            logger.error(f"Failed to connect to Modbus device: {e}")
            self.is_connected = False
            return False
        except Exception as e:
            logger.error(f"Unexpected error during connection: {e}")
            self.is_connected = False
            return False
    
    async def disconnect(self):
        """Disconnect from the Modbus device."""
        if self.client:
            try:
                if hasattr(self.client, 'close'):
                    await self.client.close()
                self.is_connected = False
                device_name = self.config.description or f"Slave {self.config.slave_id}"
                logger.info(f"Disconnected from Modbus device: {device_name}")
            except Exception as e:
                logger.error(f"Error disconnecting: {e}")
        else:
            logger.debug("No client to disconnect")
    
    def _decode_register_data(self, register_def: RegisterConfig, raw_data) -> Any:
        """Decode raw register data based on configuration."""
        if raw_data is None:
            return None
        
        data_type = register_def.data_type
        scale = register_def.scale
        
        # Handle special data types
        if data_type == DataType.BITMASK and register_def.bits:
            status = {}
            for bit_name, bit_pos in register_def.bits.items():
                status[bit_name] = bool(raw_data & (1 << bit_pos))
            return status
        
        if data_type == DataType.ENUM:
            return raw_data
        
        if data_type == DataType.VERSION:
            if isinstance(raw_data, int):
                major = (raw_data >> 8) & 0xFF
                minor = raw_data & 0xFF
                return f"{major}.{minor}"
            return str(raw_data)
        
        if data_type == DataType.MAC_ADDRESS:
            if isinstance(raw_data, list) and len(raw_data) >= 3:
                bytes_list = []
                for reg in raw_data[:3]:
                    bytes_list.extend([(reg >> 8) & 0xFF, reg & 0xFF])
                return ':'.join(f'{b:02x}' for b in bytes_list)
            return str(raw_data)
        
        if data_type == DataType.STRING:
            if not isinstance(raw_data, list):
                raise ValueError("STRING data type requires list of registers")
            byte_string = b''.join(struct.pack('>H', reg) for reg in raw_data)
            result = byte_string.split(b'\x00')[0].decode('utf-8', errors='ignore')
            return result
        
        # Convert single register to list for consistent processing
        if not isinstance(raw_data, list):
            raw_data = [raw_data]
        
        # Handle single register (16-bit) values
        if len(raw_data) == 1:
            if data_type == DataType.INT16:
                value = raw_data[0] if raw_data[0] < 0x8000 else raw_data[0] - 0x10000
            elif data_type == DataType.UINT16:
                value = raw_data[0]
            else:
                value = raw_data[0]
            # Apply scale
            value = value * scale
            # Return as int if scale is 1.0 and it's an integer type
            if scale == 1.0 and data_type in [DataType.INT16, DataType.UINT16]:
                return int(value)
            # Format float to 2 decimal places
            if isinstance(value, float):
                return round(value, 2)
            return value
        
        # Handle multi-register values (32-bit)
        byte_order = register_def.byte_order
        if byte_order in ["AB", "ABCD"]:
            combined_bytes = b''.join(struct.pack('>H', reg) for reg in raw_data)
        elif byte_order in ["BA", "DCBA"]:
            combined_bytes = b''.join(struct.pack('<H', reg) for reg in raw_data)
        else:
            combined_bytes = b''.join(struct.pack('>H', reg) for reg in raw_data)
        
        # Decode based on data type
        if data_type in [DataType.INT32, DataType.UINT32]:
            format_char = '>i' if data_type == DataType.INT32 else '>I'
            value = struct.unpack(format_char, combined_bytes)[0]
            # Return as int if scale is 1.0
            if scale == 1.0:
                return int(value)
        elif data_type == DataType.FLOAT32:
            value = struct.unpack('>f', combined_bytes)[0]
        else:
            value = raw_data[0]
        
        # Apply scale
        value = value * scale
        
        # Format float to 2 decimal places
        if isinstance(value, float):
            return round(value, 2)
        
        return value
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        retry=retry_if_exception(lambda e: isinstance(e, (ModbusException, ConnectionException)))
    )
    async def read_register(self, register_def: RegisterConfig) -> Optional[Any]:
        """Read a single register with retry logic."""
        async with self._lock:
            if not self.is_connected or self.client is None:
                # Try to reconnect
                logger.debug(f"Not connected, attempting to reconnect...")
                if not await self.connect():
                    raise ConnectionException("Device not connected")
            
            try:
                address = register_def.address
                register_type = register_def.type
                register_count = register_def.register_count or 1
                
                # Determine register count for multi-register data types
                if register_def.data_type in [DataType.INT32, DataType.UINT32, DataType.FLOAT32]:
                    register_count = 2
                elif register_def.data_type == DataType.STRING:
                    register_count = register_def.register_count or 4
                elif register_def.data_type == DataType.MAC_ADDRESS:
                    register_count = 3
                
                # Read based on register type
                if register_type == RegisterType.HOLDING:
                    response = await self.client.read_holding_registers(
                        address, 
                        count=register_count, 
                        device_id=self.config.slave_id
                    )
                elif register_type == RegisterType.INPUT:
                    response = await self.client.read_input_registers(
                        address, 
                        count=register_count, 
                        device_id=self.config.slave_id
                    )
                elif register_type == RegisterType.COIL:
                    response = await self.client.read_coils(
                        address, 
                        count=register_count, 
                        device_id=self.config.slave_id
                    )
                elif register_type == RegisterType.DISCRETE_INPUT:
                    response = await self.client.read_discrete_inputs(
                        address, 
                        count=register_count, 
                        device_id=self.config.slave_id
                    )
                else:
                    raise ValueError(f"Unsupported register type: {register_type}")
                
                # Check if response is None
                if response is None:
                    logger.error(f"Null response reading {register_def.name}")
                    return None
                
                if response.isError():
                    logger.error(f"Error reading {register_def.name}: {response}")
                    return None
                
                # Extract raw data
                if register_type in [RegisterType.COIL, RegisterType.DISCRETE_INPUT]:
                    raw_data = response.bits[0] if response.bits else None
                else:
                    if hasattr(response, 'registers') and response.registers:
                        raw_data = response.registers if len(response.registers) > 1 else response.registers[0]
                    else:
                        raw_data = None
                
                if raw_data is None:
                    return None
                
                return self._decode_register_data(register_def, raw_data)
                
            except ModbusException as e:
                # If we get a Modbus exception, the connection might be dead
                self.is_connected = False
                logger.error(f"Modbus exception reading {register_def.name}: {e}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error reading {register_def.name}: {e}")
                raise
    
    async def read_all_registers(self, register_defs: List[RegisterConfig]) -> Dict[str, Any]:
        """Read all registers in parallel."""
        tasks = [self.read_register(reg_def) for reg_def in register_defs]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        data = {}
        for reg_def, result in zip(register_defs, results):
            if isinstance(result, Exception):
                logger.debug(f"Failed to read {reg_def.name}: {result}")
                data[reg_def.name] = None
            else:
                data[reg_def.name] = result
        
        return data
    
    async def check_connection(self) -> bool:
        """Check if the device is still connected."""
        if not self.is_connected or self.client is None:
            return False
        
        try:
            # Try a simple read to verify connection
            await self.client.read_holding_registers(0, count=1, device_id=self.config.slave_id)
            return True
        except Exception as e:
            logger.debug(f"Connection check failed: {e}")
            self.is_connected = False
            return False