#!/usr/bin/env python3
"""
Modbus TCP Server emulating Power-Elec 6 sensor.
"""
import asyncio
import logging
import struct
import math
from datetime import datetime
from typing import Dict

from pymodbus.server import StartAsyncTcpServer
from pymodbus.simulator import DataType, SimData, SimDevice

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PowerElec6Emulator:
    """Emulator for Power-Elec 6 power meter."""
    
    def __init__(self):
        self.store: Dict[int, int] = {}
        self._initialize_registers()
        
    def _initialize_registers(self):
        """Initialize register values with realistic data."""
        self.store[0] = 0x0106  # Software version v1.6
        self.store[1] = 0x0002  # Modbus table version
        self.store[2] = 0x001A  # MAC address part 1
        self.store[3] = 0x2B3C  # MAC address part 2
        self.store[4] = 0x4D5E  # MAC address part 3
        
        self._update_float_register(352, 230.5)   # Voltage CH1
        self._update_float_register(354, 231.2)   # Voltage CH2
        self._update_float_register(356, 229.8)   # Voltage CH3
        self._update_float_register(424, 50.02)   # Frequency CH1
        self._update_float_register(426, 49.98)   # Frequency CH2
        self._update_float_register(428, 50.01)   # Frequency CH3
        self._update_float_register(360, 1.5)     # Current CH1
        self._update_float_register(362, 1.2)     # Current CH2
        self._update_float_register(364, 1.8)     # Current CH3
        self._update_float_register(400, 345.6)   # Power CH1
        self._update_float_register(402, 276.8)   # Power CH2
        self._update_float_register(404, 414.2)   # Power CH3
        
    def _update_float_register(self, address: int, value: float):
        """Store a float as two 16-bit registers (Big Endian)."""
        packed = struct.pack('>f', value)
        high, low = struct.unpack('>HH', packed)
        self.store[address] = high
        self.store[address + 1] = low
    
    def update_values(self):
        """Update register values with realistic simulated data."""
        time = datetime.now().timestamp()
        
        v1 = 230.0 + 1.5 * math.sin(time * 0.1)
        v2 = 230.0 + 1.2 * math.sin(time * 0.1 + 2.1)
        v3 = 230.0 + 1.8 * math.sin(time * 0.1 + 1.3)
        
        self._update_float_register(352, v1)
        self._update_float_register(354, v2)
        self._update_float_register(356, v3)
        
        f1 = 50.0 + 0.05 * math.sin(time * 0.05)
        f2 = 50.0 + 0.03 * math.sin(time * 0.05 + 1.7)
        f3 = 50.0 + 0.07 * math.sin(time * 0.05 + 0.8)
        
        self._update_float_register(424, f1)
        self._update_float_register(426, f2)
        self._update_float_register(428, f3)
        
        i1 = 1.5 + 0.8 * math.sin(time * 0.15 + 0.5)
        i2 = 1.2 + 0.6 * math.sin(time * 0.13 + 2.3)
        i3 = 1.8 + 0.9 * math.sin(time * 0.17 + 1.1)
        
        self._update_float_register(360, i1)
        self._update_float_register(362, i2)
        self._update_float_register(364, i3)
        
        pf1 = 0.95 + 0.03 * math.sin(time * 0.08)
        pf2 = 0.94 + 0.04 * math.sin(time * 0.08 + 1.2)
        pf3 = 0.96 + 0.02 * math.sin(time * 0.08 + 0.7)
        
        p1 = v1 * i1 * pf1
        p2 = v2 * i2 * pf2
        p3 = v3 * i3 * pf3
        
        self._update_float_register(400, p1)
        self._update_float_register(402, p2)
        self._update_float_register(404, p3)


class ModbusTcpServer:
    """Modbus TCP Server with Power-Elec 6 emulation."""
    
    def __init__(self, host='localhost', port=5020):
        self.host = host
        self.port = port
        self.emulator = PowerElec6Emulator()
        self._running = False
        self._update_task = None
        self.context = None
        self.register_values = None
        
    def create_datastore(self):
        """Create Modbus datastore."""
        self.register_values = [0] * 0xFFFF
        
        for addr, value in self.emulator.store.items():
            if addr < len(self.register_values):
                self.register_values[addr] = value
        
        sim_data = SimData(0, datatype=DataType.REGISTERS, values=self.register_values)
        self.context = SimDevice(1, sim_data)
        
        logger.info(f"Modbus: Created datastore with {len(self.emulator.store)} registers")
    
    async def _update_emulator(self):
        """Periodically update emulator values."""
        while self._running:
            await asyncio.sleep(1.0)
            try:
                self.emulator.update_values()
                if self.register_values is not None:
                    for addr, value in self.emulator.store.items():
                        if addr < len(self.register_values):
                            self.register_values[addr] = value
            except Exception as e:
                logger.error(f"Modbus update error: {e}")
    
    async def start(self):
        """Start the Modbus TCP server."""
        self._running = True
        
        self.create_datastore()
        self._update_task = asyncio.create_task(self._update_emulator())
        
        logger.info(f"Modbus: Server started on {self.host}:{self.port}")
        
        try:
            await StartAsyncTcpServer(
                context=self.context,
                identity=None,
                address=(self.host, self.port),
                framer="socket",
            )
        except Exception as e:
            logger.error(f"Modbus server error: {e}")
            await self.stop()
    
    async def stop(self):
        """Stop the Modbus TCP server."""
        self._running = False
        if self._update_task:
            self._update_task.cancel()
            try:
                await self._update_task
            except asyncio.CancelledError:
                pass
        logger.info("Modbus: Server stopped")


async def run_modbus_server(host='localhost', port=5020):
    """Run Modbus server (convenience function)."""
    server = ModbusTcpServer(host, port)
    await server.start()


if __name__ == "__main__":
    try:
        asyncio.run(run_modbus_server())
    except KeyboardInterrupt:
        logger.info("Modbus server shutdown by user")