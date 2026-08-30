#!/usr/bin/env python3
"""
Modbus Gateway with Simplified State Machine.
Heartbeat is handled by a separate timer task.
"""
import asyncio
import signal
import logging
import sys
import os
from pathlib import Path
from datetime import datetime
from enum import Enum

# Import from the gateway library
from gateway import (
    ConfigLoader,
    GatewayConfig,
    AsyncModbusDevice,
    BackendClientFactory,
    WebSocketBackendClient,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

class GatewayState(Enum):
    """Gateway states."""
    INIT = "INIT"
    IDLE = "IDLE"
    READING = "READING"
    STREAMING = "STREAMING"
    ERROR = "ERROR"
    SHUTDOWN = "SHUTDOWN"

class ModbusGateway:
    """Modbus gateway with simplified state machine."""
    
    def __init__(self, config: GatewayConfig):
        self.config = config
        
        # Core components
        self.modbus_device = None
        self.backend_client = None
        
        # State management
        self.state = GatewayState.INIT
        self.previous_state = GatewayState.INIT
        self._running = False
        self._shutdown_requested = False
        
        # Context
        self.context = {
            "last_read_data": None,
            "read_count": 0,
            "send_count": 0,
            "heartbeat_count": 0,
            "error_count": 0,
            "retry_count": 0,
            "modbus_connected": False,
            "backend_connected": False,
            "last_error": None,
            "start_time": None,
            "last_heartbeat_time": None,
            "last_state_change": None,
            "recovery_attempts": 0,
        }
        
        # Tasks
        self._queue_task = None
        self._monitor_task = None
        self._heartbeat_task = None
        
        # Configuration from settings
        self.heartbeat_interval = self.config.settings.heartbeat_interval
        self.recovery_interval = self.config.settings.recovery_interval
        self.max_recovery_attempts = self.config.settings.max_recovery_attempts
    
    def _print_banner(self):
        """Print application info on startup."""
        app = self.config.app
        device = self.config.device
        
        # Build configuration string
        config_parts = [
            f"device={device.description}",
            f"protocol={device.protocol.value}",
            f"slave_id={device.slave_id}",
            f"table_id={device.table_id}",
            f"read_interval={self.config.settings.read_interval}s",
            f"heartbeat={self.heartbeat_interval}s"
        ]
        config_str = ", ".join(config_parts)
        
        logger.info(f"{app.name} v{app.version} starting - {config_str}")
    
    # ============ State Machine ============
    
    def set_state(self, new_state: GatewayState):
        """Set the current state."""
        if self.state != new_state:
            self.previous_state = self.state
            self.state = new_state
            self.context["last_state_change"] = datetime.now()
            logger.info(f"State: {self.previous_state.value} -> {self.state.value}")
    
    def _check_connections(self) -> bool:
        """Check if both connections are up."""
        return self.context["modbus_connected"] and self.context["backend_connected"]
    
    async def _handle_state(self):
        """Handle the current state."""
        if self.state == GatewayState.INIT:
            await self._handle_init()
        elif self.state == GatewayState.IDLE:
            await self._handle_idle()
        elif self.state == GatewayState.READING:
            await self._handle_reading()
        elif self.state == GatewayState.STREAMING:
            await self._handle_streaming()
        elif self.state == GatewayState.ERROR:
            await self._handle_error()
        elif self.state == GatewayState.SHUTDOWN:
            await self._handle_shutdown()
    
    # ============ State Handlers ============
    
    async def _handle_init(self):
        """INIT: Initialize gateway."""
        # Print application banner
        self._print_banner()
        
        logger.info("Initializing gateway...")
        self.context["start_time"] = datetime.now()
        
        # Create Modbus device
        self.modbus_device = AsyncModbusDevice(self.config.device)
        
        # Create backend client
        self.backend_client = BackendClientFactory.create_client(self.config.backend)
        
        # Set up WebSocket callbacks
        if isinstance(self.backend_client, WebSocketBackendClient):
            self.backend_client.set_callback('on_connect', self._on_websocket_connect)
            self.backend_client.set_callback('on_disconnect', self._on_websocket_disconnect)
            self.backend_client.set_callback('on_error', self._on_websocket_error)
        
        # Connect to Modbus
        if not await self.modbus_device.connect():
            logger.error("Modbus connection failed")
            self.context["last_error"] = "Modbus connection failed"
            self.context["modbus_connected"] = False
        else:
            self.context["modbus_connected"] = True
            logger.info("Modbus connected")
        
        # Connect to backend
        if not await self.backend_client.connect():
            logger.warning("Backend connection failed, will retry later")
            self.context["backend_connected"] = False
        else:
            self.context["backend_connected"] = True
            logger.info("Backend connected")
        
        # Start queue processor
        if isinstance(self.backend_client, WebSocketBackendClient):
            self._queue_task = asyncio.create_task(self.backend_client.process_queue())
        
        # Start monitor
        self._monitor_task = asyncio.create_task(self._status_monitor())
        
        # Start heartbeat timer
        self._heartbeat_task = asyncio.create_task(self._heartbeat_timer())
        
        # Send system info
        await self._send_system_info()
        
        # Transition based on connection status
        if self._check_connections():
            self.set_state(GatewayState.IDLE)
        else:
            self.set_state(GatewayState.ERROR)
    
    async def _handle_idle(self):
        """IDLE: Wait for next cycle."""
        logger.debug("Idle...")
        
        # Wait for read interval
        await asyncio.sleep(self.config.settings.read_interval)
        
        # Check shutdown
        if self._shutdown_requested:
            self.set_state(GatewayState.SHUTDOWN)
            return
        
        # Check connections
        if not self._check_connections():
            logger.warning("Connection lost, going to ERROR")
            self.set_state(GatewayState.ERROR)
            return
        
        # Next cycle - go to READING
        self.set_state(GatewayState.READING)
    
    async def _handle_reading(self):
        """READING: Read data from Modbus."""
        logger.info("Reading data...")
        
        try:
            # Check Modbus connection
            if not await self.modbus_device.check_connection():
                self.context["modbus_connected"] = False
                self.context["last_error"] = "Modbus connection lost"
                self.set_state(GatewayState.ERROR)
                return
            
            # Read registers
            data = await self.modbus_device.read_all_registers(self.config.device.registers)
            self.context["read_count"] += 1
            
            # Log summary
            valid_count = len([v for v in data.values() if v is not None])
            logger.info(f"Read {valid_count}/{len(data)} registers")
            
            # Add metadata
            data["timestamp"] = datetime.now().isoformat()
            data["device"] = self.config.device.description
            data["table_id"] = self.config.device.table_id
            data["read_count"] = self.context["read_count"]
            
            # Store data
            self.context["last_read_data"] = data
            
            # Transition to STREAMING
            self.set_state(GatewayState.STREAMING)
            
        except Exception as e:
            logger.error(f"Error reading: {e}")
            self.context["error_count"] += 1
            self.context["last_error"] = str(e)
            self.context["modbus_connected"] = False
            self.set_state(GatewayState.ERROR)
    
    async def _handle_streaming(self):
        """STREAMING: Send data to backend."""
        logger.info("Streaming data...")
        
        try:
            data = self.context["last_read_data"]
            if data is None:
                self.set_state(GatewayState.IDLE)
                return
            
            # Check backend connection
            if not await self.backend_client.health_check():
                self.context["backend_connected"] = False
                self.context["last_error"] = "Backend connection lost"
                self.set_state(GatewayState.ERROR)
                return
            
            # Send data
            if await self.backend_client.send_data(data):
                self.context["send_count"] += 1
                logger.info("Data sent successfully")
            else:
                self.context["error_count"] += 1
                self.context["last_error"] = "Send failed"
                self.context["backend_connected"] = False
                self.set_state(GatewayState.ERROR)
                return
            
            # Back to IDLE
            self.set_state(GatewayState.IDLE)
            
        except Exception as e:
            logger.error(f"Error streaming: {e}")
            self.context["error_count"] += 1
            self.context["last_error"] = str(e)
            self.context["backend_connected"] = False
            self.set_state(GatewayState.ERROR)
    
    async def _handle_error(self):
        """ERROR: Try to recover connections."""
        logger.error(f"ERROR state - trying to recover...")
        if self.context["last_error"]:
            logger.error(f"Last error: {self.context['last_error']}")
        
        # Try to reconnect Modbus if it's down
        if not self.context["modbus_connected"]:
            logger.debug("Attempting to reconnect Modbus...")
            if await self.modbus_device.connect():
                self.context["modbus_connected"] = True
                logger.info("Modbus reconnected")
        
        # Try to reconnect backend if it's down
        if not self.context["backend_connected"]:
            logger.debug("Attempting to reconnect backend...")
            if await self.backend_client.connect():
                self.context["backend_connected"] = True
                logger.info("Backend reconnected")
        
        # Check if recovered
        if self._check_connections():
            logger.info("All connections restored")
            self.context["error_count"] = 0
            self.context["retry_count"] = 0
            self.context["recovery_attempts"] = 0
            self.set_state(GatewayState.IDLE)
            return
        
        # Retry logic
        if self.context["recovery_attempts"] < self.max_recovery_attempts:
            self.context["recovery_attempts"] += 1
            wait_time = self.context["recovery_attempts"] * self.recovery_interval
            logger.debug(f"Retry recovery in {wait_time}s...")
            await asyncio.sleep(wait_time)
            # Stay in ERROR - will retry on next loop
        else:
            logger.warning("Max recovery attempts exceeded, waiting longer...")
            await asyncio.sleep(self.recovery_interval * 3)
            self.context["recovery_attempts"] = 0
            # Stay in ERROR - will retry on next loop
    
    async def _handle_shutdown(self):
        """SHUTDOWN: Clean shutdown."""
        logger.info("Shutting down...")
        self._running = False
        self._shutdown_requested = True
        
        # Disconnect
        if self.modbus_device:
            try:
                await self.modbus_device.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting Modbus: {e}")
        
        if self.backend_client:
            try:
                await self.backend_client.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting backend: {e}")
        
        # Cancel tasks
        if self._queue_task:
            self._queue_task.cancel()
            try:
                await self._queue_task
            except:
                pass
        
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except:
                pass
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except:
                pass
        
        logger.info("Gateway shutdown complete")
    
    # ============ Heartbeat Timer ============
    
    async def _heartbeat_timer(self):
        """Background heartbeat timer - runs independently of state machine."""
        while self._running and not self._shutdown_requested:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                
                # Don't send heartbeat if shutting down
                if self._shutdown_requested:
                    break
                
                # Only send heartbeat if backend is connected
                if not self.context["backend_connected"]:
                    logger.debug("Backend not connected, skipping heartbeat")
                    continue
                
                # Prepare heartbeat data
                heartbeat_data = {
                    "type": "heartbeat",
                    "timestamp": datetime.now().isoformat(),
                    "device": self.config.device.description,
                    "table_id": self.config.device.table_id,
                    "status": {
                        "state": self.state.value,
                        "modbus_connected": self.context["modbus_connected"],
                        "backend_connected": self.context["backend_connected"],
                        "read_count": self.context["read_count"],
                        "send_count": self.context["send_count"],
                        "heartbeat_count": self.context["heartbeat_count"],
                        "error_count": self.context["error_count"],
                        "uptime": int((datetime.now() - self.context["start_time"]).total_seconds()),
                        "last_error": self.context["last_error"]
                    }
                }
                
                # Send heartbeat
                if await self.backend_client.send_data(heartbeat_data):
                    self.context["heartbeat_count"] += 1
                    self.context["last_heartbeat_time"] = datetime.now()
                    logger.debug(f"Heartbeat #{self.context['heartbeat_count']} sent")
                else:
                    logger.warning("Failed to send heartbeat")
                    self.context["error_count"] += 1
                    self.context["last_error"] = "Heartbeat failed"
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat timer error: {e}")
                await asyncio.sleep(5)
    
    # ============ Helper Methods ============
    
    async def _send_system_info(self):
        """Send system info to backend."""
        device = self.config.device
        app = self.config.app
        
        system_info = {
            "type": "system_info",
            "timestamp": datetime.now().isoformat(),
            "device": device.description,
            "table_id": device.table_id,
            "app_name": app.name,
            "app_version": app.version,
            "config": {
                "protocol": device.protocol.value,
                "slave_id": device.slave_id,
                "read_interval": self.config.settings.read_interval,
                "heartbeat_interval": self.heartbeat_interval
            }
        }
        
        if self.context["backend_connected"]:
            await self.backend_client.send_data(system_info)
            logger.info("System info sent to backend")
    
    # ============ Callbacks ============
    
    async def _on_websocket_connect(self):
        """WebSocket connected callback."""
        logger.info("WebSocket connected")
        self.context["backend_connected"] = True
        
        # If in ERROR state and Modbus is connected, go to IDLE
        if self.state == GatewayState.ERROR and self.context["modbus_connected"]:
            self.context["error_count"] = 0
            self.context["retry_count"] = 0
            self.context["recovery_attempts"] = 0
            self.set_state(GatewayState.IDLE)
    
    async def _on_websocket_disconnect(self):
        """WebSocket disconnected callback."""
        logger.warning("WebSocket disconnected")
        self.context["backend_connected"] = False
        self.context["last_error"] = "Backend disconnected"
        if self.state not in [GatewayState.SHUTDOWN, GatewayState.ERROR]:
            self.set_state(GatewayState.ERROR)
    
    async def _on_websocket_error(self, error):
        """WebSocket error callback."""
        self.context["backend_connected"] = False
        self.context["last_error"] = str(error)
        if self.state not in [GatewayState.SHUTDOWN, GatewayState.ERROR]:
            self.set_state(GatewayState.ERROR)
    
    # ============ Monitor ============
    
    async def _status_monitor(self):
        """Background status monitor."""
        while self._running and not self._shutdown_requested:
            try:
                uptime = 0
                if self.context["start_time"]:
                    uptime = int((datetime.now() - self.context["start_time"]).total_seconds())
                
                logger.info(
                    f"Status: state={self.state.value}, "
                    f"uptime={uptime}s, "
                    f"reads={self.context['read_count']}, "
                    f"sends={self.context['send_count']}, "
                    f"hb={self.context['heartbeat_count']}, "
                    f"errors={self.context['error_count']}, "
                    f"M={self.context['modbus_connected']}, "
                    f"B={self.context['backend_connected']}"
                )
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Status monitor error: {e}")
                await asyncio.sleep(10)
    
    # ============ Main Loop ============
    
    async def run(self):
        """Main run loop."""
        self._running = True
        
        # Setup signal handlers (Unix only)
        if os.name != 'nt':
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(self.shutdown()))
            loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(self.shutdown()))
        
        try:
            # Start with INIT state
            self.set_state(GatewayState.INIT)
            
            # Main state machine loop
            while self._running and not self._shutdown_requested:
                await self._handle_state()
                await asyncio.sleep(0.1)
                
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
            await self.shutdown()
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            import traceback
            traceback.print_exc()
            await self.shutdown()
        finally:
            await self._cleanup()
            logger.info("Gateway stopped")
    
    async def shutdown(self):
        """Handle shutdown request."""
        if self._shutdown_requested:
            return
            
        logger.info("Shutdown requested...")
        self._shutdown_requested = True
        self.set_state(GatewayState.SHUTDOWN)
        
        # Wait for shutdown
        while self.state != GatewayState.SHUTDOWN:
            await asyncio.sleep(0.1)
    
    async def _cleanup(self):
        """Clean up resources."""
        self._running = False
        
        if self._queue_task:
            self._queue_task.cancel()
            try:
                await self._queue_task
            except:
                pass
        
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except:
                pass
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except:
                pass

async def main():
    """Main entry point."""
    config_file = "./app/config/config.json"
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    
    if not Path(config_file).exists():
        print(f"Error: Config file not found: {config_file}")
        sys.exit(1)
    
    # Load configuration
    config = ConfigLoader.load_config(config_file)
    
    # Create and run gateway with loaded config
    gateway = ModbusGateway(config)
    await gateway.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
        sys.exit(0)