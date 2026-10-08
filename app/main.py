#!/usr/bin/env python3
"""
Modbus Gateway - async gateway with a non-blocking state machine.
"""
import asyncio
import contextlib
import logging
import os
import signal
import sys
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path

from gateway import (
    ConfigLoader,
    GatewayConfig,
    AsyncModbusDevice,
    BackendClientFactory,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State + stats
# ---------------------------------------------------------------------------

class GatewayState(Enum):
    INIT = "INIT"
    IDLE = "IDLE"
    READING = "READING"
    STREAMING = "STREAMING"
    ERROR = "ERROR"
    SHUTDOWN = "SHUTDOWN"


@dataclass
class GatewayStats:
    """Runtime statistics and connection flags."""
    started_at: datetime = field(default_factory=datetime.now)
    last_state_change: datetime = field(default_factory=datetime.now)
    last_heartbeat: datetime | None = None
    last_read: dict | None = None

    read_count: int = 0
    send_count: int = 0
    heartbeat_count: int = 0
    error_count: int = 0
    recovery_attempts: int = 0

    modbus_connected: bool = False
    backend_connected: bool = False
    last_error: str | None = None

    @property
    def uptime_seconds(self) -> int:
        return int((datetime.now() - self.started_at).total_seconds())

    @property
    def healthy(self) -> bool:
        return self.modbus_connected and self.backend_connected


# ---------------------------------------------------------------------------
# Gateway
# ---------------------------------------------------------------------------

class ModbusGateway:
    """Async Modbus, backend gateway.

    The state machine is *non-blocking*: every ``_handle_*`` method returns
    quickly. Long waits live in background tasks or in ``_run_state_machine``.
    """

    # How long a state handler may run before the loop re-checks for shutdown.
    TICK = 0.1

    def __init__(self, config: GatewayConfig):
        self.config = config
        self.stats = GatewayStats()

        self.state = GatewayState.INIT
        self.previous_state = GatewayState.INIT

        # Core components (created in INIT).
        self.modbus: AsyncModbusDevice | None = None
        self.backend = None  # BackendClient

        # Lifecycle
        self._stop = asyncio.Event()
        self._tasks: set[asyncio.Task] = set()

        # Tunables
        s = config.settings
        self.read_interval = s.read_interval
        self.heartbeat_interval = s.heartbeat_interval
        self.recovery_interval = s.recovery_interval
        self.max_recovery_attempts = s.max_recovery_attempts

    def set_state(self, new_state: GatewayState):
        if new_state == self.state:
            return
        self.previous_state, self.state = self.state, new_state
        self.stats.last_state_change = datetime.now()
        logger.info(f"State: {self.previous_state.value} -> {self.state.value}")

    async def _handle_state(self) -> float:
        """Dispatch to the current handler. Returns seconds to sleep before
        the next tick (0 means 'call me again immediately')."""
        handler = {
            GatewayState.INIT: self._handle_init,
            GatewayState.IDLE: self._handle_idle,
            GatewayState.READING: self._handle_reading,
            GatewayState.STREAMING: self._handle_streaming,
            GatewayState.ERROR: self._handle_error,
            GatewayState.SHUTDOWN: self._handle_shutdown,
        }[self.state]
        return await handler()

    # ---------------------------------------------------------- 
    # state handlers
    # ---------------------------------------------------------- 

    async def _handle_init(self) -> float:
        """INIT: build components, establish connections, launch background tasks."""
        self._print_banner()
        logger.info("Initializing gateway...")

        self.modbus = AsyncModbusDevice(self.config.device)
        self.backend = BackendClientFactory.create_client(self.config.backend)

        self._register_backend_callbacks()

        # Connections are independent — try both in parallel.
        modbus_ok, backend_ok = await asyncio.gather(
            self._connect_modbus(),
            self._connect_backend(),
        )
        self.stats.modbus_connected = modbus_ok
        self.stats.backend_connected = backend_ok

        # Background tasks (they run until cancelled).
        self._spawn(self._heartbeat_loop())
        self._spawn(self._status_loop())

        await self._send_system_info()

        self.set_state(GatewayState.IDLE if self.stats.healthy else GatewayState.ERROR)
        return 0.0

    async def _handle_idle(self) -> float:
        """IDLE: wait for `read_interval` seconds before the next read cycle.

        Return value is the sleep duration the main loop should apply.
        """
        if self._stop.is_set():
            self.set_state(GatewayState.SHUTDOWN)
            return 0.0
        if not self.stats.healthy:
            logger.warning("Connection lost, going to ERROR")
            self.set_state(GatewayState.ERROR)
            return 0.0
        self.set_state(GatewayState.READING)
        return self.read_interval

    async def _handle_reading(self) -> float:
        """READING: pull one snapshot from the Modbus device."""
        logger.info("Reading data...")
        
        if self.modbus is None:
            # Programming error, not a device error.
            raise RuntimeError("Modbus client not initialized")
        
        try:
            if not await self.modbus.check_connection():
                raise ConnectionError("Modbus connection lost")

            data = await self.modbus.read_all_registers(self.config.device.registers)
            self.stats.read_count += 1
            valid = sum(1 for v in data.values() if v is not None)
            logger.info(f"Read {valid}/{len(data)} registers")

            data.update(
                timestamp=datetime.now().isoformat(),
                device=self.config.device.description,
                table_id=self.config.device.table_id,
                read_count=self.stats.read_count,
            )
            self.stats.last_read = data
            self.set_state(GatewayState.STREAMING)
        except Exception as e:
            self._fail("Read failed", e, modbus=True)
        return 0.0

    async def _handle_streaming(self) -> float:
        """STREAMING: push the last snapshot to the backend."""
        logger.info("Streaming data...")
        data = self.stats.last_read
        if data is None:
            self.set_state(GatewayState.IDLE)
            return 0.0

        try:
            if self.backend is None:
                raise RuntimeError("Backend client not initialized")
            
            if not await self.backend.health_check():
                raise ConnectionError("Backend connection lost")

            if not await self.backend.send_data(data):
                raise RuntimeError("Send failed")

            self.stats.send_count += 1
            logger.info("Data sent successfully")
            self.set_state(GatewayState.IDLE)
        except Exception as e:
            self._fail("Stream failed", e, backend=True)
        return 0.0

    async def _handle_error(self) -> float:
        """ERROR: try to restore connections, then back off."""
        logger.error(f"ERROR state - last error: {self.stats.last_error}")

        if not self.stats.modbus_connected and await self._connect_modbus():
            self.stats.modbus_connected = True
            logger.info("Modbus reconnected")

        if not self.stats.backend_connected and await self._connect_backend():
            self.stats.backend_connected = True
            logger.info("Backend reconnected")

        if self.stats.healthy:
            logger.info("All connections restored")
            self.stats.error_count = 0
            self.stats.recovery_attempts = 0
            self.set_state(GatewayState.IDLE)
            return 0.0

        # Back off - but never block the loop; just tell it how long to sleep.
        self.stats.recovery_attempts += 1
        if self.stats.recovery_attempts > self.max_recovery_attempts:
            logger.warning("Max recovery attempts exceeded - long back-off")
            self.stats.recovery_attempts = 0
            return self.recovery_interval * 3
        return self.stats.recovery_attempts * self.recovery_interval

    async def _handle_shutdown(self) -> float:
        """SHUTDOWN: tear everything down once, then signal stop."""
        logger.info("Shutting down...")
        self._stop.set()
        return 0.0

    # ---------------------------------------------------------------
    # lifecycle
    # ---------------------------------------------------------------

    async def run(self):
        """Run the gateway until stopped."""
        self._install_signal_handlers()
        self.set_state(GatewayState.INIT)

        try:
            while not self._stop.is_set():
                delay = await self._handle_state()
                # Wait for `delay` seconds OR until stop is requested.
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=delay or self.TICK)
        except asyncio.CancelledError:
            logger.info("Main loop cancelled")
        except Exception:
            logger.exception("Fatal error in main loop")
        finally:
            await self._cleanup()

    def request_shutdown(self):
        """Ask the gateway to stop. Safe to call from signals."""
        if not self._stop.is_set():
            logger.info("Shutdown requested")
            self._stop.set()

    # ------------------------------------------------------------- 
    # connections
    # ------------------------------------------------------------- 

    async def _connect_modbus(self) -> bool:
        try:
            if self.modbus is None:
                raise RuntimeError("Modbus client not initialized")
            return await self.modbus.connect()
        except Exception as e:
            logger.error(f"Modbus connect failed: {e}")
            return False

    async def _connect_backend(self) -> bool:
        try:
            if self.backend is None:
                raise RuntimeError("Backend client not initialized")
            return await self.backend.connect()
        except Exception as e:
            logger.error(f"Backend connect failed: {e}")
            return False

    def _register_backend_callbacks(self):
        """Wire WebSocket events into the gateway, if supported."""
        setter = getattr(self.backend, "set_callback", None)
        if setter is None:
            return
        setter("on_connect", self._on_backend_connect)
        setter("on_disconnect", self._on_backend_disconnect)
        setter("on_error", self._on_backend_error)

    # --------------------------------------------------------------- 
    # callbacks
    # --------------------------------------------------------------- 

    async def _on_backend_connect(self):
        logger.info("Backend connected")
        self.stats.backend_connected = True
        if self.state == GatewayState.ERROR and self.stats.modbus_connected:
            self.stats.error_count = 0
            self.stats.recovery_attempts = 0
            self.set_state(GatewayState.IDLE)

    async def _on_backend_disconnect(self):
        logger.warning("Backend disconnected")
        self.stats.backend_connected = False
        self.stats.last_error = "Backend disconnected"
        if self.state not in (GatewayState.SHUTDOWN, GatewayState.ERROR):
            self.set_state(GatewayState.ERROR)

    async def _on_backend_error(self, error):
        self.stats.backend_connected = False
        self.stats.last_error = str(error)
        if self.state not in (GatewayState.SHUTDOWN, GatewayState.ERROR):
            self.set_state(GatewayState.ERROR)

    # ------------------------------------------------------ 
    # background tasks
    # ------------------------------------------------------ 

    async def _heartbeat_loop(self):
        """Send heartbeats at a fixed interval until cancelled."""
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            if not self.stats.backend_connected:
                logger.debug("Backend not connected, skipping heartbeat")
                continue
            try:
                if self.backend is None:
                    raise RuntimeError("Backend client not initialized")
                if await self.backend.send_data(self._heartbeat_payload()):
                    self.stats.heartbeat_count += 1
                    self.stats.last_heartbeat = datetime.now()
                    logger.debug(f"Heartbeat #{self.stats.heartbeat_count} sent")
                else:
                    self._fail("Heartbeat failed", None, backend=True)
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")

    async def _status_loop(self):
        """Periodically log a status line until cancelled."""
        while True:
            await asyncio.sleep(30)
            logger.info(
                f"Status: state={self.state.value}, "
                f"uptime={self.stats.uptime_seconds}s, "
                f"reads={self.stats.read_count}, "
                f"sends={self.stats.send_count}, "
                f"hb={self.stats.heartbeat_count}, "
                f"errors={self.stats.error_count}, "
                f"M={self.stats.modbus_connected}, "
                f"B={self.stats.backend_connected}"
            )

    # ----------------------------------------------------------------- 
    # helpers
    # ----------------------------------------------------------------- 

    def _spawn(self, coro) -> asyncio.Task:
        """Launch a background task and track it for later cancellation."""
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def _fail(self, message: str, error: Exception | None, *,
              modbus: bool = False, backend: bool = False):
        """Record an error and flip the relevant connection flag."""
        self.stats.error_count += 1
        self.stats.last_error = f"{message}: {error}" if error else message
        if modbus:
            self.stats.modbus_connected = False
        if backend:
            self.stats.backend_connected = False
        logger.error(self.stats.last_error)
        if self.state != GatewayState.SHUTDOWN:
            self.set_state(GatewayState.ERROR)

    def _heartbeat_payload(self) -> dict:
        s = self.stats
        return {
            "type": "heartbeat",
            "timestamp": datetime.now().isoformat(),
            "device": self.config.device.description,
            "table_id": self.config.device.table_id,
            "status": {
                "state": self.state.value,
                "modbus_connected": s.modbus_connected,
                "backend_connected": s.backend_connected,
                "read_count": s.read_count,
                "send_count": s.send_count,
                "heartbeat_count": s.heartbeat_count,
                "error_count": s.error_count,
                "uptime": s.uptime_seconds,
                "last_error": s.last_error,
            },
        }

    async def _send_system_info(self):
        if self.backend is None or not self.backend.is_connected:
            return
        device, app = self.config.device, self.config.app
        await self.backend.send_data({
            "type": "system_info",
            "timestamp": datetime.now().isoformat(),
            "device": device.description,
            "table_id": device.table_id,
            "app_name": app.name,
            "app_version": app.version,
            "config": {
                "protocol": device.protocol.value,
                "slave_id": device.slave_id,
                "read_interval": self.read_interval,
                "heartbeat_interval": self.heartbeat_interval,
            },
        })
        logger.info("System info sent to backend")

    def _print_banner(self):
        app = self.config.app
        d = self.config.device
        logger.info(
            f"{app.name} v{app.version} starting - "
            f"device={d.description}, protocol={d.protocol.value}, "
            f"slave_id={d.slave_id}, table_id={d.table_id}, "
            f"read_interval={self.read_interval}s, "
            f"heartbeat={self.heartbeat_interval}s"
        )

    def _install_signal_handlers(self):
        if os.name == "nt":
            return
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.request_shutdown)

    # --------------------------------------------------------------- 
    # teardown
    # --------------------------------------------------------------- 

    async def _cleanup(self):
        """Cancel background tasks and disconnect everything."""
        # 1. Stop background tasks.
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        # 2. Disconnect clients.
        for name, client in (("Modbus", self.modbus), ("backend", self.backend)):
            if client is None:
                continue
            try:
                await client.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting {name}: {e}")

        self.set_state(GatewayState.SHUTDOWN)
        logger.info("Gateway stopped")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main(config_path: str):
    config = ConfigLoader.load_config(config_path)
    gateway = ModbusGateway(config)
    await gateway.run()


def _parse_args() -> str:
    path = sys.argv[1] if len(sys.argv) > 1 else "./app/config/config.json"
    if not Path(path).exists():
        print(f"Error: Config file not found: {path}")
        sys.exit(1)
    return path


if __name__ == "__main__":
    try:
        asyncio.run(main(_parse_args()))
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
        sys.exit(0)