from abc import ABC, abstractmethod
import json
import asyncio
import logging
import ssl
from typing import Optional, Dict, Any, Callable
from datetime import datetime

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed
from websockets.protocol import State
from websockets.typing import Subprotocol
from tenacity import retry, stop_after_attempt, wait_exponential

from .models import BackendConfig, BackendType

logger = logging.getLogger(__name__)


class BackendClient(ABC):
    """Abstract base class for backend clients."""

    def __init__(self, config: BackendConfig):
        self.config = config
        self.is_connected = False

    @abstractmethod
    async def connect(self) -> bool: ...

    @abstractmethod
    async def send_data(self, data: Dict[str, Any]) -> bool: ...

    @abstractmethod
    async def disconnect(self): ...

    @abstractmethod
    async def health_check(self) -> bool: ...


class WebSocketBackendClient(BackendClient):
    """Async WebSocket backend client built on `websockets` 14+."""

    def __init__(self, config: BackendConfig):
        super().__init__(config)
        self.ws: Optional[ClientConnection] = None
        self._recv_task: Optional[asyncio.Task] = None
        self._callbacks: Dict[str, Optional[Callable]] = {
            'on_message': None,
            'on_error': None,
            'on_connect': None,
            'on_disconnect': None,
        }

    # ----------------------------------------------------------
    # callbacks 
    # ----------------------------------------------------------

    def set_callback(self, event: str, callback: Callable):
        if event in self._callbacks:
            self._callbacks[event] = callback

    async def _fire(self, event: str, *args):
        cb = self._callbacks.get(event)
        if cb is None:
            return
        try:
            result = cb(*args)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            logger.error(f"Callback '{event}' raised: {e}")

    # ----------------------------------------------------------
    # helpers 
    # ----------------------------------------------------------
    
    def _is_open(self) -> bool:
        """True iff the socket exists and is in the OPEN state."""
        return self.ws is not None and self.ws.state is State.OPEN

    def _build_ssl_context(self) -> Optional[ssl.SSLContext]:
        if not self.config.tls:
            return None
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    # ----------------------------------------------------------
    # lifecycle
    # ----------------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
    )
    async def connect(self) -> bool:
        if self.is_connected and self._is_open():
            return True

        subprotocols = None
        if self.config.subprotocol and self.config.subprotocol.strip():
            subprotocols = [Subprotocol(self.config.subprotocol)]

        try:
            self.ws = await connect(
                self.config.url,
                subprotocols=subprotocols,
                open_timeout=self.config.timeout,
                ssl=self._build_ssl_context(),
                ping_interval=30,
                ping_timeout=10,
                max_size=None,
            )
        except Exception as e:
            logger.error(f"WebSocket connect failed: {e}")
            self.is_connected = False
            self.ws = None
            return False

        self.is_connected = True
        logger.info(f"WebSocket connected to {self.config.url}")

        self._recv_task = asyncio.create_task(self._recv_loop())
        await self._fire('on_connect')
        return True

    async def _recv_loop(self):
        try:
            if self.ws is None:
                logger.error("WebSocket receive loop started but ws is None")
                return
            async for message in self.ws:
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    data = message
                await self._fire('on_message', data)
        except ConnectionClosed as e:
            logger.info(f"WebSocket closed: {e}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"WebSocket receive error: {e}")
            await self._fire('on_error', str(e))
        finally:
            self.is_connected = False
            await self._fire('on_disconnect')

    async def send_data(self, data: Dict[str, Any]) -> bool:
        if not self.is_connected or not self._is_open():
            logger.warning("WebSocket not connected, dropping message")
            return False

        try:
            if "timestamp" not in data:
                data["timestamp"] = datetime.now().isoformat()
            
            if self.ws is None:
                logger.error("WebSocket send_data called but ws is None")
                return False
            
            await self.ws.send(json.dumps(data))
            logger.debug("Data sent via WebSocket")
            return True
        except Exception as e:
            logger.error(f"Error sending WebSocket message: {e}")
            self.is_connected = False
            return False

    async def disconnect(self):
        self.is_connected = False

        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            try:
                await self._recv_task
            except (asyncio.CancelledError, Exception):
                pass
        self._recv_task = None

        if self.ws is not None:
            try:
                await self.ws.close()
            except Exception as e:
                logger.debug(f"Error closing WebSocket: {e}")
            self.ws = None

        logger.info("WebSocket disconnected")

    async def health_check(self) -> bool:
        return self.is_connected and self._is_open()


class BackendClientFactory:
    @staticmethod
    def create_client(config: BackendConfig) -> BackendClient:
        if config.type == BackendType.WEBSOCKET:
            return WebSocketBackendClient(config)
        raise ValueError(f"Unsupported backend type: {config.type}")