from abc import ABC, abstractmethod
import json
import asyncio
import logging
import ssl
from typing import Optional, Dict, Any, Callable
from datetime import datetime
import threading
import time

import websocket
from tenacity import retry, stop_after_attempt, wait_exponential

from .models import BackendConfig, BackendType

logger = logging.getLogger(__name__)

class BackendClient(ABC):
    """Abstract base class for backend clients."""
    
    def __init__(self, config: BackendConfig):
        self.config = config
        self.is_connected = False
        self._lock = asyncio.Lock()
    
    @abstractmethod
    async def connect(self) -> bool:
        """Connect to the backend."""
        pass
    
    @abstractmethod
    async def send_data(self, data: Dict[str, Any]) -> bool:
        """Send data to the backend."""
        pass
    
    @abstractmethod
    async def disconnect(self):
        """Disconnect from the backend."""
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """Check if the backend is healthy."""
        pass

class WebSocketBackendClient(BackendClient):
    """Async WebSocket backend client."""
    
    def __init__(self, config: BackendConfig):
        super().__init__(config)
        self.ws = None
        self._thread = None
        self._running = False
        self._message_queue = asyncio.Queue()
        self._connected_event = asyncio.Event()
        self._callbacks = {
            'on_message': None,
            'on_error': None,
            'on_connect': None,
            'on_disconnect': None
        }
        self._main_loop = None
        self._ws_lock = asyncio.Lock()
    
    def _on_message(self, ws, message):
        """Handle incoming WebSocket messages."""
        logger.debug(f"Received message: {message}")
        callback = self._callbacks.get('on_message')
        if callback and self._main_loop and self._main_loop.is_running():
            try:
                data = json.loads(message)
                asyncio.run_coroutine_threadsafe(callback(data), self._main_loop)
            except json.JSONDecodeError:
                asyncio.run_coroutine_threadsafe(callback(message), self._main_loop)
    
    def _on_error(self, ws, error):
        """Handle WebSocket errors."""
        logger.error(f"WebSocket error: {error}")
        callback = self._callbacks.get('on_error')
        if callback and self._main_loop and self._main_loop.is_running():
            asyncio.run_coroutine_threadsafe(callback(str(error)), self._main_loop)
    
    def _on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection close."""
        logger.info(f"WebSocket connection closed: {close_status_code} - {close_msg}")
        self.is_connected = False
        self._connected_event.clear()
        callback = self._callbacks.get('on_disconnect')
        if callback and self._main_loop and self._main_loop.is_running():
            asyncio.run_coroutine_threadsafe(callback(), self._main_loop)
    
    def _on_open(self, ws):
        """Handle WebSocket connection open."""
        logger.info(f"WebSocket connected to {self.config.url}")
        self.is_connected = True
        self._connected_event.set()
        callback = self._callbacks.get('on_connect')
        if callback and self._main_loop and self._main_loop.is_running():
            asyncio.run_coroutine_threadsafe(callback(), self._main_loop)
    
    def _run_websocket(self):
        """Run the WebSocket connection in a separate thread."""
        try:
            # Don't pass subprotocols if they're None, empty, or invalid
            subprotocols = None
            if self.config.subprotocol and self.config.subprotocol.strip():
                subprotocols = [self.config.subprotocol]
            
            self.ws = websocket.WebSocketApp(
                self.config.url,
                subprotocols=subprotocols,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=self._on_open
            )
            
            sslopt = {"cert_reqs": ssl.CERT_NONE} if self.config.tls else None
            
            # Run the WebSocket connection
            self.ws.run_forever(
                sslopt=sslopt,
                ping_interval=30,
                ping_timeout=10
            )
        except Exception as e:
            logger.error(f"WebSocket thread error: {e}")
        finally:
            self._running = False
            self.is_connected = False
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5)
    )
    async def connect(self) -> bool:
        """Connect to WebSocket server with retry."""
        if self.is_connected:
            return True
        
        try:
            self._running = True
            self._connected_event.clear()
            
            # Store the main event loop for callbacks
            self._main_loop = asyncio.get_running_loop()
            
            # Start WebSocket in a separate thread
            self._thread = threading.Thread(target=self._run_websocket, daemon=True)
            self._thread.start()
            
            # Wait for connection with timeout
            try:
                await asyncio.wait_for(self._connected_event.wait(), timeout=10.0)
                return True
            except asyncio.TimeoutError:
                logger.error("Timeout waiting for WebSocket connection")
                self._running = False
                return False
                
        except Exception as e:
            logger.error(f"Failed to connect to WebSocket: {e}")
            return False
    
    async def send_data(self, data: Dict[str, Any]) -> bool:
        """Send data via WebSocket."""
        async with self._ws_lock:
            if not self.is_connected or self.ws is None:
                logger.warning("WebSocket not connected, queueing message")
                await self._message_queue.put(data)
                return False
            
            try:
                if "timestamp" not in data:
                    data["timestamp"] = datetime.now().isoformat()
                
                message = json.dumps(data)
                self.ws.send(message)
                logger.debug(f"Data sent via WebSocket")
                return True
                
            except Exception as e:
                logger.error(f"Error sending WebSocket message: {e}")
                await self._message_queue.put(data)
                return False
    
    async def disconnect(self):
        """Disconnect from WebSocket server."""
        self._running = False
        if self.ws:
            try:
                self.ws.close()
            except:
                pass
        self.is_connected = False
        self._connected_event.clear()
        logger.info("WebSocket disconnected")
    
    async def health_check(self) -> bool:
        """Check if WebSocket is connected."""
        return self.is_connected
    
    def set_callback(self, event: str, callback: Callable):
        """Set callback for WebSocket events."""
        if event in self._callbacks:
            self._callbacks[event] = callback
    
    async def process_queue(self):
        """Process queued messages."""
        while self._running:
            try:
                # Wait for message with timeout to allow checking _running
                try:
                    message = await asyncio.wait_for(self._message_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                if self.is_connected and self.ws is not None:
                    success = await self.send_data(message)
                    if not success:
                        await self._message_queue.put(message)
                else:
                    # Re-queue if not connected
                    await self._message_queue.put(message)
                    await asyncio.sleep(1)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing queue: {e}")
                await asyncio.sleep(5)

class BackendClientFactory:
    """Factory for creating backend clients."""
    
    @staticmethod
    def create_client(config: BackendConfig) -> BackendClient:
        """Create a backend client based on configuration."""
        if config.type == BackendType.WEBSOCKET:
            return WebSocketBackendClient(config)
        else:
            raise ValueError(f"Unsupported backend type: {config.type}")