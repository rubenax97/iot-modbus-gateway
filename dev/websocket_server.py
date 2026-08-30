#!/usr/bin/env python3
"""
Simple WebSocket server for testing the Modbus gateway.
"""
import asyncio
import json
import logging
from datetime import datetime
from typing import Set

import websockets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WebSocketServer:
    """Simple WebSocket server for testing."""
    
    def __init__(self, host='localhost', port=8765):
        self.host = host
        self.port = port
        self.server = None
        self.clients: Set[websockets.WebSocketServerProtocol] = set()
        self._running = False
        
    async def handler(self, websocket, path=None):
        """Handle WebSocket connections."""
        client_id = id(websocket)
        logger.info(f"WebSocket: Client connected: {client_id}")
        self.clients.add(websocket)
        
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    logger.info(f"WebSocket: Received from client {client_id}: {json.dumps(data, indent=2)}")
                    
                    response = {
                        "status": "received",
                        "timestamp": datetime.now().isoformat(),
                        "original": data
                    }
                    await websocket.send(json.dumps(response))
                    
                except json.JSONDecodeError:
                    logger.warning(f"WebSocket: Invalid JSON from client {client_id}")
                    await websocket.send(json.dumps({"error": "Invalid JSON"}))
                    
        except websockets.ConnectionClosed:
            logger.info(f"WebSocket: Client disconnected: {client_id}")
        finally:
            self.clients.discard(websocket)
    
    async def start(self):
        """Start the WebSocket server."""
        self._running = True
        
        self.server = await websockets.serve(
            self.handler,
            self.host,
            self.port,
            subprotocols=None
        )
        
        logger.info(f"WebSocket: Server started on ws://{self.host}:{self.port}")
        
        while self._running:
            await asyncio.sleep(1)
    
    async def stop(self):
        """Stop the WebSocket server."""
        self._running = False
        
        if self.clients:
            for client in self.clients:
                try:
                    await client.close()
                except:
                    pass
            self.clients.clear()
        
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("WebSocket: Server stopped")


async def run_websocket_server(host='localhost', port=8765):
    """Run WebSocket server (convenience function)."""
    server = WebSocketServer(host, port)
    await server.start()


if __name__ == "__main__":
    try:
        asyncio.run(run_websocket_server())
    except KeyboardInterrupt:
        logger.info("WebSocket server shutdown by user")