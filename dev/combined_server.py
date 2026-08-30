#!/usr/bin/env python3
"""
Combined server runner for testing the Modbus gateway.
Calls both modbus_server and websocket_server.
"""
import asyncio
import logging
import sys
from pathlib import Path

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent))

from modbus_server import run_modbus_server
from websocket_server import run_websocket_server

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


class CombinedServer:
    """Combined server running both Modbus and WebSocket."""
    
    def __init__(self, modbus_host='localhost', modbus_port=5020,
                 websocket_host='localhost', websocket_port=8765):
        self.modbus_host = modbus_host
        self.modbus_port = modbus_port
        self.websocket_host = websocket_host
        self.websocket_port = websocket_port
        self._running = False
        self._tasks = []
    
    async def _run_modbus(self):
        """Run Modbus server."""
        await run_modbus_server(self.modbus_host, self.modbus_port)
    
    async def _run_websocket(self):
        """Run WebSocket server."""
        await run_websocket_server(self.websocket_host, self.websocket_port)
    
    async def start(self):
        """Start both servers."""
        self._running = True
        
        logger.info("=" * 60)
        logger.info("Combined Server Starting...")
        logger.info("=" * 60)
        logger.info(f"Modbus TCP:  {self.modbus_host}:{self.modbus_port}")
        logger.info(f"WebSocket:   ws://{self.websocket_host}:{self.websocket_port}")
        logger.info("=" * 60)
        logger.info("Press Ctrl+C to stop both servers")
        logger.info("=" * 60)
        
        # Start both servers in parallel
        self._tasks = [
            asyncio.create_task(self._run_modbus()),
            asyncio.create_task(self._run_websocket())
        ]
        
        # Wait for any task to complete (or fail)
        done, pending = await asyncio.wait(
            self._tasks,
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # If one server stops, stop the other
        for task in pending:
            task.cancel()
        
        # Check for exceptions
        for task in done:
            try:
                task.result()
            except Exception as e:
                logger.error(f"Server error: {e}")
    
    async def stop(self):
        """Stop both servers."""
        self._running = False
        logger.info("Stopping both servers...")
        
        # Cancel any remaining tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        logger.info("Both servers stopped")


async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Combined Modbus + WebSocket Server')
    parser.add_argument('--modbus-host', default='localhost', help='Modbus host')
    parser.add_argument('--modbus-port', type=int, default=5020, help='Modbus port')
    parser.add_argument('--websocket-host', default='localhost', help='WebSocket host')
    parser.add_argument('--websocket-port', type=int, default=8765, help='WebSocket port')
    args = parser.parse_args()
    
    server = CombinedServer(
        modbus_host=args.modbus_host,
        modbus_port=args.modbus_port,
        websocket_host=args.websocket_host,
        websocket_port=args.websocket_port
    )
    
    try:
        await server.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        await server.stop()
    except Exception as e:
        logger.error(f"Error: {e}")
        await server.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server shutdown by user")