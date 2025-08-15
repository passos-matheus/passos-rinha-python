import json
import asyncio
import socket
from typing import Dict, Any, Union, Optional
from collections import deque


class TCPQueueClient:
    def __init__(self, host='worker-3', port=8888, timeout=2.0, pool_size=16):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.pool_size = pool_size
        self.pool = deque(maxlen=pool_size)
        self.pool_lock = asyncio.Lock()
        self._closing = False
        self._semaphore = asyncio.Semaphore(8)

    async def send_batch_payments(self, payment_data: Union[bytes, dict, str]) -> Dict[str, Any]:
        async with self._semaphore:
            try:
                payment_json = self._prepare_payment_data(payment_data)
                message = {
                    'action': 'push',
                    'data': payment_json
                }
                return await self._send_message_async(message)
            except Exception as e:
                return {'status': 'error', 'message': str(e)}

    def _prepare_payment_data(self, payment_data: Union[bytes, dict, str, list]) -> Union[dict, list]:
        try:
            if isinstance(payment_data, bytes):
                payment_str = payment_data.decode('utf-8')
                return json.loads(payment_str)
            elif isinstance(payment_data, str):
                return json.loads(payment_data)
            elif isinstance(payment_data, dict):
                return payment_data
            elif isinstance(payment_data, list):
                return payment_data
            else:
                raise ValueError(f"Unsupported type: {type(payment_data)}")
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"[TCPQueueClient] JSON decode/prep error: {e}")
            raise ValueError(f"Invalid payment data: {e}")

    async def get_stats(self) -> Dict[str, Any]:
        message = {'action': 'stats'}
        return await self._send_message_async(message)

    async def _create_connection(self) -> tuple[Optional[asyncio.StreamReader], Optional[asyncio.StreamWriter]]:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port, limit=1048576),
                timeout=self.timeout
            )

            sock = writer.get_extra_info('socket')
            if sock:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1048576)

            return reader, writer
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
            return None, None

    async def _get_connection(self) -> tuple[Optional[asyncio.StreamReader], Optional[asyncio.StreamWriter]]:
        async with self.pool_lock:
            while self.pool and not self._closing:
                reader, writer = self.pool.popleft()
                if not writer.is_closing():
                    return reader, writer
                writer.close()

        if self._closing:
            return None, None

        return await self._create_connection()

    async def _return_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        if not self._closing and not writer.is_closing():
            async with self.pool_lock:
                if len(self.pool) < self.pool_size:
                    self.pool.append((reader, writer))
                    return
        writer.close()

    async def _send_message_async(self, message: Dict[str, Any]) -> Dict[str, Any]:
        reader, writer = await self._get_connection()
        if not reader or not writer:
            return {'status': 'error', 'message': 'connection failed'}

        try:
            message_bytes = (json.dumps(message) + '\n').encode('utf-8')
            writer.write(message_bytes)
            await writer.drain()

            response = await asyncio.wait_for(
                reader.readline(),
                timeout=self.timeout
            )

            if not response:
                writer.close()
                return {'status': 'error', 'message': 'no response'}

            result = json.loads(response)
            await self._return_connection(reader, writer)
            return result

        except asyncio.TimeoutError:
            writer.close()
            return {'status': 'error', 'message': 'TCP timeout'}
        except (json.JSONDecodeError, UnicodeDecodeError):
            writer.close()
            return {'status': 'error', 'message': 'invalid response'}
        except Exception as e:
            writer.close()
            return {'status': 'error', 'message': str(e)}

    async def close(self):
        self._closing = True
        async with self.pool_lock:
            while self.pool:
                reader, writer = self.pool.popleft()
                writer.close()