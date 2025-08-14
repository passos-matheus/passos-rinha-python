import json
import socket
import asyncio
import threading

from typing import Dict, Any, Union


class TCPQueueClient:

    def __init__(self, host='worker-3', port=8888, timeout=5.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.connection_pool = []
        self.pool_lock = threading.Lock()
        self.max_pool_size = 10

    async def send_payment(self, payment_data: Union[bytes, dict, str]) -> Dict[str, Any]:
        try:
            payment_json = self._prepare_payment_data(payment_data)

            message = {
                'action': 'push',
                'data': payment_json
            }

            result = await self._send_message_async(message)
            return result

        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    def _prepare_payment_data(self, payment_data: Union[bytes, dict, str]) -> dict:
        try:
            if isinstance(payment_data, bytes):
                payment_str = payment_data.decode('utf-8')
                return json.loads(payment_str)
            elif isinstance(payment_data, str):
                return json.loads(payment_data)
            elif isinstance(payment_data, dict):
                return payment_data
            else:
                raise ValueError(f"type do payment não suportado: {type(payment_data)}")

        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ValueError(f"Invalid payment data format: {e}")

    async def get_stats(self) -> Dict[str, Any]:
        message = {'action': 'stats'}
        return await self._send_message_async(message)

    async def _send_message_async(self, message: Dict[str, Any]) -> Dict[str, Any]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._send_message_sync, message)

    def _send_message_sync(self, message: Dict[str, Any]) -> Dict[str, Any]:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(self.timeout)
                sock.connect((self.host, self.port))

                message_str = json.dumps(message) + '\n'
                sock.send(message_str.encode('utf-8'))

                buffer = b""
                while b'\n' not in buffer:
                    data = sock.recv(4096)
                    if not data:
                        break
                    buffer += data

                if b'\n' in buffer:
                    response_line, _ = buffer.split(b'\n', 1)
                    return json.loads(response_line.decode('utf-8'))
                else:
                    return {'status': 'error', 'message': 'no response'}
        except socket.timeout:
            return {'status': 'error', 'message': 'TCP timeout'}
        except ConnectionRefusedError:
            return {'status': 'error', 'message': 'nao encontrou o container do worker'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
