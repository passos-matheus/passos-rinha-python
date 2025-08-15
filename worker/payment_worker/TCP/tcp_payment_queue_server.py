import json
import asyncio
import socket
from typing import Optional, Set


class TCPQueueServer:
    def __init__(self, payment_queue: asyncio.Queue):
        self.host = '0.0.0.0'
        self.port = 8888
        self.payment_queue = payment_queue
        self.running = True
        self.connections: Set[asyncio.StreamWriter] = set()
        self.stats = {
            'tasks_received': 0,
            'active_connections': 0
        }
        self.server: Optional[asyncio.Server] = None

    async def start(self):
        self.server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
            limit=1048576,
            backlog=1024
        )

        sock = self.server.sockets[0]
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1048576)

        asyncio.create_task(self._run_server())

    async def _run_server(self):
        async with self.server:
            await self.server.serve_forever()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        sock = writer.get_extra_info('socket')
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        self.connections.add(writer)
        self.stats['active_connections'] += 1

        try:
            while self.running and not reader.at_eof():
                try:
                    line = await reader.readline()

                    if not line:
                        break

                    response = await self._process_raw(line)
                    writer.write(response)
                    await writer.drain()

                except (ConnectionResetError, BrokenPipeError):
                    break
                except Exception:
                    break

        finally:
            self.connections.discard(writer)
            self.stats['active_connections'] -= 1
            writer.close()
            await writer.wait_closed()

    async def _process_raw(self, line: bytes) -> bytes:
        try:
            data = json.loads(line)
            action = data.get('action')

            if action == 'push':
                payment_data = data.get('data')

                try:
                    self.payment_queue.put_nowait(json.dumps(payment_data))
                    self.stats['tasks_received'] += 1
                    return b'{"status":"success","message":"Payment queued"}\n'
                except asyncio.QueueFull:
                    return b'{"status":"error","message":"Queue full"}\n'

            elif action == 'stats':
                response = {
                    'status': 'success',
                    'queue_size': self.payment_queue.qsize(),
                    'active_connections': self.stats['active_connections'],
                    'tasks_received': self.stats['tasks_received']
                }
                return (json.dumps(response) + '\n').encode('utf-8')

            else:
                return b'{"status":"error","message":"action invalida"}\n'

        except json.JSONDecodeError:
            return b'{"status":"error","message":"JSON invalido"}\n'
        except Exception:
            return b'{"status":"error","message":"error"}\n'

    async def stop(self):
        self.running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()

        tasks = []
        for writer in list(self.connections):
            writer.close()
            tasks.append(writer.wait_closed())
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
