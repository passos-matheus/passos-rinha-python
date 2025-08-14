import json
import socket
import asyncio
import threading

from typing import Optional


class TCPQueueServer:
    def __init__(self, payment_queue: asyncio.Queue):
        self.host = '0.0.0.0'
        self.port = 8888
        self.payment_queue = payment_queue
        self.running = True
        self.connections = []
        self.stats = {
            'tasks_received': 0,
            'active_connections': 0
        }
        self.event_loop: Optional[asyncio.AbstractEventLoop] = None

    def start(self):
        try:
            self.event_loop = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError("TCPQueueServer nao foi iniciando num async context")

        tcp_thread = threading.Thread(target=self._tcp_server, daemon=True)
        tcp_thread.start()

    def _tcp_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            server_socket.bind((self.host, self.port))
            server_socket.listen(50)

            while self.running:
                try:
                    client_socket, addr = server_socket.accept()
                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, addr),
                        daemon=True
                    )
                    client_thread.start()

                except Exception as e:
                    if self.running:
                        raise e

        except Exception as e:
            raise e
        finally:
            server_socket.close()

    def _handle_client(self, client_socket: socket.socket, addr):
        self.connections.append(client_socket)
        self.stats['active_connections'] += 1

        try:
            with client_socket:
                buffer = b""

                while self.running:
                    try:
                        data = client_socket.recv(4096)
                        if not data:
                            break

                        buffer += data

                        while b'\n' in buffer:
                            line, buffer = buffer.split(b'\n', 1)
                            if line:
                                response = self._process_message(line.decode('utf-8'))
                                response_str = json.dumps(response) + '\n'
                                client_socket.send(response_str.encode('utf-8'))

                    except ConnectionResetError:
                        break
                    except:
                        break

        except Exception as e:
            raise e
        finally:
            if client_socket in self.connections:
                self.connections.remove(client_socket)
            self.stats['active_connections'] -= 1

    def _process_message(self, message: str) -> dict:
        try:
            data = json.loads(message)
            action = data.get('action')

            if action == 'push':
                payment_data = data.get('data')

                payment_json = json.dumps(payment_data)

                if self.event_loop and not self.event_loop.is_closed():
                    try:
                        future = asyncio.run_coroutine_threadsafe(
                            self.payment_queue.put(payment_json),
                            self.event_loop
                        )

                        future.result(timeout=1.0)

                        self.stats['tasks_received'] += 1

                        return {
                            'status': 'success',
                            'message': 'Payment queued',
                            'queue_size': self.payment_queue.qsize()
                        }

                    except asyncio.TimeoutError:
                        return {'status': 'error', 'message': 'Queue timeout'}
                    except Exception as e:
                        return {'status': 'error', 'message': f'Queue error: {e}'}
                else:
                    return {'status': 'error', 'message': 'event loop nao encontrado'}

            elif action == 'stats':
                return {
                    'status': 'success',
                    'queue_size': self.payment_queue.qsize(),
                    'active_connections': self.stats['active_connections'],
                    'tasks_received': self.stats['tasks_received']
                }

            else:
                return {'status': 'error', 'message': f'{action}'}

        except json.JSONDecodeError as e:
            return {'status': 'error', 'message': 'json quebrado!'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    def stop(self):
        self.running = False

        for conn in self.connections:
            try:
                conn.close()
            except:
                pass
