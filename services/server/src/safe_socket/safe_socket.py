import socket


def recv_all(sock: socket.socket, size: int) -> bytes:
    buffer = bytearray()
    while len(buffer) < size:
        chunk = sock.recv(size - len(buffer))
        if not chunk:
            raise ConnectionError(
                "connection closed before receiving all expected bytes"
            )
        buffer.extend(chunk)
    return bytes(buffer)


def send_all(sock: socket.socket, bytes_to_send: bytes) -> None:
    total_sent = 0
    while total_sent < len(bytes_to_send):
        sent = sock.send(bytes_to_send[total_sent:])
        total_sent += sent
