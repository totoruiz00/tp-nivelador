import socket
import safe_socket

_LENGTH_HEADER_SIZE = 4

FIN_MESSAGE = "FIN"
ACK_MESSAGE = "OK"


def _read_length_header(sock: socket.socket) -> int:
    header_bytes = safe_socket.recv_all(sock, _LENGTH_HEADER_SIZE)
    return int.from_bytes(header_bytes, byteorder="big")


def recv_message(sock: socket.socket) -> str:
    payload_length = _read_length_header(sock)
    payload_bytes = safe_socket.recv_all(sock, payload_length)
    return payload_bytes.decode("utf-8")


def send_message(sock: socket.socket, payload: str) -> None:
    payload_bytes = payload.encode("utf-8")
    header_bytes = len(payload_bytes).to_bytes(_LENGTH_HEADER_SIZE, byteorder="big")
    safe_socket.send_all(sock, header_bytes + payload_bytes)
