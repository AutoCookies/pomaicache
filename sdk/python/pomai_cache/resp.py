"""Minimal RESP protocol encoder/decoder for Pomai Cache."""

from __future__ import annotations

import socket
from typing import Any


def encode_command(*args: str) -> bytes:
    """Encode a command as a RESP array of bulk strings."""
    parts = [f"*{len(args)}\r\n"]
    for a in args:
        encoded = a if isinstance(a, str) else str(a)
        parts.append(f"${len(encoded.encode())}\r\n{encoded}\r\n")
    return "".join(parts).encode()


def read_line(sock: socket.socket) -> str:
    buf = b""
    while not buf.endswith(b"\r\n"):
        ch = sock.recv(1)
        if not ch:
            raise ConnectionError("Connection closed")
        buf += ch
    return buf[:-2].decode()


def read_reply(sock: socket.socket) -> Any:
    """Read a single RESP reply from the socket."""
    line = read_line(sock)
    prefix = line[0]
    body = line[1:]

    if prefix == "+":
        return body
    elif prefix == "-":
        raise RuntimeError(f"RESP error: {body}")
    elif prefix == ":":
        return int(body)
    elif prefix == "$":
        length = int(body)
        if length < 0:
            return None
        data = b""
        while len(data) < length + 2:
            chunk = sock.recv(length + 2 - len(data))
            if not chunk:
                raise ConnectionError("Connection closed")
            data += chunk
        return data[:-2].decode()
    elif prefix == "*":
        count = int(body)
        if count < 0:
            return None
        return [read_reply(sock) for _ in range(count)]
    else:
        return line
