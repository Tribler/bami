import json
import os
from pathlib import Path
from typing import Dict, Tuple, Optional, Callable

from aioquic.asyncio import serve, connect
from aioquic.h3.connection import H3_ALPN
from aioquic.quic.configuration import QuicConfiguration
from starlette.applications import Starlette
from starlette.routing import WebSocketRoute
from starlette.types import Scope, Receive, Send
from starlette.websockets import WebSocketDisconnect

from bami.quic.http3_client import HttpClient
from bami.quic.http3_server import HttpServerProtocol


callback: Optional[Callable] = None


async def ws(websocket):
    """
    WebSocket endpoint.
    """
    if "chat" in websocket.scope["subprotocols"]:
        subprotocol = "chat"
    else:
        subprotocol = None
    await websocket.accept(subprotocol=subprotocol)

    info_msg = await websocket.receive_bytes()
    info_dict = json.loads(info_msg.decode())
    response_json = {"status": "start"}
    await websocket.send_bytes(json.dumps(response_json).encode())

    received_bytes = b""
    try:
        while len(received_bytes) < info_dict["length"]:
            message = await websocket.receive_bytes()
            received_bytes += message
    except WebSocketDisconnect:
        pass

    response_json = {"status": "done"}
    await websocket.send_bytes(json.dumps(response_json).encode())

    if callback:
        callback(websocket.client, info_dict, received_bytes)


async def application(scope: Scope, receive: Receive, send: Send) -> None:
    await Starlette(
        routes=[
            WebSocketRoute("/ws", ws),
        ]
    )(scope, receive, send)


class WebSocketServerProtocol(HttpServerProtocol):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.application = application


class QUIC:
    """
    The main protocol handler.
    For authentication, it uses the TLS certificates in the source tree.
    """

    def __init__(self):
        self.server = None

    async def start_server(self, host: str, port: int) -> None:
        server_config = QuicConfiguration(
            is_client=False, alpn_protocols=H3_ALPN, max_datagram_frame_size=65536
        )
        server_config.load_cert_chain(
            Path(Path(__file__).parent, "ssl_cert.pem"),
            Path(Path(__file__).parent, "ssl_key.pem"),
        )
        self.server = await serve(
            host,
            port,
            configuration=server_config,
            create_protocol=WebSocketServerProtocol,
        )

    def stop_server(self):
        self.server.close()

    async def transfer(
        self, target_peer: Tuple[str, int], info: Dict, binary_data: bytes
    ):
        client_config = QuicConfiguration(
            alpn_protocols=H3_ALPN, max_datagram_frame_size=65536
        )
        client_config.load_verify_locations(
            cafile=os.path.join(Path(__file__).parent, "pycacert.pem")
        )
        info["length"] = len(binary_data)

        async with connect(
            *target_peer, configuration=client_config, create_protocol=HttpClient
        ) as client:
            websocket = await client.websocket(
                "ws://%s:%d/ws" % (target_peer[0], target_peer[1])
            )

            # Send the info payload
            binary_info = json.dumps(info).encode()
            await websocket.send(binary_info)
            response = await websocket.recv()
            json_response = json.loads(response.decode())
            if "status" in json_response and json_response["status"] == "start":
                await websocket.send(binary_data)

            response = await websocket.recv()
            json_response = json.loads(response.decode())
            if "status" in json_response and json_response["status"] == "done":
                await websocket.send(binary_data)
