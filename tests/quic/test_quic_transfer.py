from asyncio import Future, ensure_future

import pytest

import bami.quic.quic
from bami.quic.quic import QUIC


@pytest.mark.asyncio
async def test_simple_transfer():
    done_future = Future()

    def on_receive(peer, info, data):
        done_future.set_result(None)

    server = QUIC()
    bami.quic.quic.callback = on_receive
    await server.start_server("localhost", 1234)

    client = QUIC()
    await client.transfer(("localhost", 1234), {"a": "b"}, b"a" * 400)
    await done_future


@pytest.mark.asyncio
async def test_large_transfer():
    done_future = Future()

    def on_receive(peer, info, data):
        done_future.set_result(None)

    server = QUIC()
    bami.quic.quic.callback = on_receive
    await server.start_server("localhost", 1234)

    client = QUIC()
    await client.transfer(("localhost", 1234), {"a": "b"}, b"a" * 600000)
    await done_future


@pytest.mark.asyncio
async def test_multiple_clients():
    done_future = Future()

    def on_receive(peer, info, data):
        on_receive.received += 1
        if on_receive.received == 2:
            done_future.set_result(None)

    on_receive.received = 0

    server = QUIC()
    bami.quic.quic.callback = on_receive
    await server.start_server("localhost", 1235)

    client1 = QUIC()
    ensure_future(client1.transfer(("localhost", 1235), {"a": "b"}, b"a" * 400))
    client2 = QUIC()
    ensure_future(client2.transfer(("localhost", 1235), {"a": "b"}, b"b" * 400))
    await done_future
