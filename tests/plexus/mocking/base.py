from asyncio import all_tasks, sleep, Task

from ipv8.peer import Peer
from ipv8.test.mocking.endpoint import internet
from ipv8.test.mocking.ipv8 import MockIPv8


def create_node(overlay_class, work_dir=".", **kwargs):
    ipv8 = MockIPv8("curve25519", overlay_class, work_dir=work_dir, **kwargs)
    ipv8.overlay.ipv8 = ipv8
    return ipv8


def connect_nodes(nodes, overlay_class):
    # Add nodes to each other
    for node in nodes:
        for other in nodes:
            if other == node:
                continue
            private_peer = other.my_peer
            public_peer = Peer(private_peer.public_key, private_peer.address)
            node.network.add_verified_peer(public_peer)
            node.network.discover_services(public_peer, [overlay_class.community_id])


def create_and_connect_nodes(num_nodes, work_dirs, ov_class):
    nodes = [
        create_node(ov_class, work_dir=str(work_dirs[i])) for i in range(num_nodes)
    ]
    connect_nodes(nodes, ov_class)
    return nodes


async def unload_nodes(nodes):
    for node in nodes:
        await node.stop()
    internet.clear()


def is_background_task(task: Task):
    # Only in Python 3.8+ will we have a get_name function
    name = (
        task.get_name()
        if hasattr(task, "get_name")
        else getattr(task, "name", f"Task-{id(task)}")
    )
    return name.endswith("_check_tasks")


async def deliver_messages(timeout=0.1):
    """
    Allow peers to communicate.

    The strategy is as follows:
     1. Measure the amount of existing asyncio tasks
     2. After 10 milliseconds, check if we are below 2 tasks twice in a row
     3. If not, go back to handling calls (step 2) or return, if the timeout has been reached

    :param timeout: the maximum time to wait for messages to be delivered
    """
    rtime = 0
    probable_exit = False

    while rtime < timeout:
        await sleep(0.01)
        rtime += 0.01
        if len([task for task in all_tasks() if not is_background_task(task)]) < 2:
            if probable_exit:
                break
            probable_exit = True
        else:
            probable_exit = False


async def introduce_nodes(nodes):
    for node in nodes:
        for other in nodes:
            if other != node:
                node.overlay.walk_to(other.endpoint.wan_address)
    await deliver_messages()


class SetupValues:
    def __init__(self, **kwargs) -> None:
        self.__dict__.update(kwargs)
