import os
import random
import shutil
import string
import sys
import threading
import time
from asyncio import all_tasks, get_event_loop, sleep

import asynctest

from tests.mocking.endpoint import internet
from tests.mocking.ipv8 import MockIPv8
from python_project.peer import Peer

get_event_loop().set_debug(True)


class TestBase(asynctest.TestCase):

    __testing__ = True
    __lockup_timestamp__ = 0

    # The time after which the whole test suite is os.exited
    MAX_TEST_TIME = 10

    def __init__(self, methodName="runTest"):
        super(TestBase, self).__init__(methodName)
        self.nodes = []
        self.overlay_class = object
        internet.clear()
        self._tempdirs = []

    def initialize(self, overlay_class, node_count, *args, **kwargs):
        self.overlay_class = overlay_class
        self.nodes = [self.create_node(*args, **kwargs) for _ in range(node_count)]

        # Add nodes to each other
        for node in self.nodes:
            for other in self.nodes:
                if other == node:
                    continue
                private_peer = other.my_peer
                public_peer = Peer(private_peer.public_key, private_peer.address)
                node.network.add_verified_peer(public_peer)
                node.network.discover_services(
                    public_peer, [overlay_class.master_peer.mid]
                )

    def setUp(self):
        super(TestBase, self).setUp()
        TestBase.__lockup_timestamp__ = time.time()

    async def tearDown(self):
        try:
            for node in self.nodes:
                await node.unload()
            internet.clear()
        finally:
            while self._tempdirs:
                shutil.rmtree(self._tempdirs.pop(), ignore_errors=True)
        super(TestBase, self).tearDown()

    @classmethod
    def setUpClass(cls):
        TestBase.__lockup_timestamp__ = time.time()

        def check_loop():
            while time.time() - TestBase.__lockup_timestamp__ < cls.MAX_TEST_TIME:
                time.sleep(2)
                # If the test class completed normally, exit
                if not cls.__testing__:
                    return
            # If we made it here, there is a serious issue which we cannot recover from.
            # Most likely the threadpool got into a deadlock while shutting down.
            import traceback

            print(
                "The test-suite locked up! Force quitting! Thread dump:",
                file=sys.stderr,
            )
            for tid, stack in sys._current_frames().items():
                if tid != threading.currentThread().ident:
                    print("THREAD#%d" % tid, file=sys.stderr)
                    for line in traceback.format_list(traceback.extract_stack(stack)):
                        print("|", line[:-1].replace("\n", "\n|   "), file=sys.stderr)

            tasks = all_tasks(get_event_loop())
            if tasks:
                print("Pending tasks:")
                for task in tasks:
                    print(">     %s" % task)

            # Our test suite catches the SIGINT signal, this allows it to print debug information before force exiting.
            # If we were to hard exit here (through os._exit) we would lose this additional information.
            import signal

            os.kill(os.getpid(), signal.SIGINT)
            # But sometimes it just flat out refuses to die (sys.exit will also not work in this case).
            # So we double kill ourselves:
            os._exit(1)  # pylint: disable=W0212

        t = threading.Thread(target=check_loop)
        t.daemon = True
        t.start()

    @classmethod
    def tearDownClass(cls):
        cls.__testing__ = False

    def create_node(self, *args, **kwargs):
        return MockIPv8(u"low", self.overlay_class, *args, **kwargs)

    def add_node_to_experiment(self, node):
        """
        Add a new node to this experiment (use `create_node()`).
        """
        for other in self.nodes:
            private_peer = other.my_peer
            public_peer = Peer(private_peer.public_key, private_peer.address)
            node.network.add_verified_peer(public_peer)
            node.network.discover_services(public_peer, node.overlay.master_peer.mid)
        self.nodes.append(node)

    async def deliver_messages(self, timeout=0.1):
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
            if len(all_tasks()) < 2:
                if probable_exit:
                    break
                probable_exit = True
            else:
                probable_exit = False

    async def introduce_nodes(self):
        for node in self.nodes:
            for other in self.nodes:
                if other != node:
                    node.overlay.walk_to(other.endpoint.wan_address)
        await self.deliver_messages()

    def temporary_directory(self):
        rndstr = "temp_".join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(10)
        )
        d = os.path.abspath(self.__class__.__name__ + rndstr)
        self._tempdirs.append(d)
        os.makedirs(d)
        return d
