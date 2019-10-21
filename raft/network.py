import enum
import random
import asyncio
import logging
import dataclasses
from typing import Tuple
import raft.machine as machine
from .machine import State


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


# Declare some types
Address = Tuple[str, int]


@enum.unique
class NodeStatus(enum.IntEnum):
    DEAD = 0
    ALIVE = 1


@dataclasses.dataclass
class Node:
    addr: Address
    state: NodeStatus = NodeStatus.DEAD

    @property
    def name(self):
        # A dumb enough ID
        host, port = self.addr
        return f'{host}:{port}'


class RaftServerProtocol(asyncio.DatagramProtocol):

    def __init__(self, node_id, timeout, election_timeout, on_con_lost, nodes=()):
        self.node_id = node_id
        self.timeout = timeout
        self.nodes = nodes
        self.election_timeout = election_timeout
        self.on_con_lost = on_con_lost
        self.loop = asyncio.get_running_loop()
        self.machine = machine.RaftMachine()

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        log.info('Connection from %s', peername)
        self.transport = transport
        self.loop.call_later(self.election_timeout, self.init_state)

    def datagram_received(self, data, addr):
        message = data.decode()
        log.info('Data received from %s: %s', addr, message)
        if message == 'VOTE REQUEST':
            self.transport.sendto(b'OK', addr)

    def error_received(self, exc):
        log.info('Received an error %s', exc)

    def connection_lost(self, exc):
        log.info('Connection closed')
        self.on_con_lost.set_result(True)

    def init_state(self):
        if self.machine.state != State.FOLLOWER:
            return
        log.info("Becoming candidate")
        self.machine.become_candidate()
        self.machine.vote = self.node_id
        self.send_vote_request()

    def send_vote_request(self):
        for node, transport in self.nodes:
            transport.sendto(b'VOTE REQUEST')

    def send_heartbeat(self):
        # TODO send heartbeat
        if self.machine.state != State.LEADER:
            return
        for node, transport in self.nodes:
            transport.sendto(b"BEAT")
        self.loop.call_later(self.timeout, self.send_heartbeat)


async def run_server(addr=('127.0.0.1', 20000), nodes_addrs=(), timeout=5):
    # Get a reference to the event loop as we plan to use
    # low-level APIs.
    loop = asyncio.get_running_loop()
    election_timeout = round(random.uniform(.250, .300), 3)

    node = Node(addr)

    nodes = await asyncio.gather(
        *[get_client(node_addr) for node_addr in nodes_addrs],
        loop=loop
    )

    on_con_lost = loop.create_future()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: RaftServerProtocol(
            node.name,
            timeout,
            election_timeout,
            on_con_lost,
            nodes
        ),
        local_addr=addr
    )

    try:
        await on_con_lost
    finally:
        transport.close()


class RaftClientProtocol(asyncio.DatagramProtocol):

    def connection_made(self, transport):
        self.transport = transport
        log.info('Connection made')

    def datagram_received(self, data, addr):
        log.info('Data received from %s: %s', addr, data.decode())

    def error_received(self, exc):
        log.info('Received an error %s', exc)


async def get_client(addr=('127.0.0.1', 20001)):
    node = Node(addr)
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: RaftClientProtocol(),
        remote_addr=addr
    )
    return node, transport
