import json
import enum
import random
import asyncio
import logging
import dataclasses
from typing import Tuple
import raft.machine as machine
import raft.message as message
from raft.message import MessageType
from . import LoggerMixin
from .machine import State


# Declare some types
Address = Tuple[str, int]


@enum.unique
class NodeStatus(enum.IntEnum):
    DEAD = 0
    ALIVE = 1


@dataclasses.dataclass
class Node:
    addr: Address
    status: NodeStatus = NodeStatus.DEAD

    @property
    def name(self):
        # A dumb enough ID
        host, port = self.addr
        return f'{host}:{port}'


deser = {
    MessageType.REQUEST_VOTE: message.RequestVote.from_dict,
    MessageType.REQUEST_VOTE_RESPONSE: message.ResponseVote.from_dict,
    MessageType.APPEND_ENTRIES: message.AppendEntries.from_dict,
    MessageType.APPEND_ENTRIES_RESPONSE: message.AppendEntries.from_dict
}


class RaftServerProtocol(LoggerMixin, asyncio.DatagramProtocol):

    def __init__(self, node_id, timeout, election_timeout, on_con_lost, nodes=()):
        self.nodes = nodes
        self.node_id = node_id
        self.timeout = timeout
        self.on_con_lost = on_con_lost
        self.election_timeout = election_timeout
        self.loop = asyncio.get_running_loop()
        self.machine = machine.RaftMachine()
        self.last_heartbeat = 0
        super().__init__(level=logging.DEBUG)
        self.set_formatter(f'[{self.node_id}] %(message)s')
        self.votes = set()

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        self.log.info('Connection from %s', peername)
        self.transport = transport
        self.loop.call_later(self.election_timeout, self.init_state)

    def datagram_received(self, data, addr):
        raw_message = data.decode()
        msg_dict = json.loads(raw_message)
        try:
            msg = deser[msg_dict['type']](msg_dict['payload'])
        except KeyError:
            self.log.error("Unknown message type")
        else:
            self.log.info('Data received from %s: %s', addr, msg)
            if isinstance(msg, message.AppendEntries):
                self.last_heartbeat = self.loop.time()
                if msg.command:
                    self.machine.append_entries(msg.command)
            elif isinstance(msg, message.AppendEntriesResponse):
                # TODO
                self.machine.commit()
            elif isinstance(msg, message.RequestVote):
                if not self.machine.state == State.LEADER:
                    self.machine.term += 1
                    self.machine.vote = f'{addr[0]}:{addr[1]}'
                    msg = message.ResponseVote()
                    self.transport.sendto(msg.to_json().encode(), addr)
                    self.log.info("Becoming follower")
                    self.machine.become_follower()
            elif isinstance(msg, message.ResponseVote):
                self.votes.add(addr)
                if len(self.votes) > len(self.nodes) // 2:
                    self.log.info("Becoming leader")
                    self.machine.become_leader()
                    self.send_heartbeat()

    def error_received(self, exc):
        self.log.info('Server received an error %s', exc)

    def connection_lost(self, exc):
        self.log.info('Connection closed')
        self.on_con_lost.set_result(True)

    def init_state(self):
        if self.loop.time() - self.last_heartbeat > self.election_timeout:
            self.log.info("Becoming candidate")
            self.machine.become_candidate()
            self.machine.vote = self.node_id
            self.send_vote_request()
        else:
            self.loop.call_later(self.election_timeout, self.init_state)
        self.machine.term += 1

    def send_vote_request(self):
        self.log.debug("Sending vote request")
        msg = message.RequestVote()
        for node_addr in self.nodes:
            self.transport.sendto(msg.to_json().encode(), node_addr)

    def send_heartbeat(self):
        if self.machine.state != State.LEADER:
            return
        msg = message.AppendEntries(self.machine.term, 0, None)
        for node_addr in self.nodes:
            self.transport.sendto(msg.to_json().encode(), node_addr)
        self.loop.call_later(self.timeout, self.send_heartbeat)


async def run_server(addr=('127.0.0.1', 20000), nodes_addrs=(), timeout=.1):
    # Get a reference to the event loop as we plan to use
    # low-level APIs.
    loop = asyncio.get_running_loop()
    election_timeout = round(random.uniform(.150, .300), 3)

    node = Node(addr)

    on_con_lost = loop.create_future()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: RaftServerProtocol(
            node.name,
            timeout,
            election_timeout,
            on_con_lost,
            nodes_addrs
        ),
        local_addr=addr
    )

    try:
        await on_con_lost
    finally:
        transport.close()
