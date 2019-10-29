import enum
import random
import asyncio
import logging
import dataclasses
from typing import Tuple
import raft.machine as machine
import raft.message as message
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


class RaftServerProtocol(LoggerMixin, asyncio.DatagramProtocol):

    def __init__(self, event_queue, node_id, timeout,
                 election_timeout, on_con_lost, nodes=()):
        self.event_queue = event_queue
        self.nodes = nodes
        self.node_id = node_id
        self.timeout = timeout
        self.on_con_lost = on_con_lost
        self.election_timeout = election_timeout
        self.loop = asyncio.get_running_loop()
        self.machine = machine.RaftMachine(node_id, nodes)
        self.last_heartbeat = 0
        super().__init__(level=logging.DEBUG)
        self.set_formatter(f'[{self.node_id}] %(message)s')
        self.set_level(logging.DEBUG)
        self.votes = set()
        self.append_entries_resp = set()
        self.loop.create_task(self._poll_events())

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        self.log.info('Connection from %s', peername)
        self.transport = transport
        self.loop.call_later(self.election_timeout, self.init_state)

    def datagram_received(self, data, addr):
        raw_message = data.decode()
        try:
            msg = message.deserialize(raw_message)
        except message.UnknownMessageException:
            self.log.error("Unknown message type")
        else:
            self.log.info('Data received from %s: %s', addr, msg)
            if isinstance(msg, message.AppendEntries):
                if self.machine.state == State.LEADER:
                    return
                self.last_heartbeat = self.loop.time()
                success = True
                if msg.term < self.machine.term:
                    success = False
                elif msg.command:
                    self.machine.append_entries(msg.index, msg.command)
                else:
                    self.machine.commit(addr, msg.index)
                    success = False
                self.loop.call_soon(
                    self._send_append_entries_response,
                    addr,
                    success
                )
            elif isinstance(msg, message.AppendEntriesResponse):
                # TODO
                if self.machine.state != State.LEADER:
                    return
                if msg.success:
                    self.append_entries_resp.add(addr)
                    if len(self.append_entries_resp) > len(self.nodes) // 2:
                        self.machine.commit(addr, msg.index)
                        self.append_entries_resp.clear()
                        self.answer_client()
            elif isinstance(msg, message.RequestVote):
                if self.machine.state != State.CANDIDATE:
                    return
                # Two conditions to success reply:
                # 1. false if {message term} < {current node term}
                # 2. true If {current node voted_for} is None or
                # {message candidate_id}, and candidate’s log is at least as
                # up-to-date as current node’s log (last_log_index >= index)
                success = (
                    (msg.term >= self.machine.term) and
                    (self.machine.voted_for is None or
                     msg.candidate_id == self.machine.voted_for) and
                    (msg.last_log_index >= self.machine.next_index[addr])
                )
                if success:
                    self.machine.voted_for = f'{addr[0]}:{addr[1]}'
                msg = message.RequestVoteResponse(success)
                self.send_message(msg.to_json(), addr)
                self.log.info("Becoming follower")
                self.machine.become_follower()
            elif isinstance(msg, message.RequestVoteResponse):
                if self.machine.state != State.CANDIDATE:
                    return
                self.votes.add(addr)
                if len(self.votes) > len(self.nodes) // 2:
                    self.log.info("Becoming leader")
                    self.machine.become_leader()
                    self.votes.clear()
                    # We send our first heartbeat after election
                    self._send_append_entries(None)
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
            self.machine.voted_for = self.node_id
            self.send_vote_request()
        else:
            self.loop.call_later(self.election_timeout, self.init_state)

    def send_message(self, msg, addr):
        self.transport.sendto(msg.encode('utf-8'), addr)

    def send_vote_request(self):
        self.log.debug("Sending vote request")
        msg = message.RequestVote(
            self.machine.term,
            self.machine.last_log_index(),
            self.machine.last_log_term(),
            self.node_id
        )
        for node in self.nodes:
            self.send_message(msg.to_json(), node.addr)

    def send_heartbeat(self):
        if self.machine.state != State.LEADER:
            return
        self._send_append_entries(None)
        # Re-schedule recursively after timeout
        self.loop.call_later(self.timeout, self.send_heartbeat)

    def answer_client(self):
        # TODO
        self.log.debug("Answer to client")

    def _send_append_entries(self, data):
        msg = message.AppendEntries(
            self.machine.term,
            self.machine.last_log_index(),
            data
        )
        for node in self.nodes:
            self.send_message(msg.to_json(), node.addr)

    def _send_append_entries_response(self, addr, success):
        msg = message.AppendEntriesResponse(
            self.machine.term,
            self.machine.last_log_index(),
            success
        )
        self.send_message(msg.to_json(), addr)

    async def _poll_events(self):
        while True:
            event = await self.event_queue.get()
            if self.machine.state != State.LEADER:
                continue
            self.loop.call_soon(self._send_append_entries, event)


async def run_server(event_queue, addr=('127.0.0.1', 20000),
                     nodes_addrs=(), timeout=.1):
    # Get a reference to the event loop as we plan to use
    # low-level APIs.
    loop = asyncio.get_running_loop()
    election_timeout = round(random.uniform(.150, .300), 3)

    node = Node(addr)
    nodes = [Node(node_addr) for node_addr in nodes_addrs]

    on_con_lost = loop.create_future()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: RaftServerProtocol(
            event_queue,
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
