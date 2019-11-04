import enum
import collections


LogEntry = collections.namedtuple('LogEntry', ('term', 'command'))


@enum.unique
class State(enum.IntEnum):
    LEADER = 0
    FOLLOWER = 1
    CANDIDATE = 2


class RaftMachine:

    def __init__(self, node_id, nodes):
        self._state = State.FOLLOWER  # starting state is always FOLLOWER
        self._nodes = nodes
        self._node_id = node_id
        # Persistent state for all nodes
        self._term = 0
        self._log = []
        self._voted_for = None
        # Volatile state
        self._commit_index = 0
        # Volatile state for leader
        self._next_index = dict()
        self._match_index = None
        self._pending_entries = dict()

    @property
    def state(self):
        return self._state

    @property
    def term(self):
        return self._term

    @property
    def voted_for(self):
        return self._voted_for

    @voted_for.setter
    def voted_for(self, node_name):
        self._voted_for = node_name

    @property
    def next_index(self):
        return self._next_index

    @property
    def commit_index(self):
        return self._commit_index

    def become_leader(self):
        self._state = State.LEADER
        last_index = len(self._log)
        self._next_index = {node.name: last_index for node in self._nodes}

    def become_follower(self):
        self._state = State.FOLLOWER

    def become_candidate(self):
        self._state = State.CANDIDATE
        self._term += 1

    def append_entries(self, index, entries):
        # TODO
        if self.state == State.FOLLOWER:
            index += 1
        self._pending_entries[index] = LogEntry(self.term, entries)

    def commit(self, addr, index):
        if self.state == State.LEADER:
            self._next_index[addr] = index + 1
        if index in self._pending_entries:
            self._log.append(self._pending_entries.pop(index))
            self._commit_index = index

    def last_log_index(self, addr=None):
        return len(self._log) \
            if (not addr or addr not in self._next_index) \
            else self._next_index[addr]

    def last_log_term(self):
        return self._log[-1].term if self._log else 0
