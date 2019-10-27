import enum
import collections


LogEntry = collections.namedtuple('LogEntry', ('term', 'command'))


@enum.unique
class State(enum.IntEnum):
    LEADER = 0
    FOLLOWER = 1
    CANDIDATE = 2


class RaftMachine:

    def __init__(self, node_id):
        self._state = State.FOLLOWER  # starting state is always FOLLOWER
        self._node_id = node_id
        # Persistent state for all nodes
        self._term = 0
        self._log = []
        self._voted_for = None
        # Volatile state
        self._commit_index = 0
        # Volatile state for leader
        self._next_index = 0
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

    def become_follower(self):
        self._state = State.FOLLOWER

    def become_candidate(self):
        self._state = State.CANDIDATE
        self._term += 1

    def append_entries(self, index, entries):
        # TODO
        self._pending_entries[index] = LogEntry(self.term, entries)

    def commit(self, index):
        if index in self._pending_entries:
            self._log.append(self._pending_entries.pop(index))
            self._next_index += 1
            self._commit_index = index

    def last_log_term(self):
        return self._log[-1].term if self._log else 0
