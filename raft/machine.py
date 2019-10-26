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
        self._term = 0
        self._log = []
        self._vote = None
        self._next_index = 0
        self._pending_entries = dict()

    @property
    def state(self):
        return self._state

    @property
    def term(self):
        return self._term

    @term.setter
    def term(self, term):
        self._term = term

    @property
    def vote(self):
        return self._vote

    @vote.setter
    def vote(self, node_name):
        self._vote = node_name

    @property
    def next_index(self):
        return self._next_index

    @next_index.setter
    def next_index(self, index):
        self._next_index = index

    def become_leader(self):
        self._state = State.LEADER

    def become_follower(self):
        self._state = State.FOLLOWER

    def become_candidate(self):
        self._state = State.CANDIDATE
        self.term += 1

    def append_entries(self, index, entries):
        # TODO
        self._pending_entries[index] = LogEntry(self.term, entries)

    def commit(self, index):
        if index in self._pending_entries:
            self._log.append(self._pending_entries.pop(index))
            self.next_index += 1
