import enum


@enum.unique
class State(enum.IntEnum):
    LEADER = 0
    FOLLOWER = 1
    CANDIDATE = 2


class RaftMachine:

    def __init__(self):
        self._state = State.FOLLOWER  # starting state is always FOLLOWER
        self._term = 0
        self._log = []
        self._vote = None

    @property
    def state(self):
        return self._state

    @property
    def term(self):
        return self._term

    @property
    def vote(self):
        return self._vote

    @vote.setter
    def vote(self, node_name):
        self._vote = node_name

    def become_leader(self):
        self._state = State.LEADER

    def become_follower(self):
        self._state = State.FOLLOWER

    def become_candidate(self):
        self._state = State.CANDIDATE

    def append_entries(self, entries):
        # TODO
        pass
