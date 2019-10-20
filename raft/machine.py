import enum


class State(enum.IntEnum):
    LEADER
    FOLLOWER
    CANDIDATE


class RaftMachine:

    def __init__(self, net):
        self._state = FOLLOWER  # starting state is always FOLLOWER
        self._term = 0
        self._log = []
        sel._net = net

    @property
    def state(self):
        return self._state

    @property
    def term(self):
        return self._term

    def become_leader(self):
        self._state = LEADER

    def become_follower(self):
        self._state = FOLLOWER

    def become_candidate(self):
        self._state = CANDIDATE

    def append_entries(self, entries):
        # TODO
        pass
