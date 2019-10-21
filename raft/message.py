import enum
import json
import dataclasses


@enum.unique
class MessageType(enum.IntEnum):
    REQUEST_VOTE = 0
    APPEND_ENTRIES = 1
    RESPONSE_VOTE = 3


@dataclasses.dataclass
class Message:

    @classmethod
    def from_json(cls, json_dict):
        return cls(**json.loads(json_dict)['type'])

    @classmethod
    def from_dict(cls, dic):
        return cls(**dic)


@dataclasses.dataclass
class RequestVote(Message):

    def to_json(self):
        return json.dumps({
            'type': MessageType.REQUEST_VOTE,
            'payload': {}
        })


@dataclasses.dataclass
class ResponseVote(Message):

    def to_json(self):
        return json.dumps({
            'type': MessageType.RESPONSE_VOTE,
            'payload': {}
        })


@dataclasses.dataclass
class AppendEntries(Message):

    term: int
    index: int
    command: dict

    def to_json(self):
        return json.dumps({
            'type': MessageType.APPEND_ENTRIES,
            'payload': {
                'term': self.term,
                'index': self.index,
                'command': self.command
            }
        })
