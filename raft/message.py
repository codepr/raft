import enum
import json
import dataclasses


class UnknownMessageException(Exception):
    pass


@enum.unique
class MessageType(enum.IntEnum):
    REQUEST_VOTE = 0
    APPEND_ENTRIES = 1
    REQUEST_VOTE_RESPONSE = 3
    APPEND_ENTRIES_RESPONSE = 4


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
            'type': MessageType.REQUEST_VOTE_RESPONSE,
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


@dataclasses.dataclass
class AppendEntriesResponse(Message):

    def to_json(self):
        return json.dumps({
            'type': MessageType.APPEND_ENTRIES_RESPONSE,
            'payload': {}
        })


deser_map = {
    MessageType.REQUEST_VOTE: RequestVote.from_dict,
    MessageType.REQUEST_VOTE_RESPONSE: ResponseVote.from_dict,
    MessageType.APPEND_ENTRIES: AppendEntries.from_dict,
    MessageType.APPEND_ENTRIES_RESPONSE: AppendEntries.from_dict
}


def deserialize(raw_message):
    try:
        msg_dict = json.loads(raw_message)
        msg = deser_map[msg_dict['type']](msg_dict['payload'])
    except (json.JSONDecodeError, KeyError) as e:
        raise UnknownMessageException(e)
    else:
        return msg
