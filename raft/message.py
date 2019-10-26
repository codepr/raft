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
        return cls(**json.loads(json_dict)['payload'])

    @classmethod
    def from_dict(cls, dic):
        return cls(**dic)


@dataclasses.dataclass
class RequestVote(Message):

    term: int
    last_log_term: int
    last_log_index: int
    candidate_id: str

    def to_json(self):
        return json.dumps({
            'type': MessageType.REQUEST_VOTE,
            'payload': {
                'term': self.term,
                'last_log_term': self.last_log_term,
                'last_log_index': self.last_log_index,
                'candidate_id': self.candidate_id
            }
        })


@dataclasses.dataclass
class RequestVoteResponse(Message):

    vote_granted: bool = False

    def to_json(self):
        return json.dumps({
            'type': MessageType.REQUEST_VOTE_RESPONSE,
            'payload': {
                'vote_granted': self.vote_granted
            }
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

    term: int
    index: int
    success: bool = False

    def to_json(self):
        return json.dumps({
            'type': MessageType.APPEND_ENTRIES_RESPONSE,
            'payload': {
                'term': self.term,
                'index': self.index,
                'success': self.success
            }
        })


deser_map = {
    MessageType.REQUEST_VOTE: RequestVote.from_dict,
    MessageType.REQUEST_VOTE_RESPONSE: RequestVoteResponse.from_dict,
    MessageType.APPEND_ENTRIES: AppendEntries.from_dict,
    MessageType.APPEND_ENTRIES_RESPONSE: AppendEntriesResponse.from_dict
}


def deserialize(raw_message):
    try:
        msg_dict = json.loads(raw_message)
        msg = deser_map[msg_dict['type']](msg_dict['payload'])
    except (json.JSONDecodeError, KeyError) as e:
        raise UnknownMessageException(e)
    else:
        return msg
