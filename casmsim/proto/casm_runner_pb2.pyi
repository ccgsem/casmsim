from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RunState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUN_STATE_UNSPECIFIED: _ClassVar[RunState]
    RUN_STATE_INITIALIZING: _ClassVar[RunState]
    RUN_STATE_RUNNING: _ClassVar[RunState]
    RUN_STATE_COMPLETED: _ClassVar[RunState]
    RUN_STATE_FAILED: _ClassVar[RunState]
    RUN_STATE_CANCELLED: _ClassVar[RunState]
RUN_STATE_UNSPECIFIED: RunState
RUN_STATE_INITIALIZING: RunState
RUN_STATE_RUNNING: RunState
RUN_STATE_COMPLETED: RunState
RUN_STATE_FAILED: RunState
RUN_STATE_CANCELLED: RunState

class StartRequest(_message.Message):
    __slots__ = ("run_id", "config_json", "artifact_root")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    CONFIG_JSON_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_ROOT_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    config_json: bytes
    artifact_root: str
    def __init__(self, run_id: _Optional[str] = ..., config_json: _Optional[bytes] = ..., artifact_root: _Optional[str] = ...) -> None: ...

class StartResponse(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class CancelRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class CancelResponse(_message.Message):
    __slots__ = ("acknowledged",)
    ACKNOWLEDGED_FIELD_NUMBER: _ClassVar[int]
    acknowledged: bool
    def __init__(self, acknowledged: _Optional[bool] = ...) -> None: ...

class GetStateRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class StateResponse(_message.Message):
    __slots__ = ("run_id", "state", "status_message", "sim_time", "tick")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    STATUS_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    SIM_TIME_FIELD_NUMBER: _ClassVar[int]
    TICK_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    state: RunState
    status_message: str
    sim_time: float
    tick: int
    def __init__(self, run_id: _Optional[str] = ..., state: _Optional[_Union[RunState, str]] = ..., status_message: _Optional[str] = ..., sim_time: _Optional[float] = ..., tick: _Optional[int] = ...) -> None: ...

class StreamObsRequest(_message.Message):
    __slots__ = ("run_id", "channel", "start_tick")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    START_TICK_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    channel: str
    start_tick: int
    def __init__(self, run_id: _Optional[str] = ..., channel: _Optional[str] = ..., start_tick: _Optional[int] = ...) -> None: ...

class ObsBatch(_message.Message):
    __slots__ = ("channel", "tick", "arrow_ipc")
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    TICK_FIELD_NUMBER: _ClassVar[int]
    ARROW_IPC_FIELD_NUMBER: _ClassVar[int]
    channel: str
    tick: int
    arrow_ipc: bytes
    def __init__(self, channel: _Optional[str] = ..., tick: _Optional[int] = ..., arrow_ipc: _Optional[bytes] = ...) -> None: ...
