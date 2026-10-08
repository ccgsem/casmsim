"""Bounded, transport-neutral live observation broker.

Runners publish immutable Arrow tables here.  gRPC and Arrow Flight adapters
then read the same batches without either transport becoming a dependency of a
simulation backend.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import Enum
from threading import Condition

import pyarrow as pa


class ObservationBrokerError(RuntimeError):
    """Base class for broker failures exposed to transport adapters."""


class ObservationBackpressureError(ObservationBrokerError):
    """Raised when a publish would exceed a non-evicting channel limit."""


class ObservationCursorExpiredError(ObservationBrokerError):
    """Raised when a consumer asks to resume before retained history."""


class ObservationBrokerClosedError(ObservationBrokerError):
    """Raised when publishing after the run has reached a terminal state."""


class RetentionPolicy(str, Enum):
    """How a channel behaves when its configured capacity is exhausted."""

    EVICT_OLDEST = "evict_oldest"
    FAIL_PUBLISH = "fail_publish"


@dataclass(frozen=True)
class ObservationBrokerLimits:
    """Per-channel retention limits for immutable observation batches."""

    max_batches_per_channel: int = 128
    max_bytes_per_channel: int = 64 * 1024 * 1024
    retention_policy: RetentionPolicy = RetentionPolicy.EVICT_OLDEST

    def __post_init__(self) -> None:
        if self.max_batches_per_channel <= 0:
            raise ValueError("max_batches_per_channel must be positive")
        if self.max_bytes_per_channel <= 0:
            raise ValueError("max_bytes_per_channel must be positive")


@dataclass(frozen=True)
class ObservationBatch:
    """One immutable channel batch and its transport-independent cursor."""

    channel: str
    batch_id: int
    table: pa.Table

    @property
    def size_bytes(self) -> int:
        return int(self.table.nbytes)


@dataclass(frozen=True)
class ObservationRead:
    """A consistent retained slice for one channel.

    ``next_batch_id`` is the cursor a consumer supplies to resume after the
    returned batches.  A closed read means no later batch can arrive.
    """

    batches: tuple[ObservationBatch, ...]
    next_batch_id: int
    closed: bool


class ObservationBroker:
    """Thread-safe bounded retention for runner observation channels.

    Batch IDs are per-channel and strictly increasing.  They are intentionally
    independent of a backend's simulation tick or time.  Consumers pass the
    next desired batch ID to :meth:`read`; retained history that predates that
    cursor fails explicitly rather than returning an incomplete replay.
    """

    def __init__(self, limits: ObservationBrokerLimits | None = None) -> None:
        self._limits = limits or ObservationBrokerLimits()
        self._condition = Condition()
        self._batches: dict[str, deque[ObservationBatch]] = {}
        self._next_batch_ids: dict[str, int] = {}
        self._bytes: dict[str, int] = {}
        self._closed = False

    def publish(self, channel: str, table: pa.Table) -> ObservationBatch:
        """Publish one immutable table, subject to this channel's limits."""
        if not channel:
            raise ValueError("channel must not be empty")
        size_bytes = table.nbytes
        if size_bytes > self._limits.max_bytes_per_channel:
            raise ObservationBackpressureError("observation batch exceeds max_bytes_per_channel")
        with self._condition:
            if self._closed:
                raise ObservationBrokerClosedError("cannot publish after broker closure")
            batches = self._batches.setdefault(channel, deque())
            current_bytes = self._bytes.setdefault(channel, 0)
            if self._limits.retention_policy is RetentionPolicy.FAIL_PUBLISH and (
                len(batches) >= self._limits.max_batches_per_channel
                or current_bytes + size_bytes > self._limits.max_bytes_per_channel
            ):
                raise ObservationBackpressureError(f"observation channel {channel!r} reached its retention limit")
            while batches and (
                len(batches) >= self._limits.max_batches_per_channel
                or current_bytes + size_bytes > self._limits.max_bytes_per_channel
            ):
                current_bytes -= batches.popleft().size_bytes
            batch_id = self._next_batch_ids.get(channel, 0)
            batch = ObservationBatch(channel=channel, batch_id=batch_id, table=table)
            batches.append(batch)
            self._next_batch_ids[channel] = batch_id + 1
            self._bytes[channel] = current_bytes + size_bytes
            self._condition.notify_all()
            return batch

    def read(self, channel: str, *, start_batch_id: int = 0) -> ObservationRead:
        """Return every retained batch at or after ``start_batch_id``."""
        if start_batch_id < 0:
            raise ValueError("start_batch_id must not be negative")
        with self._condition:
            batches = self._batches.get(channel, deque())
            next_batch_id = self._next_batch_ids.get(channel, 0)
            if batches and start_batch_id < batches[0].batch_id:
                raise ObservationCursorExpiredError(
                    f"channel {channel!r} retains batches from {batches[0].batch_id}, not {start_batch_id}"
                )
            if start_batch_id > next_batch_id:
                raise ValueError(f"start_batch_id {start_batch_id} is after channel {channel!r}'s next batch")
            result = tuple(batch for batch in batches if batch.batch_id >= start_batch_id)
            return ObservationRead(batches=result, next_batch_id=next_batch_id, closed=self._closed)

    def subscribe(
        self,
        channel: str,
        *,
        start_batch_id: int = 0,
        poll_timeout: float = 0.5,
        is_active: Callable[[], bool] | None = None,
    ) -> Iterator[ObservationBatch]:
        """Yield batches at or after ``start_batch_id`` as they are published.

        Yields retained batches in order, then blocks until :meth:`publish` or
        :meth:`close` wakes it. The iterator ends once the broker is closed and
        every remaining batch has been yielded. Long-lived transports (gRPC
        server streaming) use this instead of polling :meth:`read`.

        ``poll_timeout`` bounds each wait so ``is_active`` is re-checked
        regularly; when it returns ``False`` (for example, the gRPC client
        disconnected) the iterator stops without waiting for closure.

        Raises :class:`ObservationCursorExpiredError` if the cursor falls behind
        retained history, either at the start or because eviction overtook a
        slow consumer, and :class:`ValueError` if ``start_batch_id`` is ahead of
        the channel's next batch (the same contract as :meth:`read`). The lock
        is never held while a batch is yielded.
        """
        if start_batch_id < 0:
            raise ValueError("start_batch_id must not be negative")
        if poll_timeout <= 0:
            raise ValueError("poll_timeout must be positive")
        with self._condition:
            next_batch_id = self._next_batch_ids.get(channel, 0)
        if start_batch_id > next_batch_id:
            raise ValueError(f"start_batch_id {start_batch_id} is after channel {channel!r}'s next batch")
        cursor = start_batch_id
        while True:
            if is_active is not None and not is_active():
                return
            with self._condition:
                batches = self._batches.get(channel, deque())
                if batches and cursor < batches[0].batch_id:
                    raise ObservationCursorExpiredError(
                        f"channel {channel!r} retains batches from {batches[0].batch_id}, not {cursor}"
                    )
                pending = tuple(batch for batch in batches if batch.batch_id >= cursor)
                closed = self._closed
                if not pending and not closed:
                    self._condition.wait(timeout=poll_timeout)
                    continue
            yield from pending
            if pending:
                cursor = pending[-1].batch_id + 1
            elif closed:
                return

    def close(self) -> None:
        """Mark the run terminal and wake transports waiting for new batches."""
        with self._condition:
            self._closed = True
            self._condition.notify_all()

    def channels(self) -> tuple[str, ...]:
        """Return the currently known observation-channel names."""
        with self._condition:
            return tuple(self._batches)

    @property
    def closed(self) -> bool:
        with self._condition:
            return self._closed
