"""Request/response schemas and shared time helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator, model_validator

#: Maximum number of items (snapshots + log segments) accepted in one batch.
MAX_BATCH_SIZE = 100

#: Cleanup limits: distinct record ids and protected query moments per request.
MAX_CLEANUP_IDS = 100
MIN_CLEANUP_IDS = 1
MAX_PROTECTED_TIMES = 8
MIN_PROTECTED_TIMES = 1


def to_utc(value: datetime) -> datetime:
    """Normalize a datetime to aware UTC. Naive input is assumed to be UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class SnapshotIn(BaseModel):
    """A database snapshot as registered in a batch."""

    model_config = ConfigDict(extra="forbid")

    id: StrictStr = Field(min_length=1, max_length=128)
    created_at: datetime
    applied_seq: StrictInt = Field(ge=0)
    format: StrictStr = Field(min_length=1, max_length=64)

    @field_validator("created_at")
    @classmethod
    def _normalize_created_at(cls, value: datetime) -> datetime:
        return to_utc(value)


class LogSegmentIn(BaseModel):
    """A log segment covering the closed interval [first_seq, last_seq]."""

    model_config = ConfigDict(extra="forbid")

    id: StrictStr = Field(min_length=1, max_length=128)
    created_at: datetime
    format: StrictStr = Field(min_length=1, max_length=64)
    first_seq: StrictInt = Field(ge=1)
    last_seq: StrictInt = Field(ge=1)

    @field_validator("created_at")
    @classmethod
    def _normalize_created_at(cls, value: datetime) -> datetime:
        return to_utc(value)

    @model_validator(mode="after")
    def _check_interval(self) -> "LogSegmentIn":
        if self.last_seq < self.first_seq:
            raise ValueError(
                f"log segment {self.id!r}: last_seq ({self.last_seq}) "
                f"must be >= first_seq ({self.first_seq})"
            )
        return self


class BatchIn(BaseModel):
    """One registration batch. Either everything is stored or nothing is."""

    model_config = ConfigDict(extra="forbid")

    snapshots: List[SnapshotIn] = Field(default_factory=list)
    log_segments: List[LogSegmentIn] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_batch(self) -> "BatchIn":
        total = len(self.snapshots) + len(self.log_segments)
        if total > MAX_BATCH_SIZE:
            raise ValueError(
                f"batch holds {total} items, at most {MAX_BATCH_SIZE} allowed; batch rejected"
            )
        ids = [s.id for s in self.snapshots] + [s.id for s in self.log_segments]
        duplicates = sorted({i for i in ids if ids.count(i) > 1})
        if duplicates:
            raise ValueError(f"duplicate id(s) within batch: {duplicates}; batch rejected")
        return self


class CleanupIn(BaseModel):
    """One batch-cleanup request.

    ``ids`` lists registered snapshots/log segments to delete; ``protected_at``
    lists UTC moments whose best recovery endpoint must not decrease. Either
    every listed id is deleted or none is.
    """

    model_config = ConfigDict(extra="forbid")

    ids: List[StrictStr] = Field(
        min_length=MIN_CLEANUP_IDS, max_length=MAX_CLEANUP_IDS
    )
    protected_at: List[datetime] = Field(
        min_length=MIN_PROTECTED_TIMES, max_length=MAX_PROTECTED_TIMES
    )

    @field_validator("ids")
    @classmethod
    def _check_nonempty_ids(cls, value: List[str]) -> List[str]:
        empty = [i for i, v in enumerate(value) if not v]
        if empty:
            raise ValueError(f"ids[{empty[0]}] must be a non-empty string")
        return value

    @field_validator("protected_at")
    @classmethod
    def _normalize_protected_at(cls, value: List[datetime]) -> List[datetime]:
        return [to_utc(v) for v in value]

    @model_validator(mode="after")
    def _check_distinct_ids(self) -> "CleanupIn":
        duplicates = sorted({i for i in self.ids if self.ids.count(i) > 1})
        if duplicates:
            raise ValueError(f"duplicate id(s) in request: {duplicates}; cleanup rejected")
        return self
