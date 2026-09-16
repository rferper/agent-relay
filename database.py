"""Database setup and durable Agent Relay models.

PostgreSQL is the default backend; SQLite remains supported so the suite can
run without a database server.  This module is the only place that knows which
dialect is in use: it owns the engine, the connection pragmas, and the writer
transaction that :mod:`storage` opens before claiming or finishing work.

The two dialects reach the same guarantee by different means.  PostgreSQL
selects claimable rows with ``FOR UPDATE SKIP LOCKED`` inside an ordinary
transaction, so concurrent workers skip locked rows instead of blocking.
SQLite has no such clause, so it serializes writers with ``BEGIN IMMEDIATE``.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, create_engine, event, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker


# Matches the service name and credentials in compose.yaml: inside the Compose
# network the API reaches the database at the service hostname ``postgres``.
DEFAULT_DATABASE_URL = "postgresql+psycopg://relay:relay@postgres:5432/relay"


def _database_url() -> str:
    return os.getenv("RELAY_DATABASE_URL") or os.getenv("DATABASE_URL") or DEFAULT_DATABASE_URL


def positive_int(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        return default
    return value if value > 0 else default


DATABASE_URL = _database_url()
IS_SQLITE = DATABASE_URL.startswith("sqlite")
IS_POSTGRES = not IS_SQLITE
LEASE_SECONDS = positive_int("RELAY_LEASE_SECONDS", 60)
MAX_ATTEMPTS = positive_int("RELAY_MAX_ATTEMPTS", 5)
RECOVERY_INTERVAL_SECONDS = max(1, positive_int("RELAY_RECOVERY_INTERVAL_SECONDS", 5))
MAX_BODY_BYTES = positive_int("RELAY_MAX_BODY_BYTES", 256 * 1024)
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 100


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def as_db_time(value: datetime) -> datetime:
    """SQLite's DateTime implementation is most portable with naive UTC."""

    return value.astimezone(timezone.utc).replace(tzinfo=None)


def db_time(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def iso_time(value: datetime | None) -> str | None:
    value = db_time(value)
    if value is None:
        return None
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


class Base(DeclarativeBase):
    pass


class Agent(Base):
    __tablename__ = "agents"

    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    sent_tasks: Mapped[list[Task]] = relationship(
        "Task", foreign_keys="Task.sender_id", back_populates="sender", passive_deletes=True
    )
    received_tasks: Mapped[list[Task]] = relationship(
        "Task", foreign_keys="Task.recipient_id", back_populates="recipient", passive_deletes=True
    )


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (UniqueConstraint("sender_id", "idempotency_key", name="uq_task_sender_idempotency"),)

    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    sender_id: Mapped[str] = mapped_column(String(100), ForeignKey("agents.id"), nullable=False, index=True)
    recipient_id: Mapped[str] = mapped_column(String(100), ForeignKey("agents.id"), nullable=False, index=True)
    input: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    output: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    sender: Mapped[Agent] = relationship("Agent", foreign_keys=[sender_id], back_populates="sent_tasks")
    recipient: Mapped[Agent] = relationship("Agent", foreign_keys=[recipient_id], back_populates="received_tasks")
    attempts: Mapped[list[Attempt]] = relationship(
        "Attempt", back_populates="task", cascade="all, delete-orphan", order_by="Attempt.attempt_number"
    )


class Attempt(Base):
    __tablename__ = "attempts"
    __table_args__ = (UniqueConstraint("task_id", "attempt_number", name="uq_attempt_task_number"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(
        String(100), ForeignKey("tasks.id", ondelete="CASCADE"), nullable=False, index=True
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    claim_token_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    claimed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    lease_expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    outcome: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    terminal_action: Mapped[str | None] = mapped_column(String(10), nullable=True)
    terminal_payload_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)

    task: Mapped[Task] = relationship("Task", back_populates="attempts")


def _is_sqlite(url: str) -> bool:
    return url.startswith("sqlite")


engine_kwargs: dict[str, Any] = {"future": True, "pool_pre_ping": True}
if _is_sqlite(DATABASE_URL):
    engine_kwargs.update({"connect_args": {"check_same_thread": False, "timeout": 30}})
    if DATABASE_URL in {"sqlite://", "sqlite:///:memory:"}:
        from sqlalchemy.pool import StaticPool

        engine_kwargs["poolclass"] = StaticPool
else:
    # Long-polling claims hold a connection for up to 30 seconds, so the pool
    # needs room for more concurrent workers than the default five.
    engine_kwargs.update({"pool_size": 10, "max_overflow": 20, "pool_recycle": 1800})

engine: Engine = create_engine(DATABASE_URL, **engine_kwargs)

if _is_sqlite(DATABASE_URL):

    @event.listens_for(engine, "connect")
    def _sqlite_pragmas(dbapi_connection: Any, _connection_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()


SessionLocal = sessionmaker(bind=engine, class_=Session, expire_on_commit=False, autoflush=True)


def init_db() -> None:
    Base.metadata.create_all(engine)


@contextmanager
def db_session() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@contextmanager
def writer_transaction() -> Generator[Session, None, None]:
    """Open one transaction before selecting or changing work.

    On PostgreSQL this is an ordinary transaction: exclusivity comes from the
    ``FOR UPDATE SKIP LOCKED`` row locks that :mod:`storage` takes inside it,
    so two workers claiming at once lock different rows and neither waits.

    On SQLite there is no such clause, so the transaction opens with
    ``BEGIN IMMEDIATE`` to reserve the single writer slot up front.  That
    serializes claims across processes at the cost of concurrency, which is
    why PostgreSQL is the default.
    """

    connection = engine.connect()
    session = Session(bind=connection, expire_on_commit=False, autoflush=True)
    try:
        if IS_SQLITE:
            connection.exec_driver_sql("BEGIN IMMEDIATE")
        yield session
        session.flush()
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        session.close()
        connection.close()


# The pre-PostgreSQL name for the same seam.
immediate_transaction = writer_transaction


def recover_expired_in_session(db: Session, now: datetime) -> int:
    """Expire active leases and requeue/fail their tasks within ``db``."""

    now_db = as_db_time(now)
    expired_query = (
        select(Attempt)
        .where(Attempt.outcome == "processing", Attempt.lease_expires_at <= now_db)
        .order_by(Attempt.lease_expires_at, Attempt.id)
    )
    if IS_POSTGRES:
        # Several API replicas run this loop. Skipping locked rows lets one
        # replica recover an attempt while another moves on to the next.
        expired_query = expired_query.with_for_update(skip_locked=True)
    expired = list(db.scalars(expired_query))
    count = 0
    for attempt in expired:
        task = db.get(Task, attempt.task_id)
        if task is None or attempt.outcome != "processing":
            continue
        attempt.outcome = "expired"
        attempt.finished_at = now_db
        if task.status == "processing":
            if task.attempt_count >= MAX_ATTEMPTS:
                task.status = "failed"
                task.error = "attempts_exhausted"
                task.output = None
                task.finished_at = now_db
            else:
                task.status = "queued"
                task.finished_at = None
        count += 1
    return count


def recover_expired() -> int:
    """Run one recovery pass and return the number of expired attempts."""

    with immediate_transaction() as db:
        return recover_expired_in_session(db, utcnow())


__all__ = [
    "Agent",
    "Attempt",
    "Base",
    "DATABASE_URL",
    "DEFAULT_PAGE_SIZE",
    "LEASE_SECONDS",
    "MAX_ATTEMPTS",
    "MAX_BODY_BYTES",
    "MAX_PAGE_SIZE",
    "RECOVERY_INTERVAL_SECONDS",
    "Task",
    "as_db_time",
    "db_session",
    "db_time",
    "IS_POSTGRES",
    "IS_SQLITE",
    "engine",
    "immediate_transaction",
    "init_db",
    "iso_time",
    "recover_expired",
    "recover_expired_in_session",
    "utcnow",
    "writer_transaction",
]
