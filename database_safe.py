"""
Safe database operations with proper transaction handling and error recovery.

Key improvements:
- Context managers for session handling
- Transaction rollback on errors  
- WAL mode for SQLite
- Parameterized queries
- Retry logic for critical operations
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Generator, Optional, NamedTuple

from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

# Configuration
from config import DB_PATH

Base = declarative_base()


# ---------------------------------------------------------------------------------
# Data structures for safe data transfer
# ---------------------------------------------------------------------------------

class EventData(NamedTuple):
    """Safe data structure for event information."""
    user_id: int
    event_type: str
    timestamp: datetime
    details: Optional[str] = None


class MoodRequestData(NamedTuple):
    """Safe data structure for mood request information."""
    user_id: int
    request_time: datetime
    response_time: Optional[datetime] = None
    status: str = "pending"


# ---------------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------------

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, unique=True, nullable=False)
    timezone = Column(String, nullable=True)


class Log(Base):
    __tablename__ = "logs"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    event_type = Column(String, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    details = Column(String, nullable=True)


class MoodRequest(Base):
    __tablename__ = "mood_requests"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    request_time = Column(DateTime, nullable=False)
    response_time = Column(DateTime, nullable=True)
    status = Column(String, nullable=False, default="pending")


# ---------------------------------------------------------------------------------
# Database setup with WAL mode for concurrent access
# ---------------------------------------------------------------------------------

engine = create_engine(
    f"sqlite:///{DB_PATH}",
    pool_pre_ping=True,  # Проверяем соединение перед использованием
    echo=False,  # Set to True for SQL debugging
)


@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """Enable WAL mode and optimize SQLite for concurrent access."""
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA temp_store=memory")
    cursor.execute("PRAGMA mmap_size=268435456")  # 256MB
    cursor.close()


SessionLocal = sessionmaker(bind=engine)
Base.metadata.create_all(engine)


# ---------------------------------------------------------------------------------
# Safe session context manager
# ---------------------------------------------------------------------------------

@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """Context manager for safe database operations with automatic rollback."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        logger.error(f"Database operation failed: {e}")
        session.rollback()
        raise
    finally:
        session.close()


def with_retry(max_attempts: int = 3, delay: float = 0.1):
    """Decorator for retrying database operations on transient failures."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except SQLAlchemyError as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        logger.warning(f"DB operation failed (attempt {attempt + 1}), retrying: {e}")
                        time.sleep(delay * (2 ** attempt))  # Exponential backoff
                    else:
                        logger.error(f"DB operation failed after {max_attempts} attempts: {e}")
            raise last_exception
        return wrapper
    return decorator


# ---------------------------------------------------------------------------------
# Safe CRUD operations
# ---------------------------------------------------------------------------------

@with_retry()
def save_user(user_id: int, timezone: Optional[str] = None) -> bool:
    """Save or update user with timezone. Returns True if successful."""
    try:
        with get_db_session() as session:
            user = session.query(User).filter_by(user_id=user_id).first()
            if not user:
                user = User(user_id=user_id, timezone=timezone)
                session.add(user)
                logger.info(f"Created new user {user_id}")
            else:
                user.timezone = timezone
                logger.info(f"Updated timezone for user {user_id}")
            return True
    except Exception as e:
        logger.error(f"Failed to save user {user_id}: {e}")
        return False


@with_retry()
def save_log(user_id: int, event_type: str, timestamp: datetime, details: Optional[str] = None) -> bool:
    """Save log entry. Returns True if successful."""
    try:
        with get_db_session() as session:
            log = Log(
                user_id=user_id,
                event_type=event_type,
                timestamp=timestamp,
                details=details
            )
            session.add(log)
            logger.debug(f"Saved log: user={user_id}, event={event_type}")
            return True
    except Exception as e:
        logger.error(f"Failed to save log for user {user_id}: {e}")
        return False


@with_retry()
def save_mood_request(user_id: int, request_time: datetime) -> bool:
    """Save new mood request. Returns True if successful."""
    try:
        with get_db_session() as session:
            mood_request = MoodRequest(
                user_id=user_id,
                request_time=request_time,
                status="pending"
            )
            session.add(mood_request)
            logger.debug(f"Saved mood request for user {user_id}")
            return True
    except Exception as e:
        logger.error(f"Failed to save mood request for user {user_id}: {e}")
        return False


@with_retry()
def update_mood_request(user_id: int, response_time: datetime) -> bool:
    """Update latest pending mood request. Returns True if successful."""
    try:
        with get_db_session() as session:
            request = (
                session.query(MoodRequest)
                .filter_by(user_id=user_id, status="pending")
                .order_by(MoodRequest.request_time.desc())
                .first()
            )
            if request:
                request.response_time = response_time
                request.status = "answered"
                logger.debug(f"Updated mood request for user {user_id}")
                return True
            else:
                logger.warning(f"No pending mood request found for user {user_id}")
                return False
    except Exception as e:
        logger.error(f"Failed to update mood request for user {user_id}: {e}")
        return False


@with_retry()
def mark_request_as_unanswered(user_id: int, request_time: datetime) -> bool:
    """Mark specific mood request as unanswered. Returns True if successful."""
    try:
        with get_db_session() as session:
            request = (
                session.query(MoodRequest)
                .filter_by(user_id=user_id, request_time=request_time, status="pending")
                .first()
            )
            if request:
                request.status = "not_answered"
                logger.debug(f"Marked request as unanswered for user {user_id}")
                return True
            else:
                logger.warning(f"No matching pending request found for user {user_id}")
                return False
    except Exception as e:
        logger.error(f"Failed to mark request as unanswered for user {user_id}: {e}")
        return False


# ---------------------------------------------------------------------------------
# Atomic compound operations
# ---------------------------------------------------------------------------------

@with_retry()
def save_activity_and_create_mood_request(
    user_id: int, 
    activity: str, 
    timestamp: datetime
) -> tuple[bool, bool]:
    """
    Atomically save activity log and create mood request.
    Returns (activity_saved, mood_request_created).
    """
    try:
        with get_db_session() as session:
            # Save activity log
            activity_log = Log(
                user_id=user_id,
                event_type="answer_activity",
                timestamp=timestamp,
                details=activity
            )
            session.add(activity_log)
            
            # Create mood request
            mood_request = MoodRequest(
                user_id=user_id,
                request_time=timestamp,
                status="pending"
            )
            session.add(mood_request)
            
            logger.info(f"Atomically saved activity and mood request for user {user_id}")
            return True, True
            
    except Exception as e:
        logger.error(f"Failed atomic operation for user {user_id}: {e}")
        return False, False


@with_retry()
def save_emotion_and_update_request(
    user_id: int,
    emotion: str,
    timestamp: datetime
) -> tuple[bool, bool]:
    """
    Atomically save emotion log and update mood request.
    Returns (emotion_saved, request_updated).
    """
    try:
        with get_db_session() as session:
            # Save emotion log
            emotion_log = Log(
                user_id=user_id,
                event_type="answer_emotional",
                timestamp=timestamp,
                details=emotion
            )
            session.add(emotion_log)
            
            # Update mood request
            request = (
                session.query(MoodRequest)
                .filter_by(user_id=user_id, status="pending")
                .order_by(MoodRequest.request_time.desc())
                .first()
            )
            
            request_updated = False
            if request:
                request.response_time = timestamp
                request.status = "answered"
                request_updated = True
            
            logger.info(f"Atomically saved emotion and updated request for user {user_id}")
            return True, request_updated
            
    except Exception as e:
        logger.error(f"Failed atomic emotion operation for user {user_id}: {e}")
        return False, False


# ---------------------------------------------------------------------------------
# Safe read operations
# ---------------------------------------------------------------------------------

def get_user(user_id: int) -> Optional[User]:
    """Get user by ID. Returns None if not found."""
    try:
        with get_db_session() as session:
            return session.query(User).filter_by(user_id=user_id).first()
    except Exception as e:
        logger.error(f"Failed to get user {user_id}: {e}")
        return None


def get_last_event(user_id: int) -> Optional[EventData]:
    """Get last log event for user. Returns None if not found."""
    try:
        with get_db_session() as session:
            log = (
                session.query(Log)
                .filter_by(user_id=user_id)
                .order_by(Log.timestamp.desc())
                .first()
            )
            if log:
                return EventData(
                    user_id=log.user_id,
                    event_type=log.event_type,
                    timestamp=log.timestamp,
                    details=log.details
                )
            return None
    except Exception as e:
        logger.error(f"Failed to get last event for user {user_id}: {e}")
        return None


def get_pending_requests() -> list[MoodRequestData]:
    """Get all pending mood requests. Returns empty list on error."""
    try:
        with get_db_session() as session:
            requests = session.query(MoodRequest).filter_by(status="pending").all()
            return [
                MoodRequestData(
                    user_id=req.user_id,
                    request_time=req.request_time,
                    response_time=req.response_time,
                    status=req.status
                )
                for req in requests
            ]
    except Exception as e:
        logger.error(f"Failed to get pending requests: {e}")
        return []


def get_user_activities(user_id: int) -> list[dict]:
    """Get all activities for user. Returns empty list on error."""
    try:
        with get_db_session() as session:
            activities = (
                session.query(Log)
                .filter_by(user_id=user_id, event_type="answer_activity")
                .order_by(Log.timestamp.desc())
                .all()
            )
            return [
                {
                    "user_id": activity.user_id,
                    "activity": activity.details,
                    "timestamp": activity.timestamp
                }
                for activity in activities
            ]
    except Exception as e:
        logger.error(f"Failed to get activities for user {user_id}: {e}")
        return [] 