from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

# Укажите путь к базе данных
DB_PATH = "/MoodTrackerBot_data/mood_tracker.db"

# Убедитесь, что папка существует
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

Base = declarative_base()
engine = create_engine(f"sqlite:///{DB_PATH}")
Session = sessionmaker(bind=engine)
session = Session()

# Модель пользователей
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, unique=True, nullable=False)
    timezone = Column(String, nullable=True)

# Модель логов
class Log(Base):
    __tablename__ = "logs"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    event_type = Column(String, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    details = Column(String, nullable=True)

# Модель для запросов настроения
class MoodRequest(Base):
    __tablename__ = "mood_requests"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    request_time = Column(DateTime, nullable=False)
    response_time = Column(DateTime, nullable=True)
    status = Column(String, nullable=False, default="pending")

Base.metadata.create_all(engine)

def save_user(user_id, timezone=None):
    user = session.query(User).filter_by(user_id=user_id).first()
    if not user:
        user = User(user_id=user_id, timezone=timezone)
        session.add(user)
    else:
        user.timezone = timezone
    session.commit()

def get_user(user_id):
    return session.query(User).filter_by(user_id=user_id).first()

def save_log(user_id, event_type, timestamp, details=None):
    log = Log(user_id=user_id, event_type=event_type, timestamp=timestamp, details=details)
    session.add(log)
    session.commit()

def get_last_event(user_id):
    return session.query(Log).filter_by(user_id=user_id).order_by(Log.timestamp.desc()).first()

def save_mood_request(user_id, request_time):
    """Сохраняет новый запрос настроения."""
    mood_request = MoodRequest(user_id=user_id, request_time=request_time, status="pending")
    session.add(mood_request)
    session.commit()

def update_mood_request(user_id, response_time):
    """Обновляет последний активный (pending) запрос настроения, помечая его как отвеченный."""
    request = (
        session.query(MoodRequest)
        .filter_by(user_id=user_id, status="pending")
        .order_by(MoodRequest.request_time.desc())
        .first()
    )
    if request:
        request.response_time = response_time
        request.status = "answered"
        session.commit()

def mark_request_as_unanswered(user_id, request_time):
    """Помечает запрос настроения как неотвеченный."""
    request = (
        session.query(MoodRequest)
        .filter_by(user_id=user_id, request_time=request_time, status="pending")
        .first()
    )
    if request:
        request.status = "not_answered"
        session.commit()
