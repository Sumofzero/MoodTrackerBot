from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import datetime

# Настройка базы данных
Base = declarative_base()
engine = create_engine("sqlite:///mood_tracker.db")  # База данных mood_tracker.db
Session = sessionmaker(bind=engine)
session = Session()

# ------------------------- Модели базы данных -------------------------

# Модель для пользователей
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)  # Уникальный идентификатор записи
    user_id = Column(Integer, unique=True, nullable=False)  # Telegram ID пользователя
    timezone = Column(String, nullable=True)  # Таймзона пользователя

# Модель для логов
class Log(Base):
    __tablename__ = "logs"

    id = Column(Integer, primary_key=True)  # Уникальный идентификатор записи
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)  # Telegram ID пользователя
    event_type = Column(String, nullable=False)  # Тип события: response, answer, notification
    timestamp = Column(DateTime, nullable=False)  # Время события
    details = Column(String, nullable=True)  # Дополнительные сведения (например, настроение)

# Модель для запросов настроения
class MoodRequest(Base):
    __tablename__ = "mood_requests"

    id = Column(Integer, primary_key=True)  # Уникальный идентификатор записи
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)  # Telegram ID пользователя
    request_time = Column(DateTime, nullable=False)  # Время отправки запроса
    response_time = Column(DateTime, nullable=True)  # Время ответа
    status = Column(String, nullable=False, default="pending")  # Статус: pending, answered, not_answered

# Создание таблиц
Base.metadata.create_all(engine)

# ------------------------- Функции для работы с пользователями -------------------------

def save_user(user_id, timezone=None):
    """
    Сохраняет или обновляет информацию о пользователе.
    :param user_id: Telegram ID пользователя.
    :param timezone: Таймзона пользователя (строка).
    """
    user = session.query(User).filter_by(user_id=user_id).first()
    if not user:
        user = User(user_id=user_id, timezone=timezone)
        session.add(user)
    else:
        user.timezone = timezone
    session.commit()

def get_user(user_id):
    """
    Возвращает информацию о пользователе.
    :param user_id: Telegram ID пользователя.
    :return: Объект User или None.
    """
    return session.query(User).filter_by(user_id=user_id).first()

# ------------------------- Функции для работы с логами -------------------------

def save_log(user_id, event_type, timestamp, details=None):
    """
    Сохраняет событие в лог.
    :param user_id: Telegram ID пользователя.
    :param event_type: Тип события (response, answer, notification).
    :param timestamp: Время события.
    :param details: Дополнительная информация (например, настроение).
    """
    log = Log(user_id=user_id, event_type=event_type, timestamp=timestamp, details=details)
    session.add(log)
    session.commit()

def get_last_event(user_id):
    """
    Возвращает последнее событие пользователя.
    :param user_id: Telegram ID пользователя.
    :return: Объект Log или None.
    """
    return session.query(Log).filter_by(user_id=user_id).order_by(Log.timestamp.desc()).first()

# ------------------------- Функции для работы с запросами настроения -------------------------

def save_mood_request(user_id, request_time):
    """
    Сохраняет отправленный запрос настроения в таблицу.
    :param user_id: Telegram ID пользователя.
    :param request_time: Время отправки запроса.
    """
    mood_request = MoodRequest(user_id=user_id, request_time=request_time, status="pending")
    session.add(mood_request)
    session.commit()

def update_mood_request(user_id, response_time):
    """
    Обновляет последний запрос пользователя, помечая его как отвеченный.
    :param user_id: Telegram ID пользователя.
    :param response_time: Время ответа.
    """
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
    """
    Помечает запрос как неотвеченный.
    :param user_id: Telegram ID пользователя.
    :param request_time: Время отправки запроса.
    """
    request = (
        session.query(MoodRequest)
        .filter_by(user_id=user_id, request_time=request_time, status="pending")
        .first()
    )
    if request:
        request.status = "not_answered"
        session.commit()

def get_pending_requests(user_id):
    """
    Возвращает все активные (неотвеченные) запросы пользователя.
    :param user_id: Telegram ID пользователя.
    :return: Список объектов MoodRequest.
    """
    return session.query(MoodRequest).filter_by(user_id=user_id, status="pending").all()
