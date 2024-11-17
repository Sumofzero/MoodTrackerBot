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

    logs = relationship("Log", back_populates="user")  # Связь с логами

# Модель для логов
class Log(Base):
    __tablename__ = "logs"

    id = Column(Integer, primary_key=True)  # Уникальный идентификатор записи
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)  # Ссылка на пользователя
    message = Column(String, nullable=False)  # Сообщение (настроение)
    request_timestamp = Column(DateTime, nullable=False)  # Время запроса
    response_timestamp = Column(DateTime, nullable=True)  # Время ответа (может быть None)

    user = relationship("User", back_populates="logs")  # Связь с пользователем

# Модель для запросов настроения
class MoodRequest(Base):
    __tablename__ = "mood_requests"

    id = Column(Integer, primary_key=True)  # Уникальный идентификатор записи
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)  # Ссылка на пользователя
    request_time = Column(DateTime, default=datetime.datetime.utcnow)  # Время отправки запроса
    answered = Column(Integer, default=0)  # 0 = Не ответил, 1 = Ответил

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

def get_all_users():
    """
    Возвращает всех пользователей.
    :return: Список объектов User.
    """
    return session.query(User).all()

# ------------------------- Функции для работы с логами -------------------------

def save_log(user_id, message, request_time, response_time=None):
    """
    Сохраняет лог с настроением пользователя.
    :param user_id: Telegram ID пользователя.
    :param message: Сообщение (настроение).
    :param request_time: Время запроса.
    :param response_time: Время ответа (None, если нет ответа).
    """
    log = Log(
        user_id=user_id,
        message=message,
        request_timestamp=request_time,
        response_timestamp=response_time,
    )
    session.add(log)
    session.commit()

def get_logs(user_id):
    """
    Возвращает все логи пользователя.
    :param user_id: Telegram ID пользователя.
    :return: Список объектов Log.
    """
    return session.query(Log).filter_by(user_id=user_id).all()

def get_recent_logs(user_id, limit=10):
    """
    Возвращает последние логи пользователя.
    :param user_id: Telegram ID пользователя.
    :param limit: Количество последних записей.
    :return: Список объектов Log.
    """
    return session.query(Log).filter_by(user_id=user_id).order_by(Log.request_timestamp.desc()).limit(limit).all()

# ------------------------- Функции для работы с запросами настроения -------------------------

def save_mood_request(user_id):
    """
    Сохраняет отправленный запрос настроения в таблицу.
    :param user_id: Telegram ID пользователя.
    """
    mood_request = MoodRequest(user_id=user_id)
    session.add(mood_request)
    session.commit()

def mark_request_as_answered(user_id):
    """
    Отмечает последний запрос пользователя как отвеченный.
    :param user_id: Telegram ID пользователя.
    """
    request = session.query(MoodRequest).filter_by(user_id=user_id, answered=0).order_by(MoodRequest.request_time.desc()).first()
    if request:
        request.answered = 1
        session.commit()

def get_unanswered_requests(user_id):
    """
    Возвращает все неотвеченные запросы пользователя.
    :param user_id: Telegram ID пользователя.
    :return: Список объектов MoodRequest.
    """
    return session.query(MoodRequest).filter_by(user_id=user_id, answered=0).all()