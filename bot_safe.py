import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, Message

from config import BOT_TOKEN, DB_PATH, LOG_LEVEL
from database_safe import (
    save_user, save_log,
    save_activity_and_create_mood_request,
    save_emotion_and_update_request,
    mark_request_as_unanswered,
    get_pending_requests
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pandas as pd
from analytics_safe import generate_and_send_charts

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Создаём бота и диспетчер
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Планировщик задач
scheduler = AsyncIOScheduler()

# Определяем клавиатуры
timezones = ["+1 GMT", "+2 GMT", "+3 GMT", "+4 GMT"]
timezone_keyboard = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text=tz)] for tz in timezones],
    resize_keyboard=True
)

analytics_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Эмоциональное состояние")],
        [KeyboardButton(text="Физическое состояние")],
    ],
    resize_keyboard=True
)

mood_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="😄 10"), KeyboardButton(text="😊 9"),
         KeyboardButton(text="🙂 8"), KeyboardButton(text="😌 7"),
         KeyboardButton(text="😐 6")],
        [KeyboardButton(text="😕 5"), KeyboardButton(text="😟 4"),
         KeyboardButton(text="😢 3"), KeyboardButton(text="😭 2"),
         KeyboardButton(text="🤢 1")],
    ],
    resize_keyboard=True
)

physical_state_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="💪 5"), KeyboardButton(text="🙂 4"),
         KeyboardButton(text="😐 3"), KeyboardButton(text="😟 2"),
         KeyboardButton(text="🤢 1")]
    ],
    resize_keyboard=True
)

activity_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Работаю / Учусь")],
        [KeyboardButton(text="Гуляю")],
        [KeyboardButton(text="Занимаюсь спортом")],
        [KeyboardButton(text="Отдыхаю / Смотрю видео")],
        [KeyboardButton(text="Читаю статью / книгу")],
        [KeyboardButton(text="Общаюсь с друзьями")],
        [KeyboardButton(text="Другое")],
    ],
    resize_keyboard=True
)

# ----------------------- Обработчики команд и сообщений -----------------------

@dp.message(Command("start"))
async def start_command(message: Message):
    """Приветствие и выбор таймзоны."""
    await message.answer(
        "Привет! Я помогу отслеживать твоё настроение и самочувствие. Выбери свою таймзону:",
        reply_markup=timezone_keyboard,
    )

@dp.message(lambda msg: msg.text in timezones)
async def handle_timezone_selection(message: Message):
    """Сохраняет выбранную таймзону и запускает первый запрос активности."""
    gmt_offset = int(message.text.split(" ")[0])
    tz_str = f"Etc/GMT{gmt_offset:+d}"
    
    success = save_user(message.from_user.id, tz_str)
    if success:
        await message.answer(
            f"Таймзона {message.text} успешно сохранена! Теперь я буду спрашивать твоё состояние каждый час.",
            reply_markup=ReplyKeyboardRemove()
        )
        await send_activity_request(message.from_user.id)
    else:
        await message.answer(
            "Произошла ошибка при сохранении настроек. Попробуйте позже."
        )

async def send_activity_request(user_id):
    """
    Отправляет запрос о текущей деятельности пользователю и сохраняет в логе.
    Это начало нового цикла запроса.
    """
    utc_now = datetime.now(timezone.utc)
    success = save_log(user_id, "response_activity", utc_now)
    if not success:
        logger.error(f"Failed to log activity request for user {user_id}")
    
    await bot.send_message(
        user_id,
        "Чем ты сейчас занят? Выбери подходящий вариант:",
        reply_markup=activity_keyboard,
    )

@dp.message(lambda msg: msg.text in [
    "Работаю / Учусь",
    "Гуляю",
    "Занимаюсь спортом",
    "Отдыхаю / Смотрю видео",
    "Читаю статью / книгу",
    "Общаюсь с друзьями",
    "Другое",
])
async def handle_activity(message: Message):
    """
    Обрабатывает выбор текущей деятельности.
    Атомарно сохраняет активность и создает запрос настроения.
    """
    activity = message.text
    utc_now = datetime.now(timezone.utc)
    
    # Атомарная операция: сохранение активности + создание mood request
    activity_saved, mood_request_created = save_activity_and_create_mood_request(
        message.from_user.id, activity, utc_now
    )
    
    if activity_saved and mood_request_created:
        await bot.send_message(
            message.from_user.id,
            f"Спасибо! Я записал твою текущую деятельность как: {activity}.\nТеперь давай узнаем, как ты себя чувствуешь эмоционально.",
            reply_markup=mood_keyboard,
        )
    elif activity_saved:
        await bot.send_message(
            message.from_user.id,
            f"Активность записана: {activity}. Продолжаем...",
            reply_markup=mood_keyboard,
        )
        logger.warning(f"Activity saved but mood request failed for user {message.from_user.id}")
    else:
        await message.answer(
            "Произошла ошибка при записи данных. Попробуйте ещё раз через минуту."
        )
        logger.error(f"Failed to save activity for user {message.from_user.id}")

@dp.message(lambda msg: msg.text in [
    "😄 10", "😊 9", "🙂 8", "😌 7", "😐 6",
    "😕 5", "😟 4", "😢 3", "😭 2", "🤢 1"
])
async def handle_emotional_state(message: Message):
    """
    Обрабатывает выбор эмоционального состояния.
    Атомарно сохраняет эмоцию и обновляет mood request.
    """
    mood_map = {
        "😄 10": "Прекрасное",
        "😊 9": "Очень хорошее",
        "🙂 8": "Хорошее",
        "😌 7": "Удовлетворительное",
        "😐 6": "Нормальное",
        "😕 5": "Среднее",
        "😟 4": "Плохое",
        "😢 3": "Очень плохое",
        "😭 2": "Ужасное",
        "🤢 1": "Критически плохое",
    }
    mood = mood_map[message.text]
    utc_now = datetime.now(timezone.utc)
    
    # Атомарная операция: сохранение эмоции + обновление mood request
    emotion_saved, request_updated = save_emotion_and_update_request(
        message.from_user.id, mood, utc_now
    )
    
    if emotion_saved:
        await message.answer(f"Спасибо! Я записал твоё эмоциональное состояние как: {mood}")
        if not request_updated:
            logger.warning(f"Emotion saved but request update failed for user {message.from_user.id}")
        # Переходим к запросу физического состояния
        await send_physical_state_request(message.from_user.id)
    else:
        await message.answer(
            "Произошла ошибка при записи эмоционального состояния. Попробуйте ещё раз."
        )
        logger.error(f"Failed to save emotion for user {message.from_user.id}")

async def send_physical_state_request(user_id):
    """
    Отправляет запрос о физическом состоянии.
    """
    utc_now = datetime.now(timezone.utc)
    success = save_log(user_id, "response_physical", utc_now)
    if not success:
        logger.error(f"Failed to log physical request for user {user_id}")
    
    await bot.send_message(
        user_id,
        "Как ты себя чувствуешь физически? Выбери оценку:\n💪 5  🙂 4  😐 3  😟 2  🤢 1",
        reply_markup=physical_state_keyboard,
    )

@dp.message(lambda msg: msg.text in ["💪 5", "🙂 4", "😐 3", "😟 2", "🤢 1"])
async def handle_physical_state(message: Message):
    """
    Обрабатывает выбор физического состояния.
    После ответа планирует следующий цикл запроса через 1 час.
    """
    physical_state_map = {
        "💪 5": "Отличное",
        "🙂 4": "Хорошее",
        "😐 3": "Нормальное",
        "😟 2": "Плохое",
        "🤢 1": "Очень плохое",
    }
    physical_state = physical_state_map[message.text]
    utc_now = datetime.now(timezone.utc)
    
    success = save_log(message.from_user.id, "answer_physical", utc_now, details=physical_state)
    if success:
        await message.answer(
            f"Спасибо! Я записал твоё физическое состояние как: {physical_state}.\nХочешь получить аналитику? Нажми на кнопку.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="Запросить аналитику")]], resize_keyboard=True
            )
        )
        # Планируем следующий запрос активности через 1 час после ответа
        scheduler.add_job(
            send_activity_request,
            'date',
            run_date=datetime.now(timezone.utc) + timedelta(hours=1),
            args=[message.from_user.id]
        )
    else:
        await message.answer(
            "Физическое состояние временно не записалось. Данные о настроении сохранены."
        )
        logger.error(f"Failed to save physical state for user {message.from_user.id}")

@dp.message(lambda msg: msg.text == "Запросить аналитику")
async def request_analytics(message: Message):
    await message.answer(
        "Какую аналитику вы хотите посмотреть? Выберите:",
        reply_markup=analytics_keyboard
    )

@dp.message(lambda msg: msg.text in ["Эмоциональное состояние", "Физическое состояние"])
async def send_selected_analytics(message: Message):
    try:
        conn = sqlite3.connect(DB_PATH)
        # Безопасный SQL-запрос с параметрами
        query = "SELECT * FROM logs WHERE user_id = ?"
        logs = pd.read_sql_query(query, conn, params=[message.from_user.id])
        conn.close()

        logs['timestamp'] = pd.to_datetime(logs['timestamp'])

        if message.text == "Эмоциональное состояние":
            mood_map = {
                "Прекрасное": 10,
                "Очень хорошее": 9,
                "Хорошее": 8,
                "Удовлетворительное": 7,
                "Нормальное": 6,
                "Среднее": 5,
                "Плохое": 4,
                "Очень плохое": 3,
                "Ужасное": 2,
                "Критически плохое": 1,
            }
            df = logs[logs['event_type'] == 'answer_emotional'].copy()
            df['score'] = df['details'].map(mood_map)
            df['hour'] = df['timestamp'].dt.hour
            df['day_type'] = df['timestamp'].dt.weekday.apply(lambda x: 'Будний день' if x < 5 else 'Выходной')

            point_count = len(df)
            await message.answer(f"У вас собрано {point_count} точек данных для анализа эмоционального состояния.")

            if df.empty:
                await message.answer("Недостаточно данных для генерации аналитики по эмоциональному состоянию.")
                return

            generate_and_send_charts(BOT_TOKEN, message.chat.id, df, "emotion", logger)

        elif message.text == "Физическое состояние":
            physical_state_map = {
                "Отличное": 5,
                "Хорошее": 4,
                "Нормальное": 3,
                "Плохое": 2,
                "Очень плохое": 1,
            }
            df = logs[logs['event_type'] == 'answer_physical'].copy()
            df['score'] = df['details'].map(physical_state_map)
            df['hour'] = df['timestamp'].dt.hour
            df['day_type'] = df['timestamp'].dt.weekday.apply(lambda x: 'Будний день' if x < 5 else 'Выходной')

            point_count = len(df)
            await message.answer(f"У вас собрано {point_count} точек данных для анализа физического состояния.")

            if df.empty:
                await message.answer("Недостаточно данных для генерации аналитики по физическому состоянию.")
                return

            generate_and_send_charts(BOT_TOKEN, message.chat.id, df, "physical", logger)

    except Exception as e:
        logger.error(f"Analytics generation error: {e}")
        await message.answer("Произошла ошибка при генерации аналитики. Попробуйте позже.")

# ----------------------- Периодическая проверка запросов настроения -----------------------

async def check_pending_requests():
    """
    Каждые 10 минут проверяем в БД все запросы настроения со статусом "pending":
    - Если прошло более 1 часа с момента запроса (но менее 2 часов) – отправляем напоминание.
    - Если прошло более 2 часов – помечаем запрос как неотвеченный и уведомляем пользователя.
    """
    try:
        now = datetime.now(timezone.utc)
        pending_requests = get_pending_requests()
        
        for req in pending_requests:
            time_diff = now - req.request_time
            if timedelta(hours=1) < time_diff <= timedelta(hours=2):
                try:
                    await bot.send_message(
                        req.user_id,
                        "Напоминаем: пожалуйста, ответьте на запрос о настроении."
                    )
                except Exception as e:
                    logger.error(f"Failed to send reminder to user {req.user_id}: {e}")
                    
            elif time_diff > timedelta(hours=2):
                success = mark_request_as_unanswered(req.user_id, req.request_time)
                if success:
                    try:
                        await bot.send_message(
                            req.user_id,
                            "Мы зафиксировали, что вы не ответили на запрос о настроении."
                        )
                    except Exception as e:
                        logger.error(f"Failed to send timeout message to user {req.user_id}: {e}")
                else:
                    logger.error(f"Failed to mark request as unanswered for user {req.user_id}")
                    
    except Exception as e:
        logger.error(f"Error in check_pending_requests: {e}")

# Запуск периодической проверки каждые 10 минут
scheduler.add_job(check_pending_requests, 'interval', minutes=10)

# ----------------------- Запуск бота -----------------------

async def main():
    try:
        scheduler.start()  # Запускаем планировщик
        logger.info("Bot started successfully")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Bot startup failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 