from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, Message
from config import BOT_TOKEN
from database import save_user, save_log, get_last_event
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
import pytz
from datetime import datetime, timedelta, timezone

# Создаём бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Создаём планировщик задач
scheduler = AsyncIOScheduler()

# Таймзоны для кнопок
timezones = ["+1 GMT", "+2 GMT", "+3 GMT", "+4 GMT"]

# Генерация клавиатуры с таймзонами
timezone_keyboard = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text=tz)] for tz in timezones],
    resize_keyboard=True
)

# Клавиатура для выбора настроения (5 кнопок в ряд)
mood_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="😄 5"),  # Отличное настроение
            KeyboardButton(text="😊 4"),  # Хорошее настроение
            KeyboardButton(text="😐 3"),  # Нормальное настроение
            KeyboardButton(text="😟 2"),  # Плохое настроение
            KeyboardButton(text="😢 1"),  # Очень плохое настроение
        ]
    ],
    resize_keyboard=True
)

# Обработчик команды /start
@dp.message(Command("start"))
async def start_command(message: Message):
    """Приветствует пользователя и предлагает выбрать таймзону."""
    await message.answer(
        "Привет! Я помогу отслеживать твоё настроение. Выбери свою таймзону:",
        reply_markup=timezone_keyboard,
    )

# Обработчик выбора таймзоны
@dp.message(lambda msg: msg.text in timezones)
async def handle_timezone_selection(message: Message):
    """Сохраняет выбранную таймзону и запускает расписание запросов."""
    gmt_offset = int(message.text.split(" ")[0])  # Извлекаем +1, +2 и т.д.
    timezone = f"Etc/GMT{gmt_offset:+d}"  # Конвертируем в формат таймзоны
    save_user(message.from_user.id, timezone)  # Сохраняем таймзону в базу

    await message.answer(
        f"Таймзона {message.text} успешно сохранена! Теперь я буду спрашивать твоё настроение каждый час.",
        reply_markup=mood_keyboard,
    )

    # Отправляем первый запрос
    await send_mood_request(message.from_user.id)

async def send_mood_request(user_id):
    """Отправляет запрос о настроении пользователю и планирует проверку ответа."""
    utc_now = datetime.now(timezone.utc)

    # Сохраняем событие `response` в логи
    save_log(user_id, "response", utc_now)

    # Отправляем сообщение пользователю
    await bot.send_message(
        user_id,
        "Как ты себя чувствуешь? Выбери оценку:\n"
        "😄 5   😊 4   😐 3   😟 2   😢 1",
        reply_markup=mood_keyboard,
    )

    # Планируем проверку ответа через 5 минут
    scheduler.add_job(
        check_for_response,
        "date",
        run_date=utc_now + timedelta(minutes=5),
        args=[user_id],
        id=f"check_response_{user_id}",
        misfire_grace_time=300,
    )

async def check_for_response(user_id):
    """Проверяет, ответил ли пользователь на запрос."""
    last_event = get_last_event(user_id)

    # Если последнее событие — `response` и ответа нет, отправляем напоминание
    if last_event and last_event.event_type == "response":
        time_since_response = datetime.now(timezone.utc) - last_event.timestamp.replace(tzinfo=timezone.utc)

        if time_since_response > timedelta(minutes=5):
            # Отправляем уведомление
            await bot.send_message(user_id, "Нельзя пропускать сбор данных!")

            # Сохраняем событие `notification` в логи
            save_log(user_id, "notification", datetime.now(timezone.utc))

            # Ждём ответа, следующий запрос не отправляем пока нет ответа

@dp.message(lambda msg: msg.text in ["😄 5", "😊 4", "😐 3", "😟 2", "😢 1"])
async def handle_mood(message: Message):
    """Обрабатывает выбор настроения."""
    mood_map = {
        "😄 5": "Отличное",
        "😊 4": "Хорошее",
        "😐 3": "Нормальное",
        "😟 2": "Плохое",
        "😢 1": "Очень плохое",
    }
    mood = mood_map[message.text]
    utc_now = datetime.now(timezone.utc)

    # Сохраняем событие `answer` в логи
    save_log(message.from_user.id, "answer", utc_now, details=mood)

    # Запускаем следующий запрос через 1 час
    scheduler.add_job(
        send_mood_request,
        "date",
        run_date=utc_now + timedelta(hours=1),
        args=[message.from_user.id],
        id=f"mood_request_{message.from_user.id}",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    await message.answer(
        f"Спасибо! Я записал твоё настроение как: {mood}",
        reply_markup=ReplyKeyboardRemove()
    )

# Запуск планировщика и бота
async def main():
    scheduler.start()  # Запускаем планировщик
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
