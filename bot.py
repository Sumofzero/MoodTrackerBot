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

# Клавиатура для выбора настроения (2x2)
mood_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="😊 Отлично"), KeyboardButton(text="🙂 Хорошо")],
        [KeyboardButton(text="😐 Нормально"), KeyboardButton(text="😟 Плохо")],
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
        f"Таймзона {message.text} успешно сохранена! Теперь я буду спрашивать твоё настроение каждые 2 минуты.",
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
        "Как ты себя чувствуешь?\n"
        "😊 Отлично   🙂 Хорошо\n"
        "😐 Нормально 😟 Плохо",
        reply_markup=mood_keyboard,
    )

    # Планируем проверку ответа через 1 минуту
    scheduler.add_job(
        check_for_response,
        "date",
        run_date=utc_now + timedelta(minutes=1),
        args=[user_id],
        id=f"check_response_{user_id}",
        misfire_grace_time=120,
    )

async def check_for_response(user_id):
    """Проверяет, ответил ли пользователь на запрос."""
    last_event = get_last_event(user_id)

    # Если последнее событие — `response` и ответа нет, отправляем напоминание
    if last_event and last_event.event_type == "response":
        time_since_response = datetime.now(timezone.utc) - last_event.timestamp.replace(tzinfo=timezone.utc)

        if time_since_response >= timedelta(minutes=1):
            # Отправляем уведомление
            await bot.send_message(user_id, "Нельзя пропускать сбор данных!")

            # Сохраняем событие `notification` в логи
            save_log(user_id, "notification", datetime.now(timezone.utc))

            # Ожидание ответа продолжается, новый запрос не отправляется

# Обработчик выбора настроения
@dp.message(lambda msg: msg.text in ["😊 Отлично", "🙂 Хорошо", "😐 Нормально", "😟 Плохо"])
async def handle_mood(message: Message):
    """Обрабатывает выбор настроения."""
    mood_map = {
        "😊 Отлично": "Отлично",
        "🙂 Хорошо": "Хорошо",
        "😐 Нормально": "Нормально",
        "😟 Плохо": "Плохо",
    }
    mood = mood_map[message.text]
    utc_now = datetime.now(timezone.utc)

    # Сохраняем событие `answer` в логи
    save_log(message.from_user.id, "answer", utc_now, details=mood)

    # Запускаем следующий запрос через 2 минуты
    scheduler.add_job(
        send_mood_request,
        "date",
        run_date=utc_now + timedelta(minutes=2),
        args=[message.from_user.id],
        id=f"mood_request_{message.from_user.id}",
        replace_existing=True,
        misfire_grace_time=120,
    )

    await message.answer(
        f"Спасибо! Я записал: {mood}",
        reply_markup=ReplyKeyboardRemove()
    )

# Запуск планировщика и бота
async def main():
    scheduler.start()  # Запускаем планировщик
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
