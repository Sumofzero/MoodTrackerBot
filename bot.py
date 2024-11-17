from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, Message
from config import BOT_TOKEN
from database import save_user, get_user, save_log, save_mood_request, mark_request_as_answered, get_unanswered_requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
import pytz
from datetime import datetime, timedelta

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

    # Удаляем задачу, если она уже существует, чтобы избежать дублирования
    job_id = f"mood_request_{message.from_user.id}"
    scheduler.remove_job(job_id) if scheduler.get_job(job_id) else None

    # Планируем новую задачу с учётом таймзоны
    scheduler.add_job(
        send_mood_request,
        "interval",
        hours=2,  # Запрашиваем настроение каждые 2 часа
        args=[message.from_user.id],
        id=job_id,
        timezone=pytz.timezone(timezone),
    )

    await message.answer(
        f"Таймзона {message.text} успешно сохранена! Теперь я буду спрашивать твоё настроение каждые 2 часа.",
        reply_markup=mood_keyboard,  # Показать клавиатуру для настроения
    )

async def send_mood_request(user_id):
    """Отправляет запрос о настроении пользователю и планирует проверку ответа."""
    try:
        # Сохраняем новый запрос настроения в базу данных
        request_time = datetime.utcnow()
        save_mood_request(user_id)

        # Отправляем сообщение пользователю
        await bot.send_message(
            user_id,
            "Как ты себя чувствуешь?\n"
            "😊 Отлично   🙂 Хорошо\n"
            "😐 Нормально 😟 Плохо",
            reply_markup=mood_keyboard,
        )

        # Планируем проверку ответа через 5 минуту
        scheduler.add_job(
            check_for_response,
            "date",
            run_date=request_time + timedelta(minutes=5),
            args=[user_id, request_time],
            id=f"check_response_{user_id}_{request_time.timestamp()}",
        )
    except Exception as e:
        print(f"Ошибка при отправке сообщения: {e}")

async def check_for_response(user_id, request_time):
    """Проверяет, ответил ли пользователь на запрос."""
    unanswered_requests = get_unanswered_requests(user_id)
    for req in unanswered_requests:
        if req.request_time == request_time:
            # Если ответа нет, отправляем напоминание (но не дублируем запросы настроения)
            await bot.send_message(user_id, "Нельзя пропускать сбор данных!")
            break  # Уведомление отправляется только один раз

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

    # Отмечаем последний запрос настроения как отвеченный
    mark_request_as_answered(message.from_user.id)

    # Сохраняем лог с временем ответа
    save_log(message.from_user.id, mood, datetime.utcnow(), datetime.utcnow())

    # Переносим следующий запрос на 2 часа после ответа
    job_id = f"mood_request_{message.from_user.id}"
    scheduler.reschedule_job(job_id, trigger="date", run_date=datetime.utcnow() + timedelta(hours=2))

    await message.answer(
        f"Спасибо! Я записал: {mood}",
        reply_markup=ReplyKeyboardRemove()  # Убираем клавиатуру
    )

# Запуск планировщика и бота
async def main():
    scheduler.start()  # Запускаем планировщик
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
