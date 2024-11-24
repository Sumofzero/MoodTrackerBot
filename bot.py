from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, Message
from config import BOT_TOKEN
from database import save_user, save_log
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
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

# Клавиатура для выбора эмоционального состояния (10 кнопок в два ряда)
mood_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="😄 10"),  # Прекрасное
            KeyboardButton(text="😊 9"),
            KeyboardButton(text="🙂 8"),
            KeyboardButton(text="😌 7"),
            KeyboardButton(text="😐 6"),
        ],
        [
            KeyboardButton(text="😕 5"),  # Среднее
            KeyboardButton(text="😟 4"),
            KeyboardButton(text="😢 3"),
            KeyboardButton(text="😭 2"),
            KeyboardButton(text="🤢 1"),  # Очень плохое
        ],
    ],
    resize_keyboard=True
)

# Клавиатура для выбора физического состояния (5 кнопок в ряд)
physical_state_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="💪 5"),  # Отличное физическое состояние
            KeyboardButton(text="🙂 4"),  # Хорошее физическое состояние
            KeyboardButton(text="😐 3"),  # Нормальное физическое состояние
            KeyboardButton(text="😟 2"),  # Плохое физическое состояние
            KeyboardButton(text="🤢 1"),  # Очень плохое физическое состояние
        ]
    ],
    resize_keyboard=True
)

# Клавиатура для выбора текущей деятельности
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

# Обработчик команды /start
@dp.message(Command("start"))
async def start_command(message: Message):
    """Приветствует пользователя и предлагает выбрать таймзону."""
    await message.answer(
        "Привет! Я помогу отслеживать твоё настроение и самочувствие. Выбери свою таймзону:",
        reply_markup=timezone_keyboard,
    )

# Обработчик выбора таймзоны
@dp.message(lambda msg: msg.text in timezones)
async def handle_timezone_selection(message: Message):
    """Сохраняет выбранную таймзону и запускает опросы."""
    gmt_offset = int(message.text.split(" ")[0])  # Извлекаем +1, +2 и т.д.
    timezone = f"Etc/GMT{gmt_offset:+d}"  # Конвертируем в формат таймзоны
    save_user(message.from_user.id, timezone)  # Сохраняем таймзону в базу

    await message.answer(
        f"Таймзона {message.text} успешно сохранена! Теперь я буду спрашивать твоё состояние каждый час.",
        reply_markup=ReplyKeyboardRemove()
    )

    # Запускаем запрос активности
    await send_activity_request(message.from_user.id)

async def send_activity_request(user_id):
    """Запрашивает текущую деятельность у пользователя."""
    utc_now = datetime.now(timezone.utc)

    # Сохраняем событие `response_activity` в логи
    save_log(user_id, "response_activity", utc_now)

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
    """Обрабатывает выбор текущей деятельности."""
    activity = message.text
    utc_now = datetime.now(timezone.utc)

    # Сохраняем выбранную деятельность в логи
    save_log(message.from_user.id, "answer_activity", utc_now, details=activity)

    # Переходим к запросу эмоционального состояния
    await bot.send_message(
        message.from_user.id,
        f"Спасибо! Я записал твою текущую деятельность как: {activity}.\nТеперь давай узнаем, как ты себя чувствуешь эмоционально.",
        reply_markup=mood_keyboard,
    )

@dp.message(lambda msg: msg.text in [
    "😄 10", "😊 9", "🙂 8", "😌 7", "😐 6",
    "😕 5", "😟 4", "😢 3", "😭 2", "🤢 1"
])
async def handle_emotional_state(message: Message):
    """Обрабатывает выбор эмоционального состояния."""
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

    # Сохраняем эмоциональное состояние
    save_log(message.from_user.id, "answer_emotional", utc_now, details=mood)

    # Благодарим пользователя за ответ
    await message.answer(
        f"Спасибо! Я записал твоё эмоциональное состояние как: {mood}"
    )

    # Переходим к запросу физического состояния
    await send_physical_state_request(message.from_user.id)

async def send_physical_state_request(user_id):
    """Запрашивает физическое состояние у пользователя."""
    utc_now = datetime.now(timezone.utc)

    # Сохраняем событие `response_physical` в логи
    save_log(user_id, "response_physical", utc_now)

    await bot.send_message(
        user_id,
        "Как ты себя чувствуешь физически? Выбери оценку:\n"
        "💪 5  🙂 4  😐 3  😟 2  🤢 1",
        reply_markup=physical_state_keyboard,
    )

@dp.message(lambda msg: msg.text in ["💪 5", "🙂 4", "😐 3", "😟 2", "🤢 1"])
async def handle_physical_state(message: Message):
    """Обрабатывает выбор физического состояния."""
    physical_state_map = {
        "💪 5": "Отличное",
        "🙂 4": "Хорошее",
        "😐 3": "Нормальное",
        "😟 2": "Плохое",
        "🤢 1": "Очень плохое",
    }
    physical_state = physical_state_map[message.text]
    utc_now = datetime.now(timezone.utc)

    # Сохраняем физическое состояние
    save_log(message.from_user.id, "answer_physical", utc_now, details=physical_state)

    # Запускаем следующий запрос через 1 час
    scheduler.add_job(
        send_activity_request,
        "date",
        run_date=utc_now + timedelta(hours=1),
        args=[message.from_user.id],
        id=f"activity_request_{message.from_user.id}",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    await message.answer(
        f"Спасибо! Я записал твоё физическое состояние как: {physical_state}",
        reply_markup=ReplyKeyboardRemove()
    )

# Запуск планировщика и бота
async def main():
    scheduler.start()  # Запускаем планировщик
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())