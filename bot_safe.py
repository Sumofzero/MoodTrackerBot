import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, Message
)

from config import BOT_TOKEN, DB_PATH, LOG_LEVEL
from database_safe import (
    save_user, save_log,
    save_activity_and_create_mood_request,
    save_emotion_and_update_request,
    mark_request_as_unanswered,
    get_pending_requests,
    get_last_event,
    get_user_activities,
    EventData, MoodRequestData, UserSettingsData,
    get_user_settings, update_user_settings, should_send_survey
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pandas as pd
from analytics_safe import (
    generate_and_send_charts, generate_and_send_correlation_analysis, should_generate_correlation_analysis,
    generate_and_send_new_charts, should_generate_new_charts,
    generate_smart_insights, should_generate_smart_insights
)

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

# ID администратора (замените на ваш Telegram ID)
ADMIN_USER_ID = 331482026  # Замените на ваш ID

# --------- runtime state ---------
# Users who invoked /start and should always receive first survey right after choosing TZ
_force_first_survey: set[int] = set()
# Users who came from settings (should go to main menu after timezone selection, not survey)
_from_settings: set[int] = set()

# ======================== REPLY КЛАВИАТУРЫ ========================

# Главное меню
def get_main_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Аналитика")],
            [KeyboardButton(text="⚙️ Настройки")],
            [KeyboardButton(text="ℹ️ Помощь")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )

# Клавиатура выбора таймзоны
def get_timezone_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="+1 GMT"), KeyboardButton(text="+2 GMT")],
            [KeyboardButton(text="+3 GMT"), KeyboardButton(text="+4 GMT")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

# Клавиатура выбора активности
def get_activity_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            # Работа и обучение
            [KeyboardButton(text="💼 Работаю / Учусь"), KeyboardButton(text="📚 Читаю статью / книгу")],
            # Физическая активность  
            [KeyboardButton(text="🏃 Занимаюсь спортом"), KeyboardButton(text="🚶 Гуляю")],
            # Отдых и развлечения
            [KeyboardButton(text="📺 Отдыхаю / Смотрю видео"), KeyboardButton(text="👥 Общаюсь с друзьями")],
            # Другое
            [KeyboardButton(text="🎯 Другое")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

# Эмоциональное состояние
def get_mood_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            # Первая строка: хорошие состояния (10-6) - слева для удобства правшей
            [KeyboardButton(text="🔥 10"), KeyboardButton(text="😎 9"), KeyboardButton(text="💅 8"), KeyboardButton(text="🙃 7"), KeyboardButton(text="🤗 6")],
            # Вторая строка: плохие состояния (5-1) 
            [KeyboardButton(text="🤔 5"), KeyboardButton(text="🙄 4"), KeyboardButton(text="😩 3"), KeyboardButton(text="💔 2"), KeyboardButton(text="💀 1")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

# Физическое состояние
def get_physical_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚀 5"), KeyboardButton(text="💯 4"), KeyboardButton(text="🤷‍♂️ 3"), KeyboardButton(text="🥴 2"), KeyboardButton(text="☠️ 1")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

# Аналитика теперь использует reply клавиатуры для удобства

# ======================== КОНСТАНТЫ ========================

# Доступные таймзоны
timezones = ["+1 GMT", "+2 GMT", "+3 GMT", "+4 GMT"]

# Мапинги для преобразования данных
ACTIVITY_MAP = {
    "💼 Работаю / Учусь": "Работаю / Учусь",
    "🚶 Гуляю": "Гуляю",
    "🏃 Занимаюсь спортом": "Занимаюсь спортом", 
    "📺 Отдыхаю / Смотрю видео": "Отдыхаю / Смотрю видео",
    "📚 Читаю статью / книгу": "Читаю статью / книгу",
    "👥 Общаюсь с друзьями": "Общаюсь с друзьями",
    "🎯 Другое": "Другое"
}

MOOD_MAP = {
    "🔥 10": "Прекрасное",
    "😎 9": "Очень хорошее",
    "💅 8": "Хорошее",
    "🙃 7": "Удовлетворительное",
    "🤗 6": "Нормальное",
    "🤔 5": "Среднее",
    "🙄 4": "Плохое",
    "😩 3": "Очень плохое",
    "💔 2": "Ужасное", 
    "💀 1": "Критически плохое",
}

PHYSICAL_STATE_MAP = {
    "🚀 5": "Отличное",
    "💯 4": "Хорошее",
    "🤷‍♂️ 3": "Нормальное",
    "🥴 2": "Плохое", 
    "☠️ 1": "Очень плохое",
}

# Мапинги для аналитики
MOOD_SCORE_MAP = {
    "Прекрасное": 10, "Очень хорошее": 9, "Хорошее": 8,
    "Удовлетворительное": 7, "Нормальное": 6, "Среднее": 5,
    "Плохое": 4, "Очень плохое": 3, "Ужасное": 2, "Критически плохое": 1
}

PHYSICAL_SCORE_MAP = {
    "Отличное": 5, "Хорошее": 4, "Нормальное": 3, 
    "Плохое": 2, "Очень плохое": 1
}

# ======================== ОБРАБОТЧИКИ КОМАНД ========================

@dp.message(Command("start"))
async def start_command(message: Message):
    """Приветствие и выбор таймзоны."""
    # Помечаем пользователя, чтобы после выбора таймзоны сразу отправить опрос
    _force_first_survey.add(message.from_user.id)
    await message.answer(
        "🤖 Привет! Я помогу отслеживать твоё настроение и самочувствие.\n\n"
        "🌍 Для начала выбери свою таймзону:",
        reply_markup=get_timezone_keyboard(),
    )

@dp.message(Command("menu"))
async def menu_command(message: Message):
    """Показать главное меню."""
    await message.answer(
        "📱 Главное меню:\n\n"
        "📊 Аналитика - просмотр графиков и статистики\n"
        "⚙️ Настройки - изменение таймзоны и настроек\n"
        "ℹ️ Помощь - информация о функциях бота",
        reply_markup=get_main_menu()
    )

@dp.message(Command("help"))
async def help_command(message: Message):
    """Показать справку."""
    help_text = (
        "ℹ️ Помощь по использованию бота:\n\n"
        "🕐 Каждый час я буду спрашивать:\n"
        "• Что ты делаешь сейчас\n"
        "• Твоё эмоциональное состояние (1-10)\n"
        "• Твоё физическое состояние (1-5)\n\n"
        "📊 Команды:\n"
        "/start – перезапуск и выбор таймзоны\n"
        "/menu – главное меню\n"
        "/help – эта справка\n\n"
        "📱 Для навигации используйте кнопки в главном меню."
    )
    await message.answer(help_text)

@dp.message(Command("restore_backup"))
async def restore_backup_command(message: Message):
    """Команда восстановления данных для администратора."""
    if message.from_user.id != ADMIN_USER_ID:
        return
    
    await message.answer("🔄 Начинаю восстановление данных из бэкапа от 27 июня...")
    
    try:
        # Импортируем функцию восстановления
        import sys
        import csv
        from pathlib import Path
        
        # Встроенная функция восстановления (упрощенная версия)
        async def restore_backup_simple():
            backup_dir = Path(__file__).parent / "backups" / "20250627_201421"
            csv_path = backup_dir / "logs.csv"
            
            if not csv_path.exists():
                return False, "Файл бэкапа не найден"
            
            restored_count = 0
            try:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        user_id = int(row['user_id'])
                        event_type = row['event_type']
                        timestamp_str = row['timestamp']
                        details = row['details'] if row['details'] else None
                        
                        try:
                            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        except ValueError:
                            timestamp = datetime.fromisoformat(timestamp_str)
                        
                        # Создаем пользователя и сохраняем лог
                        save_user(user_id, timezone=None)
                        success = save_log(user_id, event_type, timestamp, details)
                        if success:
                            restored_count += 1
                            
                return True, f"Восстановлено {restored_count} записей"
            except Exception as e:
                return False, str(e)
        
        # Запускаем восстановление
        success, result_message = await restore_backup_simple()
        
        if success:
            await message.answer(
                f"✅ Данные успешно восстановлены!\n\n"
                f"📊 {result_message}\n\n"
                f"🎉 Все исторические данные доступны!\n"
                f"📈 Проверьте аналитику для подтверждения."
            )
        else:
            await message.answer(f"❌ Ошибка при восстановлении данных: {result_message}")
            
    except Exception as e:
        logger.error(f"Restore backup error: {e}")
        await message.answer(f"❌ Ошибка восстановления: {str(e)}")

# ======================== REPLY MESSAGE ОБРАБОТЧИКИ ========================

@dp.message(lambda msg: msg.text in timezones)
async def handle_timezone_selection(message: Message):
    """Сохраняет выбранную таймзону и запускает опрос активности или показывает главное меню."""
    gmt_offset = int(message.text.split(" ")[0])
    tz_str = f"Etc/GMT{gmt_offset:+d}"
    
    success = save_user(message.from_user.id, tz_str)
    if success:
        await message.answer(
            f"Таймзона {message.text} успешно сохранена!",
            reply_markup=ReplyKeyboardRemove()
        )
        
        # Проверяем контекст: из настроек или из первого запуска
        from_settings = message.from_user.id in _from_settings
        from_start = message.from_user.id in _force_first_survey
        
        # Очищаем флаги
        _from_settings.discard(message.from_user.id)
        _force_first_survey.discard(message.from_user.id)
        
        if from_settings:
            # Пришли из настроек - всегда показываем главное меню, опрос не запускаем
            await main_menu_handler(message)
        elif from_start:
            # Пришли из /start - принудительно запускаем опрос
            await send_activity_request(message.from_user.id)
        else:
            # Другие случаи - проверяем нужен ли опрос
            last_ev = get_last_event(message.from_user.id)
            if last_ev is None:
                # Новый пользователь - запускаем опрос
                await send_activity_request(message.from_user.id)
            elif should_send_survey(message.from_user.id, last_ev.timestamp.replace(tzinfo=timezone.utc)):
                # Давно не было опроса - запускаем
                await send_activity_request(message.from_user.id)
            else:
                # Показываем главное меню
                await main_menu_handler(message)
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
    
    # Используем функцию клавиатуры активности
    
    await bot.send_message(
        user_id,
        "🎯 Чем ты сейчас занят?\n\nВыбери подходящий вариант:",
        reply_markup=get_activity_keyboard(),
    )

@dp.message(lambda msg: msg.text in [
    "💼 Работаю / Учусь",
    "🚶 Гуляю",
    "🏃 Занимаюсь спортом",
    "📺 Отдыхаю / Смотрю видео",
    "📚 Читаю статью / книгу",
    "👥 Общаюсь с друзьями",
    "🎯 Другое",
    # Старые варианты для совместимости
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
    # Убираем эмодзи для сохранения в базу
    activity = ACTIVITY_MAP.get(message.text, message.text)
    utc_now = datetime.now(timezone.utc)
    
    # Атомарная операция: сохранение активности + создание mood request
    activity_saved, mood_request_created = save_activity_and_create_mood_request(
        message.from_user.id, activity, utc_now
    )
    
    # Используем функцию клавиатуры настроения
    
    if activity_saved and mood_request_created:
        await bot.send_message(
            message.from_user.id,
            f"✅ Записал: {activity}\n\n😊 Теперь оцени своё эмоциональное состояние:",
            reply_markup=get_mood_keyboard(),
        )
    elif activity_saved:
        await bot.send_message(
            message.from_user.id,
            f"✅ Активность записана: {activity}\n\n😊 Оцени своё эмоциональное состояние:",
            reply_markup=get_mood_keyboard(),
        )
        logger.warning(f"Activity saved but mood request failed for user {message.from_user.id}")
    else:
        await message.answer(
            "Произошла ошибка при записи данных. Попробуйте ещё раз через минуту."
        )
        logger.error(f"Failed to save activity for user {message.from_user.id}")

@dp.message(lambda msg: msg.text in [
    "🔥 10", "😎 9", "💅 8", "🙃 7", "🤗 6",
    "🤔 5", "🙄 4", "😩 3", "💔 2", "💀 1"
])
async def handle_emotional_state(message: Message):
    """
    Обрабатывает выбор эмоционального состояния.
    Атомарно сохраняет эмоцию и обновляет mood request.
    """
    mood = MOOD_MAP[message.text]
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
    
    # Используем функцию клавиатуры физического состояния
    await bot.send_message(
        user_id,
        "💪 Как ты себя чувствуешь физически?\n\nВыбери оценку:",
        reply_markup=get_physical_keyboard(),
    )

@dp.message(lambda msg: msg.text in ["🚀 5", "💯 4", "🤷‍♂️ 3", "🥴 2", "☠️ 1"])
async def handle_physical_state(message: Message):
    """
    Обрабатывает выбор физического состояния.
    После ответа планирует следующий цикл запроса через 1 час.
    """
    physical_state = PHYSICAL_STATE_MAP[message.text]
    utc_now = datetime.now(timezone.utc)
    
    success = save_log(message.from_user.id, "answer_physical", utc_now, details=physical_state)
    if success:
        # Главное меню после завершения цикла
        completion_keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📊 Аналитика")],
                [KeyboardButton(text="📱 Главное меню")]
            ],
            resize_keyboard=True,
            one_time_keyboard=False  # остаётся видимым
        )
        await message.answer(
            f"💪 Физическое состояние: {physical_state}\n\n"
            f"✅ Спасибо! Все данные записаны.\n"
            f"📊 Хочешь посмотреть аналитику?",
            reply_markup=completion_keyboard
        )
        # Планируем следующий запрос активности на основе пользовательских настроек
        settings = get_user_settings(message.from_user.id)
        if settings:
            next_survey_time = datetime.now(timezone.utc) + timedelta(minutes=settings.survey_interval)
            scheduler.add_job(
                send_activity_request,
                'date',
                run_date=next_survey_time,
                args=[message.from_user.id]
            )
        else:
            # Fallback: если настройки не найдены, используем 1 час
            scheduler.add_job(
                send_activity_request,
                'date',
                run_date=datetime.now(timezone.utc) + timedelta(hours=1),
                args=[message.from_user.id]
            )
        # Показываем главное меню
        await main_menu_handler(message)
    else:
        await message.answer(
            "Физическое состояние временно не записалось. Данные о настроении сохранены."
        )
        logger.error(f"Failed to save physical state for user {message.from_user.id}")

@dp.message(lambda msg: msg.text in ["Запросить аналитику", "📊 Аналитика"])
async def request_analytics(message: Message):
    analytics_reply_keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💭 Эмоциональное состояние")],
            [KeyboardButton(text="💪 Физическое состояние")],
            [KeyboardButton(text="🔗 Корреляционный анализ")],
            [KeyboardButton(text="📊 Расширенная аналитика")],
            [KeyboardButton(text="🧠 Умные инсайты")],
            [KeyboardButton(text="🔙 Главное меню")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False  # Не скрывать после выбора, удобнее
    )
    await message.answer(
        "📊 Какую аналитику хочешь посмотреть?",
        reply_markup=analytics_reply_keyboard
    )

@dp.message(lambda msg: msg.text == "📱 Главное меню")
async def main_menu_handler(message: Message):
    await message.answer(
        "📱 Главное меню:\n\n"
        "📊 Аналитика - просмотр графиков и статистики\n"
        "⚙️ Настройки - изменение настроек\n"
        "ℹ️ Помощь - справка по использованию",
        reply_markup=get_main_menu()
    )

@dp.message(lambda msg: msg.text == "🔙 Главное меню")
async def back_to_main_menu(message: Message):
    await main_menu_handler(message)

@dp.message(lambda msg: msg.text in ["Эмоциональное состояние", "Физическое состояние", "💭 Эмоциональное состояние", "💪 Физическое состояние", "🔗 Корреляционный анализ", "📊 Расширенная аналитика", "🧠 Умные инсайты"])
async def send_selected_analytics(message: Message):
    try:
        conn = sqlite3.connect(DB_PATH)
        # Безопасный SQL-запрос с параметрами
        query = "SELECT * FROM logs WHERE user_id = ?"
        logs = pd.read_sql_query(query, conn, params=[message.from_user.id])
        conn.close()

        logs['timestamp'] = pd.to_datetime(logs['timestamp'])

        # Определяем тип аналитики по содержимому текста (учитываем варианты с эмодзи)
        text_lower = message.text.lower()

        if "эмоцион" in text_lower:
            df = logs[logs['event_type'] == 'answer_emotional'].copy()
            df['score'] = df['details'].map(MOOD_SCORE_MAP)
            df['hour'] = df['timestamp'].dt.hour
            df['day_type'] = df['timestamp'].dt.weekday.apply(lambda x: 'Будний день' if x < 5 else 'Выходной')

            point_count = len(df)
            await message.answer(f"У вас собрано {point_count} точек данных для анализа эмоционального состояния.")

            if df.empty:
                await message.answer("Недостаточно данных для генерации аналитики по эмоциональному состоянию.")
                return

            generate_and_send_charts(BOT_TOKEN, message.chat.id, df, "emotion", logger)

            # Показываем главное меню после отправки графиков
            await main_menu_handler(message)

        elif "физичес" in text_lower:
            df = logs[logs['event_type'] == 'answer_physical'].copy()
            df['score'] = df['details'].map(PHYSICAL_SCORE_MAP)
            df['hour'] = df['timestamp'].dt.hour
            df['day_type'] = df['timestamp'].dt.weekday.apply(lambda x: 'Будний день' if x < 5 else 'Выходной')

            point_count = len(df)
            await message.answer(f"У вас собрано {point_count} точек данных для анализа физического состояния.")

            if df.empty:
                await message.answer("Недостаточно данных для генерации аналитики по физическому состоянию.")
                return

            generate_and_send_charts(BOT_TOKEN, message.chat.id, df, "physical", logger)

            # Показываем главное меню после отправки графиков
            await main_menu_handler(message)

        elif "корреляцион" in text_lower:
            # Корреляционный анализ активностей и состояний
            await message.answer("🔍 Анализирую корреляции между активностями и состояниями...")
            
            # Получаем все данные для корреляционного анализа
            df_emotion = logs[logs['event_type'] == 'answer_emotional'].copy()
            df_physical = logs[logs['event_type'] == 'answer_physical'].copy()
            
            # Получаем активности из базы данных
            activities_data = get_user_activities(message.from_user.id)
            if activities_data:
                df_activities = pd.DataFrame(activities_data)
                df_activities['timestamp'] = pd.to_datetime(df_activities['timestamp'])
            else:
                df_activities = pd.DataFrame()
            
            # Проверяем, достаточно ли данных
            if should_generate_correlation_analysis(df_emotion, df_physical, df_activities):
                # Подготавливаем данные для анализа
                if not df_emotion.empty:
                    mood_map = {
                        "Прекрасное": 10, "Очень хорошее": 9, "Хорошее": 8,
                        "Удовлетворительное": 7, "Нормальное": 6, "Среднее": 5,
                        "Плохое": 4, "Очень плохое": 3, "Ужасное": 2, "Критически плохое": 1
                    }
                    df_emotion['score'] = df_emotion['details'].map(mood_map)
                
                if not df_physical.empty:
                    physical_map = {
                        "Отличное": 5, "Хорошее": 4, "Нормальное": 3, 
                        "Плохое": 2, "Очень плохое": 1
                    }
                    df_physical['score'] = df_physical['details'].map(physical_map)
                
                # Генерируем корреляционный анализ
                generate_and_send_correlation_analysis(
                    BOT_TOKEN, message.chat.id, df_emotion, df_physical, df_activities, logger
                )
                
                await message.answer("✅ Корреляционный анализ отправлен!")
            else:
                await message.answer(
                    "📊 Недостаточно данных для корреляционного анализа.\n\n"
                    "Необходимо минимум:\n"
                    "• 5 записей активностей\n"
                    "• 5 записей эмоционального состояния\n"
                    "• 3 записи физического состояния\n\n"
                    "Продолжайте заполнять дневник!"
                )
            
            # Показываем главное меню после анализа
            await main_menu_handler(message)
            
        elif "расширенная" in text_lower:
            # Показываем меню расширенной аналитики
            extended_analytics_reply_keyboard = ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="🔥 Тепловая карта")],
                    [KeyboardButton(text="📈 Тренды по неделям")],
                    [KeyboardButton(text="📊 Тренды по месяцам")],
                    [KeyboardButton(text="⚖️ Сравнение периодов")],
                    [KeyboardButton(text="🔙 Назад к аналитике")]
                ],
                resize_keyboard=True,
                one_time_keyboard=False
            )
            await message.answer(
                "📊 Расширенная аналитика:\n\n"
                "🔥 Тепловая карта - настроение по дням недели и часам\n"
                "📈 Тренды по неделям - еженедельная динамика\n"
                "📊 Тренды по месяцам - месячная динамика\n"
                "⚖️ Сравнение периодов - сравнение временных отрезков",
                reply_markup=extended_analytics_reply_keyboard
            )
            
        elif "умные" in text_lower or "инсайты" in text_lower:
            # Умные инсайты - персональный анализ паттернов
            await message.answer("🧠 Анализирую ваши данные и составляю персональные инсайты...")
            
            # Получаем все данные для анализа
            df_emotion = logs[logs['event_type'] == 'answer_emotional'].copy()
            df_physical = logs[logs['event_type'] == 'answer_physical'].copy()
            
            # Получаем активности из базы данных
            activities_data = get_user_activities(message.from_user.id)
            if activities_data:
                df_activities = pd.DataFrame(activities_data)
                df_activities['timestamp'] = pd.to_datetime(df_activities['timestamp'])
            else:
                df_activities = pd.DataFrame()
            
            # Проверяем, достаточно ли данных для инсайтов
            if should_generate_smart_insights(df_emotion, df_physical, df_activities):
                # Подготавливаем данные для анализа
                if not df_emotion.empty:
                    mood_map = {
                        "Прекрасное": 10, "Очень хорошее": 9, "Хорошее": 8,
                        "Удовлетворительное": 7, "Нормальное": 6, "Среднее": 5,
                        "Плохое": 4, "Очень плохое": 3, "Ужасное": 2, "Критически плохое": 1
                    }
                    df_emotion['score'] = df_emotion['details'].map(mood_map)
                
                if not df_physical.empty:
                    physical_map = {
                        "Отличное": 5, "Хорошее": 4, "Нормальное": 3, 
                        "Плохое": 2, "Очень плохое": 1
                    }
                    df_physical['score'] = df_physical['details'].map(physical_map)
                
                # Генерируем умные инсайты
                insights_text = generate_smart_insights(df_emotion, df_physical, df_activities)
                
                # Отправляем инсайты пользователю
                await message.answer(
                    f"🧠 ВАШИ ПЕРСОНАЛЬНЫЕ ИНСАЙТЫ\n\n{insights_text}",
                    reply_markup=ReplyKeyboardMarkup(
                        keyboard=[[KeyboardButton(text="📱 Главное меню")]],
                        resize_keyboard=True,
                        one_time_keyboard=False
                    )
                )
                
            else:
                await message.answer(
                    "🧠 Недостаточно данных для генерации умных инсайтов.\n\n"
                    "Необходимо минимум 5 записей эмоционального состояния.\n"
                    "У вас пока: {} записей.\n\n"
                    "Продолжайте заполнять дневник!".format(len(df_emotion))
                )
            
            # Показываем главное меню после анализа
            await main_menu_handler(message)

    except Exception as e:
        logger.error(f"Analytics generation error: {e}")
        await message.answer("Произошла ошибка при генерации аналитики. Попробуйте позже.")

@dp.message(lambda msg: msg.text == "⚙️ Настройки")
async def settings_handler(message: Message):
    """Обработчик кнопки Настройки: меню различных настроек."""
    settings_keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🌍 Изменить таймзону")],
            [KeyboardButton(text="⏰ Интервал опросов")],
            [KeyboardButton(text="🔕 Режим тишины")],
            [KeyboardButton(text="📱 Главное меню")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    
    # Получаем текущие настройки пользователя
    settings = get_user_settings(message.from_user.id)
    if settings:
        interval_text = f"{settings.survey_interval} мин"
        quiet_text = "выключен"
        if settings.quiet_hours_start is not None and settings.quiet_hours_end is not None:
            quiet_text = f"{settings.quiet_hours_start:02d}:00 - {settings.quiet_hours_end:02d}:00"
        
        settings_info = (
            f"⚙️ НАСТРОЙКИ\n\n"
            f"⏰ Интервал опросов: {interval_text}\n"
            f"🔕 Режим тишины: {quiet_text}\n"
            f"📅 Выходные: {settings.weekend_mode}\n\n"
            f"Выберите что хотите изменить:"
        )
    else:
        settings_info = "⚙️ НАСТРОЙКИ\n\nВыберите что хотите настроить:"
    
    await message.answer(settings_info, reply_markup=settings_keyboard)

@dp.message(lambda msg: msg.text == "ℹ️ Помощь")
async def help_reply_handler(message: Message):
    """Показывает справку по использованию бота."""
    help_text = (
        "ℹ️ Помощь по использованию бота:\n\n"
        "1. Каждый час я спрашиваю вашу текущую деятельность и состояние.\n"
        "2. Используйте кнопки для быстрого ответа.\n"
        "3. Нажмите 📊 Аналитика, чтобы получить графики.\n\n"
        "Команды:\n"
        "/start – перезапуск бота\n"
        "/menu – главное меню\n"
    )
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Главное меню")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await message.answer(help_text, reply_markup=back_keyboard)

@dp.message(lambda msg: msg.text in ["🔥 Тепловая карта", "📈 Тренды по неделям", "📊 Тренды по месяцам", "⚖️ Сравнение периодов"])
async def handle_extended_analytics_reply(message: Message):
    """Обработка кнопок расширенной аналитики через reply клавиатуру."""
    # Карта типов графиков
    chart_mapping = {
        "🔥 Тепловая карта": ("heatmap", "тепловую карту"),
        "📈 Тренды по неделям": ("weekly_trends", "тренды по неделям"), 
        "📊 Тренды по месяцам": ("monthly_trends", "тренды по месяцам"),
        "⚖️ Сравнение периодов": ("period_comparison", "сравнение периодов")
    }
    
    chart_type, chart_name = chart_mapping[message.text]
    await message.answer(f"📊 Генерирую {chart_name}...")
    
    try:
        # Загружаем данные эмоционального состояния (по умолчанию)
        conn = sqlite3.connect(DB_PATH)
        query = """
            SELECT timestamp, event_type, details
            FROM logs 
            WHERE user_id = ? AND event_type IN ('answer_emotional') 
            ORDER BY timestamp
        """
        df = pd.read_sql_query(query, conn, params=(message.from_user.id,))
        conn.close()
        
        if df.empty:
            await message.answer(
                f"📊 Пока нет данных для создания графика.\n"
                f"Собери больше записей!"
            )
            return
        
        # Преобразуем данные
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        mood_map = {
            "Прекрасное": 10, "Очень хорошее": 9, "Хорошее": 8,
            "Удовлетворительное": 7, "Нормальное": 6, "Среднее": 5,
            "Плохое": 4, "Очень плохое": 3, "Ужасное": 2, "Критически плохое": 1
        }
        df['score'] = df['details'].map(mood_map)
        
        # Проверяем, достаточно ли данных для конкретного типа графика
        if should_generate_new_charts(df, chart_type):
            # Генерируем и отправляем график
            success = generate_and_send_new_charts(
                BOT_TOKEN, message.from_user.id, df, chart_type, "emotion", logger
            )
            
            if success:
                await message.answer(f"✅ {chart_name.capitalize()} отправлен!")
            else:
                await message.answer(f"❌ Ошибка при генерации графика.\nПопробуйте позже.")
        else:
            min_data_requirements = {
                "heatmap": "10 записей",
                "weekly_trends": "14 записей (2 недели)",
                "monthly_trends": "14 записей (2 недели)",
                "period_comparison": "20 записей"
            }
            
            requirement = min_data_requirements.get(chart_type, "больше записей")
            await message.answer(
                f"📊 Недостаточно данных для создания графика.\n\n"
                f"Для графика '{chart_name}' необходимо минимум {requirement}.\n"
                f"У вас: {len(df)} записей.\n\n"
                f"Продолжайте заполнять дневник!"
            )
        
        # Показываем главное меню после генерации
        await main_menu_handler(message)
        
    except Exception as e:
        logger.error(f"Error generating extended chart {chart_type} for user {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка при генерации графика.\nПопробуйте позже.")

@dp.message(lambda msg: msg.text == "🔙 Назад к аналитике")
async def back_to_analytics_menu(message: Message):
    """Возврат к меню аналитики."""
    await request_analytics(message)

# ======================== ОБРАБОТЧИКИ НАСТРОЕК ========================

def cancel_user_survey_jobs(user_id: int):
    """Отменяет все активные jobs опросов для указанного пользователя."""
    try:
        jobs = scheduler.get_jobs()
        cancelled_count = 0
        for job in jobs:
            # Проверяем что это job функции send_activity_request с нужным user_id
            if (job.func == send_activity_request and 
                len(job.args) > 0 and 
                job.args[0] == user_id):
                scheduler.remove_job(job.id)
                cancelled_count += 1
                logger.info(f"Cancelled survey job {job.id} for user {user_id}")
        
        if cancelled_count > 0:
            logger.info(f"Cancelled {cancelled_count} survey jobs for user {user_id}")
            
    except Exception as e:
        logger.error(f"Error cancelling survey jobs for user {user_id}: {e}")

@dp.message(lambda msg: msg.text == "🌍 Изменить таймзону")
async def timezone_settings_handler(message: Message):
    """Обработчик изменения таймзоны."""
    # Отмечаем что пользователь пришел из настроек
    _from_settings.add(message.from_user.id)
    await message.answer(
        "🌍 Выберите новую таймзону:",
        reply_markup=get_timezone_keyboard()
    )

@dp.message(lambda msg: msg.text == "⏰ Интервал опросов")
async def interval_settings_handler(message: Message):
    """Обработчик настройки интервала опросов."""
    interval_keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⚡ 30 минут"), KeyboardButton(text="⏰ 1 час")],
            [KeyboardButton(text="🕐 2 часа")],
            [KeyboardButton(text="🔙 К настройкам")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    
    current_settings = get_user_settings(message.from_user.id)
    current_interval = "1 час"
    if current_settings:
        if current_settings.survey_interval == 30:
            current_interval = "30 минут" 
        elif current_settings.survey_interval == 120:
            current_interval = "2 часа"
    
    await message.answer(
        f"⏰ ИНТЕРВАЛ ОПРОСОВ\n\n"
        f"Текущий интервал: {current_interval}\n\n"
        f"Как часто вы хотите получать опросы?",
        reply_markup=interval_keyboard
    )

@dp.message(lambda msg: msg.text in ["⚡ 30 минут", "⏰ 1 час", "🕐 2 часа"])
async def handle_interval_selection(message: Message):
    """Обработчик выбора интервала опросов."""
    interval_map = {
        "⚡ 30 минут": 30,
        "⏰ 1 час": 60,
        "🕐 2 часа": 120
    }
    
    new_interval = interval_map[message.text]
    success = update_user_settings(message.from_user.id, survey_interval=new_interval)
    
    if success:
        # Отменяем все старые jobs опросов для этого пользователя
        cancel_user_survey_jobs(message.from_user.id)
        
        # Проверяем, нужно ли создать новый job
        last_ev = get_last_event(message.from_user.id)
        if last_ev and should_send_survey(message.from_user.id, last_ev.timestamp.replace(tzinfo=timezone.utc)):
            # Если пора отправлять опрос - создаем job на ближайшее время (через 1 минуту)
            scheduler.add_job(
                send_activity_request,
                'date',
                run_date=datetime.now(timezone.utc) + timedelta(minutes=1),
                args=[message.from_user.id]
            )
            logger.info(f"Scheduled immediate survey for user {message.from_user.id} after interval change")
        
        await message.answer(
            f"✅ Интервал опросов изменен на {message.text}\n"
            f"🔄 Старые запланированные опросы отменены",
            reply_markup=ReplyKeyboardRemove()
        )
    else:
        await message.answer("❌ Ошибка при сохранении настроек")
    
    # Возвращаемся к настройкам
    await settings_handler(message)

@dp.message(lambda msg: msg.text == "🔕 Режим тишины")
async def quiet_mode_settings_handler(message: Message):
    """Обработчик настройки режима тишины."""
    quiet_keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🌙 23:00 - 07:00"), KeyboardButton(text="🌛 22:00 - 08:00")],
            [KeyboardButton(text="🏠 Настроить вручную")],
            [KeyboardButton(text="🔊 Отключить тишину")],
            [KeyboardButton(text="🔙 К настройкам")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    
    current_settings = get_user_settings(message.from_user.id)
    quiet_status = "выключен"
    if current_settings and current_settings.quiet_hours_start is not None:
        quiet_status = f"{current_settings.quiet_hours_start:02d}:00 - {current_settings.quiet_hours_end:02d}:00"
    
    await message.answer(
        f"🔕 РЕЖИМ ТИШИНЫ\n\n"
        f"Текущий режим: {quiet_status}\n\n"
        f"В период тишины опросы не отправляются.\n"
        f"Выберите подходящий вариант:",
        reply_markup=quiet_keyboard
    )

@dp.message(lambda msg: msg.text in ["🌙 23:00 - 07:00", "🌛 22:00 - 08:00", "🔊 Отключить тишину"])
async def handle_quiet_mode_selection(message: Message):
    """Обработчик выбора режима тишины."""
    if message.text == "🌙 23:00 - 07:00":
        success = update_user_settings(message.from_user.id, quiet_hours_start=23, quiet_hours_end=7)
        result_text = "режим тишины: 23:00 - 07:00"
    elif message.text == "🌛 22:00 - 08:00":
        success = update_user_settings(message.from_user.id, quiet_hours_start=22, quiet_hours_end=8)  
        result_text = "режим тишины: 22:00 - 08:00"
    else:  # Отключить тишину
        success = update_user_settings(message.from_user.id, quiet_hours_start=None, quiet_hours_end=None)
        result_text = "режим тишины отключен"
    
    if success:
        await message.answer(
            f"✅ Настройка сохранена: {result_text}",
            reply_markup=ReplyKeyboardRemove()
        )
    else:
        await message.answer("❌ Ошибка при сохранении настроек")
    
    # Возвращаемся к настройкам
    await settings_handler(message)

@dp.message(lambda msg: msg.text == "🔙 К настройкам")
async def back_to_settings(message: Message):
    """Возврат к меню настроек."""
    await settings_handler(message)

# ----------------------- Периодическая проверка запросов настроения -----------------------

async def check_pending_requests():
    """
    Каждые 10 минут проверяем в БД все запросы настроения со статусом "pending":
    - Персональные напоминания на основе настроек пользователя
    - Если прошло времени больше чем 2x интервал - помечаем как неотвеченный
    """
    try:
        now = datetime.now(timezone.utc)
        pending_requests = get_pending_requests()
        
        for req in pending_requests:
            # Получаем персональные настройки пользователя
            settings = get_user_settings(req.user_id)
            if not settings:
                continue  # Пропускаем если настройки не найдены
            
            time_diff = now - req.request_time
            interval_minutes = settings.survey_interval
            
            # Напоминание приходит через interval_minutes, но не более 2 интервалов
            reminder_threshold = timedelta(minutes=interval_minutes)
            timeout_threshold = timedelta(minutes=interval_minutes * 2)
            
            if reminder_threshold < time_diff <= timeout_threshold:
                try:
                    await bot.send_message(
                        req.user_id,
                        f"⏰ Напоминание: пожалуйста, ответьте на запрос о настроении.\n"
                        f"(Интервал опросов: {interval_minutes} мин)"
                    )
                except Exception as e:
                    logger.error(f"Failed to send reminder to user {req.user_id}: {e}")
                    
            elif time_diff > timeout_threshold:
                success = mark_request_as_unanswered(req.user_id, req.request_time)
                if success:
                    try:
                        await bot.send_message(
                            req.user_id,
                            f"⏱️ Запрос о настроении пропущен (лимит времени: {interval_minutes * 2} мин)."
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

# ----------------------- HELPERS -----------------------

# Функция build_timezone_keyboard удалена - используется get_timezone_keyboard()

if __name__ == "__main__":
    asyncio.run(main()) 