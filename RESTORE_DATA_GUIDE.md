# 🔄 Инструкция по восстановлению данных в Render

## 📋 Проблема
После развертывания в Render все данные пользователей (4,210+ объектов) были утеряны из-за пересоздания базы данных.

## ✅ Решение 
У нас есть бэкап от **27 июня 2025** с полными данными. Скрипт восстановления готов и протестирован локально.

---

## 🛠️ Вариант 1: Восстановление через Web Service

### Шаг 1: Подготовка файлов
```bash
# Убедитесь что файлы в репозитории:
git add scripts/restore_data.py
git add backups/20250627_201421/
git commit -m "📦 Добавлен скрипт восстановления данных"
git push origin main
```

### Шаг 2: Выполнение в Render
1. Зайдите в Dashboard Render.com
2. Откройте ваш Web Service
3. Перейдите в **Shell** или используйте **Deploy Logs**
4. Выполните команды:

```bash
# Переход в директорию проекта
cd /opt/render/project/src

# Проверка файлов бэкапа
ls -la backups/20250627_201421/

# Запуск восстановления
python scripts/restore_data.py 20250627_201421
```

---

## 🛠️ Вариант 2: Восстановление через Environment Variables

Если в Render другая структура базы (PostgreSQL), нужно адаптировать скрипт.

### Проверьте DATABASE_URL в Render:
```bash
echo $DATABASE_URL
```

Если это PostgreSQL, создайте `scripts/restore_data_postgres.py`:

```python
import os
import psycopg2
from urllib.parse import urlparse

# Парсим DATABASE_URL
url = urlparse(os.environ['DATABASE_URL'])
conn = psycopg2.connect(
    host=url.hostname,
    port=url.port,
    user=url.username,
    password=url.password,
    database=url.path[1:]
)
```

---

## 🛠️ Вариант 3: Альтернативное восстановление

### Если доступ к Shell ограничен:

1. **Создайте эндпоинт восстановления в боте:**

```python
@dp.message(Command("restore_backup"))
async def restore_backup_command(message: Message):
    """Команда восстановления для администратора."""
    if message.from_user.id != ADMIN_USER_ID:  # Ваш Telegram ID
        return
        
    await message.answer("🔄 Начинаю восстановление данных...")
    
    # Вызов функции восстановления
    success = restore_from_backup("20250627_201421")
    
    if success:
        await message.answer("✅ Данные восстановлены!")
    else:
        await message.answer("❌ Ошибка восстановления")
```

2. **Отправьте команду `/restore_backup` в Telegram**

---

## 📊 Ожидаемые результаты

После успешного восстановления:
- **👥 Пользователей**: 4 (основные пользователи)
- **📝 Записей в логах**: 4,210
- **📊 Доступна аналитика** за весь период
- **⚙️ Настройки по умолчанию** создадутся автоматически

---

## 🔍 Проверка восстановления

### В Telegram боте:
1. Отправьте команду `/start`
2. Перейдите в **📊 Аналитика**
3. Проверьте графики - должны показать исторические данные

### В логах Render:
```bash
# Проверка количества записей
python -c "
from database_safe import *
with get_db_session() as session:
    print(f'Users: {session.query(User).count()}')
    print(f'Logs: {session.query(Log).count()}')
"
```

---

## ⚠️ Важные моменты

1. **Backup совместим** - скрипт учитывает новую структуру БД с таблицами `UserSettings`
2. **Дубликаты не создаются** - скрипт проверяет существующих пользователей  
3. **Таймзоны** будут переустановлены при первом входе пользователей
4. **Настройки уведомлений** создадутся со значениями по умолчанию

---

## 🆘 Если что-то не работает

1. **Проверьте логи Render** на ошибки импорта
2. **Проверьте путь к файлам** бэкапа
3. **Убедитесь в правах доступа** к файлу базы данных
4. **Свяжитесь со мной** - отправлю альтернативные варианты

---

*Создано: 29 июня 2025*  
*Протестировано локально: ✅ 4,210 записей восстановлено* 