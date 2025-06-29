#!/usr/bin/env python3
"""
Восстановление данных непосредственно на сервере Render.

Этот скрипт предназначен для запуска на самом сервере Render
и восстанавливает данные из CSV файла бэкапа.

Usage:
    python scripts/local_restore.py [backup_date]
    
Example:
    python scripts/local_restore.py 20250627_201421
"""

import csv
import sys
import os
from datetime import datetime
from pathlib import Path

# Добавляем путь к проекту
sys.path.append('/opt/render/project/src')

try:
    from database_safe import save_user, save_log, Base, engine, get_db_session, User, Log
except ImportError:
    # Для локального тестирования
    from database_safe import save_user, save_log, Base, engine, get_db_session, User, Log

def find_backup_file(backup_date: str) -> Path:
    """Находит файл бэкапа по дате."""
    # Проверяем разные возможные пути
    possible_paths = [
        Path(f"/opt/render/project/src/backups/{backup_date}/logs.csv"),  # На Render
        Path(f"./backups/{backup_date}/logs.csv"),  # Локально
        Path(f"backups/{backup_date}/logs.csv"),  # Относительный путь
        Path(f"/MoodTrackerBot_data/backups/{backup_date}/logs.csv"),  # На диске Render
    ]
    
    for path in possible_paths:
        if path.exists():
            return path
    
    raise FileNotFoundError(f"Файл бэкапа не найден для даты: {backup_date}")

def restore_data(backup_date: str = "20250627_201421"):
    """Восстанавливает данные из CSV файла."""
    
    print(f"🔄 Начинаем восстановление данных из бэкапа: {backup_date}")
    
    try:
        # Находим файл бэкапа
        csv_path = find_backup_file(backup_date)
        print(f"📁 Найден файл бэкапа: {csv_path}")
        
        # Создаем таблицы если их нет
        print("🏗️ Создаем таблицы базы данных...")
        Base.metadata.create_all(engine)
        
        restored_logs = 0
        created_users = set()
        errors = 0
        
        print("📊 Начинаем импорт данных...")
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row_num, row in enumerate(reader, 1):
                try:
                    user_id = int(row['user_id'])
                    event_type = row['event_type']
                    timestamp_str = row['timestamp']
                    details = row['details'] if row['details'] else None
                    
                    # Создаем пользователя если его еще нет
                    if user_id not in created_users:
                        save_user(user_id, timezone=None)
                        created_users.add(user_id)
                        print(f"👤 Создан пользователь: {user_id}")
                    
                    # Парсим timestamp
                    try:
                        if timestamp_str.endswith('Z'):
                            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        else:
                            timestamp = datetime.fromisoformat(timestamp_str)
                    except ValueError:
                        # Пробуем другие форматы
                        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', ''))
                    
                    # Сохраняем лог
                    success = save_log(user_id, event_type, timestamp, details)
                    if success:
                        restored_logs += 1
                        if restored_logs % 100 == 0:
                            print(f"📝 Восстановлено {restored_logs} записей...")
                    else:
                        errors += 1
                        print(f"⚠️ Ошибка при сохранении записи {row_num}")
                        
                except Exception as e:
                    errors += 1
                    print(f"❌ Ошибка в строке {row_num}: {e}")
                    continue
        
        print(f"\n✅ Импорт завершен!")
        print(f"📊 Восстановлено: {restored_logs} записей")
        print(f"👥 Создано пользователей: {len(created_users)}")
        print(f"❌ Ошибок: {errors}")
        
        # Проверяем итоговое состояние базы данных
        print("\n🔍 Проверяем состояние базы данных...")
        with get_db_session() as session:
            total_logs = session.query(Log).count()
            total_users = session.query(User).count()
            print(f"📈 Итого в базе: {total_users} пользователей, {total_logs} записей")
        
        return True
        
    except Exception as e:
        print(f"❌ Критическая ошибка при восстановлении: {e}")
        return False

def main():
    """Основная функция."""
    backup_date = sys.argv[1] if len(sys.argv) > 1 else "20250627_201421"
    
    print("🔄 ЛОКАЛЬНОЕ ВОССТАНОВЛЕНИЕ ДАННЫХ")
    print("=" * 50)
    print(f"📅 Дата бэкапа: {backup_date}")
    print(f"📁 Рабочая директория: {os.getcwd()}")
    print(f"🐍 Python путь: {sys.path[0]}")
    
    success = restore_data(backup_date)
    
    if success:
        print("\n🎉 Данные успешно восстановлены!")
        print("📱 Теперь можете проверить аналитику в боте")
    else:
        print("\n❌ Восстановление не удалось")
        sys.exit(1)

if __name__ == "__main__":
    main() 