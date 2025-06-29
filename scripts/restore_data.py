"""Restore MoodTrackerBot data from backup.

Восстанавливает данные из бэкапа в текущую базу данных.
Совместим с новой структурой базы (таблицы User, UserSettings, Log, MoodRequest).

Usage:
    python scripts/restore_data.py [backup_date]
    
Example:
    python scripts/restore_data.py 20250627_201421
"""

import os
import sys
import csv
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

# Добавляем родительскую папку в path для импорта модулей
sys.path.append(str(Path(__file__).parent.parent))

from database_safe import (
    get_db_session, save_user, save_log, 
    User, UserSettings, Log, MoodRequest, 
    Base, engine
)

def restore_from_backup(backup_date: str = "20250627_201421"):
    """Восстанавливает данные из указанного бэкапа."""
    
    # Пути к файлам бэкапа
    backup_dir = Path(__file__).parent.parent / "backups" / backup_date
    csv_path = backup_dir / "logs.csv"
    
    if not csv_path.exists():
        print(f"❌ Файл бэкапа не найден: {csv_path}")
        return False
    
    print(f"📦 Восстанавливаем данные из бэкапа: {backup_date}")
    print(f"📁 Путь к файлу: {csv_path}")
    
    # Создаем таблицы если их нет
    print("🗄️  Создаем таблицы...")
    Base.metadata.create_all(engine)
    
    # Читаем данные из CSV
    print("📖 Читаем данные из CSV...")
    restored_logs = 0
    created_users = set()
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                user_id = int(row['user_id'])
                event_type = row['event_type']
                timestamp_str = row['timestamp']
                details = row['details'] if row['details'] else None
                
                # Создаем пользователя если его еще нет
                if user_id not in created_users:
                    success = save_user(user_id, timezone=None)  # Таймзона будет установлена при первом входе
                    if success:
                        created_users.add(user_id)
                        print(f"👤 Создан пользователь {user_id}")
                
                # Парсим timestamp
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                except ValueError:
                    # Старый формат без timezone
                    timestamp = datetime.fromisoformat(timestamp_str)
                
                # Сохраняем лог
                success = save_log(user_id, event_type, timestamp, details)
                if success:
                    restored_logs += 1
                    if restored_logs % 500 == 0:
                        print(f"📝 Восстановлено {restored_logs} записей...")
                else:
                    print(f"❌ Ошибка при сохранении записи: {row}")
        
        print(f"✅ Восстановление завершено!")
        print(f"👥 Пользователей: {len(created_users)}")
        print(f"📝 Записей восстановлено: {restored_logs}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при восстановлении: {e}")
        return False

def check_current_data():
    """Проверяет текущее состояние базы данных."""
    print("🔍 Проверяем текущее состояние базы...")
    
    try:
        with get_db_session() as session:
            users_count = session.query(User).count()
            logs_count = session.query(Log).count()
            settings_count = session.query(UserSettings).count()
            
            print(f"👥 Пользователей в базе: {users_count}")
            print(f"📝 Записей в логах: {logs_count}")
            print(f"⚙️ Настроек пользователей: {settings_count}")
            
            return logs_count
            
    except Exception as e:
        print(f"❌ Ошибка при проверке базы: {e}")
        return 0

def main():
    """Основная функция восстановления."""
    backup_date = sys.argv[1] if len(sys.argv) > 1 else "20250627_201421"
    
    print("🔄 ВОССТАНОВЛЕНИЕ ДАННЫХ MOODTRACKERBOT")
    print("=" * 50)
    
    # Проверяем текущее состояние
    current_logs = check_current_data()
    
    if current_logs > 0:
        response = input(f"\n⚠️  В базе уже есть {current_logs} записей. Продолжить восстановление? (y/N): ")
        if response.lower() != 'y':
            print("❌ Восстановление отменено")
            return
    
    # Восстанавливаем данные
    success = restore_from_backup(backup_date)
    
    if success:
        print("\n🎉 Данные успешно восстановлены!")
        check_current_data()
    else:
        print("\n❌ Восстановление не удалось")

if __name__ == "__main__":
    main() 