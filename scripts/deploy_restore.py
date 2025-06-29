#!/usr/bin/env python3
"""
Восстановление данных на сервере Render через SSH.

Этот скрипт:
1. Подключается к серверу Render через SSH
2. Загружает файл бэкапа на сервер
3. Запускает восстановление данных на сервере
4. Проверяет результат

Usage:
    python scripts/deploy_restore.py [backup_date]
    
Example:
    python scripts/deploy_restore.py 20250627_201421
"""

import os
import sys
import paramiko
import tempfile
from pathlib import Path
from typing import Final

# SSH настройки для Render
SSH_HOST: Final[str] = "ssh.oregon.render.com"
SSH_USER: Final[str] = "srv-cssvk3ogph6c7399j0gg"
SSH_KEY_PATH: Final[str] = str(Path.home() / ".ssh/id_rsa")

def upload_backup_and_restore(backup_date: str = "20250627_201421"):
    """Загружает бэкап на сервер и восстанавливает данные."""
    
    # Локальные пути
    backup_dir = Path(__file__).parent.parent / "backups" / backup_date
    csv_path = backup_dir / "logs.csv"
    
    if not csv_path.exists():
        print(f"❌ Файл бэкапа не найден: {csv_path}")
        return False
    
    print(f"📦 Восстанавливаем данные из бэкапа: {backup_date}")
    print(f"📁 Локальный файл: {csv_path}")
    
    try:
        # Подключаемся к серверу
        print("🔌 Подключаемся к серверу Render...")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=SSH_HOST, username=SSH_USER, key_filename=SSH_KEY_PATH)
        
        # Создаем SFTP соединение
        sftp = ssh.open_sftp()
        
        # Загружаем файл бэкапа на сервер
        remote_csv_path = f"/tmp/restore_backup_{backup_date}.csv"
        print(f"📤 Загружаем бэкап на сервер: {remote_csv_path}")
        sftp.put(str(csv_path), remote_csv_path)
        
        # Создаем скрипт восстановления на сервере
        restore_script = f"""
import csv
import sys
import os
from datetime import datetime
from pathlib import Path

# Добавляем путь к проекту
sys.path.append('/opt/render/project/src')

from database_safe import save_user, save_log, Base, engine, get_db_session, User, Log

def restore_data():
    print("🔄 Начинаем восстановление данных...")
    
    # Создаем таблицы
    Base.metadata.create_all(engine)
    
    restored_logs = 0
    created_users = set()
    
    with open("{remote_csv_path}", 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            user_id = int(row['user_id'])
            event_type = row['event_type']
            timestamp_str = row['timestamp']
            details = row['details'] if row['details'] else None
            
            # Создаем пользователя
            if user_id not in created_users:
                save_user(user_id, timezone=None)
                created_users.add(user_id)
            
            # Парсим timestamp
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            except ValueError:
                timestamp = datetime.fromisoformat(timestamp_str)
            
            # Сохраняем лог
            success = save_log(user_id, event_type, timestamp, details)
            if success:
                restored_logs += 1
    
    print(f"✅ Восстановлено {{restored_logs}} записей, {{len(created_users)}} пользователей")
    
    # Проверяем результат
    with get_db_session() as session:
        total_logs = session.query(Log).count()
        total_users = session.query(User).count()
        print(f"📊 Итого в базе: {{total_users}} пользователей, {{total_logs}} записей")

if __name__ == "__main__":
    restore_data()
"""
        
        # Сохраняем скрипт на сервере
        remote_script_path = "/tmp/restore_script.py"
        print("📝 Создаем скрипт восстановления на сервере...")
        with sftp.open(remote_script_path, 'w') as f:
            f.write(restore_script)
        
        # Запускаем восстановление
        print("🚀 Запускаем восстановление данных...")
        stdin, stdout, stderr = ssh.exec_command(f"cd /opt/render/project/src && python {remote_script_path}")
        
        # Читаем результат
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        
        if output:
            print("📊 Результат восстановления:")
            print(output)
        
        if error:
            print("❌ Ошибки:")
            print(error)
        
        # Очищаем временные файлы
        print("🧹 Очищаем временные файлы...")
        ssh.exec_command(f"rm {remote_csv_path} {remote_script_path}")
        
        sftp.close()
        ssh.close()
        
        print("✅ Восстановление завершено!")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при восстановлении: {e}")
        return False

def main():
    """Основная функция."""
    backup_date = sys.argv[1] if len(sys.argv) > 1 else "20250627_201421"
    
    print("🔄 ВОССТАНОВЛЕНИЕ ДАННЫХ НА RENDER")
    print("=" * 50)
    
    success = upload_backup_and_restore(backup_date)
    
    if success:
        print("\n🎉 Данные успешно восстановлены на сервере!")
        print("📱 Теперь можете проверить аналитику в боте")
    else:
        print("\n❌ Восстановление не удалось")

if __name__ == "__main__":
    main() 