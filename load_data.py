import paramiko
import sqlite3
import csv
import os

# Настройки подключения
ssh_user = "srv-cssvk3ogph6c7399j0gg"
ssh_host = "ssh.oregon.render.com"
ssh_key_path = os.path.expanduser("~/.ssh/id_rsa")
remote_db_path = "/MoodTrackerBot_data/mood_tracker.db"
local_db_path = "mood_tracker_local.db"

# Подключение к серверу
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(ssh_host, username=ssh_user, key_filename=ssh_key_path)

# Скачиваем файл базы данных на локальную машину
sftp = ssh.open_sftp()
sftp.get(remote_db_path, local_db_path)
sftp.close()

# Закрываем SSH-подключение
ssh.close()

print("База данных успешно скачана!")

# Подключение к локальной копии базы данных
conn = sqlite3.connect(local_db_path)
cursor = conn.cursor()

# Экспорт логов в CSV
output_csv = "logs.csv"
query = "SELECT * FROM logs"

cursor.execute(query)
rows = cursor.fetchall()

with open(output_csv, "w", newline="", encoding="utf-8") as csv_file:
    writer = csv.writer(csv_file)
    headers = [description[0] for description in cursor.description]
    writer.writerow(headers)
    writer.writerows(rows)

print(f"Логи успешно экспортированы в файл {output_csv}")

# Закрываем соединение с локальной базой данных
conn.close()

# Удаляем временный файл базы данных
os.remove(local_db_path)
print("Локальная копия базы данных удалена.")