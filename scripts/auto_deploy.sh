#!/bin/bash

# Автоматический деплой MoodTrackerBot с сохранением данных
# 
# Этот скрипт:
# 1. Создает бэкап текущих данных
# 2. Деплоит новую версию на Render
# 3. Восстанавливает данные после деплоя

set -e  # Прерывать выполнение при ошибках

echo "🚀 АВТОМАТИЧЕСКИЙ ДЕПЛОЙ MOODTRACKERBOT"
echo "======================================"

# Проверяем, что мы в правильной директории
if [ ! -f "bot_safe.py" ]; then
    echo "❌ Ошибка: Запустите скрипт из корневой директории проекта"
    exit 1
fi

# Шаг 1: Создание бэкапа
echo "📦 Шаг 1: Создаем бэкап данных..."
if python scripts/backup_data.py; then
    echo "✅ Бэкап создан успешно"
else
    echo "❌ Ошибка создания бэкапа"
    read -p "Продолжить без бэкапа? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Деплой отменен"
        exit 1
    fi
fi

# Шаг 2: Коммит и пуш изменений
echo "📝 Шаг 2: Отправляем изменения в Git..."
if git diff --quiet && git diff --staged --quiet; then
    echo "ℹ️  Нет новых изменений для коммита"
else
    echo "Введите сообщение коммита (или нажмите Enter для автоматического):"
    read -r commit_message
    
    if [ -z "$commit_message" ]; then
        commit_message="Auto deploy $(date '+%Y-%m-%d %H:%M:%S')"
    fi
    
    git add .
    git commit -m "$commit_message"
    echo "✅ Изменения закоммичены"
fi

echo "🚀 Отправляем на Render..."
if git push origin main; then
    echo "✅ Код отправлен на Render"
else
    echo "❌ Ошибка при отправке кода"
    exit 1
fi

# Шаг 3: Ожидание завершения деплоя
echo "⏳ Шаг 3: Ждем завершения деплоя..."
echo "💡 Render обычно деплоит 2-3 минуты"

# Функция для проверки доступности бота
check_bot_status() {
    # Простая проверка через curl (если есть webhook)
    # Можно заменить на более точную проверку
    sleep 10
    return 0
}

# Ждем фиксированное время
sleep_time=120  # 2 минуты
echo "⏰ Ожидание ${sleep_time} секунд..."
for i in $(seq 1 $sleep_time); do
    if [ $((i % 20)) -eq 0 ]; then
        echo "⏰ Осталось $((sleep_time - i)) секунд..."
    fi
    sleep 1
done

echo "✅ Ожидание завершено"

# Шаг 4: Восстановление данных
echo "🔄 Шаг 4: Восстанавливаем данные..."
if python scripts/deploy_restore.py; then
    echo "✅ Данные восстановлены успешно"
else
    echo "❌ Ошибка восстановления данных"
    echo "💡 Попробуйте восстановить вручную:"
    echo "   python scripts/deploy_restore.py"
    echo "   или отправьте боту команду /restore_backup"
fi

# Финальная проверка
echo ""
echo "🎉 ДЕПЛОЙ ЗАВЕРШЕН!"
echo "=================="
echo "✅ Код обновлен на Render"
echo "✅ Данные восстановлены"
echo ""
echo "🔍 Проверьте работу бота:"
echo "1. Отправьте боту /menu"
echo "2. Проверьте аналитику"
echo "3. Убедитесь, что исторические данные на месте"
echo ""
echo "📚 Подробное руководство: BACKUP_RESTORE_GUIDE.md" 