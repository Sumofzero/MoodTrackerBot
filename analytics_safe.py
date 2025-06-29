import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
from scipy.stats import sem, pearsonr
from sklearn.linear_model import LinearRegression
import os
import requests
from datetime import datetime, timedelta
import seaborn as sns
import tempfile

from config import DATA_DIR

def send_photo_via_api(token, chat_id, file_path, caption=None):
    """Отправляет фото через Telegram Bot API."""
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    with open(file_path, "rb") as photo:
        data = {"chat_id": chat_id}
        if caption:
            data["caption"] = caption  # Добавление подписи, если она передана
        response = requests.post(url, data=data, files={"photo": photo})
    return response.json()


def cleanup_temp_files(*file_paths):
    """Удаляет временные файлы графиков."""
    for file_path in file_paths:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            print(f"Не удалось удалить файл {file_path}: {e}")


def save_plot_as_image(func, filename, *args, **kwargs):
    """Сохраняет график как временный файл."""
    # Используем временную директорию для графиков
    temp_dir = tempfile.gettempdir()
    filepath = os.path.join(temp_dir, filename)
    
    func(*args, **kwargs)
    plt.savefig(filepath, format='png', dpi=300)
    plt.close()
    return filepath

def generate_and_send_charts(token, chat_id, df, state_type, logger, df_activities=None):
    """Универсальная функция для генерации и отправки графиков с автоматической очисткой."""
    # Настраиваем заголовки и файлы
    titles = {
        "emotion": {
            "daily": "Ежедневное эмоциональное состояние",
            "trend": "Тренд эмоционального состояния",
            "freq": "Частотный анализ эмоционального состояния",
        },
        "physical": {
            "daily": "Ежедневное физическое состояние",
            "trend": "Тренд физического состояния",
            "freq": "Частотный анализ физического состояния",
        },
    }

    # Создаем список временных файлов для последующей очистки
    temp_files = []
    
    # Проверяем количество данных
    data_count = len(df)
    logger.info(f"Generating charts for {data_count} data points")

    # Генерация графиков
    try:
        if data_count >= 3:
            # Полная аналитика для достаточного количества данных
            # Ежедневное состояние
            stats = calculate_stats(df)
            daily_states_path = save_plot_as_image(plot_daily_states, f"{state_type}_daily_states.png", stats, titles[state_type]["daily"], "Среднее состояние")
            temp_files.append(daily_states_path)

            # Тренд состояния
            trend_path = save_plot_as_image(plot_trend, f"{state_type}_trend.png", df, titles[state_type]["trend"], "Среднее состояние")
            temp_files.append(trend_path)

            # Частотный анализ (только если данных много)
            if data_count >= 5:
                freq_analysis_path = save_plot_as_image(plot_frequency_analysis, f"{state_type}_freq_analysis.png", df, titles[state_type]["freq"], "Амплитуда")
                temp_files.append(freq_analysis_path)

            # Отправка графиков через API
            charts_to_send = [
                (daily_states_path, titles[state_type]["daily"]),
                (trend_path, titles[state_type]["trend"]),
            ]
            if data_count >= 5 and len(temp_files) >= 3:
                charts_to_send.append((temp_files[2], titles[state_type]["freq"]))

        else:
            # Упрощённая аналитика для малых данных
            daily_states_path = save_plot_as_image(plot_simple_summary, f"{state_type}_daily_states.png", df, titles[state_type]["daily"], "Значения")
            temp_files.append(daily_states_path)
            charts_to_send = [(daily_states_path, f"{titles[state_type]['daily']} (базовый вид)")]

        # Отправка графиков
        for file_path, caption in charts_to_send:
            response = send_photo_via_api(token, chat_id, file_path, caption=caption)
            if response.get("ok"):
                logger.info(f"График {file_path} успешно отправлен")
            else:
                logger.error(f"Ошибка при отправке {file_path}: {response}")

    except Exception as e:
        logger.error(f"Ошибка при генерации или отправке графиков ({state_type}): {e}")
    finally:
        # Очищаем временные файлы в любом случае
        cleanup_temp_files(*temp_files)
        logger.info(f"Удалено {len(temp_files)} временных файлов графиков")



def calculate_stats(df, group_col='hour', confidence=0.8):
    stats = df.groupby(['day_type', group_col])['score'].agg(['mean', 'std']).reset_index()
    counts = df.groupby(['day_type', group_col])['score'].size().reset_index(name='count')
    stats = pd.merge(stats, counts, on=['day_type', group_col])

    # Для одиночных записей заполняем std нулём
    stats['std'] = stats['std'].fillna(0)

    def safe_confidence_interval(mean, std, count):
        if count > 1 and std > 0:
            margin = 1.28 * (std / (count ** 0.5))  # z-score for 80% CI
            return mean - margin, mean + margin
        else:
            # Для одиночных записей или нулевого std - узкий интервал
            margin = 0.1  # Небольшая погрешность для визуализации
            return mean - margin, mean + margin

    stats[['ci_lower', 'ci_upper']] = stats.apply(
        lambda row: pd.Series(safe_confidence_interval(row['mean'], row['std'], row['count'])), axis=1
    )
    return stats


def plot_daily_states(stats, title, ylabel, colormap=None):
    """Создает красивый график ежедневных состояний."""
    # Настройка современного стиля
    plt.style.use('default')
    
    if colormap is None:
        colormap = plt.cm.get_cmap('viridis')
    norm = mcolors.Normalize(vmin=stats['count'].min(), vmax=stats['count'].max())

    fig, axes = plt.subplots(2, 1, figsize=(16, 14), sharex=True)  # Увеличили высоту
    
    # Цветовая схема для разных дней
    weekday_color = '#3498DB'  # Синий для будних дней
    weekend_color = '#E74C3C'  # Красный для выходных

    for i, day_type in enumerate(['Будний день', 'Выходной']):
        df_day = stats[stats['day_type'] == day_type]
        
        # Выбираем цвет в зависимости от типа дня
        main_color = weekday_color if day_type == 'Будний день' else weekend_color
        
        # Проверяем, есть ли данные для текущего day_type
        if df_day.empty:
            axes[i].text(0.5, 0.5, f'Нет данных для\n{day_type}', 
                        ha='center', va='center', fontsize=16,
                        bbox=dict(boxstyle="round,pad=0.5", facecolor='lightgray', alpha=0.8))
            axes[i].set_title(f'{title} ({day_type})', fontsize=16, fontweight='bold', pad=25)  # Увеличили pad
            axes[i].grid(axis='y', linestyle='--', alpha=0.3)
            continue

        # Создаем красивые цвета на основе количества записей
        colors = colormap(norm(df_day['count']))

        # Основная линия с градиентом
        axes[i].plot(
            df_day['hour'].values, df_day['mean'].values, 
            color=main_color, linestyle='-', linewidth=3, 
            label=day_type, alpha=0.9
        )
        
        # Точки с размером, зависящим от количества записей
        scatter = axes[i].scatter(
            df_day['hour'].values, df_day['mean'].values,
            c=colors, s=df_day['count']*20+50, edgecolor='white', 
            linewidth=2, alpha=0.9, zorder=5
        )
        
        # Доверительные интервалы с красивой заливкой
        axes[i].fill_between(
            df_day['hour'].values,
            df_day['ci_lower'].values,
            df_day['ci_upper'].values,
            color=main_color, alpha=0.2, label='Доверительный интервал'
        )
        
        # Стилизация
        axes[i].set_title(f'{title} ({day_type})', fontsize=16, fontweight='bold', pad=25)  # Увеличили pad
        axes[i].set_ylabel(ylabel, fontsize=14, fontweight='bold')
        axes[i].grid(True, alpha=0.3, linestyle='--')
        axes[i].set_xticks(range(0, 24, 2))  # Каждые 2 часа
        axes[i].tick_params(axis='both', labelsize=12)
        
        # Добавляем подписи к точкам для важных значений
        for hour, mean_val, count in zip(df_day['hour'], df_day['mean'], df_day['count']):
            if count >= 3:  # Показываем подпись только для точек с достаточным количеством данных
                axes[i].annotate(f'{mean_val:.1f}', 
                               (hour, mean_val), 
                               textcoords="offset points", 
                               xytext=(0,10), 
                               ha='center', fontsize=10, fontweight='bold',
                               bbox=dict(boxstyle="round,pad=0.2", facecolor='white', alpha=0.8))

    # Общие настройки
    axes[1].set_xlabel('Час дня', fontsize=14, fontweight='bold')
    
    # Добавляем красивую легенду
    axes[0].legend(loc='upper right', fontsize=12, framealpha=0.9)
    
    # Общий заголовок с большим отступом
    fig.suptitle(f'{title}', fontsize=18, fontweight='bold', y=0.96)  # Увеличили y и убрали эмодзи
    
    # Добавляем информационную подпись
    fig.text(0.5, 0.02, 
            'Размер точек показывает количество записей. Затененная область - доверительный интервал.',
            ha='center', fontsize=11, style='italic',
            bbox=dict(boxstyle="round,pad=0.5", facecolor='lightblue', alpha=0.3))
    
    plt.tight_layout()
    plt.subplots_adjust(top=0.88, bottom=0.12)  # Уменьшили top


def plot_trend(df, title, ylabel):
    """Создает красивый график тренда с современным дизайном."""
    plt.style.use('default')
    
    daily_stats = df.groupby(df['timestamp'].dt.date)['score'].agg(['mean', sem]).reset_index()
    daily_stats.rename(columns={'mean': 'daily_mean', 'sem': 'daily_sem'}, inplace=True)
    daily_stats['ci_lower'] = daily_stats['daily_mean'] - 1.28 * daily_stats['daily_sem']
    daily_stats['ci_upper'] = daily_stats['daily_mean'] + 1.28 * daily_stats['daily_sem']

    daily_stats['timestamp'] = pd.to_datetime(daily_stats['timestamp'])
    daily_stats['date'] = pd.to_datetime(daily_stats['timestamp'].dt.date)
    daily_stats['date_index'] = (daily_stats['date'] - daily_stats['date'].min()).dt.days

    X = daily_stats['date_index'].values.reshape(-1, 1)
    regressor = LinearRegression()
    regressor.fit(X, daily_stats['daily_mean'])
    daily_stats['trend'] = regressor.predict(X)

    # Определяем направление тренда
    trend_slope = regressor.coef_[0]
    if trend_slope > 0.05:
        trend_color = '#27AE60'  # Зеленый для роста
        trend_text = 'Позитивный тренд'
    elif trend_slope < -0.05:
        trend_color = '#E74C3C'  # Красный для снижения
        trend_text = 'Негативный тренд'
    else:
        trend_color = '#F39C12'  # Оранжевый для стабильности
        trend_text = 'Стабильный тренд'

    plt.figure(figsize=(16, 10))
    
    # Основная линия данных
    plt.plot(daily_stats['date'].values, daily_stats['daily_mean'].values, 
            marker='o', color='#3498DB', linewidth=3, markersize=8, 
            label='Среднее значение', alpha=0.9, markeredgecolor='white', markeredgewidth=2)
    
    # Доверительный интервал с градиентом
    plt.fill_between(
        daily_stats['date'], daily_stats['ci_lower'], daily_stats['ci_upper'], 
        color='#3498DB', alpha=0.2, label='Доверительный интервал'
    )
    
    # Линия тренда
    plt.plot(daily_stats['date'].values, daily_stats['trend'].values, 
            color=trend_color, linestyle='--', linewidth=3, 
            label=f'{trend_text}', alpha=0.8)  # Убрали эмодзи
    
    # Стилизация
    plt.title(f'{title}', fontsize=18, fontweight='bold', pad=25)  # Убрали эмодзи и увеличили pad
    plt.xlabel('Дата', fontsize=14, fontweight='bold')
    plt.ylabel(ylabel, fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3, linestyle='--')
    plt.legend(fontsize=12, loc='best', framealpha=0.9)
    
    # Добавляем аннотации для экстремальных значений
    max_idx = daily_stats['daily_mean'].idxmax()
    min_idx = daily_stats['daily_mean'].idxmin()
    
    plt.annotate(f'Максимум: {daily_stats.loc[max_idx, "daily_mean"]:.1f}',
                xy=(daily_stats.loc[max_idx, 'date'], daily_stats.loc[max_idx, 'daily_mean']),
                xytext=(10, 10), textcoords='offset points',
                bbox=dict(boxstyle="round,pad=0.3", facecolor='lightgreen', alpha=0.8),
                arrowprops=dict(arrowstyle='->', color='green'))
    
    plt.annotate(f'Минимум: {daily_stats.loc[min_idx, "daily_mean"]:.1f}',
                xy=(daily_stats.loc[min_idx, 'date'], daily_stats.loc[min_idx, 'daily_mean']),
                xytext=(10, -20), textcoords='offset points',
                bbox=dict(boxstyle="round,pad=0.3", facecolor='lightcoral', alpha=0.8),
                arrowprops=dict(arrowstyle='->', color='red'))
    
    # Поворачиваем подписи дат для лучшей читаемости
    plt.xticks(rotation=45)
    plt.tick_params(axis='both', labelsize=12)
    
    # Добавляем информационную подпись
    plt.figtext(0.5, 0.02, 
               f'Анализ показывает {trend_text.lower()} за период. '
               f'Наклон тренда: {trend_slope:.3f} единиц в день.',
               ha='center', fontsize=11, style='italic',
               bbox=dict(boxstyle="round,pad=0.5", facecolor='lightblue', alpha=0.3))
    
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.15, top=0.92)  # Добавили top для отступа от заголовка


def plot_frequency_analysis(df, title, ylabel):
    daily_mean = df.groupby(df['timestamp'].dt.date)['score'].mean()
    fft_result = np.fft.fft(daily_mean)
    frequencies = np.fft.fftfreq(len(daily_mean))

    periods = 1 / frequencies[1:len(frequencies) // 2]
    amplitudes = np.abs(fft_result[1:len(frequencies) // 2])

    plt.figure(figsize=(14, 8))
    plt.plot(periods, amplitudes, color='green')
    plt.title(title, fontsize=16)
    plt.xlabel('Период (дни)', fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    # plt.xscale('log')
    plt.grid(axis='y', linestyle='--', alpha=0.7)


def plot_simple_summary(df, title, ylabel):
    """Простой график для малого количества данных."""
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Простой bar chart с датами
    dates = pd.to_datetime(df['timestamp']).dt.strftime('%m-%d %H:%M')
    scores = df['score']
    
    bars = ax.bar(range(len(dates)), scores, color='lightblue', alpha=0.7)
    ax.set_xlabel('Время записи')
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticks(range(len(dates)))
    ax.set_xticklabels(dates, rotation=45, ha='right')
    ax.grid(True, alpha=0.3)
    
    # Добавляем значения на столбцы
    for i, (bar, score) in enumerate(zip(bars, scores)):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
                f'{score:.1f}', ha='center', va='bottom')
    
    plt.tight_layout()
    return fig


def analyze_activity_correlation(df_emotion, df_physical, df_activities):
    """
    Анализирует корреляцию между активностями и эмоциональным/физическим состоянием.
    
    Args:
        df_emotion: DataFrame с эмоциональными данными
        df_physical: DataFrame с физическими данными  
        df_activities: DataFrame с активностями
    
    Returns:
        dict: Результаты корреляционного анализа
    """
    correlations = {
        'emotion': {},
        'physical': {},
        'activity_stats': {},
        'insights': []
    }
    
    # Объединяем данные по времени (в пределах 1 часа)
    merged_emotion = merge_activities_with_states(df_activities, df_emotion, 'emotion')
    merged_physical = merge_activities_with_states(df_activities, df_physical, 'physical')
    
    if len(merged_emotion) < 5:  # Недостаточно данных для анализа
        return correlations
    
    # Анализируем корреляции для эмоций
    activity_emotion_stats = merged_emotion.groupby('activity')['score'].agg(['mean', 'std', 'count']).reset_index()
    activity_emotion_stats = activity_emotion_stats[activity_emotion_stats['count'] >= 2]  # Минимум 2 записи
    
    # Анализируем корреляции для физического состояния
    activity_physical_stats = merged_physical.groupby('activity')['score'].agg(['mean', 'std', 'count']).reset_index()
    activity_physical_stats = activity_physical_stats[activity_physical_stats['count'] >= 2]
    
    # Сохраняем статистики
    correlations['emotion'] = activity_emotion_stats.to_dict('records')
    correlations['physical'] = activity_physical_stats.to_dict('records')
    
    # Генерируем инсайты
    correlations['insights'] = generate_activity_insights(activity_emotion_stats, activity_physical_stats)
    
    return correlations


def merge_activities_with_states(df_activities, df_states, state_type):
    """Объединяет активности с состояниями по времени."""
    merged_data = []
    
    for _, activity_row in df_activities.iterrows():
        activity_time = pd.to_datetime(activity_row['timestamp'])
        
        # Ищем ближайшее состояние в пределах 2 часов после активности
        time_diff = pd.to_datetime(df_states['timestamp']) - activity_time
        valid_states = df_states[
            (time_diff >= pd.Timedelta(0)) & 
            (time_diff <= pd.Timedelta(hours=2))
        ]
        
        if not valid_states.empty:
            # Берем ближайшее по времени состояние
            closest_state = valid_states.loc[time_diff[valid_states.index].idxmin()]
            merged_data.append({
                'activity': activity_row['activity'],
                'score': closest_state['score'],
                'timestamp': activity_time,
                'state_timestamp': closest_state['timestamp']
            })
    
    return pd.DataFrame(merged_data)


def generate_activity_insights(emotion_stats, physical_stats):
    """Генерирует красивые и информативные текстовые инсайты на основе корреляций."""
    insights = []
    
    if not emotion_stats.empty:
        # Сортируем по среднему значению
        emotion_stats_sorted = emotion_stats.sort_values('mean', ascending=False)
        best_emotion = emotion_stats_sorted.iloc[0]
        worst_emotion = emotion_stats_sorted.iloc[-1]
        
        # Определяем уровень настроения
        def get_mood_level(score):
            if score >= 8:
                return "отличное", "🌟"
            elif score >= 6:
                return "хорошее", "😊"
            elif score >= 4:
                return "среднее", "😐"
            else:
                return "низкое", "😔"
        
        best_level, best_emoji = get_mood_level(best_emotion['mean'])
        worst_level, worst_emoji = get_mood_level(worst_emotion['mean'])
        
        insights.append(
            f"{best_emoji} *Лучшая активность для настроения:* "
            f"**{best_emotion['activity']}**\n"
            f"   └ Средний балл: {best_emotion['mean']:.1f}/10 ({best_level} настроение)\n"
            f"   └ Количество записей: {best_emotion['count']}"
        )
        
        if len(emotion_stats) > 1:  # Показываем худшую только если есть разные активности
            insights.append(
                f"{worst_emoji} *Активность с низким настроением:* "
                f"**{worst_emotion['activity']}**\n"
                f"   └ Средний балл: {worst_emotion['mean']:.1f}/10 ({worst_level} настроение)\n"
                f"   └ Количество записей: {worst_emotion['count']}"
            )
        
        # Добавляем общую статистику по эмоциям
        total_records = emotion_stats['count'].sum()
        avg_mood = (emotion_stats['mean'] * emotion_stats['count']).sum() / total_records
        avg_level, avg_emoji = get_mood_level(avg_mood)
        
        insights.append(
            f"📊 *Общая статистика настроения:*\n"
            f"   └ Средний балл по всем активностям: {avg_mood:.1f}/10 ({avg_level})\n"
            f"   └ Всего проанализировано записей: {total_records}"
        )
    
    if not physical_stats.empty:
        # Сортируем по среднему значению
        physical_stats_sorted = physical_stats.sort_values('mean', ascending=False)
        best_physical = physical_stats_sorted.iloc[0]
        worst_physical = physical_stats_sorted.iloc[-1]
        
        # Определяем уровень физического состояния
        def get_physical_level(score):
            if score >= 4:
                return "отличное", "💪"
            elif score >= 3:
                return "нормальное", "👍"
            elif score >= 2:
                return "плохое", "😟"
            else:
                return "очень плохое", "🤢"
        
        best_p_level, best_p_emoji = get_physical_level(best_physical['mean'])
        worst_p_level, worst_p_emoji = get_physical_level(worst_physical['mean'])
        
        insights.append(
            f"{best_p_emoji} *Лучшая активность для физического состояния:* "
            f"**{best_physical['activity']}**\n"
            f"   └ Средний балл: {best_physical['mean']:.1f}/5 ({best_p_level} состояние)\n"
            f"   └ Количество записей: {best_physical['count']}"
        )
        
        if len(physical_stats) > 1:
            insights.append(
                f"{worst_p_emoji} *Активность с низким физическим состоянием:* "
                f"**{worst_physical['activity']}**\n"
                f"   └ Средний балл: {worst_physical['mean']:.1f}/5 ({worst_p_level} состояние)\n"
                f"   └ Количество записей: {worst_physical['count']}"
            )
        
        # Добавляем общую статистику по физическому состоянию
        total_p_records = physical_stats['count'].sum()
        avg_physical = (physical_stats['mean'] * physical_stats['count']).sum() / total_p_records
        avg_p_level, avg_p_emoji = get_physical_level(avg_physical)
        
        insights.append(
            f"🏃 *Общая статистика физического состояния:*\n"
            f"   └ Средний балл по всем активностям: {avg_physical:.1f}/5 ({avg_p_level})\n"
            f"   └ Всего проанализировано записей: {total_p_records}"
        )
    
    # Добавляем персонализированные рекомендации
    if not emotion_stats.empty and not physical_stats.empty:
        # Находим активности, которые хороши и для настроения, и для физического состояния
        emotion_dict = {row['activity']: row['mean'] for _, row in emotion_stats.iterrows()}
        physical_dict = {row['activity']: row['mean'] for _, row in physical_stats.iterrows()}
        
        best_overall = []
        for activity in emotion_dict:
            if activity in physical_dict:
                combined_score = (emotion_dict[activity] / 10 * 0.6) + (physical_dict[activity] / 5 * 0.4)
                best_overall.append((activity, combined_score, emotion_dict[activity], physical_dict[activity]))
        
        if best_overall:
            best_overall.sort(key=lambda x: x[1], reverse=True)
            top_activity = best_overall[0]
            
            insights.append(
                f"🎯 *Персональная рекомендация:*\n"
                f"   └ **{top_activity[0]}** - лучший баланс для общего самочувствия\n"
                f"   └ Настроение: {top_activity[2]:.1f}/10, Физическое: {top_activity[3]:.1f}/5"
            )
    
    return insights


def plot_activity_correlation(correlations, title="Корреляция активностей и состояния"):
    """Создает красивый график корреляции между активностями и состояниями."""
    # Настройка стиля для красивых графиков
    plt.style.use('default')
    
    # Создаем фигуру с оптимальным размером для мобильных
    fig = plt.figure(figsize=(16, 12))  # Увеличили высоту
    
    # Создаем сетку для размещения элементов с большими отступами
    gs = fig.add_gridspec(2, 2, height_ratios=[1, 1], width_ratios=[1, 1], 
                         hspace=0.4, wspace=0.3)  # Увеличили hspace
    
    # Цветовые схемы для красоты
    emotion_colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD', '#98D8C8']
    physical_colors = ['#6C5CE7', '#A29BFE', '#74B9FF', '#0984E3', '#00B894', '#00CEC9', '#81ECEC']
    
    # График для эмоционального состояния
    ax1 = fig.add_subplot(gs[0, :])  # Занимает всю верхнюю строку
    
    if correlations['emotion']:
        emotion_df = pd.DataFrame(correlations['emotion'])
        emotion_df = emotion_df.sort_values('mean', ascending=True)
        
        # Создаем красивые цвета на основе значений
        colors = []
        for mean_val in emotion_df['mean']:
            if mean_val >= 8:
                colors.append('#2ECC71')  # Зеленый для отличных
            elif mean_val >= 6:
                colors.append('#F39C12')  # Оранжевый для хороших
            elif mean_val >= 4:
                colors.append('#E74C3C')  # Красный для плохих
            else:
                colors.append('#8E44AD')  # Фиолетовый для очень плохих
        
        # Создаем горизонтальный bar chart
        bars = ax1.barh(emotion_df['activity'], emotion_df['mean'], 
                       color=colors, alpha=0.8, edgecolor='white', linewidth=2)
        
        # Добавляем градиент эффект
        for i, (bar, mean_val, count) in enumerate(zip(bars, emotion_df['mean'], emotion_df['count'])):
            # Добавляем текст с количеством записей
            ax1.text(bar.get_width() + 0.2, bar.get_y() + bar.get_height()/2, 
                    f'{mean_val:.1f}\n({count} зап.)', 
                    va='center', ha='left', fontweight='bold', fontsize=11,
                    bbox=dict(boxstyle="round,pad=0.3", facecolor='white', alpha=0.8))
        
        # Стилизация оси
        ax1.set_xlabel('Среднее эмоциональное состояние (1-10)', fontsize=14, fontweight='bold')
        ax1.set_title('Влияние активностей на настроение', fontsize=16, fontweight='bold', pad=25)  # Увеличили pad
        ax1.grid(True, alpha=0.3, linestyle='--')
        ax1.set_xlim(0, 11)
        
        # Добавляем цветовую шкалу
        ax1.axvspan(8, 11, alpha=0.1, color='green', label='Отличное настроение')
        ax1.axvspan(6, 8, alpha=0.1, color='orange', label='Хорошее настроение') 
        ax1.axvspan(4, 6, alpha=0.1, color='red', label='Плохое настроение')
        ax1.axvspan(0, 4, alpha=0.1, color='purple', label='Очень плохое настроение')
        
        # Красивые подписи осей
        ax1.tick_params(axis='y', labelsize=12)
        ax1.tick_params(axis='x', labelsize=12)
        
    else:
        ax1.text(0.5, 0.5, 'Недостаточно данных\nдля анализа эмоций\n\n(нужно минимум 5 записей)', 
                ha='center', va='center', transform=ax1.transAxes, 
                fontsize=16, bbox=dict(boxstyle="round,pad=0.5", facecolor='lightgray', alpha=0.8))
        ax1.set_title('Влияние активностей на настроение', fontsize=16, fontweight='bold', pad=25)
    
    # График для физического состояния
    ax2 = fig.add_subplot(gs[1, :])  # Занимает всю нижнюю строку
    
    if correlations['physical']:
        physical_df = pd.DataFrame(correlations['physical'])
        physical_df = physical_df.sort_values('mean', ascending=True)
        
        # Создаем красивые цвета на основе значений
        colors = []
        for mean_val in physical_df['mean']:
            if mean_val >= 4:
                colors.append('#27AE60')  # Зеленый для отличного
            elif mean_val >= 3:
                colors.append('#F1C40F')  # Желтый для нормального
            elif mean_val >= 2:
                colors.append('#E67E22')  # Оранжевый для плохого
            else:
                colors.append('#E74C3C')  # Красный для очень плохого
        
        bars = ax2.barh(physical_df['activity'], physical_df['mean'], 
                       color=colors, alpha=0.8, edgecolor='white', linewidth=2)
        
        # Добавляем информативные подписи
        for bar, mean_val, count in zip(bars, physical_df['mean'], physical_df['count']):
            ax2.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2, 
                    f'{mean_val:.1f}\n({count} зап.)', 
                    va='center', ha='left', fontweight='bold', fontsize=11,
                    bbox=dict(boxstyle="round,pad=0.3", facecolor='white', alpha=0.8))
        
        ax2.set_xlabel('Среднее физическое состояние (1-5)', fontsize=14, fontweight='bold')
        ax2.set_title('Влияние активностей на физическое состояние', fontsize=16, fontweight='bold', pad=25)
        ax2.grid(True, alpha=0.3, linestyle='--')
        ax2.set_xlim(0, 6)
        
        # Добавляем цветовую шкалу для физического состояния
        ax2.axvspan(4, 6, alpha=0.1, color='green', label='Отличное состояние')
        ax2.axvspan(3, 4, alpha=0.1, color='yellow', label='Нормальное состояние')
        ax2.axvspan(2, 3, alpha=0.1, color='orange', label='Плохое состояние')
        ax2.axvspan(0, 2, alpha=0.1, color='red', label='Очень плохое состояние')
        
        ax2.tick_params(axis='y', labelsize=12)
        ax2.tick_params(axis='x', labelsize=12)
        
    else:
        ax2.text(0.5, 0.5, 'Недостаточно данных\nдля анализа физического\nсостояния\n\n(нужно минимум 3 записи)', 
                ha='center', va='center', transform=ax2.transAxes, 
                fontsize=16, bbox=dict(boxstyle="round,pad=0.5", facecolor='lightgray', alpha=0.8))
        ax2.set_title('Влияние активностей на физическое состояние', fontsize=16, fontweight='bold', pad=25)
    
    # Общий заголовок с большим отступом
    fig.suptitle('Корреляционный анализ активностей и самочувствия', 
                fontsize=20, fontweight='bold', y=0.96)  # Увеличили y
    
    # Добавляем подпись с информацией
    fig.text(0.5, 0.02, 
            'Анализ показывает, какие активности лучше всего влияют на ваше состояние.\n'
            'Чем правее столбец, тем лучше влияние активности на самочувствие.',
            ha='center', fontsize=12, style='italic', 
            bbox=dict(boxstyle="round,pad=0.5", facecolor='lightblue', alpha=0.3))
    
    # Настройка общего стиля с увеличенными отступами
    plt.tight_layout()
    plt.subplots_adjust(top=0.88, bottom=0.15)  # Уменьшили top для большего отступа
    
    return fig


def generate_correlation_insights_text(correlations):
    """Генерирует красивое текстовое описание инсайтов корреляционного анализа."""
    if not correlations['insights']:
        return (
            "📊 *Корреляционный анализ*\n\n"
            "🔍 Недостаточно данных для полного анализа.\n\n"
            "📈 *Для получения инсайтов необходимо:*\n"
            "• Минимум 5 записей активностей\n"
            "• Минимум 5 записей эмоционального состояния\n"
            "• Минимум 3 записи физического состояния\n\n"
            "🎯 Продолжайте заполнять дневник, и вы получите персонализированные рекомендации!"
        )
    
    insights_text = "📊 *Корреляционный анализ активностей*\n\n"
    
    for i, insight in enumerate(correlations['insights']):
        insights_text += f"{insight}\n\n"
    
    insights_text += (
        "💡 *Как использовать эти данные:*\n"
        "• Планируйте больше активностей с высокими баллами\n"
        "• Обратите внимание на паттерны в своем поведении\n"
        "• Используйте 'хорошие' активности в трудные дни\n\n"
        "🔄 Анализ обновляется с каждой новой записью!"
    )
    
    return insights_text


def generate_and_send_correlation_analysis(token, chat_id, df_emotion, df_physical, df_activities, logger):
    """
    Генерирует и отправляет корреляционный анализ активностей и состояний.
    
    Args:
        token: Telegram bot token
        chat_id: ID чата для отправки
        df_emotion: DataFrame с эмоциональными данными
        df_physical: DataFrame с физическими данными
        df_activities: DataFrame с активностями
        logger: Logger для логирования
    """
    correlation_path = None
    try:
        # Проверяем, достаточно ли данных
        if len(df_activities) < 5 or len(df_emotion) < 5:
            logger.info("Недостаточно данных для корреляционного анализа")
            return
        
        # Анализируем корреляции
        correlations = analyze_activity_correlation(df_emotion, df_physical, df_activities)
        
        # Генерируем график корреляций
        correlation_path = save_plot_as_image(plot_activity_correlation, "activity_correlation.png", correlations)
        
        # Отправляем график
        response = send_photo_via_api(
            token, 
            chat_id, 
            correlation_path, 
            caption="📊 Корреляционный анализ активностей и состояний"
        )
        
        if response.get("ok"):
            logger.info("Корреляционный анализ успешно отправлен")
            
            # Отправляем текстовые инсайты
            insights_text = generate_correlation_insights_text(correlations)
            if insights_text:
                import requests
                url = f"https://api.telegram.org/bot{token}/sendMessage"
                data = {
                    "chat_id": chat_id,
                    "text": insights_text,
                    "parse_mode": "Markdown"
                }
                response = requests.post(url, data=data)
                if response.json().get("ok"):
                    logger.info("Инсайты корреляционного анализа отправлены")
                else:
                    logger.error(f"Ошибка при отправке инсайтов: {response.json()}")
        else:
            logger.error(f"Ошибка при отправке корреляционного анализа: {response}")
            
    except Exception as e:
        logger.error(f"Ошибка при генерации корреляционного анализа: {e}")
    finally:
        # Очищаем временный файл
        if correlation_path:
            cleanup_temp_files(correlation_path)
            logger.info("Удален временный файл корреляционного анализа")


def should_generate_correlation_analysis(df_emotion, df_physical, df_activities):
    """
    Определяет, следует ли генерировать корреляционный анализ.
    
    Returns:
        bool: True если достаточно данных для анализа
    """
    return (
        len(df_activities) >= 5 and 
        len(df_emotion) >= 5 and 
        len(df_physical) >= 3
    ) 

# ======================== НОВЫЕ ТИПЫ ГРАФИКОВ ========================

def plot_heatmap_mood(df, title="Тепловая карта настроения"):
    """Создает тепловую карту настроения по дням недели и часам."""
    plt.style.use('default')
    
    # Подготавливаем данные
    df_copy = df.copy()
    df_copy['day_of_week'] = df_copy['timestamp'].dt.day_name()
    df_copy['hour'] = df_copy['timestamp'].dt.hour
    
    # Переводим дни недели на русский и устанавливаем правильный порядок
    day_mapping = {
        'Monday': 'Понедельник',
        'Tuesday': 'Вторник', 
        'Wednesday': 'Среда',
        'Thursday': 'Четверг',
        'Friday': 'Пятница',
        'Saturday': 'Суббота',
        'Sunday': 'Воскресенье'
    }
    df_copy['day_of_week'] = df_copy['day_of_week'].map(day_mapping)
    
    # Создаем сводную таблицу
    heatmap_data = df_copy.groupby(['day_of_week', 'hour'])['score'].mean().unstack(fill_value=0)
    
    # Переупорядочиваем дни недели
    day_order = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
    heatmap_data = heatmap_data.reindex(day_order)
    
    # Создаем график
    fig, ax = plt.subplots(figsize=(16, 10))
    
    # Создаем тепловую карту
    im = ax.imshow(heatmap_data.values, cmap='RdYlGn', aspect='auto', vmin=1, vmax=10)
    
    # Настраиваем оси
    ax.set_xticks(range(len(heatmap_data.columns)))
    ax.set_xticklabels([f'{hour}:00' for hour in heatmap_data.columns], rotation=45)
    ax.set_yticks(range(len(heatmap_data.index)))
    ax.set_yticklabels(heatmap_data.index)
    
    # Добавляем значения в ячейки
    for i in range(len(heatmap_data.index)):
        for j in range(len(heatmap_data.columns)):
            value = heatmap_data.iloc[i, j]
            if value > 0:  # Показываем только непустые значения
                text_color = 'white' if value < 5.5 else 'black'
                ax.text(j, i, f'{value:.1f}', ha='center', va='center', 
                       color=text_color, fontweight='bold', fontsize=10)
    
    # Цветовая шкала
    cbar = plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label('Среднее настроение', fontsize=12, fontweight='bold')
    
    # Заголовки и подписи
    ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Час дня', fontsize=14, fontweight='bold')
    ax.set_ylabel('День недели', fontsize=14, fontweight='bold')
    
    plt.tight_layout()


def plot_weekly_monthly_trends(df, title, period='week'):
    """Создает график трендов по неделям или месяцам."""
    plt.style.use('default')
    
    df_copy = df.copy()
    
    if period == 'week':
        # Группировка по неделям
        df_copy['period'] = df_copy['timestamp'].dt.to_period('W')
        df_copy['period_str'] = df_copy['period'].dt.strftime('%Y-W%U')
        period_label = 'Неделя'
        x_rotation = 45
    else:  # month
        # Группировка по месяцам
        df_copy['period'] = df_copy['timestamp'].dt.to_period('M')
        df_copy['period_str'] = df_copy['period'].dt.strftime('%Y-%m')
        period_label = 'Месяц'
        x_rotation = 0
    
    # Вычисляем статистики по периодам
    stats = df_copy.groupby('period_str')['score'].agg(['mean', 'std', 'count']).reset_index()
    stats['std'] = stats['std'].fillna(0)
    
    # Создаем график
    fig, ax = plt.subplots(figsize=(16, 10))
    
    # Основная линия тренда
    ax.plot(range(len(stats)), stats['mean'], 
           color='#3498DB', linewidth=3, marker='o', markersize=8, 
           label='Средние значения', alpha=0.9)
    
    # Доверительные интервалы
    ax.fill_between(range(len(stats)), 
                   stats['mean'] - stats['std'], 
                   stats['mean'] + stats['std'],
                   color='#3498DB', alpha=0.2, label='Стандартное отклонение')
    
    # Добавляем точки с размером, зависящим от количества записей
    scatter = ax.scatter(range(len(stats)), stats['mean'], 
                        s=stats['count']*30+50, c=stats['mean'], 
                        cmap='RdYlGn', vmin=1, vmax=10, 
                        edgecolor='white', linewidth=2, alpha=0.9, zorder=5)
    
    # Подписи к важным точкам
    for i, (mean_val, count) in enumerate(zip(stats['mean'], stats['count'])):
        if count >= 3:  # Показываем подпись только для периодов с достаточными данными
            ax.annotate(f'{mean_val:.1f}', (i, mean_val), 
                       textcoords="offset points", xytext=(0,15), 
                       ha='center', fontsize=10, fontweight='bold',
                       bbox=dict(boxstyle="round,pad=0.2", facecolor='white', alpha=0.8))
    
    # Цветовая шкала
    cbar = plt.colorbar(scatter, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label('Среднее значение', fontsize=12, fontweight='bold')
    
    # Настройка осей
    ax.set_xticks(range(len(stats)))
    ax.set_xticklabels(stats['period_str'], rotation=x_rotation, ha='right' if x_rotation > 0 else 'center')
    ax.set_ylabel('Среднее значение', fontsize=14, fontweight='bold')
    ax.set_xlabel(period_label, fontsize=14, fontweight='bold')
    ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
    
    # Сетка и легенда
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.legend(loc='upper right', fontsize=12)
    
    # Устанавливаем пределы по Y
    y_min = max(1, stats['mean'].min() - 1)
    y_max = min(10, stats['mean'].max() + 1)
    ax.set_ylim(y_min, y_max)
    
    plt.tight_layout()


def plot_period_comparison(df, title, comparison_type='last_weeks'):
    """Создает сравнительные графики между периодами."""
    plt.style.use('default')
    
    df_copy = df.copy()
    current_time = df_copy['timestamp'].max()
    
    if comparison_type == 'last_weeks':
        # Сравнение последних недель
        period_1_start = current_time - timedelta(weeks=2)
        period_1_end = current_time - timedelta(weeks=1)
        period_2_start = current_time - timedelta(weeks=1)
        period_2_end = current_time
        
        period_1_label = 'Предыдущая неделя'
        period_2_label = 'Текущая неделя'
        
    else:  # last_months
        # Сравнение последних месяцев
        period_1_start = current_time - timedelta(days=60)
        period_1_end = current_time - timedelta(days=30)
        period_2_start = current_time - timedelta(days=30)
        period_2_end = current_time
        
        period_1_label = 'Предыдущий месяц'
        period_2_label = 'Текущий месяц'
    
    # Фильтруем данные по периодам
    period_1_data = df_copy[
        (df_copy['timestamp'] >= period_1_start) & 
        (df_copy['timestamp'] < period_1_end)
    ].copy()
    
    period_2_data = df_copy[
        (df_copy['timestamp'] >= period_2_start) & 
        (df_copy['timestamp'] < period_2_end)
    ].copy()
    
    if period_1_data.empty or period_2_data.empty:
        # Если нет данных для сравнения, показываем простое сообщение
        fig, ax = plt.subplots(figsize=(12, 8))
        ax.text(0.5, 0.5, 'Недостаточно данных\nдля сравнения периодов', 
               ha='center', va='center', fontsize=16,
               bbox=dict(boxstyle="round,pad=0.5", facecolor='lightgray', alpha=0.8))
        ax.set_title(title, fontsize=16, fontweight='bold')
        ax.axis('off')
        return
    
    # Группируем по часам для сравнения
    period_1_data['hour'] = period_1_data['timestamp'].dt.hour
    period_2_data['hour'] = period_2_data['timestamp'].dt.hour
    
    period_1_stats = period_1_data.groupby('hour')['score'].agg(['mean', 'count']).reset_index()
    period_2_stats = period_2_data.groupby('hour')['score'].agg(['mean', 'count']).reset_index()
    
    # Создаем график
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12), sharex=True)
    
    # График для первого периода
    ax1.plot(period_1_stats['hour'], period_1_stats['mean'], 
            color='#E74C3C', linewidth=3, marker='o', markersize=8, 
            label=period_1_label, alpha=0.9)
    ax1.scatter(period_1_stats['hour'], period_1_stats['mean'], 
               s=period_1_stats['count']*20+50, c='#E74C3C', 
               edgecolor='white', linewidth=2, alpha=0.7, zorder=5)
    
    # График для второго периода
    ax2.plot(period_2_stats['hour'], period_2_stats['mean'], 
            color='#3498DB', linewidth=3, marker='o', markersize=8, 
            label=period_2_label, alpha=0.9)
    ax2.scatter(period_2_stats['hour'], period_2_stats['mean'], 
               s=period_2_stats['count']*20+50, c='#3498DB', 
               edgecolor='white', linewidth=2, alpha=0.7, zorder=5)
    
    # Настройка графиков
    for ax, stats, color, label in [(ax1, period_1_stats, '#E74C3C', period_1_label),
                                   (ax2, period_2_stats, '#3498DB', period_2_label)]:
        ax.set_ylabel('Среднее значение', fontsize=12, fontweight='bold')
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.legend(loc='upper right', fontsize=12)
        ax.set_ylim(1, 10)
        ax.set_title(f'{title} - {label}', fontsize=14, fontweight='bold', pad=15)
        
        # Добавляем средние линии
        avg_value = stats['mean'].mean()
        ax.axhline(y=avg_value, color=color, linestyle='--', alpha=0.5, 
                  label=f'Среднее: {avg_value:.1f}')
        ax.legend(loc='upper right', fontsize=10)
    
    ax2.set_xlabel('Час дня', fontsize=12, fontweight='bold')
    ax2.set_xticks(range(0, 24, 2))
    
    plt.tight_layout()


def generate_and_send_new_charts(token, chat_id, df, chart_type, state_type, logger):
    """Генерирует и отправляет новые типы графиков с автоматической очисткой."""
    file_path = None
    try:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        if chart_type == "heatmap":
            filename = f"{state_type}_heatmap_{timestamp}.png"
            file_path = save_plot_as_image(plot_heatmap_mood, filename, df, f"Тепловая карта {state_type}")
            caption = f"🔥 Тепловая карта настроения по дням недели и часам"
            
        elif chart_type == "weekly_trends":
            filename = f"{state_type}_weekly_trends_{timestamp}.png"
            file_path = save_plot_as_image(plot_weekly_monthly_trends, filename, df, f"Тренды по неделям - {state_type}", 'week')
            caption = f"📈 Тренды по неделям"
            
        elif chart_type == "monthly_trends":
            filename = f"{state_type}_monthly_trends_{timestamp}.png"
            file_path = save_plot_as_image(plot_weekly_monthly_trends, filename, df, f"Тренды по месяцам - {state_type}", 'month')
            caption = f"📊 Тренды по месяцам"
            
        elif chart_type == "period_comparison":
            filename = f"{state_type}_period_comparison_{timestamp}.png"
            file_path = save_plot_as_image(plot_period_comparison, filename, df, f"Сравнение периодов - {state_type}", 'last_weeks')
            caption = f"⚖️ Сравнение последних недель"
            
        else:
            logger.error(f"Unknown chart type: {chart_type}")
            return False
        
        # Отправляем график
        response = send_photo_via_api(token, chat_id, file_path, caption=caption)
        
        if response.get("ok"):
            logger.info(f"New chart {chart_type} sent successfully")
            return True
        else:
            logger.error(f"Failed to send chart {chart_type}: {response}")
            return False
            
    except Exception as e:
        logger.error(f"Error generating new chart {chart_type}: {e}")
        return False
    finally:
        # Очищаем временный файл
        if file_path:
            cleanup_temp_files(file_path)
            logger.info(f"Удален временный файл графика {chart_type}")


def should_generate_new_charts(df, chart_type):
    """Проверяет, достаточно ли данных для генерации новых типов графиков."""
    data_count = len(df)
    
    min_requirements = {
        "heatmap": 10,        # Минимум для тепловой карты (несколько дней)
        "weekly_trends": 14,  # Минимум для недельных трендов (2 недели)
        "monthly_trends": 30, # Минимум для месячных трендов (1 месяц)
        "period_comparison": 20  # Минимум для сравнения периодов
    }
    
    required = min_requirements.get(chart_type, 5)
    return data_count >= required


# ---------------------------------------------------------------------------------
# Интеллектуальные инсайты - анализ паттернов и рекомендации
# ---------------------------------------------------------------------------------

def calculate_trend_direction(df, weeks=4):
    """Вычисляет направление тренда за последние N недель."""
    try:
        from datetime import datetime, timedelta
        from sklearn.linear_model import LinearRegression
        
        # Берем данные за последние недели
        cutoff_date = df['timestamp'].max() - timedelta(weeks=weeks)
        recent_df = df[df['timestamp'] >= cutoff_date].copy()
        
        if len(recent_df) < 5:
            return 0  # Недостаточно данных
        
        # Создаем числовые представления времени для регрессии
        recent_df['days_from_start'] = (recent_df['timestamp'] - recent_df['timestamp'].min()).dt.days
        
        X = recent_df['days_from_start'].values.reshape(-1, 1)
        y = recent_df['score'].values
        
        model = LinearRegression()
        model.fit(X, y)
        
        # Возвращаем коэффициент наклона (тренд)
        return model.coef_[0] * 7  # Умножаем на 7 для получения недельного тренда
        
    except Exception as e:
        logger.error(f"Error calculating trend: {e}")
        return 0


def analyze_mood_patterns(df):
    """Анализирует паттерны настроения и генерирует текстовые выводы."""
    insights = []
    
    if len(df) < 5:
        return ["📊 Пока данных недостаточно для глубокого анализа. Продолжайте записывать настроение!"]
    
    try:
        # Анализ по часам дня
        df['hour'] = df['timestamp'].dt.hour
        hourly_stats = df.groupby('hour')['score'].agg(['mean', 'count']).reset_index()
        hourly_stats = hourly_stats[hourly_stats['count'] >= 2]  # Только часы с достаточными данными
        
        if len(hourly_stats) > 0:
            best_hour = hourly_stats.loc[hourly_stats['mean'].idxmax()]
            worst_hour = hourly_stats.loc[hourly_stats['mean'].idxmin()]
            
            insights.append(f"⏰ Ваш лучший час: {int(best_hour['hour'])}:00 (среднее: {best_hour['mean']:.1f}/10)")
            insights.append(f"⏰ Сложное время: {int(worst_hour['hour'])}:00 (среднее: {worst_hour['mean']:.1f}/10)")
        
        # Анализ по дням недели
        df['day_name'] = df['timestamp'].dt.day_name()
        day_mapping = {
            'Monday': 'Понедельник', 'Tuesday': 'Вторник', 'Wednesday': 'Среда',
            'Thursday': 'Четверг', 'Friday': 'Пятница', 'Saturday': 'Суббота', 'Sunday': 'Воскресенье'
        }
        df['day_name_ru'] = df['day_name'].map(day_mapping)
        
        daily_stats = df.groupby('day_name_ru')['score'].agg(['mean', 'count']).reset_index()
        daily_stats = daily_stats[daily_stats['count'] >= 2]
        
        if len(daily_stats) > 0:
            best_day = daily_stats.loc[daily_stats['mean'].idxmax()]
            worst_day = daily_stats.loc[daily_stats['mean'].idxmin()]
            
            insights.append(f"📅 Лучший день недели: {best_day['day_name_ru']} ({best_day['mean']:.1f}/10)")
            insights.append(f"📅 Сложный день: {worst_day['day_name_ru']} ({worst_day['mean']:.1f}/10)")
        
        # Анализ тренда
        trend = calculate_trend_direction(df, weeks=4)
        if trend > 0.15:
            insights.append(f"📈 Отличные новости! За последний месяц ваше настроение улучшилось на {trend:.1f} балла в неделю")
        elif trend < -0.15:
            insights.append(f"📉 За последний месяц наметился спад настроения на {abs(trend):.1f} балла в неделю. Возможно, стоит обратить внимание на факторы стресса")
        else:
            insights.append("📊 Ваше настроение остается стабильным за последний месяц")
        
        # Общая статистика
        mean_score = df['score'].mean()
        std_score = df['score'].std()
        
        if mean_score >= 7:
            insights.append(f"✨ У вас отличное общее настроение! Средний балл: {mean_score:.1f}/10")
        elif mean_score >= 5:
            insights.append(f"😊 У вас хорошее настроение в целом. Средний балл: {mean_score:.1f}/10")
        else:
            insights.append(f"🤗 Есть над чем поработать. Средний балл: {mean_score:.1f}/10. Помните: каждый день - новая возможность!")
        
        if std_score < 1.5:
            insights.append("🎯 Ваше настроение довольно стабильно - это хороший признак!")
        elif std_score > 2.5:
            insights.append("🎢 Ваше настроение сильно колеблется. Попробуйте найти факторы, которые влияют на эти изменения")
        
    except Exception as e:
        logger.error(f"Error in mood pattern analysis: {e}")
        insights.append("❗ Произошла ошибка при анализе паттернов")
    
    return insights


def analyze_activity_impact(df_emotion, df_activities):
    """Анализирует влияние активностей на настроение."""
    try:
        if df_activities.empty or df_emotion.empty:
            return {}
        
        activity_impact = {}
        
        # Группируем активности
        unique_activities = df_activities['activity'].unique()
        
        for activity in unique_activities:
            if pd.isna(activity) or activity == '':
                continue
                
            # Находим записи эмоций после этой активности (в пределах 2 часов)
            activity_times = df_activities[df_activities['activity'] == activity]['timestamp']
            
            scores_after_activity = []
            scores_without_activity = []
            
            for emotion_time in df_emotion['timestamp']:
                # Проверяем, была ли активность за последние 2 часа
                recent_activities = activity_times[
                    (activity_times <= emotion_time) & 
                    (activity_times >= emotion_time - pd.Timedelta(hours=2))
                ]
                
                emotion_score = df_emotion[df_emotion['timestamp'] == emotion_time]['score'].iloc[0]
                
                if len(recent_activities) > 0:
                    scores_after_activity.append(emotion_score)
                else:
                    scores_without_activity.append(emotion_score)
            
            # Вычисляем разность средних
            if len(scores_after_activity) >= 2 and len(scores_without_activity) >= 2:
                avg_with = np.mean(scores_after_activity)
                avg_without = np.mean(scores_without_activity)
                impact = avg_with - avg_without
                activity_impact[activity] = impact
        
        return activity_impact
        
    except Exception as e:
        logger.error(f"Error analyzing activity impact: {e}")
        return {}


def generate_smart_recommendations(df_emotion, df_physical, df_activities):
    """Генерирует умные рекомендации на основе анализа данных."""
    recommendations = []
    
    try:
        if df_activities.empty:
            recommendations.append("📝 Начните отслеживать активности, чтобы получить персональные рекомендации!")
            return recommendations
        
        # Анализ влияния активностей
        activity_impact = analyze_activity_impact(df_emotion, df_activities)
        
        if activity_impact:
            # Сортируем по влиянию
            sorted_activities = sorted(activity_impact.items(), key=lambda x: x[1], reverse=True)
            
            # Топ полезных активностей
            positive_activities = [(act, impact) for act, impact in sorted_activities if impact > 0.3]
            if positive_activities:
                recommendations.append("🎯 Ваши самые эффективные активности для настроения:")
                for activity, impact in positive_activities[:3]:
                    recommendations.append(f"   • {activity}: +{impact:.1f} балла к настроению")
                recommendations.append("💡 Рекомендация: увеличьте время на эти активности!")
            
            # Активности с негативным влиянием
            negative_activities = [(act, impact) for act, impact in sorted_activities if impact < -0.3]
            if negative_activities:
                recommendations.append("⚠️ Активности, которые могут снижать настроение:")
                for activity, impact in negative_activities[-2:]:  # Берем 2 худших
                    recommendations.append(f"   • {activity}: {impact:.1f} балла")
                recommendations.append("💡 Рекомендация: попробуйте ограничить или изменить подход к этим активностям")
        
        # Рекомендации по времени
        if not df_emotion.empty:
            df_emotion['hour'] = df_emotion['timestamp'].dt.hour
            hourly_mood = df_emotion.groupby('hour')['score'].mean()
            
            if len(hourly_mood) > 5:
                best_hours = hourly_mood.nlargest(3)
                worst_hours = hourly_mood.nsmallest(2)
                
                recommendations.append(f"⏰ Планируйте важные дела на {', '.join([f'{int(h)}:00' for h in best_hours.index])} - в это время у вас лучшее настроение")
                recommendations.append(f"🛡️ Будьте осторожны в {', '.join([f'{int(h)}:00' for h in worst_hours.index])} - настроение обычно ниже")
        
        # Рекомендации по физическому состоянию
        if not df_physical.empty and not df_emotion.empty:
            # Ищем корреляцию между физическим состоянием и настроением
            merged_df = pd.merge_asof(
                df_emotion.sort_values('timestamp'),
                df_physical.sort_values('timestamp'),
                on='timestamp',
                direction='backward',
                suffixes=('_emotion', '_physical')
            )
            
            if len(merged_df) > 5:
                correlation = merged_df['score_emotion'].corr(merged_df['score_physical'])
                if correlation > 0.3:
                    recommendations.append(f"💪 Ваше физическое состояние сильно влияет на настроение (корреляция: {correlation:.2f})")
                    recommendations.append("🏃‍♂️ Рекомендация: уделите больше внимания физической активности и здоровому образу жизни")
        
    except Exception as e:
        logger.error(f"Error generating recommendations: {e}")
        recommendations.append("❗ Произошла ошибка при генерации рекомендаций")
    
    return recommendations


def generate_weekly_summary(df_emotion, df_physical):
    """Генерирует персональную еженедельную сводку."""
    try:
        from datetime import datetime, timedelta
        
        if df_emotion.empty:
            return "📊 Недостаточно данных для еженедельной сводки"
        
        now = df_emotion['timestamp'].max()
        week_ago = now - timedelta(days=7)
        two_weeks_ago = now - timedelta(days=14)
        
        current_week = df_emotion[df_emotion['timestamp'] >= week_ago]
        previous_week = df_emotion[
            (df_emotion['timestamp'] >= two_weeks_ago) & 
            (df_emotion['timestamp'] < week_ago)
        ]
        
        if current_week.empty:
            return "📊 Недостаточно данных за текущую неделю"
        
        summary = ["📊 ВАША НЕДЕЛЬНАЯ СВОДКА\n"]
        
        # Основные метрики текущей недели
        current_avg = current_week['score'].mean()
        current_count = len(current_week)
        current_stability = current_week['score'].std()
        
        summary.append(f"📈 Средний балл недели: {current_avg:.1f}/10")
        summary.append(f"📊 Количество записей: {current_count}")
        
        # Сравнение с предыдущей неделей
        if not previous_week.empty:
            previous_avg = previous_week['score'].mean()
            change = current_avg - previous_avg
            
            if change > 0.3:
                summary.append(f"📈 Настроение улучшилось на {change:.1f} балла по сравнению с прошлой неделей! 🎉")
            elif change < -0.3:
                summary.append(f"📉 Настроение снизилось на {abs(change):.1f} балла по сравнению с прошлой неделей")
            else:
                summary.append("📊 Настроение осталось примерно на том же уровне")
        
        # Стабильность
        if current_stability < 1.0:
            summary.append("🎯 Ваше настроение было очень стабильным на этой неделе!")
        elif current_stability > 2.0:
            summary.append("🎢 На этой неделе настроение сильно колебалось")
        
        # Лучший и худший день недели
        if len(current_week) > 2:
            current_week['day_name'] = current_week['timestamp'].dt.day_name()
            day_mapping = {
                'Monday': 'Понедельник', 'Tuesday': 'Вторник', 'Wednesday': 'Среда',
                'Thursday': 'Четверг', 'Friday': 'Пятница', 'Saturday': 'Суббота', 'Sunday': 'Воскресенье'
            }
            current_week['day_name_ru'] = current_week['day_name'].map(day_mapping)
            
            daily_avg = current_week.groupby('day_name_ru')['score'].mean()
            if len(daily_avg) > 1:
                best_day = daily_avg.idxmax()
                worst_day = daily_avg.idxmin()
                
                summary.append(f"✨ Лучший день: {best_day} ({daily_avg[best_day]:.1f}/10)")
                summary.append(f"😔 Сложный день: {worst_day} ({daily_avg[worst_day]:.1f}/10)")
        
        # Физическое состояние (если есть данные)
        if not df_physical.empty:
            current_week_physical = df_physical[df_physical['timestamp'] >= week_ago]
            if not current_week_physical.empty:
                physical_avg = current_week_physical['score'].mean()
                summary.append(f"💪 Среднее физическое состояние: {physical_avg:.1f}/10")
        
        return "\n".join(summary)
        
    except Exception as e:
        logger.error(f"Error generating weekly summary: {e}")
        return "❗ Произошла ошибка при генерации недельной сводки"


def generate_smart_insights(df_emotion, df_physical, df_activities):
    """Главная функция генерации интеллектуальных инсайтов."""
    try:
        all_insights = []
        
        # Анализ паттернов настроения
        mood_patterns = analyze_mood_patterns(df_emotion)
        all_insights.extend(mood_patterns)
        
        # Умные рекомендации
        if len(df_emotion) >= 5:  # Только если достаточно данных
            all_insights.append("")  # Пустая строка для разделения
            recommendations = generate_smart_recommendations(df_emotion, df_physical, df_activities)
            all_insights.extend(recommendations)
        
        # Еженедельная сводка (если данных много)
        if len(df_emotion) >= 10:
            all_insights.append("")
            weekly_summary = generate_weekly_summary(df_emotion, df_physical)
            all_insights.append(weekly_summary)
        
        if not all_insights:
            return "📊 Продолжайте отслеживать настроение для получения персональных инсайтов!"
        
        return "\n".join(all_insights)
        
    except Exception as e:
        logger.error(f"Error generating smart insights: {e}")
        return "❗ Произошла ошибка при генерации инсайтов. Попробуйте позже."


def should_generate_smart_insights(df_emotion, df_physical, df_activities):
    """Проверяет, достаточно ли данных для генерации умных инсайтов."""
    return len(df_emotion) >= 5  # Минимум 5 записей для базового анализа 