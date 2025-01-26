import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
from scipy.stats import sem
from sklearn.linear_model import LinearRegression
import os
import requests
from datetime import datetime

def send_photo_via_api(token, chat_id, file_path, caption=None):
    """Отправляет фото через Telegram Bot API."""
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    with open(file_path, "rb") as photo:
        data = {"chat_id": chat_id}
        if caption:
            data["caption"] = caption  # Добавление подписи, если она передана
        response = requests.post(url, data=data, files={"photo": photo})
    return response.json()


def save_plot_as_image(func, filename, *args, **kwargs):
    BASE_DIR = "/MoodTrackerBot_data"  # Путь к SSD
    if not os.path.exists(BASE_DIR):
        raise ValueError(f"Директория {BASE_DIR} недоступна для записи.")

    filepath = os.path.join(BASE_DIR, filename)  # Полный путь к файлу
    func(*args, **kwargs)
    plt.savefig(filepath, format='png', dpi=300)
    plt.close()
    return filepath

def generate_and_send_charts(token, chat_id, df, state_type, logger):
    """Универсальная функция для генерации и отправки графиков."""
    BASE_DIR = "/MoodTrackerBot_data"  # Путь к SSD

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

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    daily_states_path = os.path.join(BASE_DIR, f"{state_type}_daily_states.png")
    trend_path = os.path.join(BASE_DIR, f"{state_type}_trend.png")
    freq_analysis_path = os.path.join(BASE_DIR, f"{state_type}_freq_analysis.png")

    # Генерация графиков
    try:
        # Ежедневное состояние
        stats = calculate_stats(df)
        save_plot_as_image(plot_daily_states, daily_states_path, stats, titles[state_type]["daily"], "Среднее состояние")

        # Тренд состояния
        save_plot_as_image(plot_trend, trend_path, df, titles[state_type]["trend"], "Среднее состояние")

        # Частотный анализ
        save_plot_as_image(plot_frequency_analysis, freq_analysis_path, df, titles[state_type]["freq"], "Амплитуда")

        # Отправка графиков через API
        for file_path, caption in [
            (daily_states_path, titles[state_type]["daily"]),
            (trend_path, titles[state_type]["trend"]),
            (freq_analysis_path, titles[state_type]["freq"]),
        ]:
            response = send_photo_via_api(token, chat_id, file_path, caption=caption)
            if response.get("ok"):
                logger.info(f"График {file_path} успешно отправлен")
            else:
                logger.error(f"Ошибка при отправке {file_path}: {response}")

    except Exception as e:
        logger.error(f"Ошибка при генерации или отправке графиков ({state_type}): {e}")



def calculate_stats(df, group_col='hour', confidence=0.8):
    stats = df.groupby(['day_type', group_col])['score'].agg(['mean', 'std']).reset_index()
    counts = df.groupby(['day_type', group_col])['score'].size().reset_index(name='count')
    stats = pd.merge(stats, counts, on=['day_type', group_col])

    # Исключаем группы с недостаточным количеством данных
    stats = stats[stats['count'] > 1]  # Минимум 2 записи для расчетов

    def safe_confidence_interval(mean, std, count):
        if count > 1:
            margin = 1.28 * (std / (count ** 0.5))  # z-score for 80% CI
            return mean - margin, mean + margin
        else:
            return mean, mean

    stats[['ci_lower', 'ci_upper']] = stats.apply(
        lambda row: pd.Series(safe_confidence_interval(row['mean'], row['std'], row['count'])), axis=1
    )
    return stats


def plot_daily_states(stats, title, ylabel, colormap=plt.cm.coolwarm):
    norm = mcolors.Normalize(vmin=stats['count'].min(), vmax=stats['count'].max())

    fig, axes = plt.subplots(2, 1, figsize=(14, 12), sharex=True)

    for i, day_type in enumerate(['Будний день', 'Выходной']):
        df_day = stats[stats['day_type'] == day_type]

        # Проверяем, есть ли данные для текущего day_type
        if df_day.empty:
            axes[i].text(0.5, 0.5, f'Нет данных для {day_type}', ha='center', va='center', fontsize=14)
            axes[i].set_title(f'{title} ({day_type})', fontsize=14)
            axes[i].grid(axis='y', linestyle='--', alpha=0.7)
            continue

        colors = colormap(norm(df_day['count']))

        axes[i].plot(
            df_day['hour'].values, df_day['mean'].values, color='black', linestyle='-', label=day_type
        )
        axes[i].scatter(
            df_day['hour'].values, df_day['mean'].values,
            color=colors, s=100, edgecolor='black'
        )
        axes[i].errorbar(
            df_day['hour'].values, df_day['mean'].values,
            yerr=[
                (df_day['mean'] - df_day['ci_lower']).values,
                (df_day['ci_upper'] - df_day['mean']).values
            ],
            fmt='none', ecolor='gray', elinewidth=2, capsize=4
        )
        axes[i].set_title(f'{title} ({day_type})', fontsize=14)
        axes[i].set_ylabel(ylabel, fontsize=12)
        axes[i].grid(axis='y', linestyle='--', alpha=0.7)
        axes[i].set_xticks(range(0, 24))

    axes[1].set_xlabel('Час', fontsize=12)
    plt.tight_layout()



def plot_trend(df, title, ylabel):
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

    plt.figure(figsize=(14, 8))
    plt.plot(daily_stats['date'].values, daily_stats['daily_mean'].values, marker='o', color='blue', label='Среднее значение')
    plt.fill_between(
        daily_stats['date'], daily_stats['ci_lower'], daily_stats['ci_upper'], color='lightblue', alpha=0.4,
        label='Доверительный интервал'
    )
    plt.plot(daily_stats['date'].values, daily_stats['trend'].values, color='red', linestyle='--', label='Тренд')
    plt.title(title, fontsize=16)
    plt.xlabel('Дата', fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.legend()


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
