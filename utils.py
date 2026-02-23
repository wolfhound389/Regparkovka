"""
Утилиты для бота управления парковкой
"""

from datetime import datetime, timedelta
from typing import Optional, Tuple

import pytz

# Часовой пояс Москвы
moscow_tz = pytz.timezone('Europe/Moscow')


class Emoji:
    """Константы эмодзи для единообразия интерфейса"""
    # Навигация
    BACK = "↩️"
    CANCEL = "❌"
    MENU = "📋"
    SWITCH = "🔄"
    UPDATE = "🔄"

    # Информация
    INFO = "ℹ️"
    WARNING = "⚠️"
    ERROR = "❌"
    SUCCESS = "✅"

    # Транспорт
    ARRIVAL = "🅿️"
    DEPARTURE = "🚗"
    GATE = "🚪"
    PARKING = "🅿️"
    VEHICLE = "🚛"
    HITCH = "🔗"

    # Роли
    ROLE = "👤"
    OPERATOR = "🎛️"
    DRIVER_TRANSFER = "🦑"
    ADMIN = "👑"
    DEB = "👮"
    REQUEST = "🙏"

    # Задачи
    TASK = "📋"
    STATUS = "📊"
    PRIORITY = "⚠️"
    PENDING = "⏳"
    IN_PROGRESS = "🚗"
    COMPLETED = "✅"
    STUCK = "⚠️"
    TASK_POOL = "🗂️"

    # Отчеты
    REPORT = "📈"
    EXCEL = "📊"
    DOWNLOAD = "💾"
    CALENDAR = "📅"
    TIME = "⏰"

    # Смена и обед
    SHIFT_START = "🟢"
    SHIFT_END = "🔴"
    BREAK_START = "🍽️"
    BREAK_END = "🔙"
    BREAK_TIME = "⏱️"
    COFFEE = "☕"

    # Пользователи
    PERSON = "👤"
    ID = "🆔"
    CLOCK = "⏰"

    # Действия
    CHECK = "✅"
    SETTINGS = "⚙️"
    STATS = "📊"

    # Очередь и парковка
    QUEUE = "🗂️"
    WAITING = "⏳"
    NOTIFIED = "🔔"
    PARKING_SPOT = "🅿️"
    DRIVE = "🚚"
    ARRIVED = "📍"

    # статусы для ворот
    GATE_OCCUPIED = "🚫"
    GATE_FREE = "✅"
    GATE_BUSY = "⛔"
    RETRY = "🔄"
    ASSIGN_AGAIN = "📌"
    QUESTION = "❓"


# Словари статусов
STATUS_NAMES = {
    "PENDING": "⏳ Ожидание",
    "IN_PROGRESS": "🚗 В работе",
    "COMPLETED": "✅ Выполнено",
    "STUCK": "⚠️ Зависла",
    "CANCELLED": "❌ Отмена"
}

TASK_STATUS_EMOJI = {
    "PENDING": "⏳",
    "IN_PROGRESS": "🚗",
    "COMPLETED": "✅",
    "STUCK": "⚠️",
    "CANCELLED": "❌"
}

TASK_TYPE_NAMES = {
    True: "🔗 Перецепной",
    False: "🚛 Не перецепной"
}

PRIORITY_NAMES = {
    0: "⚪ Низкий",
    1: "🟡 Средний",
    2: "🟠 Выше среднего",
    3: "🔴 Высокий",
    4: "🔥 Очень высокий",
    5: "⚠️ Критический"
}


def get_priority_name(priority: int) -> str:
    """Получить название приоритета"""
    if priority >= 5:
        return "⚠️ Критический"
    elif priority >= 3:
        return "🔴 Высокий"
    elif priority >= 1:
        return "🟠 Средний"
    else:
        return "⚪ Низкий"


def ensure_timezone_aware(dt: Optional[datetime], tz: pytz.timezone = moscow_tz) -> Optional[datetime]:
    """
    Приводит datetime к timezone-aware формату (с часовым поясом)

    Args:
        dt: Дата/время для преобразования
        tz: Часовой пояс (по умолчанию Москва)

    Returns:
        datetime с часовым поясом или None
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return tz.localize(dt)
    return dt.astimezone(tz)


def get_timezone_aware_now() -> datetime:
    """Возвращает текущее время с часовым поясом Москвы"""
    return datetime.now(moscow_tz)


def format_duration(seconds: int) -> str:
    """
    Форматирует длительность в секундах в читаемый вид

    Args:
        seconds: Количество секунд

    Returns:
        Строка вида "X ч Y мин" или "Y мин"
    """
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60

    if hours > 0:
        return f"{hours} ч {minutes} мин"
    return f"{minutes} мин"


def get_current_shift_period(now: datetime = None) -> Tuple[datetime, datetime, str]:
    """
    Определение текущего периода смены
    Возвращает: (start_time, end_time, period_name)
    Периоды: 9:00 - 21:00 (дневная смена)
             21:00 - 9:00 (ночная смена)
    """
    if now is None:
        now = get_timezone_aware_now()

    current_hour = now.hour

    if 9 <= current_hour < 21:
        # Дневная смена: сегодня 9:00 - 21:00
        start_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
        end_time = now.replace(hour=21, minute=0, second=0, microsecond=0)
        period_name = "дневная смена (9:00-21:00)"
    else:
        # Ночная смена: вчера/сегодня 21:00 - 9:00
        if current_hour >= 21:
            # После 21:00 - сегодня 21:00 до завтра 9:00
            start_time = now.replace(hour=21, minute=0, second=0, microsecond=0)
            end_time = (now + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
        else:
            # До 9:00 - вчера 21:00 до сегодня 9:00
            start_time = (now - timedelta(days=1)).replace(hour=21, minute=0, second=0, microsecond=0)
            end_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
        period_name = "ночная смена (21:00-9:00)"

    return start_time, end_time, period_name
