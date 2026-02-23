"""
Модуль клавиатур для бота управления парковкой
"""

from aiogram.types import (
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardButton
)
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder

from utils import Emoji


def get_main_menu_keyboard(user) -> ReplyKeyboardMarkup:
    """
    Генерация главного меню в зависимости от роли пользователя

    Args:
        user: Объект пользователя

    Returns:
        Клавиатура с кнопками меню
    """
    builder = ReplyKeyboardBuilder()
    user_roles = [role.name for role in user.roles]

    # Если нет ролей - даем меню водителя
    if not user_roles:
        user_roles = ["DRIVER"]

    main_role = user.current_role or "DRIVER"

    # МЕНЮ ДЛЯ ВОДИТЕЛЯ
    if main_role == "DRIVER":
        buttons = [
            KeyboardButton(text=f"{Emoji.ARRIVAL} Прибытие"),
            KeyboardButton(text=f"{Emoji.DEPARTURE} Убытие"),
            KeyboardButton(text=f"{Emoji.GATE} Встать на ворота"),
            KeyboardButton(text=f"{Emoji.TASK} Мои задачи"),
        ]

        # Добавляем кнопку смены роли или запроса роли
        other_roles = [r for r in user_roles if r != "DRIVER"]
        if other_roles:
            buttons.append(KeyboardButton(text=f"{Emoji.SWITCH} Сменить роль"))
        else:
            buttons.append(KeyboardButton(text=f"{Emoji.REQUEST} Запросить роль"))

        for button in buttons:
            builder.add(button)
        builder.adjust(2)
        return builder.as_markup(resize_keyboard=True)

    # МЕНЮ ДЛЯ ОПЕРАТОРА
    elif main_role == "OPERATOR":
        buttons = [
            KeyboardButton(text=f"{Emoji.TASK} Дать задание"),
            KeyboardButton(text=f"{Emoji.TASK_POOL} Пул задач"),
            KeyboardButton(text=f"{Emoji.TASK_POOL} Очистить пул"),
            KeyboardButton(text=f"{Emoji.STUCK} Зависшие задачи"),
            KeyboardButton(text=f"{Emoji.STATUS} Статусы"),  # Новая кнопка
            KeyboardButton(text=f"{Emoji.REPORT} Отчет"),
        ]

        other_roles = [r for r in user_roles if r != "OPERATOR"]
        if other_roles:
            buttons.append(KeyboardButton(text=f"{Emoji.SWITCH} Сменить роль"))

        for button in buttons:
            builder.add(button)
        builder.adjust(2)
        return builder.as_markup(resize_keyboard=True)

    # МЕНЮ ДЛЯ ВОДИТЕЛЯ ПЕРЕГОНА
    elif main_role == "DRIVER_TRANSFER":
        buttons = []

        if user.is_on_shift:
            buttons.extend([
                KeyboardButton(text=f"{Emoji.SHIFT_END} Закончить смену"),
                KeyboardButton(text=f"{Emoji.TASK} Взять задачу"),
                KeyboardButton(text=f"{Emoji.TASK} Текущая задача"),
                KeyboardButton(text=f"{Emoji.COMPLETED} Завершить задачу")
            ])

            if user.is_on_break:
                buttons.append(KeyboardButton(text=f"{Emoji.BREAK_END} Вернуться с обеда"))
            else:
                buttons.append(KeyboardButton(text=f"{Emoji.BREAK_START} Уйти на обед"))
        else:
            buttons.append(KeyboardButton(text=f"{Emoji.SHIFT_START} Начать смену"))

        buttons.append(KeyboardButton(text=f"{Emoji.STATS} Статистика за смену"))
        buttons.append(KeyboardButton(text=f"{Emoji.UPDATE} Обновить меню"))

        other_roles = [r for r in user_roles if r != "DRIVER_TRANSFER"]
        if other_roles:
            buttons.append(KeyboardButton(text=f"{Emoji.SWITCH} Сменить роль"))

        for button in buttons:
            builder.add(button)
        builder.adjust(2)
        return builder.as_markup(resize_keyboard=True)

    # МЕНЮ ДЛЯ АДМИНИСТРАТОРА
    elif main_role == "ADMIN":
        buttons = [
            KeyboardButton(text=f"{Emoji.SETTINGS} Выдать роли"),
            KeyboardButton(text=f"{Emoji.SETTINGS} Забрать роли"),
            KeyboardButton(text=f"{Emoji.QUEUE} Статус очереди"),
            KeyboardButton(text=f"{Emoji.SWITCH} Сменить роль"),
            KeyboardButton(text=f"{Emoji.UPDATE} Обновить меню"),
            KeyboardButton(text=f"{Emoji.ARRIVAL} Прибытие"),
            KeyboardButton(text=f"{Emoji.DEPARTURE} Убытие"),
            KeyboardButton(text=f"{Emoji.GATE} Встать на ворота"),
        ]

        if "OPERATOR" in user_roles:
            buttons.extend([
                KeyboardButton(text=f"{Emoji.TASK} Дать задание"),
                KeyboardButton(text=f"{Emoji.TASK_POOL} Пул задач"),
            ])

        if "DRIVER_TRANSFER" in user_roles:
            if user.is_on_shift:
                buttons.append(KeyboardButton(text=f"{Emoji.SHIFT_END} Закончить смену"))
            else:
                buttons.append(KeyboardButton(text=f"{Emoji.SHIFT_START} Начать смену"))

        for button in buttons:
            builder.add(button)
        builder.adjust(2)
        return builder.as_markup(resize_keyboard=True)

    # МЕНЮ ДЛЯ СОТРУДНИКА ДЭБ
    elif main_role == "DEB_EMPLOYEE":
        buttons = [
            KeyboardButton(text=f"{Emoji.DEPARTURE} Зарегистрировать убытие"),
            KeyboardButton(text=f"{Emoji.REPORT} Отчет по парковке"),
        ]

        other_roles = [r for r in user_roles if r != "DEB_EMPLOYEE"]
        if other_roles:
            buttons.append(KeyboardButton(text=f"{Emoji.SWITCH} Сменить роль"))

        for button in buttons:
            builder.add(button)
        builder.adjust(2)
        return builder.as_markup(resize_keyboard=True)

    # МЕНЮ ПО УМОЛЧАНИЮ
    else:
        buttons = [
            KeyboardButton(text=f"{Emoji.ARRIVAL} Прибытие"),
            KeyboardButton(text=f"{Emoji.DEPARTURE} Убытие"),
            KeyboardButton(text=f"{Emoji.GATE} Встать на ворота"),
            KeyboardButton(text=f"{Emoji.REQUEST} Запросить роль")
        ]

        for button in buttons:
            builder.add(button)
        builder.adjust(2)
        return builder.as_markup(resize_keyboard=True)


def get_cancel_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура с кнопкой отмены"""
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text=f"{Emoji.CANCEL} Отмена"))
    return builder.as_markup(resize_keyboard=True)


def get_vehicle_type_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора типа ТС"""
    builder = InlineKeyboardBuilder()
    builder.button(
        text=f"{Emoji.VEHICLE} Не перецепной (7т,10т,20т)",
        callback_data="vehicle_non_hitch"
    )
    builder.button(
        text=f"{Emoji.HITCH} Перецепной",
        callback_data="vehicle_hitch"
    )
    builder.button(
        text=f"{Emoji.CANCEL} Отмена",
        callback_data="menu_main"
    )
    builder.adjust(1)
    return builder.as_markup()


def get_role_selection_keyboard(user_roles) -> InlineKeyboardMarkup:
    """
    Клавиатура выбора роли для запроса

    Args:
        user_roles: Список текущих ролей пользователя

    Returns:
        Клавиатура с доступными ролями или None
    """
    builder = InlineKeyboardBuilder()

    all_roles = {
        "OPERATOR": f"{Emoji.OPERATOR} Оператор",
        "DRIVER_TRANSFER": f"{Emoji.DRIVER_TRANSFER} Водитель перегона",
        "ADMIN": f"{Emoji.ADMIN} Администратор",
        "DEB_EMPLOYEE": f"{Emoji.DEB} Сотрудник ДЭБ"
    }

    for role_key, role_text in all_roles.items():
        if role_key not in user_roles:
            builder.button(text=role_text, callback_data=f"request_role_{role_key}")

    if builder.buttons:
        builder.button(text=f"{Emoji.CANCEL} Отмена", callback_data="menu_main")
        builder.adjust(2)
        return builder.as_markup()
    return None


def get_switch_role_keyboard(user_roles, current_role) -> InlineKeyboardMarkup:
    """
    Клавиатура для переключения между ролями

    Args:
        user_roles: Список ролей пользователя
        current_role: Текущая выбранная роль

    Returns:
        Клавиатура с ролями для переключения
    """
    builder = InlineKeyboardBuilder()

    role_names = {
        "DRIVER": f"{Emoji.DEPARTURE} Водитель",
        "DRIVER_TRANSFER": f"{Emoji.DRIVER_TRANSFER} Водитель перегона",
        "OPERATOR": f"{Emoji.OPERATOR} Оператор",
        "ADMIN": f"{Emoji.ADMIN} Администратор",
        "DEB_EMPLOYEE": f"{Emoji.DEB} Сотрудник ДЭБ"
    }

    # Сортируем роли: DRIVER всегда первая
    sorted_roles = sorted(user_roles, key=lambda x: (x != "DRIVER", x))

    for role_key in sorted_roles:
        if role_key in role_names:
            check_mark = " ✓" if role_key == current_role else ""
            builder.button(
                text=f"{role_names[role_key]}{check_mark}",
                callback_data=f"switch_role_{role_key}"
            )

    # Кнопка запроса новых ролей
    available_roles = {"OPERATOR", "DRIVER_TRANSFER", "ADMIN", "DEB_EMPLOYEE"}
    roles_to_request = available_roles - set(user_roles)
    if roles_to_request:
        builder.button(
            text=f"{Emoji.REQUEST} Запросить роль",
            callback_data="show_requestable_roles"
        )

    builder.button(text=f"{Emoji.BACK} Назад", callback_data="menu_main")
    builder.adjust(1)
    return builder.as_markup()


def get_break_menu_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура меню обеда"""
    builder = InlineKeyboardBuilder()
    builder.button(
        text=f"{Emoji.BREAK_START} Уйти на обед",
        callback_data="break_start"
    )
    builder.button(
        text=f"{Emoji.BREAK_END} Вернуться с обеда",
        callback_data="break_end"
    )
    builder.button(
        text=f"{Emoji.BACK} Назад",
        callback_data="back_to_driver_transfer_menu"
    )
    builder.adjust(1)
    return builder.as_markup()


def get_break_confirmation_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура подтверждения ухода на обед"""
    builder = InlineKeyboardBuilder()
    builder.button(
        text=f"{Emoji.CHECK} Подтвердить",
        callback_data="break_confirm"
    )
    builder.button(
        text=f"{Emoji.CANCEL} Отмена",
        callback_data="break_cancel"
    )
    builder.adjust(2)
    return builder.as_markup()


def get_task_actions_keyboard(task_id: int) -> InlineKeyboardMarkup:
    """
    Клавиатура действий с задачей для водителя перегона

    Args:
        task_id: ID задачи

    Returns:
        Клавиатура с кнопками действий
    """
    builder = InlineKeyboardBuilder()
    builder.button(
        text=f"{Emoji.COMPLETED} Выполнено",
        callback_data=f"complete_task_{task_id}"
    )
    builder.button(
        text=f"{Emoji.CANCEL} Нет ТС",
        callback_data=f"no_vehicle_{task_id}"
    )
    builder.button(
        text="🔧 Поломка ТС",
        callback_data=f"breakdown_{task_id}"
    )
    builder.button(
        text="⏳ Долгое ожидание",
        callback_data=f"stuck_timeout_{task_id}"
    )
    builder.adjust(2, 2)
    return builder.as_markup()


def get_operator_reports_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура отчетов для оператора"""
    builder = InlineKeyboardBuilder()
    builder.button(
        text=f"{Emoji.TASK} Отчет по задачам",
        callback_data="operator_report_tasks"
    )
    builder.button(
        text=f"{Emoji.PARKING} Отчет по парковке",
        callback_data="operator_report_parking"
    )
    builder.button(
        text=f"{Emoji.DOWNLOAD} Excel отчет",
        callback_data="operator_report_excel"
    )
    builder.button(
        text=f"{Emoji.BACK} Назад в меню",
        callback_data="menu_main"
    )
    builder.adjust(2)
    return builder.as_markup()


def get_report_period_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора периода для отчета"""
    builder = InlineKeyboardBuilder()
    builder.button(text="Сегодня", callback_data="report_today")
    builder.button(text="Вчера", callback_data="report_yesterday")
    builder.button(text="Текущая неделя", callback_data="report_week")
    builder.button(text="Текущий месяц", callback_data="report_month")
    builder.button(text="Выбрать период", callback_data="report_custom")
    builder.button(
        text=f"{Emoji.BACK} Назад к отчетам",
        callback_data="back_to_reports_menu"
    )
    builder.adjust(2, 2, 1, 1)
    return builder.as_markup()


def get_statuses_menu_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура меню статусов"""
    builder = InlineKeyboardBuilder()
    builder.button(
        text=f"{Emoji.TASK} Статус задач",
        callback_data="status_tasks"
    )
    builder.button(
        text=f"{Emoji.PARKING} Статус парковки",
        callback_data="status_parking"
    )
    builder.button(
        text=f"{Emoji.QUEUE} Статус очереди",
        callback_data="status_queue"
    )
    builder.button(
        text=f"{Emoji.STUCK} Зависшие задачи",
        callback_data="status_stuck"
    )
    builder.button(
        text=f"{Emoji.INFO} Информация о смене",
        callback_data="status_shift_info"
    )
    builder.button(
        text=f"{Emoji.BACK} Главное меню",
        callback_data="menu_main"
    )
    builder.adjust(2)
    return builder.as_markup()

def get_stuck_tasks_management_keyboard(tasks: list, page: int = 0, items_per_page: int = 5) -> InlineKeyboardMarkup:
    """
    Клавиатура для управления зависшими задачами с пагинацией

    Args:
        tasks: Список зависших задач
        page: Текущая страница
        items_per_page: Количество задач на странице
    """
    builder = InlineKeyboardBuilder()

    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    page_tasks = tasks[start_idx:end_idx]

    for task in page_tasks:
        gate_status = "🚫" if "Ворота" in (task.stuck_reason or "") else "⚠️"
        vehicle = task.parking.vehicle_number if task.parking else "Неизвестно"

        # Кнопка с информацией о задаче
        builder.button(
            text=f"{gate_status} Задача #{task.id} - {vehicle}",
            callback_data=f"stuck_task_info_{task.id}"
        )

        # Кнопки действий для задачи
        if task.parking and task.parking.is_hitch:
            builder.button(
                text=f"🔄 Перезапустить",
                callback_data=f"restart_task_{task.id}"
            )

        builder.button(
            text=f"❌ Закрыть",
            callback_data=f"close_stuck_task_{task.id}"
        )

    # Навигация
    nav_buttons = []
    if page > 0:
        nav_buttons.append(
            InlineKeyboardButton(
                text="◀️ Предыдущая",
                callback_data=f"stuck_page_{page-1}"
            )
        )

    if end_idx < len(tasks):
        nav_buttons.append(
            InlineKeyboardButton(
                text="Следующая ▶️",
                callback_data=f"stuck_page_{page+1}"
            )
        )

    if nav_buttons:
        builder.row(*nav_buttons)

    # Кнопки массовых действий
    hitch_tasks = [t for t in tasks if t.parking and t.parking.is_hitch]
    if hitch_tasks:
        builder.button(
            text=f"🔄 Перезапустить все перецепные ({len(hitch_tasks)})",
            callback_data="restart_all_hitch_stuck"
        )

    builder.button(
        text=f"{Emoji.UPDATE} Обновить список",
        callback_data="refresh_stuck_tasks"
    )
    builder.button(
        text=f"{Emoji.BACK} Назад в статусы",
        callback_data="back_to_statuses"
    )

    builder.adjust(1)  # По одной кнопке в ряду
    return builder.as_markup()

def get_stuck_task_actions_keyboard(task_id: int) -> InlineKeyboardMarkup:
    """Клавиатура действий с зависшей задачей для оператора"""
    builder = InlineKeyboardBuilder()
    builder.button(
        text="🔄 Переназначить ворота",
        callback_data=f"reassign_gate_{task_id}"
    )
    builder.button(
        text="❌ Закрыть задачу",
        callback_data=f"close_stuck_task_{task_id}"
    )
    builder.button(
        text="📋 Вернуться к списку",
        callback_data="back_to_stuck_list"
    )
    builder.adjust(1)
    return builder.as_markup()

def get_stuck_task_detail_keyboard(task_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для детального просмотра зависшей задачи"""
    builder = InlineKeyboardBuilder()

    builder.button(
        text=f"🔄 Перезапустить задачу",
        callback_data=f"restart_task_{task_id}"
    )
    builder.button(
        text=f"📋 Назначить другому водителю",
        callback_data=f"reassign_task_{task_id}"
    )
    builder.button(
        text=f"🔧 Отметить как поломку",
        callback_data=f"mark_breakdown_{task_id}"
    )
    builder.button(
        text=f"❌ Закрыть задачу",
        callback_data=f"close_stuck_task_{task_id}"
    )
    builder.button(
        text=f"{Emoji.BACK} Назад к списку",
        callback_data="back_to_stuck_list"
    )

    builder.adjust(1)
    return builder.as_markup()
