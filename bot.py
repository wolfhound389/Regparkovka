"""
Telegram Bot для управления парковкой и задачами
Версия: 2.1 (оптимизированная)
Автор: Команда разработки
Описание: Система управления парковкой ТС, задачами для водителей и водителей перегона,
         с ролевой моделью, статистикой, обедами и уведомлениями
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Any, Dict
from io import BytesIO
from functools import wraps
from aiogram.types import FSInputFile

import pytz
from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardButton,
    BufferedInputFile
)
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from sqlalchemy.orm import Session

from config import config
from database import init_db, SessionLocal, get_db_context
from models import (
    User, Role as RoleModel, RoleRequest,
    Parking, Task, Break, ParkingQueue
)
from vehicle_validator import VehicleNumberValidator
from utils import (
    Emoji, STATUS_NAMES, TASK_STATUS_EMOJI, PRIORITY_NAMES,
    ensure_timezone_aware, get_timezone_aware_now, format_duration,
    get_priority_name, moscow_tz
)
from keyboards import (
    get_main_menu_keyboard, get_cancel_keyboard, get_vehicle_type_keyboard,
    get_role_selection_keyboard, get_switch_role_keyboard,
    get_break_menu_keyboard, get_break_confirmation_keyboard,
    get_task_actions_keyboard, get_operator_reports_keyboard,
    get_report_period_keyboard, get_statuses_menu_keyboard,
    get_stuck_tasks_management_keyboard,
    get_stuck_task_detail_keyboard
)
from services import (
    get_user, get_or_create_user, get_user_roles, get_user_main_role,
    add_to_parking_queue, get_queue_position, get_queue_stats,
    process_parking_departure, get_free_parking_spot,
    validate_vehicle_number, validate_vehicle_number_with_explanation,
    normalize_vehicle_number, get_active_transfer_drivers,
    get_task_from_pool, generate_excel_report
)

import os
from pathlib import Path

from utils import get_current_shift_period
from services import (
    get_tasks_for_current_shift, get_parking_for_current_shift,
    get_queue_for_current_shift
)
from image_service import ImageService
from aiogram.types import FSInputFile

# В начале файла после импортов добавим константу для пути к изображениям
GATES_IMAGES_PATH = Path("gates_images")  # Папка с изображениями ворот

async def get_gate_image_path(gate_number: int) -> Optional[Path]:
    """
    Получение пути к изображению ворот по номеру

    Args:
        gate_number: Номер ворот

    Returns:
        Path к изображению или None, если файл не найден
    """
    # Проверяем различные форматы файлов
    image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp']

    for ext in image_extensions:
        image_path = GATES_IMAGES_PATH / f"{gate_number}{ext}"
        if image_path.exists():
            return image_path

    # Проверяем с ведущим нулем для номеров 1-9 (01.jpg, 02.jpg и т.д.)
    if gate_number < 10:
        for ext in image_extensions:
            image_path = GATES_IMAGES_PATH / f"0{gate_number}{ext}"
            if image_path.exists():
                return image_path

    return None
# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== ИНИЦИАЛИЗАЦИЯ БОТА ====================
bot = Bot(token=config.BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)


# ==================== ДЕКОРАТОР ДЛЯ УПРАВЛЕНИЯ СЕССИЯМИ БД ====================
def with_db(func):
    """Декоратор для автоматического управления сессией БД"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        db = SessionLocal()
        try:
            result = await func(*args, db=db, **kwargs)
            return result
        except Exception as e:
            logger.error(f"Ошибка в {func.__name__}: {e}", exc_info=True)
            raise
        finally:
            db.close()
    return wrapper


# ==================== СОСТОЯНИЯ FSM ====================
class DriverStates(StatesGroup):
    waiting_for_vehicle_number = State()
    waiting_for_vehicle_type = State()
    waiting_for_gate_confirmation = State()


class OperatorStates(StatesGroup):
    waiting_for_gate_number = State()
    waiting_for_report_period = State()
    waiting_for_new_gate_for_stuck_task = State()

class AdminStates(StatesGroup):
    waiting_for_user_selection_grant = State()
    waiting_for_user_selection_revoke = State()


class RequestStates(StatesGroup):
    waiting_for_full_name = State()
    waiting_for_position = State()
    waiting_for_role_selection = State()


class DEBStates(StatesGroup):
    waiting_for_departure_registration = State()


class DriverTransferStates(StatesGroup):
    waiting_for_break_confirmation = State()
    waiting_for_return_confirmation = State()
    waiting_for_gate_confirmation = State()


# ==================== ОБРАБОТЧИК КОМАНДЫ /START ====================
@router.message(CommandStart())
@with_db
async def cmd_start(message: Message, db: Session):
    """Обработчик команды /start"""
    user = await get_or_create_user(db, message)

    # Проверка наличия роли DRIVER
    roles_list = get_user_roles(user)
    if not roles_list:
        driver_role = db.query(RoleModel).filter(RoleModel.name == "DRIVER").first()
        if driver_role:
            user.roles.append(driver_role)
            db.commit()
            roles_list = ["DRIVER"]
            logger.info(f"✅ Пользователю {user.telegram_id} добавлена роль DRIVER")

    # Установка текущей роли
    if not user.current_role:
        user.current_role = "DRIVER"
        db.commit()

    name = f"{user.first_name} {user.last_name}".strip() or "Пользователь"

    await message.answer(
        f"👋 Добро пожаловать, {name}!\n"
        f"📋 Ваши роли: {', '.join(roles_list) if roles_list else 'Водитель'}\n"
        f"👤 Текущая роль: {get_user_main_role(user)}\n"
        f"{Emoji.SHIFT_START} Статус смены: {'На смене' if user.is_on_shift else 'Не на смене'}\n"
        f"{Emoji.BREAK_START} Статус обеда: {'На обеде' if user.is_on_break else 'Работает'}\n\n"
        f"Используйте кнопки ниже для навигации.",
        reply_markup=get_main_menu_keyboard(user)
    )

async def send_gate_image(message: Message, folder_type: str, number: int, caption: str = None):
    """
    Отправка изображения ворот или парковки

    Args:
        message: Сообщение для ответа
        folder_type: Тип папки ("ABK1", "ABK2", "PARKING")
        number: Номер изображения
        caption: Подпись к изображению
    """
    try:
        image_path = ImageService.get_image_path(folder_type, number)

        if image_path and image_path.exists():
            photo = FSInputFile(image_path)
            await message.answer_photo(
                photo=photo,
                caption=caption
            )
            logger.info(f"✅ Отправлено изображение {folder_type}/{number}")
            return True
        else:
            # Если изображение не найдено, отправляем сообщение об этом
            await message.answer(
                f"⚠️ Изображение для {folder_type} #{number} не найдено.\n"
                f"Но вы можете продолжить работу."
            )
            logger.warning(f"Изображение не найдено: {folder_type}/{number}")
            return False

    except Exception as e:
        logger.error(f"Ошибка при отправке изображения: {e}")
        return False

async def send_task_with_image(telegram_id: int, building_type: str, gate_number: int, caption: str):
    """
    Отправка задачи с изображением ворот

    Args:
        telegram_id: ID получателя
        building_type: Тип здания ("ABK1", "ABK2")
        gate_number: Номер ворот
        caption: Текст сообщения
    """
    try:
        from image_service import ImageService
        from aiogram.types import FSInputFile

        image_path = ImageService.get_image_path(building_type, gate_number)

        if image_path and image_path.exists():
            photo = FSInputFile(image_path)
            await bot.send_photo(
                chat_id=telegram_id,
                photo=photo,
                caption=caption
            )
            logger.info(f"✅ Отправлено изображение {building_type}/{gate_number} пользователю {telegram_id}")
        else:
            # Если нет изображения, отправляем только текст
            await bot.send_message(
                chat_id=telegram_id,
                text=caption + f"\n\n⚠️ Изображение для ворот #{gate_number} не найдено."
            )
            logger.warning(f"Изображение не найдено: {building_type}/{gate_number}")

    except Exception as e:
        logger.error(f"Ошибка отправки задачи с изображением: {e}")
        # Пробуем отправить хотя бы текст
        try:
            await bot.send_message(telegram_id, caption)
        except Exception as e2:
            logger.error(f"Не удалось отправить даже текст: {e2}")


# ==================== ОБРАБОТЧИКИ ДЛЯ СМЕНЫ РОЛИ ====================
@router.message(F.text.contains("Сменить роль"))
@router.message(F.text.contains("Переключить роль"))
@with_db
async def process_switch_role(message: Message, db: Session):
    """Процесс переключения между ролями"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer("❌ Сначала используйте /start для регистрации.")
        return

    user_roles = get_user_roles(user)

    if len(user_roles) <= 1:
        await message.answer(
            f"{Emoji.INFO} У вас только одна роль. "
            f"Для запроса дополнительной роли используйте '{Emoji.REQUEST} Запросить роль'"
        )
        return

    current_role = get_user_main_role(user)

    await message.answer(
        f"{Emoji.SWITCH} Переключение роли\n"
        f"Текущая роль: {current_role}\n\n"
        f"Выберите роль для переключения:",
        reply_markup=get_switch_role_keyboard(user_roles, current_role)
    )


@router.callback_query(F.data.startswith("switch_role_"))
@with_db
async def process_switch_role_selection(callback: CallbackQuery, db: Session):
    """Обработка выбора роли для переключения"""
    role_str = callback.data.replace("switch_role_", "")
    user = await get_user(db, callback.from_user.id)

    if not user:
        await callback.message.edit_text(f"{Emoji.ERROR} Пользователь не найден.")
        return

    user_roles = get_user_roles(user)
    if role_str not in user_roles:
        await callback.message.edit_text(f"{Emoji.ERROR} У вас нет этой роли.")
        return

    # Сохраняем выбранную роль
    user.current_role = role_str
    db.commit()

    role_names = {
        "DRIVER": "Водитель",
        "DRIVER_TRANSFER": "Водитель перегона",
        "OPERATOR": "Оператор",
        "ADMIN": "Администратор",
        "DEB_EMPLOYEE": "Сотрудник ДЭБ"
    }

    role_name = role_names.get(role_str, role_str)

    # Пытаемся отредактировать сообщение
    try:
        await callback.message.edit_text(f"{Emoji.SUCCESS} Переключено на роль: {role_name}")
    except Exception:
        await callback.message.delete()
        await callback.message.answer(f"{Emoji.SUCCESS} Переключено на роль: {role_name}")

    # Отправляем новое меню
    await callback.message.answer(
        f"👋 Меню для роли: {role_name}\n\n"
        f"Используйте кнопки ниже для навигации.",
        reply_markup=get_main_menu_keyboard(user)
    )


@router.callback_query(F.data == "show_requestable_roles")
@with_db
async def process_show_requestable_roles(callback: CallbackQuery, state: FSMContext, db: Session):
    """Показать доступные для запроса роли"""
    user = await get_user(db, callback.from_user.id)
    if not user:
        await callback.message.edit_text("❌ Пользователь не найден.")
        return

    user_roles = get_user_roles(user)

    # Проверка активного запроса
    active_request = db.query(RoleRequest).filter(
        RoleRequest.user_id == user.id,
        RoleRequest.status == "ожидает"
    ).first()

    if active_request:
        role_names = {
            "OPERATOR": "Оператор",
            "DRIVER_TRANSFER": "Водитель перегона",
            "ADMIN": "Администратор",
            "DEB_EMPLOYEE": "Сотрудник ДЭБ"
        }
        await callback.message.edit_text(
            f"ℹ️ У вас уже есть активный запрос на роль "
            f"'{role_names.get(active_request.requested_role, active_request.requested_role)}'.\n"
            f"⏰ Дата запроса: {active_request.created_at.strftime('%d.%m.%Y %H:%M')}\n\n"
            f"Ожидайте решения администратора.",
            reply_markup=InlineKeyboardBuilder().button(
                text=f"{Emoji.BACK} Назад",
                callback_data="back_to_switch_role"
            ).as_markup()
        )
        return

    role_keyboard = get_role_selection_keyboard(user_roles)
    if not role_keyboard:
        await callback.message.edit_text(
            "✅ У вас уже есть все доступные роли!",
            reply_markup=InlineKeyboardBuilder().button(
                text=f"{Emoji.BACK} Назад",
                callback_data="back_to_switch_role"
            ).as_markup()
        )
        return

    await callback.message.edit_text(
        "📝 Выберите роль для запроса:",
        reply_markup=role_keyboard
    )


@router.callback_query(F.data == "back_to_switch_role")
@with_db
async def process_back_to_switch_role(callback: CallbackQuery, db: Session):
    """Возврат в меню смены роли"""
    user = await get_user(db, callback.from_user.id)
    if not user:
        await callback.message.edit_text("❌ Пользователь не найден.")
        return

    user_roles = get_user_roles(user)
    current_role = get_user_main_role(user)

    await callback.message.edit_text(
        f"{Emoji.SWITCH} Переключение роли\n"
        f"Текущая роль: {current_role}\n\n"
        f"Выберите роль для переключения или запросите новую:",
        reply_markup=get_switch_role_keyboard(user_roles, current_role)
    )


# ==================== ОБРАБОТЧИКИ ДЛЯ ВОДИТЕЛЕЙ ====================
@router.message(F.text == f"{Emoji.TASK} Мои задачи")
@with_db
async def process_my_tasks(message: Message, db: Session):
    """Просмотр своих задач для водителя"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer(f"{Emoji.ERROR} Сначала используйте /start для регистрации.")
        return

    # Находим активную парковку
    active_parking = db.query(Parking).filter(
        Parking.user_id == user.id,
        Parking.departure_time == None
    ).first()

    if not active_parking:
        await message.answer(f"{Emoji.INFO} Вы не припаркованы.")
        return

    # Ищем задачи, связанные с ТС водителя
    tasks = db.query(Task).filter(
        Task.parking_id == active_parking.id
    ).order_by(Task.created_at.desc()).all()

    if not tasks:
        await message.answer(
            f"{Emoji.INFO} На ваше ТС нет задач.\n"
            f"📍 Место #{active_parking.spot_number}\n"
            f"🚗 ТС: {active_parking.vehicle_number}"
        )
        return

    response = f"{Emoji.TASK} ЗАДАЧИ ДЛЯ ВАШЕГО ТС:\n\n"
    response += f"📍 Место #{active_parking.spot_number}\n"
    response += f"🚗 ТС: {active_parking.vehicle_number}\n\n"

    for task in tasks:
        status_emoji = TASK_STATUS_EMOJI.get(task.status, "❓")
        status_text = STATUS_NAMES.get(task.status, task.status)

        response += (
            f"{status_emoji} Задача #{task.id}\n"
            f"🚪 Ворота: #{task.gate_number}\n"
            f"📌 Статус: {status_text}\n"
        )

        if task.status == "PENDING":
            response += f"⏰ Ожидает выполнения\n"
        elif task.status == "IN_PROGRESS" and task.started_at:
            started = ensure_timezone_aware(task.started_at)
            duration = get_timezone_aware_now() - started
            minutes = int(duration.total_seconds() / 60)
            response += f"⏰ В работе: {minutes} мин\n"
        elif task.status == "COMPLETED" and task.completed_at:
            completed = ensure_timezone_aware(task.completed_at)
            response += f"✅ Выполнена: {completed.strftime('%H:%M %d.%m.%Y')}\n"
        elif task.status == "STUCK":
            response += f"⚠️ Причина: {task.stuck_reason or 'Не указана'}\n"

        response += f"{'─' * 30}\n"

    await message.answer(response[:4000])


# ==================== ОБРАБОТЧИКИ ЗАПРОСА РОЛИ ====================
@router.message(F.text.contains("Запросить роль"))
@with_db
async def process_role_request_start(message: Message, state: FSMContext, db: Session):
    """Начало процесса запроса новой роли"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer("❌ Сначала используйте /start для регистрации.")
        return

    # Проверка активного запроса
    active_request = db.query(RoleRequest).filter(
        RoleRequest.user_id == user.id,
        RoleRequest.status == "ожидает"
    ).first()

    if active_request:
        role_names = {
            "OPERATOR": "Оператор",
            "DRIVER_TRANSFER": "Водитель перегона",
            "ADMIN": "Администратор",
            "DEB_EMPLOYEE": "Сотрудник ДЭБ"
        }
        await message.answer(
            f"ℹ️ У вас уже есть активный запрос на роль "
            f"'{role_names.get(active_request.requested_role, active_request.requested_role)}'.\n"
            f"⏰ Дата запроса: {active_request.created_at.strftime('%d.%m.%Y %H:%M')}\n"
            f"📋 Статус: {active_request.status}\n\n"
            f"Ожидайте решения администратора."
        )
        return

    user_roles = get_user_roles(user)
    if not get_role_selection_keyboard(user_roles):
        await message.answer("✅ У вас уже есть все доступные роли!")
        return

    await state.set_state(RequestStates.waiting_for_full_name)
    await message.answer(
        "📝 Запрос новой роли\n\n"
        "Шаг 1 из 3\n"
        "Введите ваше Имя и Фамилию через пробел:\n"
        "Пример: Иван Иванов",
        reply_markup=get_cancel_keyboard()
    )


@router.message(RequestStates.waiting_for_full_name)
@with_db
async def process_full_name(message: Message, state: FSMContext, db: Session):
    """Шаг 2: Обработка ввода имени и фамилии"""
    if message.text == f"{Emoji.CANCEL} Отмена":
        await state.clear()
        user = await get_user(db, message.from_user.id)
        await message.answer("❌ Запрос отменен.", reply_markup=get_main_menu_keyboard(user))
        return

    full_name = message.text.strip()
    parts = full_name.split()

    if len(parts) < 2:
        await message.answer(
            "❌ Пожалуйста, введите Имя и Фамилию через пробел.\n"
            "Пример: Иван Иванов\n\nПопробуйте еще раз:"
        )
        return

    first_name, last_name = parts[0], " ".join(parts[1:])

    if len(first_name) < 2 or len(last_name) < 2:
        await message.answer("❌ Имя и фамилия должны содержать минимум 2 символа. Попробуйте еще раз:")
        return

    await state.update_data(first_name=first_name, last_name=last_name)
    await state.set_state(RequestStates.waiting_for_position)
    await message.answer(
        "📝 Запрос новой роли\n\n"
        "Шаг 2 из 3\n"
        "Введите вашу Должность:"
    )


@router.message(RequestStates.waiting_for_position)
@with_db
async def process_position(message: Message, state: FSMContext, db: Session):
    """Шаг 3: Обработка ввода должности"""
    position = message.text.strip()
    if len(position) < 2:
        await message.answer("❌ Должность должна содержать минимум 2 символа. Попробуйте еще раз:")
        return

    await state.update_data(position=position)

    user = await get_user(db, message.from_user.id)
    user_roles = get_user_roles(user)

    role_keyboard = get_role_selection_keyboard(user_roles)
    if not role_keyboard:
        await message.answer("❌ У вас уже есть все доступные роли.")
        await state.clear()
        return

    await state.set_state(RequestStates.waiting_for_role_selection)
    await message.answer(
        "📝 Запрос новой роли\n\n"
        "Шаг 3 из 3\n"
        "Выберите роль, которую хотите запросить:",
        reply_markup=role_keyboard
    )


@router.callback_query(F.data.startswith("request_role_"))
@with_db
async def process_request_role_final(callback: CallbackQuery, state: FSMContext, db: Session):
    """Финальная обработка запроса роли"""
    role_str = callback.data.replace("request_role_", "")
    user = await get_user(db, callback.from_user.id)

    if not user:
        await callback.message.edit_text("❌ Пользователь не найден.")
        return

    user_roles = get_user_roles(user)
    role_names = {
        "DRIVER": "Водитель",
        "OPERATOR": "Оператор",
        "DRIVER_TRANSFER": "Водитель перегона",
        "ADMIN": "Администратор",
        "DEB_EMPLOYEE": "Сотрудник ДЭБ"
    }

    # Спецобработка для роли DRIVER (только для админов)
    if role_str == "DRIVER":
        if "ADMIN" not in user_roles:
            await callback.message.edit_text("❌ Только администраторы могут запросить роль Водитель.")
            return

        if role_str in user_roles:
            await callback.message.edit_text(f"❌ У вас уже есть роль '{role_names[role_str]}'.")
            await state.clear()
            return

        role_model = db.query(RoleModel).filter(RoleModel.name == role_str).first()
        if role_model:
            user.roles.append(role_model)
            db.commit()
            await callback.message.edit_text(f"✅ Роль '{role_names[role_str]}' успешно добавлена!")
            await callback.message.answer(
                "Теперь вы можете переключиться на роль Водитель.",
                reply_markup=get_main_menu_keyboard(user)
            )
        return

    # Обычный процесс для других ролей
    data = await state.get_data()
    first_name = data.get('first_name', '')
    last_name = data.get('last_name', '')
    position = data.get('position', '')

    if role_str in user_roles:
        await callback.message.edit_text(f"❌ У вас уже есть роль '{role_names.get(role_str, role_str)}'.")
        await state.clear()
        return

    # Создание запроса
    role_request = RoleRequest(
        user_id=user.id,
        requested_role=role_str,
        first_name=first_name,
        last_name=last_name,
        position=position,
        status="ожидает",
        created_at=get_timezone_aware_now()
    )
    db.add(role_request)
    db.commit()

    role_name = role_names.get(role_str, role_str)
    await callback.message.edit_text(
        f"✅ Запрос на роль '{role_name}' успешно отправлен!\n\n"
        f"📋 Ваши данные:\n"
        f"👤 Имя: {first_name} {last_name}\n"
        f"💼 Должность: {position}\n"
        f"📝 Запрошенная роль: {role_name}\n"
        f"⏰ Дата запроса: {get_timezone_aware_now().strftime('%d.%m.%Y %H:%M')}\n\n"
        f"Ожидайте решения администратора."
    )

    # Уведомление всех администраторов
    admins = db.query(User).filter(
        User.roles.any(RoleModel.name == "ADMIN")
    ).all()

    for admin in admins:
        try:
            await bot.send_message(
                admin.telegram_id,
                f"🆕 Новый запрос на роль!\n\n"
                f"📋 Информация:\n"
                f"👤 Пользователь: {first_name} {last_name}\n"
                f"💼 Должность: {position}\n"
                f"📝 Запрошенная роль: {role_name}\n"
                f"🆔 Telegram ID: {user.telegram_id}\n"
                f"👤 Username: @{user.username or 'нет'}\n"
                f"⏰ Время запроса: {get_timezone_aware_now().strftime('%d.%m.%Y %H:%M')}\n\n"
                f"Для обработки перейдите в меню '{Emoji.SETTINGS} Выдать роли'."
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления администратора {admin.telegram_id}: {e}")

    await state.clear()


# ==================== ОБРАБОТЧИКИ ДЛЯ ПРИБЫТИЯ/УБЫТИЯ ====================
@router.message(F.text == f"{Emoji.ARRIVAL} Прибытие")
@with_db
async def process_arrival(message: Message, state: FSMContext, db: Session):
    """Процесс регистрации прибытия ТС"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer(f"{Emoji.ERROR} Сначала используйте /start для регистрации.")
        return

    if "DRIVER" not in get_user_roles(user):
        await message.answer(f"{Emoji.ERROR} Эта функция доступна только водителям.")
        return

    # Проверка активной парковки
    active_parking = db.query(Parking).filter(
        Parking.user_id == user.id,
        Parking.departure_time == None
    ).first()

    if active_parking:
        await message.answer(
            f"{Emoji.WARNING} Вы уже припаркованы на месте #{active_parking.spot_number}.\n"
            f"⏰ Время прибытия: {active_parking.arrival_time.strftime('%H:%M %d.%m.%Y')}",
            reply_markup=get_main_menu_keyboard(user)
        )
        return

    # Проверка, не в очереди ли уже
    in_queue = db.query(ParkingQueue).filter(
        ParkingQueue.user_id == user.id,
        ParkingQueue.status.in_(["waiting", "notified"])
    ).first()

    if in_queue:
        position = await get_queue_position(db, user.id)
        await message.answer(
            f"{Emoji.QUEUE} Вы уже в очереди на парковку!\n\n"
            f"🚗 ТС: {in_queue.vehicle_number}\n"
            f"📝 Тип: {'Перецепной' if in_queue.is_hitch else 'Не перецепной'}\n"
            f"📊 Позиция в очереди: {position}\n"
            f"⏰ Время ожидания: {format_duration(int((get_timezone_aware_now() - ensure_timezone_aware(in_queue.created_at)).total_seconds()))}\n\n"
            f"Мы уведомим вас, когда освободится место."
        )
        return

    await state.set_state(DriverStates.waiting_for_vehicle_number)
    await message.answer(
        f"{Emoji.VEHICLE} Введите номер ТС в формате:\n"
        "• Х111ХХ11 (8 символов)\n"
        "• Х111ХХ111 (9 символов)\n\n"
        "Где Х - буквы, 1 - цифры\n"
        "Пример: А123КВ45 или А123КВ123",
        reply_markup=get_cancel_keyboard()
    )


@router.message(DriverStates.waiting_for_vehicle_number)
@with_db
async def process_vehicle_number(message: Message, state: FSMContext, db: Session):
    """Обработка введенного номера ТС"""
    if message.text == f"{Emoji.CANCEL} Отмена":
        await state.clear()
        user = await get_user(db, message.from_user.id)
        await message.answer("❌ Отменено", reply_markup=get_main_menu_keyboard(user))
        return

    vehicle_number = message.text.upper().strip()

    if not await validate_vehicle_number(vehicle_number):
        valid_info = VehicleNumberValidator.get_valid_letters_info()
        examples = VehicleNumberValidator.get_examples()
        await message.answer(
            f"❌ Неверный формат номера ТС!\n\n{valid_info}\n\n{examples}\n\nПопробуйте еще раз:"
        )
        return

    await state.update_data(vehicle_number=vehicle_number)
    await state.set_state(DriverStates.waiting_for_vehicle_type)
    await message.answer(
        "📋 Выберите тип транспортного средства:",
        reply_markup=get_vehicle_type_keyboard()
    )


@router.callback_query(F.data.startswith("vehicle_"))
@with_db
async def process_vehicle_type(callback: CallbackQuery, state: FSMContext, db: Session):
    """Обработка выбора типа ТС"""
    vehicle_type = callback.data.replace("vehicle_", "")
    if vehicle_type not in ["non_hitch", "hitch"]:
        await callback.message.edit_text(f"{Emoji.ERROR} Неверный тип ТС.")
        return

    data = await state.get_data()
    vehicle_number = data.get('vehicle_number', '')
    user = await get_user(db, callback.from_user.id)

    # Проверяем свободные места
    spot_number = await get_free_parking_spot(db)

    if spot_number:
        # Есть свободное место - паркуем
        is_hitch = vehicle_type == "hitch"
        parking = Parking(
            user_id=user.id,
            vehicle_number=vehicle_number,
            vehicle_type="HITCH" if is_hitch else "NON_HITCH",
            spot_number=spot_number,
            is_hitch=is_hitch,
            arrival_time=get_timezone_aware_now()
        )
        db.add(parking)
        db.commit()

        # Отправляем изображение парковочного места
        await send_gate_image(
            callback.message,
            "PARKING",
            spot_number,
            f"{Emoji.SUCCESS} Вы успешно припарковались!\n\n"
            f"📋 Информация:\n"
            f"📍 Место: #{spot_number}\n"
            f"🚗 ТС: {vehicle_number}\n"
            f"📝 Тип: {'Перецепной' if is_hitch else 'Не перецепной'}\n"
            f"⏰ Время: {parking.arrival_time.strftime('%H:%M %d.%m.%Y')}\n\n"
            f"Для убытия используйте кнопку '{Emoji.DEPARTURE} Убытие'"
        )
    else:
        # Нет свободных мест - ставим в очередь
        queue_item = await add_to_parking_queue(db, user.id, vehicle_number, vehicle_type == "hitch")
        position = await get_queue_position(db, user.id)
        queue_stats = await get_queue_stats(db)

        # Примерное время ожидания (5 минут на машину)
        estimated_minutes = position * 5

        await callback.message.edit_text(
            f"{Emoji.QUEUE} Парковка полностью заполнена!\n\n"
            f"🚗 Ваше ТС: {vehicle_number}\n"
            f"📝 Тип: {'Перецепной' if vehicle_type == 'hitch' else 'Не перецепной'}\n"
            f"📊 Поставлен в очередь\n"
            f"🔢 Позиция в очереди: {position}\n"
            f"⏰ Примерное время ожидания: ~{estimated_minutes} мин\n"
            f"📈 Всего в очереди: {queue_stats['total']} ТС\n\n"
            f"{Emoji.NOTIFIED} Вы получите уведомление, когда освободится место.\n"
            f"{Emoji.WARNING} Не нужно повторно нажимать 'Прибытие'!"
        )

    await callback.message.answer(
        "Главное меню:",
        reply_markup=get_main_menu_keyboard(user)
    )
    await state.clear()

@router.message(F.text == f"{Emoji.DEPARTURE} Убытие")
@with_db
async def process_departure(message: Message, db: Session):
    """Обработка убытия ТС с парковки"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer(f"{Emoji.ERROR} Сначала используйте /start для регистрации.")
        return

    active_parking = db.query(Parking).filter(
        Parking.user_id == user.id,
        Parking.departure_time == None
    ).first()

    if not active_parking:
        await message.answer(f"{Emoji.ERROR} Вы не припаркованы.")
        return

    # Проверка активных задач
    active_task = db.query(Task).filter(
        Task.parking_id == active_parking.id,
        Task.status.in_(["PENDING", "IN_PROGRESS"])
    ).first()

    if active_task:
        await message.answer(
            f"{Emoji.WARNING} У вас есть активное задание #{active_task.id}!\n"
            f"🚪 Ворота: #{active_task.gate_number}\n\n"
            f"Сначала выполните задание или отмените его."
        )
        return

    # Запоминаем номер освобождающегося места
    freed_spot = active_parking.spot_number

    # Фиксируем убытие
    active_parking.departure_time = get_timezone_aware_now()
    db.commit()

    await message.answer(
        f"{Emoji.SUCCESS} Убытие зафиксировано!\n\n"
        f"📋 Информация:\n"
        f"📍 Место #{freed_spot} освобождено\n"
        f"🚗 ТС: {active_parking.vehicle_number}\n"
        f"⏰ Время убытия: {active_parking.departure_time.strftime('%H:%M %d.%m.%Y')}",
        reply_markup=get_main_menu_keyboard(user)
    )

    # Обрабатываем очередь - отправляем уведомление следующему
    next_in_queue = await process_parking_departure(db, freed_spot, bot)

    if next_in_queue:
        logger.info(f"✅ Уведомление отправлено следующему в очереди (ID: {next_in_queue.user_id})")
    else:
        logger.info(f"ℹ️ Очередь пуста, место #{freed_spot} свободно")


# ==================== ОБРАБОТЧИКИ ДЛЯ ВОРОТ ====================
@router.message(F.text == f"{Emoji.GATE} Встать на ворота")
@with_db
async def process_gate_request(message: Message, state: FSMContext, db: Session):
    """Обработка постановки на ворота"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer(f"{Emoji.ERROR} Сначала используйте /start для регистрации.")
        return

    # Находим активную парковку пользователя
    active_parking = db.query(Parking).filter(
        Parking.user_id == user.id,
        Parking.departure_time == None
    ).first()

    if not active_parking:
        await message.answer(
            f"{Emoji.ERROR} Вы не припаркованы.\n"
            f"Сначала зафиксируйте прибытие."
        )
        return

    # Ищем задачу, связанную с этим ТС
    task = db.query(Task).filter(
        Task.parking_id == active_parking.id,
        Task.status.in_(["PENDING", "IN_PROGRESS"])
    ).first()

    if not task:
        await message.answer(
            f"{Emoji.INFO} На ваше ТС не назначено активных задач.\n"
            f"📍 Место #{active_parking.spot_number}\n"
            f"🚗 ТС: {active_parking.vehicle_number}\n\n"
            f"Дождитесь уведомления от оператора с номером ворот."
        )
        return

    logger.info(f"Найдена задача #{task.id}: статус={task.status}, driver_id={task.driver_id}, user_id={user.id}")

    # Проверяем, не занята ли задача другим водителем
    if task.driver_id and task.driver_id != user.id:
        other_driver = await get_user(db, task.driver_id)
        driver_name = f"{other_driver.first_name} {other_driver.last_name}".strip() if other_driver else "Другой водитель"
        await message.answer(
            f"{Emoji.WARNING} Задача #{task.id} уже выполняется другим водителем!\n"
            f"👤 Водитель: {driver_name}\n"
            f"🚪 Ворота: #{task.gate_number}\n\n"
            f"Свяжитесь с оператором для уточнения."
        )
        return

    # Если задача в статусе IN_PROGRESS, но driver_id не указан - исправляем это
    if task.status == "IN_PROGRESS" and not task.driver_id:
        task.driver_id = user.id
        if not task.started_at:
            task.started_at = get_timezone_aware_now()
        db.commit()
        logger.info(f"Задача #{task.id}: назначен водитель {user.id}")

    # Если задача в статусе PENDING, назначаем на водителя
    elif task.status == "PENDING":
        task.driver_id = user.id
        task.status = "IN_PROGRESS"
        task.started_at = get_timezone_aware_now()
        db.commit()
        logger.info(f"Задача #{task.id}: переведена в IN_PROGRESS, назначен водитель {user.id}")

    # Сохраняем данные задачи в состояние
    await state.update_data(
        task_id=task.id,
        gate_number=task.gate_number,
        parking_spot=active_parking.spot_number,
        vehicle_number=active_parking.vehicle_number
    )

    # Получаем путь к изображению ворот
    image_path = await get_gate_image_path(task.gate_number)

    # Формируем текст сообщения
    abk_info = ""
    if hasattr(task, 'abk_type') and task.abk_type:
        abk_info = f"🏢 {task.abk_type}\n"

    message_text = (
        f"{Emoji.GATE} ПОДТВЕРДИТЕ СТАТУС ВОРОТ\n\n"
        f"📋 Информация о задании:\n"
        f"🆔 Задача: #{task.id}\n"
        f"{abk_info}"
        f"🚪 Номер ворот: #{task.gate_number}\n"
        f"📍 Ваше место: #{active_parking.spot_number}\n"
        f"🚗 Ваше ТС: {active_parking.vehicle_number}\n"
        f"⏰ Время начала: {task.started_at.strftime('%H:%M %d.%m.%Y') if task.started_at else 'Только что'}\n\n"
        f"Выберите действие:"
    )

    # Создаем инлайн-клавиатуру для выбора действия
    builder = InlineKeyboardBuilder()
    builder.button(
        text=f"{Emoji.COMPLETED} Задача выполнена, встал на ворота",
        callback_data="gate_completed"
    )
    builder.button(
        text=f"{Emoji.GATE_OCCUPIED} Ворота заняты другим ТС",
        callback_data="gate_occupied"
    )
    builder.button(
        text=f"{Emoji.CANCEL} Отмена",
        callback_data="gate_cancel"
    )
    builder.adjust(1)

    # Устанавливаем состояние
    await state.set_state(DriverStates.waiting_for_gate_confirmation)

    # Отправляем сообщение с изображением, если оно есть
    if image_path and image_path.exists():
        try:
            photo = FSInputFile(image_path)
            await message.answer_photo(
                photo=photo,
                caption=message_text,
                reply_markup=builder.as_markup()
            )
        except Exception as e:
            logger.error(f"Ошибка при отправке изображения ворот: {e}")
            await message.answer(
                message_text,
                reply_markup=builder.as_markup()
            )
    else:
        await message.answer(
            message_text,
            reply_markup=builder.as_markup()
        )


@router.callback_query(F.data == "gate_completed")
@with_db
async def process_gate_completed(callback: CallbackQuery, state: FSMContext, db: Session):
    """Обработка успешного выполнения задачи"""
    data = await state.get_data()
    if not data:
        await callback.message.edit_text(f"{Emoji.ERROR} Данные не найдены. Попробуйте заново.")
        await state.clear()
        return

    task_id = data.get('task_id')
    if not task_id:
        await callback.message.edit_text(f"{Emoji.ERROR} ID задачи не найден.")
        await state.clear()
        return

    task = db.query(Task).filter(Task.id == task_id).first()
    user = await get_user(db, callback.from_user.id)

    if not task:
        await callback.message.edit_text(f"{Emoji.ERROR} Задача не найдена.")
        await state.clear()
        return

    if not user:
        await callback.message.edit_text(f"{Emoji.ERROR} Пользователь не найден.")
        await state.clear()
        return

    # Находим активную парковку
    active_parking = db.query(Parking).filter(
        Parking.user_id == user.id,
        Parking.departure_time == None
    ).first()

    # Обновляем статус задачи
    task.status = "COMPLETED"
    task.completed_at = get_timezone_aware_now()

    # Фиксируем убытие с парковки
    if active_parking:
        active_parking.departure_time = get_timezone_aware_now()
        active_parking.gate_number = task.gate_number

    db.commit()

    await callback.message.edit_text(
        f"{Emoji.SUCCESS} ЗАДАЧА ВЫПОЛНЕНА!\n\n"
        f"📋 Информация:\n"
        f"🆔 Задача: #{task.id}\n"
        f"🚪 Ворота: #{task.gate_number}\n"
        f"📍 Место #{active_parking.spot_number if active_parking else '?'} освобождено\n"
        f"⏰ Время завершения: {task.completed_at.strftime('%H:%M %d.%m.%Y')}"
    )

    # Уведомляем оператора об успешном выполнении
    if task.operator_id:
        try:
            await bot.send_message(
                task.operator_id,
                f"{Emoji.SUCCESS} ЗАДАЧА #{task.id} ВЫПОЛНЕНА!\n\n"
                f"👤 Водитель: {user.first_name} {user.last_name}\n"
                f"🚗 ТС: {task.parking.vehicle_number if task.parking else 'Неизвестно'}\n"
                f"🚪 Ворота: #{task.gate_number}\n"
                f"⏰ Время: {task.completed_at.strftime('%H:%M %d.%m.%Y')}"
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления оператора: {e}")

    # Отправляем обновленное главное меню
    await callback.message.answer(
        "Главное меню:",
        reply_markup=get_main_menu_keyboard(user)
    )

    await state.clear()


@router.callback_query(F.data == "gate_occupied")
@with_db
async def process_gate_occupied(callback: CallbackQuery, state: FSMContext, db: Session):
    """Обработка ситуации, когда ворота заняты"""
    data = await state.get_data()
    if not data:
        await callback.message.edit_text(f"{Emoji.ERROR} Данные не найдены. Попробуйте заново.")
        await state.clear()
        return

    task_id = data.get('task_id')
    gate_number = data.get('gate_number')

    if not task_id:
        await callback.message.edit_text(f"{Emoji.ERROR} ID задачи не найден.")
        await state.clear()
        return

    task = db.query(Task).filter(Task.id == task_id).first()
    user = await get_user(db, callback.from_user.id)

    if not task:
        await callback.message.edit_text(f"{Emoji.ERROR} Задача не найдена.")
        await state.clear()
        return

    if not user:
        await callback.message.edit_text(f"{Emoji.ERROR} Пользователь не найден.")
        await state.clear()
        return

    # Помечаем задачу как зависшую
    task.status = "STUCK"
    task.is_stuck = True
    task.stuck_reason = f"Ворота #{gate_number} заняты другим ТС"

    # Для перецепных ТС возвращаем в пул
    if task.parking and task.parking.is_hitch:
        task.is_in_pool = True
        task.priority += 10  # Сильно повышаем приоритет

    db.commit()

    await callback.message.edit_text(
        f"{Emoji.WARNING} ВОРОТА ЗАНЯТЫ!\n\n"
        f"📋 Информация:\n"
        f"🆔 Задача: #{task.id}\n"
        f"🚪 Ворота: #{gate_number} заняты другим ТС\n"
        f"👤 Ваш статус: Ожидайте нового назначения\n\n"
        f"Оператор уже уведомлен и назначит новую задачу."
    )

    # Находим всех операторов и администраторов для уведомления
    operators = db.query(User).filter(
        User.roles.any(RoleModel.name.in_(["OPERATOR", "ADMIN"]))
    ).all()

    # Уведомление для оператора, создавшего задачу
    if task.operator_id:
        try:
            await bot.send_message(
                task.operator_id,
                f"{Emoji.GATE_OCCUPIED} СРОЧНО! ВОРОТА #{gate_number} ЗАНЯТЫ!\n\n"
                f"📋 Детали проблемы:\n"
                f"🆔 Задача: #{task.id}\n"
                f"👤 Водитель: {user.first_name} {user.last_name}\n"
                f"🚗 ТС: {task.parking.vehicle_number if task.parking else 'Неизвестно'}\n"
                f"📍 Место: #{task.parking.spot_number if task.parking else '?'}\n"
                f"🚪 Назначенные ворота: #{gate_number}\n\n"
                f"🔧 НЕОБХОДИМЫЕ ДЕЙСТВИЯ:\n"
                f"1. Проверить, кто сейчас занимает ворота #{gate_number}\n"
                f"2. Связаться с водителем, который там находится\n"
                f"3. Освободить ворота или назначить другие ворота\n"
                f"4. Создать новую задачу для водителя {user.first_name}"
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления оператора: {e}")

    # Массовое уведомление всех операторов
    for op in operators:
        if op.id != task.operator_id:  # Не дублируем основному оператору
            try:
                await bot.send_message(
                    op.telegram_id,
                    f"{Emoji.WARNING} ПРОБЛЕМА С ЗАДАЧЕЙ #{task.id}\n\n"
                    f"🚪 Ворота #{gate_number} заняты\n"
                    f"👤 Водитель: {user.first_name} {user.last_name}\n"
                    f"🚗 ТС: {task.parking.vehicle_number if task.parking else 'Неизвестно'}\n\n"
                    f"Требуется вмешательство оператора."
                )
            except Exception as e:
                logger.error(f"Ошибка массового уведомления: {e}")

    # Предлагаем водителю варианты действий
    builder = InlineKeyboardBuilder()
    builder.button(
        text=f"{Emoji.RETRY} Попробовать снова",
        callback_data="retry_gate"
    )
    builder.button(
        text=f"{Emoji.ASSIGN_AGAIN} Запросить новые ворота",
        callback_data="request_new_gate"
    )
    builder.button(
        text=f"{Emoji.CANCEL} Отмена",
        callback_data="gate_cancel"
    )
    builder.adjust(1)

    await callback.message.answer(
        f"{Emoji.QUESTION} ЧТО ДЕЛАТЬ ДАЛЬШЕ?\n\n"
        f"Вы можете:\n"
        f"• Попробовать снова подъехать к воротам\n"
        f"• Запросить у оператора другие ворота\n"
        f"• Отменить действие",
        reply_markup=builder.as_markup()
    )


@router.callback_query(F.data == "retry_gate")
async def process_retry_gate(callback: CallbackQuery, state: FSMContext):
    """Повторная попытка встать на ворота"""
    data = await state.get_data()
    task_id = data.get('task_id')
    gate_number = data.get('gate_number')
    parking_spot = data.get('parking_spot')
    vehicle_number = data.get('vehicle_number')

    await callback.message.edit_text(
        f"{Emoji.RETRY} ПОВТОРНАЯ ПОПЫТКА\n\n"
        f"📋 Информация:\n"
        f"🆔 Задача: #{task_id}\n"
        f"🚪 Ворота: #{gate_number}\n"
        f"📍 Ваше место: #{parking_spot}\n"
        f"🚗 ТС: {vehicle_number}\n\n"
        f"Подъезжайте к воротам #{gate_number} снова.\n"
        f"Если ворота все еще заняты, запросите новые ворота."
    )

    # Возвращаемся к выбору действия
    builder = InlineKeyboardBuilder()
    builder.button(
        text=f"{Emoji.COMPLETED} Задача выполнена, встал на ворота",
        callback_data="gate_completed"
    )
    builder.button(
        text=f"{Emoji.GATE_OCCUPIED} Ворота все еще заняты",
        callback_data="gate_occupied"
    )
    builder.button(
        text=f"{Emoji.CANCEL} Отмена",
        callback_data="gate_cancel"
    )
    builder.adjust(1)

    await callback.message.answer(
        "Выберите действие после повторной попытки:",
        reply_markup=builder.as_markup()
    )


@router.callback_query(F.data == "request_new_gate")
@with_db
async def process_request_new_gate(callback: CallbackQuery, state: FSMContext, db: Session):
    """Запрос новых ворот у оператора"""
    data = await state.get_data()
    task_id = data.get('task_id')
    gate_number = data.get('gate_number')

    task = db.query(Task).filter(Task.id == task_id).first()
    user = await get_user(db, callback.from_user.id)

    if not task:
        await callback.message.edit_text(f"{Emoji.ERROR} Задача не найдена.")
        return

    await callback.message.edit_text(
        f"{Emoji.ASSIGN_AGAIN} ЗАПРОС НОВЫХ ВОРОТ ОТПРАВЛЕН\n\n"
        f"📋 Информация:\n"
        f"🆔 Задача: #{task.id}\n"
        f"🚪 Проблемные ворота: #{gate_number}\n"
        f"👤 Ваш статус: Ожидайте новые ворота от оператора\n\n"
        f"Оператор получил уведомление и скоро назначит новые ворота."
    )

    # Уведомляем оператора о запросе новых ворот
    if task.operator_id:
        try:
            await bot.send_message(
                task.operator_id,
                f"{Emoji.ASSIGN_AGAIN} ЗАПРОС НОВЫХ ВОРОТ\n\n"
                f"Водитель {user.first_name} {user.last_name} не может встать на ворота #{gate_number}\n\n"
                f"📋 Детали:\n"
                f"🆔 Задача: #{task.id}\n"
                f"🚗 ТС: {task.parking.vehicle_number if task.parking else 'Неизвестно'}\n"
                f"📍 Место: #{task.parking.spot_number if task.parking else '?'}\n\n"
                f"🔧 НЕОБХОДИМО:\n"
                f"1. Проверить свободные ворота\n"
                f"2. Назначить новые ворота для водителя\n"
                f"3. Создать новую задачу"
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления оператора: {e}")


@router.callback_query(F.data == "gate_cancel")
@with_db
async def process_gate_cancel(callback: CallbackQuery, state: FSMContext, db: Session):
    """Отмена действия с воротами"""
    user = await get_user(db, callback.from_user.id)
    await state.clear()

    await callback.message.edit_text(
        f"{Emoji.CANCEL} Действие отменено."
    )
    await callback.message.answer(
        "Главное меню:",
        reply_markup=get_main_menu_keyboard(user)
    )


# ==================== ОБРАБОТЧИКИ ДЛЯ ВОДИТЕЛЕЙ ПЕРЕГОНА ====================
@router.callback_query(F.data == "transfer_gate_occupied", DriverTransferStates.waiting_for_gate_confirmation)
@with_db
async def process_transfer_gate_occupied(callback: CallbackQuery, state: FSMContext, db: Session):
    """Обработка ситуации, когда ворота заняты для водителя перегона"""
    data = await state.get_data()
    if not data:
        await callback.message.edit_text(f"{Emoji.ERROR} Данные не найдены. Попробуйте заново.")
        await state.clear()
        return

    task_id = data.get('task_id')
    gate_number = data.get('gate_number')
    parking_spot = data.get('parking_spot')
    vehicle_number = data.get('vehicle_number')

    if not task_id:
        await callback.message.edit_text(f"{Emoji.ERROR} ID задачи не найден.")
        await state.clear()
        return

    task = db.query(Task).filter(Task.id == task_id).first()
    user = await get_user(db, callback.from_user.id)

    if not task:
        await callback.message.edit_text(f"{Emoji.ERROR} Задача не найдена.")
        await state.clear()
        return

    if not user:
        await callback.message.edit_text(f"{Emoji.ERROR} Пользователь не найден.")
        await state.clear()
        return

    # Помечаем задачу как зависшую
    task.status = "STUCK"
    task.is_stuck = True
    task.stuck_reason = f"Ворота #{gate_number} заняты другим ТС"

    # Возвращаем задачу в пул с повышенным приоритетом
    task.driver_id = None
    task.is_in_pool = True
    task.priority += 15  # Повышаем приоритет

    db.commit()

    # ОТВЕТ ВОДИТЕЛЮ - добавлено!
    await callback.message.edit_text(
        f"{Emoji.WARNING} ВОРОТА ЗАНЯТЫ!\n\n"
        f"📋 Информация:\n"
        f"🆔 Задача: #{task.id}\n"
        f"🚪 Ворота: #{gate_number} заняты другим ТС\n"
        f"📍 Ваше место: #{parking_spot}\n"
        f"🚗 Ваше ТС: {vehicle_number}\n\n"
        f"✅ Задача возвращена в пул с повышенным приоритетом {task.priority}\n"
        f"👤 Ваш статус: Ожидайте нового назначения\n\n"
        f"Оператор уже уведомлен."
    )

    # Уведомления операторам
    operators = db.query(User).filter(
        User.roles.any(RoleModel.name.in_(["OPERATOR", "ADMIN"]))
    ).all()

    # Уведомление для оператора, создавшего задачу
    if task.operator_id:
        try:
            await bot.send_message(
                task.operator_id,
                f"{Emoji.GATE_OCCUPIED} СРОЧНО! ВОРОТА #{gate_number} ЗАНЯТЫ!\n\n"
                f"📋 Детали проблемы:\n"
                f"🆔 Задача: #{task.id}\n"
                f"👤 Водитель: {user.first_name} {user.last_name}\n"
                f"🚗 ТС: {vehicle_number}\n"
                f"📍 Место: #{parking_spot}\n"
                f"🚪 Назначенные ворота: #{gate_number}\n\n"
                f"🔧 НЕОБХОДИМЫЕ ДЕЙСТВИЯ:\n"
                f"1. Проверить, кто сейчас занимает ворота #{gate_number}\n"
                f"2. Связаться с водителем, который там находится\n"
                f"3. Освободить ворота или назначить другие ворота\n"
                f"4. Создать новую задачу для водителя {user.first_name}"
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления оператора: {e}")

    # Возвращаем в главное меню
    await callback.message.answer(
        "Главное меню:",
        reply_markup=get_main_menu_keyboard(user)
    )

    await state.clear()
@router.callback_query(F.data == "transfer_no_vehicle", DriverTransferStates.waiting_for_gate_confirmation)
@with_db
async def process_transfer_no_vehicle(callback: CallbackQuery, state: FSMContext, db: Session):
    """Обработка ситуации, когда ТС отсутствует на парковочном месте"""
    data = await state.get_data()
    if not data:
        await callback.message.edit_text(f"{Emoji.ERROR} Данные не найдены. Попробуйте заново.")
        await state.clear()
        return

    task_id = data.get('task_id')
    gate_number = data.get('gate_number')
    parking_spot = data.get('parking_spot')
    vehicle_number = data.get('vehicle_number')

    if not task_id:
        await callback.message.edit_text(f"{Emoji.ERROR} ID задачи не найден.")
        await state.clear()
        return

    task = db.query(Task).filter(Task.id == task_id).first()
    user = await get_user(db, callback.from_user.id)

    if not task:
        await callback.message.edit_text(f"{Emoji.ERROR} Задача не найдена.")
        await state.clear()
        return

    # Помечаем задачу как зависшую из-за отсутствия ТС
    task.status = "STUCK"
    task.is_stuck = True
    task.stuck_reason = f"ТС отсутствует на парковочном месте #{parking_spot}"

    # Возвращаем задачу в пул с повышенным приоритетом
    task.driver_id = None
    task.is_in_pool = True
    task.priority += 20  # Сильно повышаем приоритет

    db.commit()

    # ОТВЕТ ВОДИТЕЛЮ - добавлено!
    await callback.message.edit_text(
        f"{Emoji.WARNING} ТС ОТСУТСТВУЕТ НА МЕСТЕ!\n\n"
        f"📋 Информация:\n"
        f"🆔 Задача: #{task.id}\n"
        f"🚪 Ворота: #{gate_number}\n"
        f"📍 Место #{parking_spot} - ТС не найдено\n"
        f"🚗 Ожидаемое ТС: {vehicle_number}\n\n"
        f"✅ Задача возвращена в пул с приоритетом {task.priority}\n"
        f"👤 Ваш статус: Ожидайте нового назначения\n\n"
        f"Оператор уведомлен о проблеме."
    )

    # Находим всех операторов
    operators = db.query(User).filter(
        User.roles.any(RoleModel.name.in_(["OPERATOR", "ADMIN"]))
    ).all()

    # Срочное уведомление для оператора
    if task.operator_id:
        try:
            await bot.send_message(
                task.operator_id,
                f"{Emoji.WARNING} СРОЧНО! ТС ОТСУТСТВУЕТ НА МЕСТЕ #{parking_spot}!\n\n"
                f"📋 Детали проблемы:\n"
                f"🆔 Задача: #{task.id}\n"
                f"👤 Водитель перегона: {user.first_name} {user.last_name}\n"
                f"🚗 Ожидаемое ТС: {vehicle_number}\n"
                f"📍 Назначенное место: #{parking_spot}\n"
                f"🚪 Ворота: #{gate_number}\n"
                f"📊 Новый приоритет: {task.priority}\n\n"
                f"🔧 НЕОБХОДИМЫЕ ДЕЙСТВИЯ:\n"
                f"1. Проверить, где находится ТС {vehicle_number}\n"
                f"2. Выяснить причину отсутствия\n"
                f"3. Задача автоматически вернется в пул"
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления оператора: {e}")

    # Возвращаем в главное меню
    await callback.message.answer(
        "Главное меню:",
        reply_markup=get_main_menu_keyboard(user)
    )

    await state.clear()

@router.callback_query(F.data == "transfer_breakdown", DriverTransferStates.waiting_for_gate_confirmation)
@with_db
async def process_transfer_breakdown(callback: CallbackQuery, state: FSMContext, db: Session):
    """Обработка поломки ТС для водителя перегона"""
    data = await state.get_data()
    if not data:
        await callback.message.edit_text(f"{Emoji.ERROR} Данные не найдены.")
        await state.clear()
        return

    task_id = data.get('task_id')
    gate_number = data.get('gate_number')
    parking_spot = data.get('parking_spot')
    vehicle_number = data.get('vehicle_number')

    task = db.query(Task).filter(Task.id == task_id).first()
    user = await get_user(db, callback.from_user.id)

    if not task:
        await callback.message.edit_text(f"{Emoji.ERROR} Задача не найдена.")
        await state.clear()
        return

    # Помечаем задачу как зависшую
    task.status = "STUCK"
    task.is_stuck = True
    task.stuck_reason = "Поломка ТС"
    task.driver_id = None
    task.is_in_pool = False  # Не возвращаем в пул, так как ТС сломано

    db.commit()

    # ОТВЕТ ВОДИТЕЛЮ - добавлено!
    await callback.message.edit_text(
        f"{Emoji.WARNING} ПОЛОМКА ТС!\n\n"
        f"📋 Информация:\n"
        f"🆔 Задача: #{task.id}\n"
        f"🚪 Ворота: #{gate_number}\n"
        f"📍 Место: #{parking_spot}\n"
        f"🚗 ТС: {vehicle_number}\n\n"
        f"❌ Задача закрыта (не возвращена в пул)\n"
        f"👤 Ваш статус: Ожидайте нового назначения\n\n"
        f"Оператор уведомлен о поломке."
    )

    # Уведомляем оператора
    if task.operator_id:
        try:
            await bot.send_message(
                task.operator_id,
                f"{Emoji.WARNING} ПОЛОМКА ТС В ЗАДАЧЕ #{task.id}\n\n"
                f"👤 Водитель перегона: {user.first_name} {user.last_name}\n"
                f"🚗 Ожидаемое ТС: {vehicle_number}\n"
                f"📍 Место: #{parking_spot}\n"
                f"🚪 Ворота: #{gate_number}\n\n"
                f"❌ ТС требует ремонта. Задача закрыта."
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления оператора: {e}")

    # Возвращаем в главное меню
    await callback.message.answer(
        "Главное меню:",
        reply_markup=get_main_menu_keyboard(user)
    )

    await state.clear()

@router.callback_query(F.data == "transfer_gate_completed", DriverTransferStates.waiting_for_gate_confirmation)
@with_db
async def process_transfer_gate_completed(callback: CallbackQuery, state: FSMContext, db: Session):
    """Обработка успешного выполнения задачи для водителя перегона"""
    data = await state.get_data()
    if not data:
        await callback.message.edit_text(f"{Emoji.ERROR} Данные не найдены. Попробуйте заново.")
        await state.clear()
        return

    task_id = data.get('task_id')
    if not task_id:
        await callback.message.edit_text(f"{Emoji.ERROR} ID задачи не найден.")
        await state.clear()
        return

    task = db.query(Task).filter(Task.id == task_id).first()
    user = await get_user(db, callback.from_user.id)

    if not task:
        await callback.message.edit_text(f"{Emoji.ERROR} Задача не найдена.")
        await state.clear()
        return

    # Обновляем статус задачи
    task.status = "COMPLETED"
    task.completed_at = get_timezone_aware_now()

    # Освобождаем парковочное место
    if task.parking:
        task.parking.departure_time = get_timezone_aware_now()
        task.parking.gate_number = task.gate_number

    db.commit()

    # ОТВЕТ ВОДИТЕЛЮ
    await callback.message.edit_text(
        f"{Emoji.SUCCESS} ЗАДАЧА #{task.id} ВЫПОЛНЕНА!\n\n"
        f"📋 Информация:\n"
        f"🚪 Ворота: #{task.gate_number}\n"
        f"🚗 ТС: {task.parking.vehicle_number if task.parking else 'Неизвестно'}\n"
        f"⏰ Время завершения: {task.completed_at.strftime('%H:%M %d.%m.%Y')}"
    )

    # Уведомляем оператора
    if task.operator_id:
        try:
            await bot.send_message(
                task.operator_id,
                f"{Emoji.SUCCESS} Задача #{task.id} выполнена!\n"
                f"👤 Водитель перегона: {user.first_name} {user.last_name}\n"
                f"🚗 ТС: {task.parking.vehicle_number if task.parking else 'Неизвестно'}\n"
                f"🚪 Ворота: #{task.gate_number}"
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления оператора: {e}")

    # Возвращаем в главное меню
    await callback.message.answer(
        "Главное меню:",
        reply_markup=get_main_menu_keyboard(user)
    )

    await state.clear()

@router.message(F.text == f"{Emoji.SHIFT_START} Начать смену")
@with_db
async def process_shift_start(message: Message, db: Session):
    """Начало рабочей смены водителя перегона"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer(f"{Emoji.ERROR} Сначала используйте /start для регистрации.")
        return

    if "DRIVER_TRANSFER" not in get_user_roles(user):
        await message.answer(f"{Emoji.ERROR} Эта функция доступна только водителям перегона.")
        return

    if user.is_on_shift:
        await message.answer(f"{Emoji.WARNING} Вы уже на смене!")
        return

    # Начинаем смену
    user.is_on_shift = True
    db.commit()

    # Отправляем сообщение о начале смены
    await message.answer(
        f"{Emoji.SUCCESS} Смена начата!\n\n"
        f"📋 Информация:\n"
        f"👤 Водитель: {user.first_name} {user.last_name}\n"
        f"⏰ Время начала: {get_timezone_aware_now().strftime('%H:%M %d.%m.%Y')}\n\n"
        f"Теперь вы можете принимать задания от оператора."
    )

    # Отправляем новое сообщение с обновленным меню
    await message.answer(
        f"{Emoji.MENU} Ваше меню обновлено.\n"
        f"Используйте кнопки ниже для работы:",
        reply_markup=get_main_menu_keyboard(user)
    )


@router.message(F.text == f"{Emoji.SHIFT_END} Закончить смену")
@with_db
async def process_shift_end(message: Message, db: Session):
    """Завершение рабочей смены водителя перегона"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer(f"{Emoji.ERROR} Сначала используйте /start для регистрации.")
        return

    if "DRIVER_TRANSFER" not in get_user_roles(user):
        await message.answer(f"{Emoji.ERROR} Эта функция доступна только водителям перегона.")
        return

    if not user.is_on_shift:
        await message.answer(f"{Emoji.WARNING} Вы не на смене!")
        return

    if user.is_on_break:
        await message.answer(
            f"{Emoji.ERROR} Вы на обеде!\n\n"
            f"Сначала вернитесь с обеда, затем завершите смену."
        )
        return

    # Проверка активных задач
    active_task = db.query(Task).filter(
        Task.driver_id == user.id,
        Task.status == "IN_PROGRESS"
    ).first()

    if active_task:
        await message.answer(
            f"{Emoji.WARNING} У вас есть активная задача #{active_task.id} в работе!\n"
            f"📍 Место: #{active_task.parking.spot_number}\n"
            f"🚪 Ворота: #{active_task.gate_number}\n\n"
            f"Завершите задачу перед окончанием смены."
        )
        return

    # Статистика смены
    now = get_timezone_aware_now()

    # Находим время начала смены (первое действие сегодня или created_at)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    first_task_today = db.query(Task).filter(
        Task.driver_id == user.id,
        Task.created_at >= today_start
    ).order_by(Task.created_at.asc()).first()

    if first_task_today:
        shift_start = ensure_timezone_aware(first_task_today.created_at)
    else:
        shift_start = ensure_timezone_aware(user.created_at)
        if shift_start.date() < now.date():
            shift_start = today_start

    # Получаем перерывы за смену
    breaks = db.query(Break).filter(
        Break.user_id == user.id,
        Break.start_time >= shift_start,
        Break.end_time != None
    ).all()

    total_break_seconds = sum(b.duration for b in breaks)

    # Получаем выполненные задачи за смену
    completed_tasks = db.query(Task).filter(
        Task.driver_id == user.id,
        Task.status == "COMPLETED",
        Task.completed_at >= shift_start
    ).all()

    # Получаем все задачи, которые водитель брал в работу
    all_tasks = db.query(Task).filter(
        Task.driver_id == user.id,
        Task.created_at >= shift_start
    ).all()

    # Завершаем смену
    user.is_on_shift = False
    db.commit()

    await message.answer(
        f"{Emoji.SUCCESS} Смена завершена!\n\n"
        f"📊 Итоговая статистика:\n"
        f"👤 Водитель: {user.first_name} {user.last_name}\n"
        f"⏰ Начало: {shift_start.strftime('%H:%M %d.%m.%Y')}\n"
        f"⏰ Окончание: {now.strftime('%H:%M %d.%m.%Y')}\n"
        f"{Emoji.BREAK_TIME} Время на обеде: {format_duration(total_break_seconds)}\n"
        f"{Emoji.COMPLETED} Выполнено задач: {len(completed_tasks)}\n"
        f"📝 Всего взято задач: {len(all_tasks)}\n\n"
        f"Спасибо за работу!"
    )

    await message.answer(
        f"{Emoji.MENU} Ваше меню обновлено.\n"
        f"Теперь вы можете начать новую смену.",
        reply_markup=get_main_menu_keyboard(user)
    )


@router.message(F.text == f"{Emoji.TASK} Взять задачу")
@with_db
async def process_take_task(message: Message, state: FSMContext, db: Session):
    """Взятие задачи из общего пула водителем перегона"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer(f"{Emoji.ERROR} Сначала используйте /start для регистрации.")
        return

    # Проверяем, есть ли у пользователя роль DRIVER_TRANSFER
    user_roles = get_user_roles(user)
    if "DRIVER_TRANSFER" not in user_roles:
        await message.answer(f"{Emoji.ERROR} Эта функция доступна только водителям перегона.")
        return

    if not user.is_on_shift:
        await message.answer(f"{Emoji.ERROR} Вы не на смене! Начните смену для принятия задач.")
        return

    # Проверяем, есть ли зависшие задачи у водителя
    stuck_task = db.query(Task).filter(
        Task.driver_id == user.id,
        Task.status == "STUCK"
    ).first()

    if stuck_task:
        # Автоматически снимаем зависшую задачу с водителя
        stuck_task.driver_id = None
        stuck_task.status = "PENDING"
        stuck_task.is_in_pool = True
        stuck_task.priority += 5
        db.commit()

        await message.answer(
            f"{Emoji.WARNING} С вас снята зависшая задача #{stuck_task.id}\n\n"
            f"📋 Информация о задаче:\n"
            f"📍 Место: #{stuck_task.parking.spot_number}\n"
            f"🚗 ТС: {stuck_task.parking.vehicle_number}\n"
            f"🚪 Ворота: #{stuck_task.gate_number}\n\n"
            f"✅ Задача возвращена в пул с повышенным приоритетом.\n"
            f"Теперь вы можете взять новую задачу."
        )

    # Проверяем, есть ли уже активная задача у водителя
    active_task = db.query(Task).filter(
        Task.driver_id == user.id,
        Task.status == "IN_PROGRESS"
    ).first()

    if active_task:
        await message.answer(
            f"{Emoji.WARNING} У вас уже есть активная задача #{active_task.id} в работе!\n"
            f"📍 Место: #{active_task.parking.spot_number}\n"
            f"🚪 Ворота: #{active_task.gate_number}\n\n"
            f"Сначала завершите текущую задачу."
        )
        return

    # Ищем задачи в общем пуле для перецепных ТС с максимальным приоритетом
    task = db.query(Task).join(Parking).filter(
        Task.status == "PENDING",
        Task.is_in_pool == True,
        Parking.is_hitch == True,
        Parking.departure_time == None,
        Task.driver_id == None
    ).order_by(
        Task.priority.desc(),
        Task.created_at.asc()
    ).first()

    if not task:
        await message.answer(f"{Emoji.INFO} Нет доступных задач в пуле.")
        return

    # Назначаем задачу водителю
    task.driver_id = user.id
    task.status = "IN_PROGRESS"
    task.started_at = get_timezone_aware_now()
    task.is_stuck = False
    task.stuck_reason = None
    db.commit()

    # Сохраняем данные задачи в состояние
    await state.update_data(
        task_id=task.id,
        gate_number=task.gate_number,
        parking_spot=task.parking.spot_number,
        vehicle_number=task.parking.vehicle_number,
        is_transfer_driver=True
    )

    # Получаем путь к изображению ворот
    image_path = await get_gate_image_path(task.gate_number)

    # Формируем текст сообщения
    abk_info = ""
    if hasattr(task, 'abk_type') and task.abk_type:
        abk_info = f"🏢 {task.abk_type}\n"

    message_text = (
        f"{Emoji.SUCCESS} Задача #{task.id} назначена вам!\n\n"
        f"📋 Информация:\n"
        f"🚗 ТС: {task.parking.vehicle_number}\n"
        f"📍 Место: #{task.parking.spot_number}\n"
        f"{abk_info}"
        f"🚪 Ворота: #{task.gate_number}\n"
        f"📝 Тип: Перецепной\n"
        f"⏰ Время начала: {task.started_at.strftime('%H:%M %d.%m.%Y')}\n\n"
        f"Выберите действие после подъезда к воротам:"
    )

    # Создаем инлайн-клавиатуру для действий с задачей
    builder = InlineKeyboardBuilder()
    builder.button(
        text=f"{Emoji.COMPLETED} Задача выполнена, ворота свободны",
        callback_data="transfer_gate_completed"
    )
    builder.button(
        text=f"{Emoji.GATE_OCCUPIED} Ворота заняты другим ТС",
        callback_data="transfer_gate_occupied"
    )
    builder.button(
        text=f"{Emoji.CANCEL} Нет ТС на месте",
        callback_data="transfer_no_vehicle"
    )
    builder.button(
        text="🔧 Поломка ТС",
        callback_data="transfer_breakdown"
    )
    builder.adjust(1)

    # Отправляем сообщение с изображением, если оно есть
    if image_path and image_path.exists():
        try:
            photo = FSInputFile(image_path)
            await message.answer_photo(
                photo=photo,
                caption=message_text,
                reply_markup=builder.as_markup()
            )
        except Exception as e:
            logger.error(f"Ошибка при отправке изображения ворот: {e}")
            await message.answer(
                message_text,
                reply_markup=builder.as_markup()
            )
    else:
        await message.answer(
            message_text,
            reply_markup=builder.as_markup()
        )

    # Устанавливаем состояние
    await state.set_state(DriverTransferStates.waiting_for_gate_confirmation)

    # Уведомляем оператора о том, кто взял задачу
    if task.operator_id:
        try:
            await bot.send_message(
                task.operator_id,
                f"{Emoji.DRIVER_TRANSFER} Водитель перегона {user.first_name} {user.last_name}\n"
                f"взял задачу #{task.id} в работу!\n"
                f"📍 Место: #{task.parking.spot_number}\n"
                f"🚪 Ворота: #{task.gate_number}"
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления оператора: {e}")


@router.message(F.text == f"{Emoji.TASK} Текущая задача")
@with_db
async def process_current_task(message: Message, state: FSMContext, db: Session):
    """Показать информацию о текущей задаче и предложить действия"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer(f"{Emoji.ERROR} Сначала используйте /start для регистрации.")
        return

    # Проверяем, есть ли у пользователя роль DRIVER_TRANSFER
    user_roles = get_user_roles(user)
    if "DRIVER_TRANSFER" not in user_roles:
        await message.answer(f"{Emoji.ERROR} Эта функция доступна только водителям перегона.")
        return

    # Находим активную задачу
    active_task = db.query(Task).filter(
        Task.driver_id == user.id,
        Task.status == "IN_PROGRESS"
    ).first()

    if not active_task:
        await message.answer(f"{Emoji.INFO} У вас нет активных задач.")
        return

    # Сохраняем данные задачи в состояние
    await state.update_data(
        task_id=active_task.id,
        gate_number=active_task.gate_number,
        parking_spot=active_task.parking.spot_number if active_task.parking else None,
        vehicle_number=active_task.parking.vehicle_number if active_task.parking else None,
        is_transfer_driver=True
    )

    # Время выполнения
    now = get_timezone_aware_now()
    started = ensure_timezone_aware(active_task.started_at) if active_task.started_at else now
    duration = now - started
    minutes = int(duration.total_seconds() / 60)

    if minutes < 60:
        duration_str = f"{minutes} мин"
    else:
        hours = minutes // 60
        mins = minutes % 60
        duration_str = f"{hours}ч {mins}мин"

    # Получаем путь к изображению ворот
    image_path = await get_gate_image_path(active_task.gate_number)

    # Формируем текст сообщения
    abk_info = ""
    if hasattr(active_task, 'abk_type') and active_task.abk_type:
        abk_info = f"🏢 {active_task.abk_type}\n"

    message_text = (
        f"{Emoji.TASK} ТЕКУЩАЯ ЗАДАЧА #{active_task.id}\n\n"
        f"📋 Информация:\n"
        f"🚗 ТС: {active_task.parking.vehicle_number if active_task.parking else 'Неизвестно'}\n"
        f"📍 Место: #{active_task.parking.spot_number if active_task.parking else '?'}\n"
        f"{abk_info}"
        f"🚪 Ворота: #{active_task.gate_number}\n"
        f"⏰ Время начала: {active_task.started_at.strftime('%H:%M %d.%m.%Y') if active_task.started_at else 'Неизвестно'}\n"
        f"⏱️ В работе: {duration_str}\n"
        f"📊 Приоритет: {active_task.priority}\n\n"
        f"Выберите действие:"
    )

    # Создаем инлайн-клавиатуру для действий с задачей
    builder = InlineKeyboardBuilder()
    builder.button(
        text=f"{Emoji.COMPLETED} Задача выполнена, ворота свободны",
        callback_data="transfer_gate_completed"
    )
    builder.button(
        text=f"{Emoji.GATE_OCCUPIED} Ворота заняты другим ТС",
        callback_data="transfer_gate_occupied"
    )
    builder.button(
        text=f"{Emoji.CANCEL} Нет ТС на месте",
        callback_data="transfer_no_vehicle"
    )
    builder.button(
        text="🔧 Поломка ТС",
        callback_data="transfer_breakdown"
    )
    builder.adjust(1)

    await state.set_state(DriverTransferStates.waiting_for_gate_confirmation)

    # Отправляем сообщение с изображением, если оно есть
    if image_path and image_path.exists():
        try:
            photo = FSInputFile(image_path)
            await message.answer_photo(
                photo=photo,
                caption=message_text,
                reply_markup=builder.as_markup()
            )
        except Exception as e:
            logger.error(f"Ошибка при отправке изображения ворот: {e}")
            await message.answer(
                message_text,
                reply_markup=builder.as_markup()
            )
    else:
        await message.answer(
            message_text,
            reply_markup=builder.as_markup()
        )

@router.message(F.text == f"{Emoji.COMPLETED} Завершить задачу")
@with_db
async def process_complete_current_task(message: Message, state: FSMContext, db: Session):
    """Завершение текущей активной задачи"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer(f"{Emoji.ERROR} Сначала используйте /start для регистрации.")
        return

    # Проверяем, есть ли у пользователя роль DRIVER_TRANSFER
    user_roles = get_user_roles(user)
    if "DRIVER_TRANSFER" not in user_roles:
        await message.answer(f"{Emoji.ERROR} Эта функция доступна только водителям перегона.")
        return

    # Находим активную задачу
    active_task = db.query(Task).filter(
        Task.driver_id == user.id,
        Task.status == "IN_PROGRESS"
    ).first()

    if not active_task:
        await message.answer(f"{Emoji.INFO} У вас нет активных задач.")
        return

    # Сохраняем данные задачи в состояние
    await state.update_data(
        task_id=active_task.id,
        gate_number=active_task.gate_number,
        parking_spot=active_task.parking.spot_number if active_task.parking else None,
        vehicle_number=active_task.parking.vehicle_number if active_task.parking else None,
        is_transfer_driver=True
    )

    # Создаем инлайн-клавиатуру для действий с задачей
    builder = InlineKeyboardBuilder()
    builder.button(
        text=f"{Emoji.COMPLETED} Задача выполнена, ворота свободны",
        callback_data="transfer_gate_completed"
    )
    builder.button(
        text=f"{Emoji.GATE_OCCUPIED} Ворота заняты другим ТС",
        callback_data="transfer_gate_occupied"
    )
    builder.button(
        text=f"{Emoji.CANCEL} Нет ТС на месте",
        callback_data="transfer_no_vehicle"
    )
    builder.button(
        text="🔧 Поломка ТС",
        callback_data="transfer_breakdown"
    )
    builder.adjust(1)

    await state.set_state(DriverTransferStates.waiting_for_gate_confirmation)
    await message.answer(
        f"{Emoji.QUESTION} ЗАВЕРШЕНИЕ ЗАДАЧИ #{active_task.id}\n\n"
        f"📋 Информация о текущей задаче:\n"
        f"🚗 ТС: {active_task.parking.vehicle_number if active_task.parking else 'Неизвестно'}\n"
        f"📍 Место: #{active_task.parking.spot_number if active_task.parking else '?'}\n"
        f"🚪 Ворота: #{active_task.gate_number}\n"
        f"⏰ Начало: {active_task.started_at.strftime('%H:%M %d.%m.%Y') if active_task.started_at else 'Неизвестно'}\n\n"
        f"Выберите действие:",
        reply_markup=builder.as_markup()
    )


# ==================== ОБРАБОТЧИКИ ДЛЯ ОБЕДА ====================
@router.message(F.text.contains("Обед"))
@router.message(F.text == f"{Emoji.BREAK_START} Уйти на обед")
@router.message(F.text == f"{Emoji.BREAK_END} Вернуться с обеда")
@with_db
async def process_break_menu(message: Message, db: Session):
    """Меню обеда для водителя перегона"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer("❌ Сначала используйте /start для регистрации.")
        return

    if "DRIVER_TRANSFER" not in get_user_roles(user):
        await message.answer("❌ Эта функция доступна только водителям перегона.")
        return

    if not user.is_on_shift:
        await message.answer("❌ Вы не на смене! Начните смену для использования этой функции.")
        return

    status_text = ""
    if user.is_on_break and user.break_start_time:
        break_start = ensure_timezone_aware(user.break_start_time)
        break_duration = get_timezone_aware_now() - break_start
        minutes = int(break_duration.total_seconds() // 60)
        status_text = f"\n\n{Emoji.BREAK_START} Вы на обеде уже {minutes} минут"

    await message.answer(
        f"{Emoji.BREAK_START} Меню обеда{status_text}\n\n"
        f"Выберите действие:",
        reply_markup=get_break_menu_keyboard()
    )


@router.callback_query(F.data == "break_start")
@with_db
async def process_break_start(callback: CallbackQuery, state: FSMContext, db: Session):
    """Начало процесса ухода на обед"""
    user = await get_user(db, callback.from_user.id)
    if not user:
        await callback.message.edit_text("❌ Пользователь не найден.")
        return

    if not user.is_on_shift:
        await callback.message.edit_text("❌ Вы не на смене!")
        return

    if user.is_on_break:
        await callback.message.edit_text(
            "❌ Вы уже на обеде!\n\nСначала вернитесь с обеда.",
            reply_markup=InlineKeyboardBuilder().button(
                text=f"{Emoji.BACK} Назад",
                callback_data="back_to_break_menu"
            ).as_markup()
        )
        return

    # Проверка только активных задач (IN_PROGRESS)
    active_task = db.query(Task).filter(
        Task.driver_id == user.id,
        Task.status == "IN_PROGRESS"
    ).first()

    if active_task:
        await callback.message.edit_text(
            f"⚠️ У вас есть активная задача #{active_task.id} в работе!\n\n"
            f"📍 Место: #{active_task.parking.spot_number}\n"
            f"🚪 Ворота: #{active_task.gate_number}\n\n"
            f"Сначала завершите текущую задачу перед уходом на обед.",
            reply_markup=InlineKeyboardBuilder().button(
                text=f"{Emoji.BACK} Назад",
                callback_data="back_to_break_menu"
            ).as_markup()
        )
        return

    await state.set_state(DriverTransferStates.waiting_for_break_confirmation)
    await callback.message.edit_text(
        f"{Emoji.BREAK_START} Подтвердите уход на обед\n\n"
        f"⏰ Время ухода: {get_timezone_aware_now().strftime('%H:%M %d.%m.%Y')}\n\n"
        f"Во время обеда вы не сможете брать новые задачи.\n"
        f"Время обеда будет вычтено из статистики рабочей смены.",
        reply_markup=get_break_confirmation_keyboard()
    )


@router.callback_query(F.data == "break_confirm")
@with_db
async def process_break_confirm(callback: CallbackQuery, state: FSMContext, db: Session):
    """Подтверждение ухода на обед"""
    user = await get_user(db, callback.from_user.id)
    if not user:
        await callback.message.edit_text("❌ Пользователь не найден.")
        return

    user.is_on_break = True
    user.break_start_time = get_timezone_aware_now()

    break_record = Break(
        user_id=user.id,
        break_type="LUNCH",
        start_time=user.break_start_time,
        end_time=None,
        duration=0
    )
    db.add(break_record)
    db.commit()

    await callback.message.edit_text(
        f"{Emoji.BREAK_START} Вы ушли на обед!\n\n"
        f"⏰ Время: {user.break_start_time.strftime('%H:%M %d.%m.%Y')}\n\n"
        f"Для возврата с обеда используйте кнопку '{Emoji.BREAK_END} Вернуться с обеда'."
    )
    await callback.message.answer(
        "Главное меню:",
        reply_markup=get_main_menu_keyboard(user)
    )
    await state.clear()


@router.callback_query(F.data == "break_end")
@with_db
async def process_break_end(callback: CallbackQuery, state: FSMContext, db: Session):
    """Возвращение с обеда"""
    user = await get_user(db, callback.from_user.id)
    if not user:
        await callback.message.edit_text("❌ Пользователь не найден.")
        return

    if not user.is_on_break:
        await callback.message.edit_text(
            "❌ Вы не на обеде!",
            reply_markup=InlineKeyboardBuilder().button(
                text=f"{Emoji.BACK} Назад",
                callback_data="back_to_break_menu"
            ).as_markup()
        )
        return

    now = get_timezone_aware_now()
    break_start = ensure_timezone_aware(user.break_start_time)
    break_duration = now - break_start
    break_seconds = int(break_duration.total_seconds())

    user.is_on_break = False
    user.total_break_time = (user.total_break_time or 0) + break_seconds
    user.break_start_time = None

    break_record = db.query(Break).filter(
        Break.user_id == user.id,
        Break.end_time == None
    ).order_by(Break.start_time.desc()).first()

    if break_record:
        break_record.end_time = now
        break_record.duration = break_seconds

    db.commit()

    await callback.message.edit_text(
        f"{Emoji.BREAK_END} Вы вернулись с обеда!\n\n"
        f"📊 Статистика обеда:\n"
        f"⏰ Уход: {break_start.strftime('%H:%M %d.%m.%Y')}\n"
        f"⏰ Возврат: {now.strftime('%H:%M %d.%m.%Y')}\n"
        f"⏱️ Длительность: {format_duration(break_seconds)}\n\n"
        f"Время обеда вычтено из статистики смены."
    )
    await callback.message.answer(
        "Главное меню:",
        reply_markup=get_main_menu_keyboard(user)
    )


@router.callback_query(F.data == "break_cancel")
@with_db
async def process_break_cancel(callback: CallbackQuery, state: FSMContext, db: Session):
    """Отмена ухода на обед"""
    user = await get_user(db, callback.from_user.id)
    await callback.message.edit_text(
        "❌ Уход на обед отменен.",
        reply_markup=InlineKeyboardBuilder().button(
            text=f"{Emoji.BACK} Назад",
            callback_data="back_to_break_menu"
        ).as_markup()
    )
    await state.clear()


@router.callback_query(F.data == "back_to_break_menu")
@with_db
async def process_back_to_break_menu(callback: CallbackQuery, db: Session):
    """Возврат в меню обеда"""
    user = await get_user(db, callback.from_user.id)

    status_text = ""
    if user.is_on_break and user.break_start_time:
        break_start = ensure_timezone_aware(user.break_start_time)
        break_duration = get_timezone_aware_now() - break_start
        minutes = int(break_duration.total_seconds() // 60)
        status_text = f"\n\n{Emoji.BREAK_START} Вы на обеде уже {minutes} минут"

    await callback.message.edit_text(
        f"{Emoji.BREAK_START} Меню обеда{status_text}\n\n"
        f"Выберите действие:",
        reply_markup=get_break_menu_keyboard()
    )


@router.callback_query(F.data == "back_to_driver_transfer_menu")
@with_db
async def process_back_to_driver_transfer_menu(callback: CallbackQuery, db: Session):
    """Возврат в меню водителя перегона"""
    user = await get_user(db, callback.from_user.id)
    await callback.message.edit_text("Возврат в меню...")
    await callback.message.answer(
        "Главное меню:",
        reply_markup=get_main_menu_keyboard(user)
    )


@router.message(F.text == f"{Emoji.STATS} Статистика за смену")
@with_db
async def process_shift_stats(message: Message, db: Session):
    """Статистика за смену для водителя перегона"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer("❌ Сначала используйте /start для регистрации.")
        return

    if "DRIVER_TRANSFER" not in get_user_roles(user):
        await message.answer("❌ Эта функция доступна только водителям перегона.")
        return

    now = get_timezone_aware_now()

    # Определение начала смены
    if user.is_on_shift:
        first_task_today = db.query(Task).filter(
            Task.driver_id == user.id,
            Task.created_at >= now.replace(hour=0, minute=0, second=0, microsecond=0)
        ).order_by(Task.created_at.asc()).first()

        if first_task_today:
            shift_start = ensure_timezone_aware(first_task_today.created_at)
        else:
            shift_start = ensure_timezone_aware(user.created_at)
            if shift_start.date() < now.date():
                shift_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        shift_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Статистика задач
    tasks = db.query(Task).filter(
        Task.driver_id == user.id,
        Task.created_at >= shift_start
    ).all()

    completed = [t for t in tasks if t.status == "COMPLETED"]
    stuck = [t for t in tasks if t.status == "STUCK"]
    in_progress = [t for t in tasks if t.status == "IN_PROGRESS"]

    # Статистика обедов
    breaks = db.query(Break).filter(
        Break.user_id == user.id,
        Break.start_time >= shift_start
    ).all()

    total_break_seconds = 0
    for br in breaks:
        if br.end_time:
            end = ensure_timezone_aware(br.end_time)
            start = ensure_timezone_aware(br.start_time)
            total_break_seconds += int((end - start).total_seconds())

    if user.is_on_break and user.break_start_time:
        break_start = ensure_timezone_aware(user.break_start_time)
        total_break_seconds += int((now - break_start).total_seconds())

    response = (
        f"📊 СТАТИСТИКА {'ТЕКУЩЕЙ СМЕНЫ' if user.is_on_shift else 'ЗА СЕГОДНЯ'}\n\n"
        f"👤 Водитель: {user.first_name} {user.last_name}\n"
        f"{Emoji.BREAK_START} Статус: {'НА ОБЕДЕ' if user.is_on_break else 'РАБОТАЕТ'}\n\n"
        f"⏰ Период: с {shift_start.strftime('%H:%M %d.%m.%Y')}\n"
        f"⏰ Текущее время: {now.strftime('%H:%M %d.%m.%Y')}\n"
        f"{Emoji.BREAK_TIME} Время на обеде: {format_duration(total_break_seconds)}\n\n"
        f"📋 ЗАДАЧИ:\n"
        f"{Emoji.COMPLETED} Выполнено: {len(completed)}\n"
        f"{Emoji.STUCK} Зависло: {len(stuck)}\n"
        f"{Emoji.IN_PROGRESS} В работе: {len(in_progress)}\n"
        f"📝 Всего задач: {len(tasks)}\n"
    )

    # Текущая активная задача
    active_task = next((t for t in tasks if t.status == "IN_PROGRESS"), None)
    if active_task and active_task.started_at:
        started = ensure_timezone_aware(active_task.started_at)
        duration = now - started
        minutes = int(duration.total_seconds() / 60)
        response += (
            f"\n{Emoji.IN_PROGRESS} ТЕКУЩАЯ ЗАДАЧА:\n"
            f"📍 Место #{active_task.parking.spot_number}\n"
            f"🚪 Ворота #{active_task.gate_number}\n"
            f"⏰ В работе: {minutes} мин\n"
        )

    await message.answer(response)


# ==================== ОБРАБОТЧИКИ ДЕЙСТВИЙ С ЗАДАЧАМИ ====================
@router.callback_query(F.data.startswith("complete_task_"))
@with_db
async def process_task_complete(callback: CallbackQuery, db: Session):
    """Завершение задачи"""
    task_id = int(callback.data.replace("complete_task_", ""))
    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        await callback.message.edit_text("❌ Задача не найдена.")
        return

    task.status = "COMPLETED"
    task.completed_at = get_timezone_aware_now()

    if task.parking:
        task.parking.departure_time = get_timezone_aware_now()
        task.parking.gate_number = task.gate_number

    db.commit()

    await callback.message.edit_text(
        f"{Emoji.COMPLETED} Задача #{task_id} выполнена!\n"
        f"🚗 ТС отправлено на ворота #{task.gate_number}"
    )

    if task.operator_id:
        try:
            await bot.send_message(
                task.operator_id,
                f"{Emoji.COMPLETED} Задача #{task_id} выполнена!\n"
                f"👤 Водитель: {task.driver.first_name if task.driver else 'Неизвестно'}\n"
                f"🚗 ТС: {task.parking.vehicle_number if task.parking else 'Неизвестно'}\n"
                f"🚪 Ворота: #{task.gate_number}"
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления оператора: {e}")


@router.callback_query(F.data.startswith("no_vehicle_"))
@with_db
async def process_no_vehicle(callback: CallbackQuery, db: Session):
    """Ситуация 'Нет ТС' - снятие задачи и возврат в пул"""
    task_id = int(callback.data.replace("no_vehicle_", ""))
    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        await callback.message.edit_text("❌ Задача не найдена.")
        return

    driver_id = task.driver_id

    task.status = "STUCK"
    task.is_stuck = True
    task.stuck_reason = "Нет ТС"
    task.driver_id = None
    task.is_in_pool = True
    task.priority += 5

    db.commit()

    if driver_id:
        try:
            await bot.send_message(
                driver_id,
                f"⚠️ Задача #{task.id} снята с вас.\n"
                f"Причина: ТС отсутствует на парковочном месте\n\n"
                f"✅ Вы можете взять другую задачу."
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления водителя: {e}")

    await callback.message.edit_text(
        f"⚠️ Задача #{task_id} помечена как 'Нет ТС'.\n"
        f"✅ Задача снята с водителя и возвращена в пул."
    )

    if task.operator_id:
        try:
            await bot.send_message(
                task.operator_id,
                f"⚠️ Задача #{task.id} зависла!\n"
                f"Причина: Нет ТС на месте\n"
                f"🚗 ТС: {task.parking.vehicle_number}\n"
                f"📍 Место: #{task.parking.spot_number}\n"
                f"🚪 Ворота: #{task.gate_number}\n\n"
                f"✅ Задача возвращена в пул.\n"
                f"Требуется вмешательство оператора."
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления оператора: {e}")


@router.callback_query(F.data.startswith("breakdown_"))
@with_db
async def process_breakdown(callback: CallbackQuery, db: Session):
    """Ситуация 'Поломка ТС' - снятие задачи без возврата в пул"""
    task_id = int(callback.data.replace("breakdown_", ""))
    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        await callback.message.edit_text("❌ Задача не найдена.")
        return

    driver_id = task.driver_id

    task.status = "STUCK"
    task.is_stuck = True
    task.stuck_reason = "Поломка ТС"
    task.driver_id = None
    task.is_in_pool = False

    db.commit()

    if driver_id:
        try:
            await bot.send_message(
                driver_id,
                f"⚠️ Задача #{task.id} снята с вас.\n"
                f"Причина: Поломка ТС\n\n"
                f"✅ Вы можете взять другую задачу."
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления водителя: {e}")

    await callback.message.edit_text(
        f"⚠️ Задача #{task_id} помечена как 'Поломка ТС'.\n"
        f"✅ Задача снята с водителя."
    )

    if task.operator_id:
        try:
            await bot.send_message(
                task.operator_id,
                f"⚠️ Задача #{task.id} зависла!\n"
                f"Причина: Поломка ТС\n"
                f"🚗 ТС: {task.parking.vehicle_number}\n"
                f"📍 Место: #{task.parking.spot_number}\n"
                f"🚪 Ворота: #{task.gate_number}\n\n"
                f"❌ ТС требует ремонта. Задача закрыта."
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления оператора: {e}")


@router.callback_query(F.data.startswith("stuck_timeout_"))
@with_db
async def process_stuck_timeout(callback: CallbackQuery, db: Session):
    """Ситуация 'Долгое ожидание' - снятие задачи с повышенным приоритетом"""
    task_id = int(callback.data.replace("stuck_timeout_", ""))
    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        await callback.message.edit_text("❌ Задача не найдена.")
        return

    driver_id = task.driver_id

    task.status = "STUCK"
    task.is_stuck = True
    task.stuck_reason = "Долгое ожидание на воротах"
    task.driver_id = None
    task.is_in_pool = True
    task.priority += 10

    db.commit()

    if driver_id:
        try:
            await bot.send_message(
                driver_id,
                f"⚠️ Задача #{task.id} снята с вас из-за долгого ожидания.\n\n"
                f"📋 Информация:\n"
                f"📍 Место: #{task.parking.spot_number}\n"
                f"🚗 ТС: {task.parking.vehicle_number}\n"
                f"🚪 Ворота: #{task.gate_number}\n\n"
                f"✅ Задача возвращена в пул. Вы можете взять другую задачу."
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления водителя: {e}")

    await callback.message.edit_text(
        f"⚠️ Задача #{task_id} помечена как 'Долгое ожидание'.\n"
        f"✅ Задача снята с водителя и возвращена в пул с повышенным приоритетом."
    )

    if task.operator_id:
        try:
            await bot.send_message(
                task.operator_id,
                f"⚠️ Задача #{task.id} зависла!\n"
                f"Причина: Долгое ожидание на воротах\n"
                f"🚗 ТС: {task.parking.vehicle_number}\n"
                f"📍 Место: #{task.parking.spot_number}\n"
                f"🚪 Ворота: #{task.gate_number}\n\n"
                f"✅ Задача возвращена в пул с приоритетом {task.priority}.\n"
                f"Требуется вмешательство оператора."
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления оператора: {e}")


# ==================== ОБРАБОТЧИКИ ДЛЯ ОПЕРАТОРОВ ====================
@router.message(F.text == f"{Emoji.TASK} Дать задание")
@with_db
async def process_give_task(message: Message, db: Session):
    """Создание нового задания оператором"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer(f"{Emoji.ERROR} Сначала используйте /start для регистрации.")
        return

    if "OPERATOR" not in get_user_roles(user):
        await message.answer(f"{Emoji.ERROR} Эта функция доступна только операторам.")
        return

    # Получаем все припаркованные ТС
    active_parkings = db.query(Parking).filter(
        Parking.departure_time == None
    ).all()

    if not active_parkings:
        await message.answer(f"{Emoji.INFO} Нет припаркованных ТС.")
        return

    # Получаем ID парковок, у которых уже есть активные задачи
    active_task_parking_ids = db.query(Task.parking_id).filter(
        Task.status.in_(["PENDING", "IN_PROGRESS"]),
        Task.parking_id.isnot(None)
    ).all()
    active_task_parking_ids = [p[0] for p in active_task_parking_ids]

    # Фильтруем ТС, у которых нет активных задач
    available_parkings = [p for p in active_parkings if p.id not in active_task_parking_ids]

    if not available_parkings:
        await message.answer(
            f"{Emoji.INFO} Нет доступных ТС для заданий.\n"
            f"Все припаркованные ТС уже имеют активные задачи."
        )
        return

    # Создаем клавиатуру с доступными ТС
    builder = InlineKeyboardBuilder()
    for parking in available_parkings:
        driver_name = f"{parking.user.first_name} {parking.user.last_name}".strip()
        if not driver_name:
            driver_name = "Водитель"

        # Добавляем метку о типе ТС
        type_mark = "🔗" if parking.is_hitch else "🚛"

        builder.button(
            text=f"{type_mark} 📍{parking.spot_number} - {parking.vehicle_number} ({driver_name})",
            callback_data=f"select_vehicle_{parking.id}"
        )

    # Добавляем статистику
    stats_text = (
        f"\n\n📊 Статистика:\n"
        f"• Всего ТС на парковке: {len(active_parkings)}\n"
        f"• Доступно для заданий: {len(available_parkings)}\n"
        f"• С активными задачами: {len(active_task_parking_ids)}"
    )

    builder.button(text=f"{Emoji.BACK} Назад", callback_data="menu_main")
    builder.adjust(1)

    await message.answer(
        f"🚗 Выберите транспортное средство для задания:{stats_text}",
        reply_markup=builder.as_markup()
    )


@router.callback_query(F.data == "select_abk1")
@with_db
async def process_select_abk1(callback: CallbackQuery, state: FSMContext, db: Session):
    """Выбор АБК-1"""
    await state.update_data(abk_type="ABK1")

    # Получаем данные из состояния для отображения
    data = await state.get_data()
    parking_id = data.get('parking_id')

    if parking_id:
        parking = db.query(Parking).filter(Parking.id == parking_id).first()
        if parking:
            driver_name = f"{parking.user.first_name} {parking.user.last_name}".strip() or "Водитель"
            type_mark = "🔗 Перецепной" if parking.is_hitch else "🚛 Не перецепной"

            await callback.message.edit_text(
                f"🏢 Выбран АБК-1\n\n"
                f"📋 Информация о ТС:\n"
                f"📍 Место: #{parking.spot_number}\n"
                f"🚗 Номер: {parking.vehicle_number}\n"
                f"👤 Водитель: {driver_name}\n"
                f"📝 Тип: {type_mark}\n\n"
                f"📋 Доступные ворота для АБК-1:\n"
                f"• с 1 по 59\n"
                f"• с 66 по 83\n"
                f"❌ Ворота с 60 по 65 НЕДОСТУПНЫ\n\n"
                f"Введите номер ворот:"
            )
        else:
            await callback.message.edit_text(
                f"🏢 Выбран АБК-1\n\n"
                f"📋 Доступные ворота:\n"
                f"• с 1 по 59\n"
                f"• с 66 по 83\n"
                f"❌ Ворота с 60 по 65 НЕДОСТУПНЫ\n\n"
                f"Введите номер ворот:"
            )
    else:
        await callback.message.edit_text(
            f"🏢 Выбран АБК-1\n\n"
            f"📋 Доступные ворота:\n"
            f"• с 1 по 59\n"
            f"• с 66 по 83\n"
            f"❌ Ворота с 60 по 65 НЕДОСТУПНЫ\n\n"
            f"Введите номер ворот:"
        )

    await state.set_state(OperatorStates.waiting_for_gate_number)


@router.callback_query(F.data == "select_abk2")
@with_db
async def process_select_abk2(callback: CallbackQuery, state: FSMContext, db: Session):
    """Выбор АБК-2"""
    await state.update_data(abk_type="ABK2")

    # Получаем данные из состояния для отображения
    data = await state.get_data()
    parking_id = data.get('parking_id')

    if parking_id:
        parking = db.query(Parking).filter(Parking.id == parking_id).first()
        if parking:
            driver_name = f"{parking.user.first_name} {parking.user.last_name}".strip() or "Водитель"
            type_mark = "🔗 Перецепной" if parking.is_hitch else "🚛 Не перецепной"

            await callback.message.edit_text(
                f"🏢 Выбран АБК-2\n\n"
                f"📋 Информация о ТС:\n"
                f"📍 Место: #{parking.spot_number}\n"
                f"🚗 Номер: {parking.vehicle_number}\n"
                f"👤 Водитель: {driver_name}\n"
                f"📝 Тип: {type_mark}\n\n"
                f"📋 Доступные ворота для АБК-2:\n"
                f"• с 1 по 10\n\n"
                f"Введите номер ворот:"
            )
        else:
            await callback.message.edit_text(
                f"🏢 Выбран АБК-2\n\n"
                f"📋 Доступные ворота:\n"
                f"• с 1 по 10\n\n"
                f"Введите номер ворот:"
            )
    else:
        await callback.message.edit_text(
            f"🏢 Выбран АБК-2\n\n"
            f"📋 Доступные ворота:\n"
            f"• с 1 по 10\n\n"
            f"Введите номер ворот:"
        )

    await state.set_state(OperatorStates.waiting_for_gate_number)

@router.message(OperatorStates.waiting_for_gate_number)
@with_db
async def process_gate_assignment(message: Message, state: FSMContext, db: Session):
    """Назначение ворот и создание задачи с проверкой доступности ворот"""
    try:
        gate_number = int(message.text)
        if gate_number <= 0:
            raise ValueError
    except ValueError:
        await message.answer(
            f"{Emoji.ERROR} Пожалуйста, введите положительное число для номера ворот.\n"
            f"Пример: 12, 15, 7"
        )
        return

    data = await state.get_data()
    parking_id = data['parking_id']
    abk_type = data.get('abk_type')  # Получаем выбранный АБК из состояния

    if not abk_type:
        await message.answer(f"{Emoji.ERROR} Сначала выберите АБК.")
        await state.clear()
        return

    # Проверка доступности ворот в зависимости от выбранного АБК
    is_valid_gate = False
    error_message = ""

    if abk_type == "ABK1":
        # АБК-1: ворота 1-59 и 66-83 (60-65 недоступны)
        if 1 <= gate_number <= 59:
            is_valid_gate = True
        elif 66 <= gate_number <= 83:
            is_valid_gate = True
        else:
            error_message = (
                f"{Emoji.ERROR} Для АБК-1 доступны ворота:\n"
                f"• с 1 по 59\n"
                f"• с 66 по 83\n"
                f"Ворота с 60 по 65 недоступны для использования."
            )
    elif abk_type == "ABK2":
        # АБК-2: ворота 1-10
        if 1 <= gate_number <= 10:
            is_valid_gate = True
        else:
            error_message = (
                f"{Emoji.ERROR} Для АБК-2 доступны только ворота с 1 по 10."
            )
    else:
        error_message = f"{Emoji.ERROR} Неизвестный тип АБК."

    if not is_valid_gate:
        await message.answer(error_message)
        return

    parking = db.query(Parking).filter(Parking.id == parking_id).first()
    operator = await get_user(db, message.from_user.id)

    if not parking:
        await message.answer(f"{Emoji.ERROR} ТС не найдено.")
        await state.clear()
        return

    # Финальная проверка - нет ли уже активной задачи
    existing_task = db.query(Task).filter(
        Task.parking_id == parking_id,
        Task.status.in_(["PENDING", "IN_PROGRESS"])
    ).first()

    if existing_task:
        await message.answer(
            f"{Emoji.WARNING} НЕВОЗМОЖНО СОЗДАТЬ ЗАДАНИЕ!\n\n"
            f"У этого ТС уже есть активная задача #{existing_task.id}:\n"
            f"🚪 Ворота: #{existing_task.gate_number}\n"
            f"📌 Статус: {STATUS_NAMES.get(existing_task.status, existing_task.status)}\n"
            f"⏰ Создана: {existing_task.created_at.strftime('%H:%M %d.%m.%Y')}\n\n"
            f"Дождитесь завершения текущей задачи."
        )
        await state.clear()
        return

    # Проверяем, не убыло ли ТС
    if parking.departure_time:
        await message.answer(
            f"{Emoji.ERROR} Это ТС уже убыло с парковки в "
            f"{parking.departure_time.strftime('%H:%M %d.%m.%Y')}."
        )
        await state.clear()
        return

    # Создание задачи с указанием АБК
    task = Task(
        parking_id=parking.id,
        operator_id=operator.id,
        gate_number=gate_number,
        status="PENDING",
        created_at=get_timezone_aware_now(),
        is_in_pool=parking.is_hitch  # В пул только перецепные
    )

    # Добавляем информацию об АБК
    abk_info = "АБК-1" if abk_type == "ABK1" else "АБК-2"
    building_type = abk_type  # для отправки изображений

    driver_info = ""

    if parking.is_hitch:
        # Перецепной - в общий пул водителей перегона
        driver_info = f"в общий пул водителей перегона ({abk_info})"

        # Текст уведомления для водителей перегона
        notification_text = (
            f"{Emoji.TASK_POOL} НОВАЯ ЗАДАЧА В ПУЛЕ!\n\n"
            f"📋 Информация:\n"
            f"📍 Место: #{parking.spot_number}\n"
            f"🚗 ТС: {parking.vehicle_number}\n"
            f"🏢 {abk_info}\n"
            f"🚪 Ворота: #{gate_number}\n"
            f"📝 Тип: Перецепной\n"
            f"⏰ Время: {get_timezone_aware_now().strftime('%H:%M %d.%m.%Y')}\n\n"
            f'Используйте кнопку "{Emoji.TASK} Взять задачу".'
        )

        # Уведомление всех активных водителей перегона
        from services import get_active_transfer_drivers
        active_drivers = await get_active_transfer_drivers(db)
        notified_count = 0

        for driver in active_drivers:
            try:
                # Отправляем задачу с изображением ворот
                await send_task_with_image(
                    driver.telegram_id,
                    building_type,
                    gate_number,
                    notification_text
                )
                notified_count += 1
                await asyncio.sleep(0.1)  # Небольшая задержка между отправками
            except Exception as e:
                logger.error(f"Ошибка отправки уведомления водителю {driver.telegram_id}: {e}")

        driver_info += f" (уведомлено {notified_count} водителей)"
    else:
        # Неперецепной - конкретному водителю
        driver = parking.user
        task.assigned_driver_id = driver.id
        task.is_in_pool = False
        driver_info = f"водителю ТС {driver.first_name} {driver.last_name} ({abk_info})"

        notification_text = (
            f"{Emoji.TASK} НОВОЕ ЗАДАНИЕ!\n\n"
            f"📋 Информация:\n"
            f"📍 Место: #{parking.spot_number}\n"
            f"🚗 ТС: {parking.vehicle_number}\n"
            f"🏢 {abk_info}\n"
            f"🚪 Ворота: #{gate_number}\n"
            f"📝 Тип: Не перецепной\n"
            f"⏰ Время: {get_timezone_aware_now().strftime('%H:%M %d.%m.%Y')}\n\n"
            f'Используйте кнопку "{Emoji.GATE} Встать на ворота".'
        )

        try:
            # Отправляем задачу с изображением ворот конкретному водителю
            await send_task_with_image(
                driver.telegram_id,
                building_type,
                gate_number,
                notification_text
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления водителя {driver.telegram_id}: {e}")

    db.add(task)
    db.commit()

    await message.answer(
        f"{Emoji.SUCCESS} ЗАДАНИЕ СОЗДАНО!\n\n"
        f"📋 Информация:\n"
        f"🆔 ID задачи: #{task.id}\n"
        f"👤 Назначено: {driver_info}\n"
        f"🏢 {abk_info}\n"
        f"🚪 Ворота: #{gate_number}\n"
        f"📍 Место: #{parking.spot_number}\n"
        f"🚗 ТС: {parking.vehicle_number}\n"
        f"📊 Статус: {'В пуле задач' if parking.is_hitch else 'Назначена водителю'}\n"
        f"⏰ Время создания: {task.created_at.strftime('%H:%M %d.%m.%Y')}",
        reply_markup=get_main_menu_keyboard(operator)
    )
    await state.clear()

@router.callback_query(F.data.startswith("select_vehicle_"))
@with_db
async def process_select_vehicle(callback: CallbackQuery, state: FSMContext, db: Session):
    """Выбор ТС и запрос выбора АБК"""
    parking_id = int(callback.data.replace("select_vehicle_", ""))
    parking = db.query(Parking).filter(Parking.id == parking_id).first()

    if not parking:
        await callback.message.edit_text(f"{Emoji.ERROR} ТС не найдено.")
        return

    # Дополнительная проверка - нет ли уже активной задачи
    existing_task = db.query(Task).filter(
        Task.parking_id == parking_id,
        Task.status.in_(["PENDING", "IN_PROGRESS"])
    ).first()

    if existing_task:
        await callback.message.edit_text(
            f"{Emoji.WARNING} У этого ТС уже есть активная задача #{existing_task.id}!\n"
            f"🚪 Ворота: #{existing_task.gate_number}\n"
            f"📌 Статус: {STATUS_NAMES.get(existing_task.status, existing_task.status)}\n\n"
            f"Дождитесь завершения текущей задачи."
        )
        return

    # Проверяем, не убыло ли ТС
    if parking.departure_time:
        await callback.message.edit_text(
            f"{Emoji.ERROR} Это ТС уже убыло с парковки."
        )
        return

    # Сохраняем ID парковки в состояние
    await state.update_data(parking_id=parking.id)

    driver_name = f"{parking.user.first_name} {parking.user.last_name}".strip() or "Водитель"
    type_mark = "🔗 Перецепной" if parking.is_hitch else "🚛 Не перецепной"
    type_emoji = "🔗" if parking.is_hitch else "🚛"

    # Формируем сообщение с информацией о ТС
    info_text = (
        f"{type_emoji} ВЫБРАНО ТС:\n\n"
        f"📋 Информация:\n"
        f"📍 Место: #{parking.spot_number}\n"
        f"🚗 Номер: {parking.vehicle_number}\n"
        f"👤 Водитель: {driver_name}\n"
        f"📝 Тип: {type_mark}\n"
    )

    if not existing_task:
        info_text += f"{Emoji.SUCCESS} Нет активных задач\n\n"
    else:
        info_text += f"{Emoji.WARNING} Есть активная задача\n\n"

    # Создаем клавиатуру для выбора АБК
    builder = InlineKeyboardBuilder()
    builder.button(
        text="🏢 АБК-1 (ворота 1-59, 66-83)",
        callback_data="select_abk1"
    )
    builder.button(
        text="🏢 АБК-2 (ворота 1-10)",
        callback_data="select_abk2"
    )
    builder.button(
        text=f"{Emoji.CANCEL} Отмена",
        callback_data="menu_main"
    )
    builder.adjust(1)

    info_text += "🏢 Выберите АБК для задания:"

    await callback.message.edit_text(
        info_text,
        reply_markup=builder.as_markup()
    )

@router.message(F.text == f"{Emoji.QUEUE} Статус очереди")
@with_db
async def process_queue_status(message: Message, db: Session):
    """Просмотр статуса очереди на парковку"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer(f"{Emoji.ERROR} Сначала используйте /start для регистрации.")
        return

    # Проверяем права (доступно для операторов и админов)
    user_roles = get_user_roles(user)
    if "OPERATOR" not in user_roles and "ADMIN" not in user_roles:
        # Для обычных пользователей показываем только их позицию
        queue_item = db.query(ParkingQueue).filter(
            ParkingQueue.user_id == user.id,
            ParkingQueue.status.in_(["waiting", "notified"])
        ).first()

        if queue_item:
            position = await get_queue_position(db, user.id)
            wait_time = get_timezone_aware_now() - ensure_timezone_aware(queue_item.created_at)

            await message.answer(
                f"{Emoji.QUEUE} ВАША ПОЗИЦИЯ В ОЧЕРЕДИ:\n\n"
                f"🚗 ТС: {queue_item.vehicle_number}\n"
                f"📝 Тип: {'Перецепной' if queue_item.is_hitch else 'Не перецепной'}\n"
                f"📊 Позиция: {position}\n"
                f"⏰ Ожидание: {format_duration(int(wait_time.total_seconds()))}\n"
                f"📌 Статус: {'🔔 Уведомлен' if queue_item.status == 'notified' else '⏳ В очереди'}\n"
            )
        else:
            await message.answer(f"{Emoji.INFO} Вы не в очереди на парковку.")
        return

    # Для операторов и админов показываем полную статистику
    queue_stats = await get_queue_stats(db)

    # Получаем список всех в очереди
    queue_list = db.query(ParkingQueue).filter(
        ParkingQueue.status.in_(["waiting", "notified"])
    ).order_by(ParkingQueue.created_at.asc()).limit(20).all()

    response = f"{Emoji.QUEUE} ОЧЕРЕДЬ НА ПАРКОВКУ:\n\n"
    response += f"📊 Статистика:\n"
    response += f"• Всего в очереди: {queue_stats['total']}\n"
    response += f"• ⏳ Ожидают: {queue_stats['waiting']}\n"
    response += f"• 🔔 Уведомлены: {queue_stats['notified']}\n"
    response += f"• 🔗 Перецепных: {queue_stats['hitch']}\n"
    response += f"• 🚛 Не перецепных: {queue_stats['non_hitch']}\n\n"

    if queue_list:
        response += f"📋 Первые 20 в очереди:\n\n"
        for i, item in enumerate(queue_list, 1):
            user_info = await get_user(db, item.user_id)
            name = f"{user_info.first_name} {user_info.last_name}".strip() or f"ID: {item.user_id}"
            wait_time = get_timezone_aware_now() - ensure_timezone_aware(item.created_at)

            response += (
                f"{i}. {name}\n"
                f"   🚗 {item.vehicle_number}\n"
                f"   📝 {'Перецепной' if item.is_hitch else 'Не перецепной'}\n"
                f"   ⏰ {format_duration(int(wait_time.total_seconds()))}\n"
                f"   📌 {item.status}\n"
                f"   {'📍 Место #' + str(item.spot_number) if item.spot_number else ''}\n\n"
            )

    await message.answer(response[:4000])


@router.message(F.text == f"{Emoji.TASK_POOL} Пул задач")
@with_db
async def process_task_pool(message: Message, db: Session):
    """Просмотр пула задач оператором"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer("❌ Сначала используйте /start для регистрации.")
        return

    if "OPERATOR" not in get_user_roles(user) and "ADMIN" not in get_user_roles(user):
        await message.answer("❌ Эта функция доступна только операторам и администраторам.")
        return

    # Получаем задачи в пуле
    pool_tasks = db.query(Task).join(Parking).filter(
        Task.status == "PENDING",
        Task.is_in_pool == True,
        Parking.is_hitch == True,
        Parking.departure_time == None
    ).order_by(
        Task.priority.desc(),
        Task.created_at.asc()
    ).all()

    if not pool_tasks:
        await message.answer(f"{Emoji.INFO} Пул задач пуст.")
        return

    response = f"{Emoji.TASK_POOL} ПУЛ ЗАДАЧ ДЛЯ ВОДИТЕЛЕЙ ПЕРЕГОНА:\n\n"

    now = get_timezone_aware_now()

    for task in pool_tasks:
        # Время ожидания
        created = ensure_timezone_aware(task.created_at)
        wait_time = now - created
        minutes = int(wait_time.total_seconds() / 60)

        if minutes < 60:
            wait_str = f"{minutes} мин"
        else:
            hours = minutes // 60
            mins = minutes % 60
            wait_str = f"{hours}ч {mins}мин"

        # Приоритет
        priority_emoji = "🔴" if task.priority >= 10 else "🟠" if task.priority >= 5 else "⚪"
        priority_text = f"{priority_emoji} Приоритет: {task.priority}"

        # Дополнительная метка для срочных задач
        urgent_mark = " ⚠️ СРОЧНО!" if minutes > 30 else ""

        response += (
            f"🆔 ЗАДАЧА #{task.id}{urgent_mark}\n"
            f"{priority_text}\n"
            f"🚪 Ворота: #{task.gate_number}\n"
            f"🚗 ТС: {task.parking.vehicle_number}\n"
            f"📍 Место: #{task.parking.spot_number}\n"
            f"⏰ Ожидание: {wait_str}\n"
            f"{'─' * 40}\n\n"
        )

    # Добавляем статистику
    total_tasks = len(pool_tasks)
    high_priority = len([t for t in pool_tasks if t.priority >= 10])
    medium_priority = len([t for t in pool_tasks if 5 <= t.priority < 10])

    response += (
        f"📊 СТАТИСТИКА ПУЛА:\n"
        f"• Всего задач: {total_tasks}\n"
        f"• 🔴 Высокий приоритет: {high_priority}\n"
        f"• 🟠 Средний приоритет: {medium_priority}\n"
        f"• ⚪ Низкий приоритет: {total_tasks - high_priority - medium_priority}\n"
    )

    await message.answer(response[:4000])


@router.message(F.text == f"{Emoji.TASK_POOL} Очистить пул")
@with_db
async def process_clear_task_pool(message: Message, db: Session):
    """Очистка пула задач от зависших и неактуальных задач"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer(f"{Emoji.ERROR} Сначала используйте /start для регистрации.")
        return

    if "OPERATOR" not in get_user_roles(user) and "ADMIN" not in get_user_roles(user):
        await message.answer(f"{Emoji.ERROR} Доступ запрещен.")
        return

    # Находим все задачи в пуле
    pool_tasks = db.query(Task).join(Parking).filter(
        Task.status == "PENDING",
        Task.is_in_pool == True,
        Parking.departure_time == None
    ).all()

    if not pool_tasks:
        await message.answer(f"{Emoji.INFO} Пул задач пуст.")
        return

    # Создаем клавиатуру для выбора действия
    builder = InlineKeyboardBuilder()
    builder.button(
        text="🔄 Перезапустить все",
        callback_data="clear_pool_restart"
    )
    builder.button(
        text="❌ Удалить все",
        callback_data="clear_pool_delete"
    )
    builder.button(
        text="📋 Показать список",
        callback_data="clear_pool_list"
    )
    builder.button(
        text=f"{Emoji.BACK} Отмена",
        callback_data="menu_main"
    )
    builder.adjust(1)

    await message.answer(
        f"{Emoji.TASK_POOL} ОЧИСТКА ПУЛА ЗАДАЧ\n\n"
        f"📊 Статистика:\n"
        f"• Всего задач в пуле: {len(pool_tasks)}\n"
        f"• Перецепных: {len([t for t in pool_tasks if t.parking and t.parking.is_hitch])}\n"
        f"• Не перецепных: {len([t for t in pool_tasks if t.parking and not t.parking.is_hitch])}\n\n"
        f"Выберите действие:",
        reply_markup=builder.as_markup()
    )


@router.callback_query(F.data == "clear_pool_list")
@with_db
async def process_clear_pool_list(callback: CallbackQuery, db: Session):
    """Показать список задач в пуле"""
    pool_tasks = db.query(Task).join(Parking).filter(
        Task.status == "PENDING",
        Task.is_in_pool == True,
        Parking.departure_time == None
    ).all()

    if not pool_tasks:
        await callback.message.edit_text(f"{Emoji.INFO} Пул задач пуст.")
        return

    response = f"{Emoji.TASK_POOL} ЗАДАЧИ В ПУЛЕ:\n\n"

    for task in pool_tasks:
        created = ensure_timezone_aware(task.created_at)
        wait_time = get_timezone_aware_now() - created
        minutes = int(wait_time.total_seconds() / 60)

        driver_name = "Не назначен"
        if task.driver:
            driver_name = f"{task.driver.first_name} {task.driver.last_name}".strip()

        response += (
            f"🆔 Задача #{task.id}\n"
            f"🚗 ТС: {task.parking.vehicle_number}\n"
            f"📍 Место: #{task.parking.spot_number}\n"
            f"🚪 Ворота: #{task.gate_number}\n"
            f"👤 Водитель: {driver_name}\n"
            f"⏰ Ожидание: {minutes} мин\n"
            f"📊 Приоритет: {task.priority}\n"
            f"{'─' * 40}\n\n"
        )

    # Добавляем кнопки действий
    builder = InlineKeyboardBuilder()
    builder.button(
        text="🔄 Перезапустить все",
        callback_data="clear_pool_restart"
    )
    builder.button(
        text="❌ Удалить все",
        callback_data="clear_pool_delete"
    )
    builder.button(
        text=f"{Emoji.BACK} Назад",
        callback_data="menu_main"
    )
    builder.adjust(1)

    await callback.message.edit_text(
        response[:4000],
        reply_markup=builder.as_markup()
    )


@router.callback_query(F.data == "clear_pool_restart")
@with_db
async def process_clear_pool_restart(callback: CallbackQuery, db: Session):
    """Перезапустить все задачи в пуле"""
    # Находим все зависшие задачи в пуле
    stuck_tasks = db.query(Task).join(Parking).filter(
        Task.status == "STUCK",
        Task.is_in_pool == True,
        Parking.departure_time == None
    ).all()

    # Сбрасываем их статус
    count = 0
    for task in stuck_tasks:
        task.status = "PENDING"
        task.driver_id = None
        task.is_stuck = False
        task.stuck_reason = None
        task.priority = max(0, task.priority - 5)  # Снижаем приоритет
        count += 1

    # Также проверяем задачи, которые слишком долго висят
    old_tasks = db.query(Task).join(Parking).filter(
        Task.status == "PENDING",
        Task.is_in_pool == True,
        Task.created_at <= get_timezone_aware_now() - timedelta(hours=24),
        Parking.departure_time == None
    ).all()

    for task in old_tasks:
        task.priority = 0  # Сбрасываем приоритет для очень старых задач
        count += 1

    db.commit()

    await callback.message.edit_text(
        f"{Emoji.SUCCESS} Пул задач очищен!\n\n"
        f"📊 Результат:\n"
        f"• Перезапущено зависших задач: {len(stuck_tasks)}\n"
        f"• Сброшен приоритет старых задач: {len(old_tasks)}\n"
        f"• Всего обработано: {count}"
    )


@router.callback_query(F.data == "clear_pool_delete")
@with_db
async def process_clear_pool_delete(callback: CallbackQuery, db: Session):
    """Удалить все задачи из пула"""
    # Находим все задачи в пуле
    pool_tasks = db.query(Task).join(Parking).filter(
        Task.status == "PENDING",
        Task.is_in_pool == True
    ).all()

    count = len(pool_tasks)

    # Удаляем задачи
    for task in pool_tasks:
        db.delete(task)

    db.commit()

    await callback.message.edit_text(
        f"{Emoji.SUCCESS} Пул задач очищен!\n\n"
        f"📊 Результат:\n"
        f"• Удалено задач: {count}"
    )


@router.message(F.text == f"{Emoji.STATUS} Статус заданий")
@with_db
async def process_tasks_status(message: Message, db: Session):
    """Просмотр статуса активных заданий"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer("❌ Сначала используйте /start для регистрации.")
        return

    if "OPERATOR" not in get_user_roles(user) and "ADMIN" not in get_user_roles(user):
        await message.answer("❌ Эта функция доступна только операторам и администраторам.")
        return

    # Получаем активные задания
    tasks = db.query(Task).filter(
        Task.status.in_(["PENDING", "IN_PROGRESS", "STUCK"])
    ).order_by(
        Task.priority.desc(),
        Task.created_at.desc()
    ).limit(20).all()

    if not tasks:
        await message.answer(f"{Emoji.INFO} Нет активных заданий.")
        return

    response = f"{Emoji.TASK} СТАТУС ЗАДАНИЙ:\n\n"

    for task in tasks:
        # Русское название статуса
        status_text = STATUS_NAMES.get(task.status, task.status)
        status_emoji = TASK_STATUS_EMOJI.get(task.status, "❓")

        # Время выполнения
        duration = ""
        if task.started_at:
            started = ensure_timezone_aware(task.started_at)
            delta = get_timezone_aware_now() - started
            hours = delta.seconds // 3600
            minutes = (delta.seconds % 3600) // 60
            if hours > 0:
                duration = f" ({hours}ч {minutes}м)"
            else:
                duration = f" ({minutes}м)"
        elif task.status == "PENDING" and task.created_at:
            created = ensure_timezone_aware(task.created_at)
            delta = get_timezone_aware_now() - created
            hours = delta.seconds // 3600
            minutes = (delta.seconds % 3600) // 60
            if hours > 0:
                duration = f" (ожидает {hours}ч {minutes}м)"
            else:
                duration = f" (ожидает {minutes}м)"

        # Информация о водителе
        driver_name = "Не назначен"
        if task.driver:
            driver_name = f"{task.driver.first_name} {task.driver.last_name}".strip()
            if not driver_name:
                driver_name = f"ID: {task.driver.telegram_id}"
        elif task.assigned_driver_id:
            driver_name = "Назначен, но не взят"

        # Информация о ТС
        vehicle_number = "Неизвестно"
        spot_number = "?"
        if task.parking:
            vehicle_number = task.parking.vehicle_number
            spot_number = task.parking.spot_number

        # Тип задачи
        task_type = "🔗 Перецепной" if task.parking and task.parking.is_hitch else "🚛 Не перецепной"

        response += (
            f"{status_emoji} ЗАДАЧА #{task.id}\n"
            f"📌 Статус: {status_text}{duration}\n"
            f"🚪 Ворота: #{task.gate_number}\n"
            f"🚗 ТС: {vehicle_number}\n"
            f"📍 Место: #{spot_number}\n"
            f"👤 Водитель: {driver_name}\n"
            f"📊 Приоритет: {task.priority}\n"
            f"📝 Тип: {task_type}\n"
        )

        # Причина зависания
        if task.is_stuck and task.stuck_reason:
            response += f"⚠️ Причина: {task.stuck_reason}\n"

        # Время создания
        if task.created_at:
            created = ensure_timezone_aware(task.created_at)
            response += f"⏰ Создана: {created.strftime('%H:%M %d.%m.%Y')}\n"

        response += f"{'─' * 40}\n\n"

    await message.answer(response[:4000])

@router.message(F.text == f"{Emoji.PARKING} Статус парковки")
@with_db
async def process_parking_status(message: Message, db: Session):
    """Просмотр статуса парковки"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer("❌ Сначала используйте /start для регистрации.")
        return

    user_roles = get_user_roles(user)
    if not any(r in user_roles for r in ["OPERATOR", "ADMIN", "DEB_EMPLOYEE"]):
        await message.answer("❌ Доступ запрещен.")
        return

    total_spots = config.PARKING_SPOTS
    now = get_timezone_aware_now()
    active_parkings = db.query(Parking).filter(Parking.departure_time == None).all()
    occupied_spots = len(active_parkings)

    # Анализ времени стоянки
    time_stats = {"1h": 0, "2h": 0, "3h": 0, "6h": 0, "12h": 0, "24h": 0}

    for parking in active_parkings:
        hours = (now - ensure_timezone_aware(parking.arrival_time)).total_seconds() / 3600
        if hours > 24:
            time_stats["24h"] += 1
        elif hours > 12:
            time_stats["12h"] += 1
        elif hours > 6:
            time_stats["6h"] += 1
        elif hours > 3:
            time_stats["3h"] += 1
        elif hours > 2:
            time_stats["2h"] += 1
        elif hours > 1:
            time_stats["1h"] += 1

    response = (
        f"{Emoji.PARKING} Статус парковки\n\n"
        f"📊 Общая статистика:\n"
        f"• Всего мест: {total_spots}\n"
        f"• Занято: {occupied_spots}\n"
        f"• Свободно: {total_spots - occupied_spots}\n\n"
        f"⏰ Время стоянки:\n"
        f"• До 1 часа: {time_stats['1h']}\n"
        f"• 1-2 часа: {time_stats['2h']}\n"
        f"• 2-3 часа: {time_stats['3h']}\n"
        f"• 3-6 часов: {time_stats['6h']}\n"
        f"• 6-12 часов: {time_stats['12h']}\n"
        f"• Более 12 часов: {time_stats['24h']}\n\n"
        f"🔄 Обновлено: {now.strftime('%H:%M %d.%m.%Y')}"
    )

    if active_parkings:
        response += "\n\n🚗 Список припаркованных ТС:\n"
        for parking in active_parkings[:5]:
            driver_name = f"{parking.user.first_name} {parking.user.last_name}".strip() or "Водитель"
            duration = now - ensure_timezone_aware(parking.arrival_time)
            hours = int(duration.total_seconds() // 3600)
            minutes = int((duration.total_seconds() % 3600) // 60)
            response += f"• #{parking.spot_number}: {parking.vehicle_number} ({driver_name}) - {hours}ч {minutes}м\n"

        if len(active_parkings) > 5:
            response += f"• ... и еще {len(active_parkings) - 5} ТС"

    await message.answer(response)


@router.message(F.text == f"{Emoji.REPORT} Отчет")
@with_db
async def process_operator_report(message: Message, db: Session):
    """Меню отчетов для оператора"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer("❌ Сначала используйте /start для регистрации.")
        return

    if "OPERATOR" not in get_user_roles(user):
        await message.answer("❌ Эта функция доступна только операторам.")
        return

    await message.answer(
        "📊 Выберите тип отчета:",
        reply_markup=get_operator_reports_keyboard()
    )


@router.callback_query(F.data == "operator_report_tasks")
@with_db
async def process_operator_report_tasks(callback: CallbackQuery, db: Session):
    """Отчет по задачам за сегодня"""
    user = await get_user(db, callback.from_user.id)
    if not user or "OPERATOR" not in get_user_roles(user):
        await callback.message.edit_text("❌ Доступ запрещен.")
        return

    now = get_timezone_aware_now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    tasks_today = db.query(Task).filter(Task.created_at >= today_start).all()

    completed = len([t for t in tasks_today if t.status == "COMPLETED"])
    pending = len([t for t in tasks_today if t.status == "PENDING"])
    in_progress = len([t for t in tasks_today if t.status == "IN_PROGRESS"])
    stuck = len([t for t in tasks_today if t.status == "STUCK"])

    response = (
        f"📊 Отчет по задачам за сегодня\n\n"
        f"📅 Дата: {now.strftime('%d.%m.%Y')}\n"
        f"⏰ Время: {now.strftime('%H:%M')}\n\n"
        f"📋 Статистика задач:\n"
        f"{Emoji.COMPLETED} Выполнено: {completed}\n"
        f"{Emoji.PENDING} Ожидание: {pending}\n"
        f"{Emoji.IN_PROGRESS} В работе: {in_progress}\n"
        f"{Emoji.STUCK} Зависло: {stuck}\n"
        f"📝 Всего: {len(tasks_today)}"
    )

    builder = InlineKeyboardBuilder()
    builder.button(
        text=f"{Emoji.BACK} Назад к отчетам",
        callback_data="back_to_reports_menu"
    )
    builder.adjust(1)

    await callback.message.edit_text(response, reply_markup=builder.as_markup())


@router.callback_query(F.data == "operator_report_parking")
@with_db
async def process_operator_report_parking(callback: CallbackQuery, db: Session):
    """Отчет по парковке"""
    user = await get_user(db, callback.from_user.id)
    if not user or "OPERATOR" not in get_user_roles(user):
        await callback.message.edit_text("❌ Доступ запрещен.")
        return

    now = get_timezone_aware_now()
    total_spots = config.PARKING_SPOTS
    active_parkings = db.query(Parking).filter(Parking.departure_time == None).all()
    occupied_spots = len(active_parkings)

    response = (
        f"📊 Отчет по парковке\n\n"
        f"📅 Дата: {now.strftime('%d.%m.%Y')}\n"
        f"⏰ Время: {now.strftime('%H:%M')}\n\n"
        f"🅿️ Статистика:\n"
        f"• Всего мест: {total_spots}\n"
        f"• Занято: {occupied_spots}\n"
        f"• Свободно: {total_spots - occupied_spots}"
    )

    builder = InlineKeyboardBuilder()
    builder.button(
        text=f"{Emoji.BACK} Назад к отчетам",
        callback_data="back_to_reports_menu"
    )
    builder.adjust(1)

    await callback.message.edit_text(response, reply_markup=builder.as_markup())


@router.callback_query(F.data == "operator_report_excel")
async def process_operator_report_excel(callback: CallbackQuery):
    """Меню выбора периода для Excel отчета"""
    await callback.message.edit_text(
        "📊 Excel отчет\n\n"
        "📅 Выберите период для отчета:",
        reply_markup=get_report_period_keyboard()
    )


@router.callback_query(F.data == "back_to_reports_menu")
async def process_back_to_reports_menu(callback: CallbackQuery):
    """Возврат в меню отчетов"""
    await callback.message.edit_text(
        "📊 Выберите тип отчета:",
        reply_markup=get_operator_reports_keyboard()
    )


@router.callback_query(F.data.startswith("report_"))
@with_db
async def process_report_selection(callback: CallbackQuery, state: FSMContext, db: Session):
    """Обработка выбора периода для Excel отчета"""
    period = callback.data.replace("report_", "")
    user = await get_user(db, callback.from_user.id)

    if not user or "OPERATOR" not in get_user_roles(user):
        await callback.message.edit_text("❌ Доступ запрещен.")
        return

    now = get_timezone_aware_now()
    start_date = None
    end_date = now
    period_name = ""

    if period == "today":
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        period_name = "сегодня"
    elif period == "yesterday":
        yesterday = now - timedelta(days=1)
        start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        period_name = "вчера"
    elif period == "week":
        start_date = (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        period_name = "текущую_неделю"
    elif period == "month":
        start_date = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        period_name = "текущий_месяц"
    elif period == "custom":
        await callback.message.edit_text(
            "📅 Введите период в формате:\n"
            "дд.мм.гггг-дд.мм.гггг\n\n"
            "Пример: 01.01.2024-31.01.2024\n\n"
            f"Для отмены нажмите {Emoji.CANCEL} Отмена"
        )
        await state.set_state(OperatorStates.waiting_for_report_period)
        return
    else:
        await callback.message.edit_text("❌ Неизвестный период.")
        return

    await callback.message.edit_text(f"📊 Генерация Excel отчета за {period_name}...")

    parkings = db.query(Parking).filter(
        Parking.arrival_time >= start_date,
        Parking.arrival_time <= end_date
    ).order_by(Parking.arrival_time).all()

    if not parkings:
        await callback.message.edit_text(f"❌ Нет данных за {period_name}.")
        return

    excel_file = await generate_excel_report(parkings, period_name)

    await callback.message.answer_document(
        document=BufferedInputFile(
            excel_file.getvalue(),
            filename=f"отчет_парковки_{period_name}.xlsx"
        ),
        caption=f"📊 Excel отчет за {period_name}\n"
               f"📋 Всего записей: {len(parkings)}"
    )

    await callback.message.answer(
        "📊 Меню отчетов:",
        reply_markup=get_operator_reports_keyboard()
    )


@router.message(OperatorStates.waiting_for_report_period)
@with_db
async def process_custom_period(message: Message, state: FSMContext, db: Session):
    """Обработка пользовательского периода для отчета"""
    if message.text == f"{Emoji.CANCEL} Отмена":
        await state.clear()
        user = await get_user(db, message.from_user.id)
        await message.answer(
            "❌ Ввод периода отменен.",
            reply_markup=get_main_menu_keyboard(user)
        )
        return

    user = await get_user(db, message.from_user.id)
    if not user or "OPERATOR" not in get_user_roles(user):
        await message.answer("❌ Доступ запрещен.")
        return

    try:
        dates = message.text.strip().split('-')
        if len(dates) != 2:
            raise ValueError

        start_date = moscow_tz.localize(datetime.strptime(dates[0].strip(), '%d.%m.%Y'))
        end_date = moscow_tz.localize(datetime.strptime(dates[1].strip(), '%d.%m.%Y').replace(
            hour=23, minute=59, second=59
        ))

        if start_date > end_date:
            await message.answer("❌ Дата начала не может быть позже даты окончания.")
            return

        parkings = db.query(Parking).filter(
            Parking.arrival_time >= start_date,
            Parking.arrival_time <= end_date
        ).order_by(Parking.arrival_time).all()

        if not parkings:
            await message.answer("❌ Нет данных за выбранный период.")
            return

        period_name = f"{dates[0].strip()}_{dates[1].strip()}"
        excel_file = await generate_excel_report(parkings, period_name)

        await message.answer_document(
            document=BufferedInputFile(
                excel_file.getvalue(),
                filename=f"отчет_парковки_{period_name}.xlsx"
            ),
            caption=f"📊 Excel отчет за период {dates[0].strip()} - {dates[1].strip()}\n"
                   f"📋 Всего записей: {len(parkings)}"
        )

        await state.clear()
        await message.answer(
            "Главное меню:",
            reply_markup=get_main_menu_keyboard(user)
        )

    except ValueError:
        await message.answer(
            "❌ Неверный формат периода!\n\n"
            "Введите период в формате:\n"
            "дд.мм.гггг-дд.мм.гггг\n\n"
            "Пример: 01.01.2024-31.01.2024\n\n"
            f"Для отмены нажмите {Emoji.CANCEL} Отмена"
        )


@router.message(F.text == f"{Emoji.STUCK} Зависшие задачи")
@with_db
async def process_stuck_tasks(message: Message, db: Session):
    """Просмотр зависших задач"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer("❌ Сначала используйте /start для регистрации.")
        return

    user_roles = get_user_roles(user)
    if "OPERATOR" not in user_roles and "ADMIN" not in user_roles:
        await message.answer("❌ Эта функция доступна только операторам и администраторам.")
        return

    # Получаем все зависшие задачи
    stuck_tasks = db.query(Task).filter(
        Task.status == "STUCK"
    ).order_by(
        Task.priority.desc(),
        Task.created_at.desc()
    ).all()

    if not stuck_tasks:
        await message.answer(f"{Emoji.INFO} Нет зависших задач.")
        return

    response = f"{Emoji.STUCK} ЗАВИСШИЕ ЗАДАЧИ:\n\n"

    # Сохраняем список задач в состояние для последующего использования
    # Для этого нужно получить FSMContext, но здесь его нет, поэтому будем использовать callback_data

    now = get_timezone_aware_now()

    for task in stuck_tasks[:10]:  # Показываем последние 10
        created = ensure_timezone_aware(task.created_at)
        wait_time = now - created
        minutes = int(wait_time.total_seconds() / 60)

        if minutes < 60:
            wait_str = f"{minutes} мин"
        else:
            hours = minutes // 60
            mins = minutes % 60
            wait_str = f"{hours}ч {mins}мин"

        # Информация о водителе
        driver_info = "Не был назначен"
        if task.driver:
            driver_info = f"{task.driver.first_name} {task.driver.last_name}".strip()
            if not driver_info:
                driver_info = f"ID: {task.driver.telegram_id}"
        elif task.assigned_driver_id:
            driver_info = "Был назначен, но не взял"

        # Тип ТС
        vehicle_type = "🔗 Перецепной" if task.parking and task.parking.is_hitch else "🚛 Не перецепной"

        response += (
            f"🆔 ЗАДАЧА #{task.id}\n"
            f"📌 Причина: {task.stuck_reason or 'Не указана'}\n"
            f"⏰ Зависла: {wait_str} назад\n"
            f"🚗 ТС: {task.parking.vehicle_number if task.parking else 'Неизвестно'}\n"
            f"📍 Место: #{task.parking.spot_number if task.parking else '?'}\n"
            f"🚪 Ворота: #{task.gate_number}\n"
            f"👤 Водитель: {driver_info}\n"
            f"📊 Приоритет: {task.priority}\n"
            f"📝 Тип: {vehicle_type}\n"
            f"⏰ Создана: {created.strftime('%H:%M %d.%m.%Y')}\n"
        )

        # Добавляем инлайн-кнопки для каждой задачи
        builder = InlineKeyboardBuilder()
        builder.button(
            text=f"🔄 Переназначить ворота #{task.id}",
            callback_data=f"reassign_gate_{task.id}"
        )
        builder.button(
            text=f"❌ Закрыть задачу #{task.id}",
            callback_data=f"close_stuck_task_{task.id}"
        )
        builder.adjust(2)

        # Отправляем каждую задачу отдельно с кнопками
        await message.answer(
            response,
            reply_markup=builder.as_markup()
        )
        response = ""  # Сбрасываем для следующей задачи

    # Клавиатура для массовых действий
    builder = InlineKeyboardBuilder()
    restartable_tasks = [t for t in stuck_tasks if t.parking and t.parking.is_hitch and t.parking.departure_time is None]
    if restartable_tasks:
        builder.button(
            text=f"🔄 Перезапустить все в пул ({len(restartable_tasks)})",
            callback_data="restart_all_stuck"
        )
    builder.button(text=f"{Emoji.BACK} Назад", callback_data="menu_main")
    builder.adjust(1)

    if response:  # Если остался неотправленный текст
        await message.answer(response[:4000], reply_markup=builder.as_markup())
    else:
        await message.answer("Выберите действие:", reply_markup=builder.as_markup())

@router.callback_query(F.data.startswith("reassign_gate_"))
@with_db
async def process_reassign_gate(callback: CallbackQuery, state: FSMContext, db: Session):
    """Начало процесса переназначения ворот для зависшей задачи"""
    task_id = int(callback.data.replace("reassign_gate_", ""))

    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        await callback.message.edit_text(f"{Emoji.ERROR} Задача не найдена.")
        return

    # Сохраняем ID задачи в состояние
    await state.update_data(stuck_task_id=task_id)
    await state.set_state(OperatorStates.waiting_for_new_gate_for_stuck_task)

    # Определяем доступные ворота в зависимости от АБК
    # Здесь нужно определить логику определения АБК для задачи
    # Например, по номеру ворот или по дополнительному полю

    # Примерная логика определения АБК (можно настроить под ваши нужды)
    if 1 <= task.gate_number <= 59 or 66 <= task.gate_number <= 83:
        abk_type = "ABK1"
        available_gates = "1-59, 66-83"
    elif 1 <= task.gate_number <= 10:
        abk_type = "ABK2"
        available_gates = "1-10"
    else:
        abk_type = "неизвестно"
        available_gates = "уточните у администратора"

    await callback.message.edit_text(
        f"{Emoji.GATE} ПЕРЕНАЗНАЧЕНИЕ ВОРОТ ДЛЯ ЗАДАЧИ #{task_id}\n\n"
        f"📋 Текущая информация:\n"
        f"🚗 ТС: {task.parking.vehicle_number if task.parking else 'Неизвестно'}\n"
        f"📍 Место: #{task.parking.spot_number if task.parking else '?'}\n"
        f"🚪 Текущие ворота: #{task.gate_number}\n"
        f"🏢 АБК: {abk_type}\n"
        f"📝 Тип: {'Перецепной' if task.parking and task.parking.is_hitch else 'Не перецепной'}\n\n"
        f"📌 Доступные ворота: {available_gates}\n\n"
        f"Введите новый номер ворот:"
    )

@router.message(OperatorStates.waiting_for_new_gate_for_stuck_task)
@with_db
async def process_new_gate_for_stuck_task(message: Message, state: FSMContext, db: Session):
    """Обработка ввода нового номера ворот для зависшей задачи"""
    try:
        new_gate_number = int(message.text)
        if new_gate_number <= 0:
            raise ValueError
    except ValueError:
        await message.answer(
            f"{Emoji.ERROR} Пожалуйста, введите положительное число для номера ворот.\n"
            f"Пример: 12, 15, 7"
        )
        return

    data = await state.get_data()
    task_id = data.get('stuck_task_id')

    if not task_id:
        await message.answer(f"{Emoji.ERROR} ID задачи не найден.")
        await state.clear()
        return

    task = db.query(Task).filter(Task.id == task_id).first()
    operator = await get_user(db, message.from_user.id)

    if not task:
        await message.answer(f"{Emoji.ERROR} Задача не найдена.")
        await state.clear()
        return

    # Проверяем доступность новых ворот (аналогично проверке при создании задачи)
    is_valid_gate = False
    error_message = ""

    # Определяем АБК по старым воротам (можно добавить логику определения)
    if 1 <= task.gate_number <= 59 or 66 <= task.gate_number <= 83:
        # АБК-1
        if 1 <= new_gate_number <= 59 or 66 <= new_gate_number <= 83:
            is_valid_gate = True
        else:
            error_message = (
                f"{Emoji.ERROR} Для этой задачи доступны ворота АБК-1:\n"
                f"• с 1 по 59\n"
                f"• с 66 по 83\n"
                f"Ворота с 60 по 65 недоступны для использования."
            )
    elif 1 <= task.gate_number <= 10:
        # АБК-2
        if 1 <= new_gate_number <= 10:
            is_valid_gate = True
        else:
            error_message = (
                f"{Emoji.ERROR} Для этой задачи доступны только ворота АБК-2 с 1 по 10."
            )
    else:
        # Если не можем определить, разрешаем любые ворота
        is_valid_gate = True

    if not is_valid_gate:
        await message.answer(error_message)
        return

    # Сохраняем старый номер ворот для истории
    old_gate_number = task.gate_number

    # Обновляем задачу
    task.gate_number = new_gate_number
    task.status = "PENDING"  # Возвращаем в ожидание
    task.is_stuck = False
    task.stuck_reason = f"Переназначены ворота с {old_gate_number} на {new_gate_number} оператором {operator.first_name}"
    task.driver_id = None  # Сбрасываем водителя

    # Если перецепная, возвращаем в пул
    if task.parking and task.parking.is_hitch:
        task.is_in_pool = True
        task.priority += 5  # Повышаем приоритет

    db.commit()

    # Определяем тип здания для отправки изображения
    if 1 <= new_gate_number <= 59 or 66 <= new_gate_number <= 83:
        building_type = "ABK1"
        abk_name = "АБК-1"
    elif 1 <= new_gate_number <= 10:
        building_type = "ABK2"
        abk_name = "АБК-2"
    else:
        building_type = None
        abk_name = "неизвестно"

    # Уведомляем водителя, если он был назначен
    if task.assigned_driver_id:
        driver = await get_user(db, task.assigned_driver_id)
        if driver:
            try:
                notification_text = (
                    f"{Emoji.TASK} ЗАДАЧА ПЕРЕНАЗНАЧЕНА!\n\n"
                    f"📋 Информация:\n"
                    f"🆔 Задача: #{task.id}\n"
                    f"📍 Место: #{task.parking.spot_number if task.parking else '?'}\n"
                    f"🚗 ТС: {task.parking.vehicle_number if task.parking else 'Неизвестно'}\n"
                    f"🏢 {abk_name}\n"
                    f"🚪 Новые ворота: #{new_gate_number} (были #{old_gate_number})\n"
                    f"📝 Тип: {'Перецепной' if task.parking and task.parking.is_hitch else 'Не перецепной'}\n"
                    f"👤 Оператор: {operator.first_name} {operator.last_name}\n\n"
                    f'Используйте кнопку "{Emoji.GATE} Встать на ворота" для выполнения.'
                )

                if building_type:
                    await send_task_with_image(
                        driver.telegram_id,
                        building_type,
                        new_gate_number,
                        notification_text
                    )
                else:
                    await bot.send_message(driver.telegram_id, notification_text)

            except Exception as e:
                logger.error(f"Ошибка уведомления водителя: {e}")

    # Уведомляем всех активных водителей перегона, если задача в пуле
    elif task.is_in_pool:
        from services import get_active_transfer_drivers
        active_drivers = await get_active_transfer_drivers(db)

        notification_text = (
            f"{Emoji.TASK_POOL} ЗАДАЧА ПЕРЕНАЗНАЧЕНА В ПУЛЕ!\n\n"
            f"📋 Информация:\n"
            f"🆔 Задача: #{task.id}\n"
            f"📍 Место: #{task.parking.spot_number if task.parking else '?'}\n"
            f"🚗 ТС: {task.parking.vehicle_number if task.parking else 'Неизвестно'}\n"
            f"🏢 {abk_name}\n"
            f"🚪 Новые ворота: #{new_gate_number} (были #{old_gate_number})\n"
            f"📊 Приоритет: {task.priority}\n"
            f"👤 Оператор: {operator.first_name} {operator.last_name}\n\n"
            f'Используйте кнопку "{Emoji.TASK} Взять задачу".'
        )

        for driver in active_drivers:
            try:
                if building_type:
                    await send_task_with_image(
                        driver.telegram_id,
                        building_type,
                        new_gate_number,
                        notification_text
                    )
                else:
                    await bot.send_message(driver.telegram_id, notification_text)
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Ошибка уведомления водителя {driver.telegram_id}: {e}")

    await message.answer(
        f"{Emoji.SUCCESS} ВОРОТА ПЕРЕНАЗНАЧЕНЫ!\n\n"
        f"📋 Информация:\n"
        f"🆔 Задача: #{task.id}\n"
        f"🚪 Старые ворота: #{old_gate_number}\n"
        f"🚪 Новые ворота: #{new_gate_number}\n"
        f"🏢 {abk_name}\n"
        f"📊 Новый статус: {'В пуле задач' if task.is_in_pool else 'Ожидает водителя'}\n"
        f"📈 Новый приоритет: {task.priority}\n\n"
        f"✅ Водители уведомлены."
    )

    await state.clear()

@router.callback_query(F.data.startswith("close_stuck_task_"))
@with_db
async def process_close_stuck_task(callback: CallbackQuery, state: FSMContext, db: Session):
    """Закрытие зависшей задачи"""
    task_id = int(callback.data.replace("close_stuck_task_", ""))

    task = db.query(Task).filter(Task.id == task_id).first()
    operator = await get_user(db, callback.from_user.id)

    if not task:
        await callback.message.edit_text(f"{Emoji.ERROR} Задача не найдена.")
        return

    # Закрываем задачу
    task.status = "CANCELLED"
    task.stuck_reason = f"Закрыта оператором {operator.first_name} {operator.last_name}"
    task.driver_id = None
    task.is_in_pool = False

    db.commit()

    await callback.message.edit_text(
        f"{Emoji.SUCCESS} ЗАДАЧА #{task.id} ЗАКРЫТА!\n\n"
        f"📋 Информация:\n"
        f"🚗 ТС: {task.parking.vehicle_number if task.parking else 'Неизвестно'}\n"
        f"📍 Место: #{task.parking.spot_number if task.parking else '?'}\n"
        f"🚪 Ворота: #{task.gate_number}\n"
        f"👤 Оператор: {operator.first_name} {operator.last_name}"
    )



@router.callback_query(F.data.startswith("stuck_page_"))
@with_db
async def process_stuck_page(callback: CallbackQuery, db: Session):
    """Пагинация по зависшим задачам"""
    page = int(callback.data.replace("stuck_page_", ""))
    await show_stuck_tasks_list(callback, db, page)


@router.callback_query(F.data == "refresh_stuck_tasks")
@with_db
async def process_refresh_stuck_tasks(callback: CallbackQuery, db: Session):
    """Обновление списка зависших задач"""
    await show_stuck_tasks_list(callback, db, 0)


@router.callback_query(F.data == "back_to_stuck_list")
@with_db
async def process_back_to_stuck_list(callback: CallbackQuery, db: Session):
    """Возврат к списку зависших задач"""
    await show_stuck_tasks_list(callback, db, 0)


@router.callback_query(F.data.startswith("stuck_task_info_"))
@with_db
async def process_stuck_task_info(callback: CallbackQuery, db: Session):
    """Детальная информация о зависшей задаче"""
    task_id = int(callback.data.replace("stuck_task_info_", ""))
    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        await callback.message.edit_text("❌ Задача не найдена.")
        return

    created = ensure_timezone_aware(task.created_at)
    now = get_timezone_aware_now()
    wait_time = now - created

    response = (
        f"{Emoji.STUCK} ДЕТАЛИ ЗАВИСШЕЙ ЗАДАЧИ #{task.id}\n\n"
        f"📋 Информация:\n"
        f"🚗 ТС: {task.parking.vehicle_number if task.parking else 'Неизвестно'}\n"
        f"📍 Место: #{task.parking.spot_number if task.parking else '?'}\n"
        f"🚪 Ворота: #{task.gate_number}\n"
        f"📌 Причина: {task.stuck_reason or 'Не указана'}\n"
        f"⏰ Создана: {created.strftime('%H:%M %d.%m.%Y')}\n"
        f"⏱️ В ожидании: {format_duration(int(wait_time.total_seconds()))}\n"
        f"📊 Приоритет: {task.priority}\n"
        f"📝 Тип: {'Перецепной' if task.parking and task.parking.is_hitch else 'Не перецепной'}\n\n"
    )

    if task.driver:
        response += f"👤 Последний водитель: {task.driver.first_name} {task.driver.last_name}\n"

    if task.operator:
        response += f"🎛️ Оператор: {task.operator.first_name} {task.operator.last_name}\n"

    await callback.message.edit_text(
        response,
        reply_markup=get_stuck_task_detail_keyboard(task.id)
    )


@router.callback_query(F.data.startswith("restart_task_"))
@with_db
async def process_restart_stuck_task(callback: CallbackQuery, db: Session):
    """Перезапуск зависшей задачи"""
    task_id = int(callback.data.replace("restart_task_", ""))
    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        await callback.message.edit_text("❌ Задача не найдена.")
        return

    # Сбрасываем статус задачи
    task.status = "PENDING"
    task.driver_id = None
    task.is_stuck = False
    task.stuck_reason = None
    task.is_in_pool = True  # Возвращаем в пул
    task.priority = min(task.priority + 3, 20)  # Повышаем приоритет

    db.commit()

    await callback.message.edit_text(
        f"✅ Задача #{task.id} перезапущена!\n\n"
        f"📋 Новая информация:\n"
        f"• Статус: PENDING\n"
        f"• В пуле: Да\n"
        f"• Новый приоритет: {task.priority}\n\n"
        f"Задача снова доступна водителям."
    )

    # Уведомляем активных водителей перегона
    if task.parking and task.parking.is_hitch:
        active_drivers = await get_active_transfer_drivers(db)
        for driver in active_drivers:
            try:
                await bot.send_message(
                    driver.telegram_id,
                    f"{Emoji.TASK_POOL} ПЕРЕЗАПУЩЕНА ЗАДАЧА #{task.id}!\n\n"
                    f"📍 Место: #{task.parking.spot_number}\n"
                    f"🚗 ТС: {task.parking.vehicle_number}\n"
                    f"🚪 Ворота: #{task.gate_number}\n"
                    f"📊 Приоритет: {task.priority}\n\n"
                    f'Используйте кнопку "{Emoji.TASK} Взять задачу".'
                )
            except Exception as e:
                logger.error(f"Ошибка уведомления водителя: {e}")

    # Возвращаемся к списку
    await show_stuck_tasks_list(callback, db, 0)


@router.callback_query(F.data == "restart_all_hitch_stuck")
@with_db
async def process_restart_all_hitch_stuck(callback: CallbackQuery, db: Session):
    """Перезапуск всех перецепных зависших задач"""
    stuck_tasks = db.query(Task).join(Parking).filter(
        Task.status == "STUCK",
        Parking.is_hitch == True,
        Parking.departure_time == None
    ).all()

    count = 0
    for task in stuck_tasks:
        task.status = "PENDING"
        task.driver_id = None
        task.is_stuck = False
        task.stuck_reason = None
        task.is_in_pool = True
        task.priority += 3
        count += 1

    db.commit()

    await callback.message.edit_text(
        f"✅ Перезапущено {count} перецепных задач!\n\n"
        f"Все задачи возвращены в пул с повышенным приоритетом."
    )

    # Уведомляем активных водителей
    if count > 0:
        active_drivers = await get_active_transfer_drivers(db)
        for driver in active_drivers:
            try:
                await bot.send_message(
                    driver.telegram_id,
                    f"{Emoji.TASK_POOL} ПЕРЕЗАПУЩЕНО {count} ЗАДАЧ!\n\n"
                    f"В пуле появились новые задачи с повышенным приоритетом.\n"
                    f'Используйте кнопку "{Emoji.TASK} Взять задачу".'
                )
            except Exception as e:
                logger.error(f"Ошибка уведомления водителя: {e}")

    # Возвращаемся к списку
    await show_stuck_tasks_list(callback, db, 0)


@router.callback_query(F.data.startswith("reassign_task_"))
@with_db
async def process_reassign_task(callback: CallbackQuery, db: Session, state: FSMContext):
    """Назначение задачи другому водителю"""
    task_id = int(callback.data.replace("reassign_task_", ""))
    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        await callback.message.edit_text("❌ Задача не найдена.")
        return

    # Сохраняем ID задачи в состояние
    await state.update_data(reassign_task_id=task_id)

    # Получаем список активных водителей перегона
    active_drivers = await get_active_transfer_drivers(db)

    if not active_drivers:
        await callback.message.edit_text(
            "❌ Нет активных водителей перегона для назначения.\n\n"
            "Задача будет возвращена в пул."
        )
        # Возвращаем в пул
        task.status = "PENDING"
        task.driver_id = None
        task.is_stuck = False
        task.stuck_reason = None
        task.is_in_pool = True
        db.commit()
        return

    builder = InlineKeyboardBuilder()
    for driver in active_drivers[:10]:  # Ограничим 10 водителями
        name = f"{driver.first_name} {driver.last_name}".strip() or f"ID: {driver.telegram_id}"
        builder.button(
            text=f"👤 {name}",
            callback_data=f"assign_to_driver_{driver.id}_{task_id}"
        )

    builder.button(
        text=f"{Emoji.BACK} Назад",
        callback_data=f"stuck_task_info_{task_id}"
    )
    builder.adjust(1)

    await callback.message.edit_text(
        f"📋 Выберите водителя для назначения задачи #{task_id}:",
        reply_markup=builder.as_markup()
    )


@router.callback_query(F.data.startswith("assign_to_driver_"))
@with_db
async def process_assign_to_driver(callback: CallbackQuery, db: Session):
    """Назначение задачи выбранному водителю"""
    parts = callback.data.replace("assign_to_driver_", "").split("_")
    driver_id = int(parts[0])
    task_id = int(parts[1])

    task = db.query(Task).filter(Task.id == task_id).first()
    driver = db.query(User).filter(User.id == driver_id).first()

    if not task or not driver:
        await callback.message.edit_text("❌ Задача или водитель не найдены.")
        return

    # Назначаем задачу водителю
    task.driver_id = driver.id
    task.status = "IN_PROGRESS"
    task.started_at = get_timezone_aware_now()
    task.is_stuck = False
    task.stuck_reason = None
    task.is_in_pool = False

    db.commit()

    # Уведомляем водителя
    try:
        await bot.send_message(
            driver.telegram_id,
            f"{Emoji.TASK} ВАМ НАЗНАЧЕНА ЗАДАЧА #{task.id}!\n\n"
            f"📍 Место: #{task.parking.spot_number}\n"
            f"🚗 ТС: {task.parking.vehicle_number}\n"
            f"🚪 Ворота: #{task.gate_number}\n\n"
            f'Используйте кнопку "{Emoji.GATE} Встать на ворота".'
        )
    except Exception as e:
        logger.error(f"Ошибка уведомления водителя: {e}")

    await callback.message.edit_text(
        f"✅ Задача #{task.id} назначена водителю {driver.first_name} {driver.last_name}!"
    )


@router.callback_query(F.data.startswith("mark_breakdown_"))
@with_db
async def process_mark_breakdown(callback: CallbackQuery, db: Session):
    """Отметить задачу как поломку ТС"""
    task_id = int(callback.data.replace("mark_breakdown_", ""))
    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        await callback.message.edit_text("❌ Задача не найдена.")
        return

    task.stuck_reason = "Поломка ТС (подтверждено оператором)"
    task.is_in_pool = False  # Не возвращаем в пул
    # Статус оставляем STUCK

    db.commit()

    await callback.message.edit_text(
        f"🔧 Задача #{task.id} отмечена как ПОЛОМКА ТС.\n\n"
        f"Задача закрыта и не будет возвращена в пул."
    )

    # Уведомляем последнего водителя
    if task.driver_id:
        try:
            await bot.send_message(
                task.driver_id,
                f"🔧 По задаче #{task.id} подтверждена поломка ТС.\n"
                f"Задача закрыта. Вы можете взять новую задачу."
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления водителя: {e}")


@router.callback_query(F.data.startswith("close_stuck_task_"))
@with_db
async def process_close_stuck_task(callback: CallbackQuery, db: Session):
    """Закрыть зависшую задачу"""
    task_id = int(callback.data.replace("close_stuck_task_", ""))
    task = db.query(Task).filter(Task.id == task_id).first()

    if not task:
        await callback.message.edit_text("❌ Задача не найдена.")
        return

    # Удаляем задачу или помечаем как отмененную
    task.status = "CANCELLED"
    task.stuck_reason = "Закрыта оператором"
    task.is_in_pool = False

    db.commit()

    await callback.message.edit_text(
        f"❌ Задача #{task.id} закрыта оператором."
    )

    # Возвращаемся к списку
    await show_stuck_tasks_list(callback, db, 0)

# ==================== ОБРАБОТЧИКИ ДЛЯ МЕНЮ СТАТУСОВ ====================

@router.message(F.text == f"{Emoji.STATUS} Статусы")
@with_db
async def process_statuses_menu(message: Message, db: Session):
    """Меню статусов для оператора"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer(f"{Emoji.ERROR} Сначала используйте /start для регистрации.")
        return

    if "OPERATOR" not in get_user_roles(user) and "ADMIN" not in get_user_roles(user):
        await message.answer(f"{Emoji.ERROR} Доступ запрещен.")
        return

    start_time, end_time, period_name = get_current_shift_period()

    await message.answer(
        f"{Emoji.STATUS} МЕНЮ СТАТУСОВ\n\n"
        f"📅 Текущий период: {period_name}\n"
        f"⏰ Период: {start_time.strftime('%H:%M %d.%m')} - {end_time.strftime('%H:%M %d.%m')}\n\n"
        f"Выберите тип статуса для просмотра:",
        reply_markup=get_statuses_menu_keyboard()
    )


@router.callback_query(F.data == "status_tasks")
@with_db
async def process_status_tasks(callback: CallbackQuery, db: Session):
    """Статус задач за текущую смену"""
    user = await get_user(db, callback.from_user.id)
    if not user or ("OPERATOR" not in get_user_roles(user) and "ADMIN" not in get_user_roles(user)):
        await callback.message.edit_text("❌ Доступ запрещен.")
        return

    start_time, end_time, period_name = get_current_shift_period()

    # Получаем задачи за текущую смену (без выполненных)
    tasks = get_tasks_for_current_shift(db, include_completed=False)

    if not tasks:
        await callback.message.edit_text(
            f"{Emoji.INFO} Нет активных задач за {period_name}.",
            reply_markup=InlineKeyboardBuilder().button(
                text=f"{Emoji.BACK} Назад",
                callback_data="back_to_statuses"
            ).as_markup()
        )
        return

    # Группируем по статусам
    pending = [t for t in tasks if t.status == "PENDING"]
    in_progress = [t for t in tasks if t.status == "IN_PROGRESS"]
    stuck = [t for t in tasks if t.status == "STUCK"]

    response = (
        f"{Emoji.TASK} СТАТУС ЗАДАЧ\n\n"
        f"📅 Период: {period_name}\n"
        f"⏰ {start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}\n\n"
        f"📊 СТАТИСТИКА:\n"
        f"{Emoji.PENDING} Ожидают: {len(pending)}\n"
        f"{Emoji.IN_PROGRESS} В работе: {len(in_progress)}\n"
        f"{Emoji.STUCK} Зависло: {len(stuck)}\n"
        f"📝 Всего активных: {len(tasks)}\n\n"
    )

    if in_progress:
        response += f"{Emoji.IN_PROGRESS} ЗАДАЧИ В РАБОТЕ:\n"
        for task in in_progress[:5]:
            driver_name = f"{task.driver.first_name} {task.driver.last_name}".strip() if task.driver else "Не назначен"
            started = ensure_timezone_aware(task.started_at) if task.started_at else task.created_at
            duration = get_timezone_aware_now() - started
            minutes = int(duration.total_seconds() / 60)
            response += (
                f"• Задача #{task.id}: {task.parking.vehicle_number}\n"
                f"  👤 {driver_name}, ⏰ {minutes} мин\n"
            )
        response += "\n"

    if pending:
        response += f"{Emoji.PENDING} ОЖИДАЮТ:\n"
        for task in pending[:5]:
            response += f"• Задача #{task.id}: {task.parking.vehicle_number} (🚪{task.gate_number})\n"
        if len(pending) > 5:
            response += f"  ... и еще {len(pending) - 5}\n"
        response += "\n"

    if stuck:
        response += f"{Emoji.STUCK} ЗАВИСЛИ:\n"
        for task in stuck[:3]:
            response += f"• Задача #{task.id}: {task.stuck_reason or 'Причина не указана'}\n"
        if len(stuck) > 3:
            response += f"  ... и еще {len(stuck) - 3}\n"

    builder = InlineKeyboardBuilder()
    builder.button(text=f"{Emoji.BACK} Назад", callback_data="back_to_statuses")
    builder.button(text=f"{Emoji.UPDATE} Обновить", callback_data="status_tasks")
    builder.adjust(2)

    await callback.message.edit_text(response[:4000], reply_markup=builder.as_markup())


@router.callback_query(F.data == "status_parking")
@with_db
async def process_status_parking(callback: CallbackQuery, db: Session):
    """Статус парковки за текущую смену"""
    user = await get_user(db, callback.from_user.id)
    if not user or ("OPERATOR" not in get_user_roles(user) and "ADMIN" not in get_user_roles(user)):
        await callback.message.edit_text("❌ Доступ запрещен.")
        return

    start_time, end_time, period_name = get_current_shift_period()

    # Получаем записи парковки за смену
    parkings = get_parking_for_current_shift(db)

    total_spots = config.PARKING_SPOTS
    active_parkings = [p for p in parkings if p.departure_time is None]
    departed_parkings = [p for p in parkings if p.departure_time is not None]

    response = (
        f"{Emoji.PARKING} СТАТУС ПАРКОВКИ\n\n"
        f"📅 Период: {period_name}\n"
        f"⏰ {start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}\n\n"
        f"📊 СТАТИСТИКА:\n"
        f"• Всего мест: {total_spots}\n"
        f"• Занято сейчас: {len(active_parkings)}\n"
        f"• Свободно: {total_spots - len(active_parkings)}\n"
        f"• Прибыло за смену: {len(parkings)}\n"
        f"• Убыло за смену: {len(departed_parkings)}\n\n"
    )

    if active_parkings:
        response += f"🚗 ТЕКУЩИЕ НА ПАРКОВКЕ:\n"
        for parking in active_parkings[:5]:
            driver_name = f"{parking.user.first_name} {parking.user.last_name}".strip() or "Водитель"
            duration = get_timezone_aware_now() - ensure_timezone_aware(parking.arrival_time)
            hours = int(duration.total_seconds() / 3600)
            minutes = int((duration.total_seconds() % 3600) / 60)
            response += f"• #{parking.spot_number}: {parking.vehicle_number} ({driver_name}) - {hours}ч {minutes}м\n"
        if len(active_parkings) > 5:
            response += f"  ... и еще {len(active_parkings) - 5}\n"

    builder = InlineKeyboardBuilder()
    builder.button(text=f"{Emoji.BACK} Назад", callback_data="back_to_statuses")
    builder.button(text=f"{Emoji.UPDATE} Обновить", callback_data="status_parking")
    builder.adjust(2)

    await callback.message.edit_text(response, reply_markup=builder.as_markup())


@router.callback_query(F.data == "status_queue")
@with_db
async def process_status_queue(callback: CallbackQuery, db: Session):
    """Статус очереди за текущую смену"""
    user = await get_user(db, callback.from_user.id)
    if not user or ("OPERATOR" not in get_user_roles(user) and "ADMIN" not in get_user_roles(user)):
        await callback.message.edit_text("❌ Доступ запрещен.")
        return

    start_time, end_time, period_name = get_current_shift_period()

    # Получаем записи очереди за смену
    queue_items = get_queue_for_current_shift(db)

    waiting = [q for q in queue_items if q.status == "waiting"]
    notified = [q for q in queue_items if q.status == "notified"]
    completed = [q for q in queue_items if q.status == "assigned"]

    response = (
        f"{Emoji.QUEUE} СТАТУС ОЧЕРЕДИ\n\n"
        f"📅 Период: {period_name}\n"
        f"⏰ {start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}\n\n"
        f"📊 СТАТИСТИКА:\n"
        f"{Emoji.WAITING} В очереди: {len(waiting)}\n"
        f"{Emoji.NOTIFIED} Уведомлены: {len(notified)}\n"
        f"{Emoji.COMPLETED} Припаркованы: {len(completed)}\n"
        f"📝 Всего за смену: {len(queue_items)}\n\n"
    )

    if waiting:
        response += f"{Emoji.WAITING} ТЕКУЩАЯ ОЧЕРЕДЬ:\n"
        for i, item in enumerate(waiting[:10], 1):
            wait_time = get_timezone_aware_now() - ensure_timezone_aware(item.created_at)
            minutes = int(wait_time.total_seconds() / 60)
            response += f"{i}. {item.vehicle_number} ({minutes} мин)\n"
        if len(waiting) > 10:
            response += f"  ... и еще {len(waiting) - 10}\n"

    builder = InlineKeyboardBuilder()
    builder.button(text=f"{Emoji.BACK} Назад", callback_data="back_to_statuses")
    builder.button(text=f"{Emoji.UPDATE} Обновить", callback_data="status_queue")
    builder.adjust(2)

    await callback.message.edit_text(response, reply_markup=builder.as_markup())


@router.callback_query(F.data == "status_stuck")
@with_db
async def process_status_stuck(callback: CallbackQuery, db: Session):
    """Зависшие задачи за текущую смену (в меню статусов)"""
    user = await get_user(db, callback.from_user.id)
    if not user or ("OPERATOR" not in get_user_roles(user) and "ADMIN" not in get_user_roles(user)):
        await callback.message.edit_text("❌ Доступ запрещен.")
        return

    start_time, end_time, period_name = get_current_shift_period()

    # Показываем краткую статистику, но предлагаем перейти в управление
    stuck_tasks = db.query(Task).filter(
        Task.status == "STUCK",
        Task.created_at >= start_time,
        Task.created_at <= end_time
    ).order_by(Task.priority.desc(), Task.created_at.desc()).all()

    if not stuck_tasks:
        await callback.message.edit_text(
            f"{Emoji.SUCCESS} Зависших задач за {period_name} нет!",
            reply_markup=InlineKeyboardBuilder().button(
                text=f"{Emoji.BACK} Назад",
                callback_data="back_to_statuses"
            ).as_markup()
        )
        return

    # Группируем по причинам
    gate_occupied = len([t for t in stuck_tasks if t.stuck_reason and "Ворота" in t.stuck_reason])
    no_vehicle = len([t for t in stuck_tasks if t.stuck_reason and "Нет ТС" in t.stuck_reason])
    timeout = len([t for t in stuck_tasks if t.stuck_reason and "таймаут" in (t.stuck_reason or "").lower()])
    breakdown = len([t for t in stuck_tasks if t.stuck_reason and "Поломка" in t.stuck_reason])

    response = (
        f"{Emoji.STUCK} ЗАВИСШИЕ ЗАДАЧИ\n\n"
        f"📅 Период: {period_name}\n"
        f"📊 Всего: {len(stuck_tasks)}\n\n"
        f"По причинам:\n"
        f"{Emoji.GATE_OCCUPIED} Занятые ворота: {gate_occupied}\n"
        f"{Emoji.CANCEL} Нет ТС на месте: {no_vehicle}\n"
        f"⏱️ Таймаут: {timeout}\n"
        f"🔧 Поломка: {breakdown}\n"
    )

    builder = InlineKeyboardBuilder()
    builder.button(
        text=f"{Emoji.STUCK} Управление зависшими",
        callback_data="manage_stuck_tasks"
    )
    builder.button(
        text=f"{Emoji.BACK} Назад",
        callback_data="back_to_statuses"
    )
    builder.adjust(1)

    await callback.message.edit_text(response, reply_markup=builder.as_markup())


@router.callback_query(F.data == "manage_stuck_tasks")
@with_db
async def process_manage_stuck_tasks(callback: CallbackQuery, db: Session):
    """Переход в режим управления зависшими задачами"""
    await show_stuck_tasks_list(callback, db, 0)


@router.callback_query(F.data == "status_shift_info")
@with_db
async def process_status_shift_info(callback: CallbackQuery, db: Session):
    """Информация о текущей смене"""
    user = await get_user(db, callback.from_user.id)

    start_time, end_time, period_name = get_current_shift_period()
    now = get_timezone_aware_now()

    # Общая статистика за смену
    tasks = get_tasks_for_current_shift(db, include_completed=True)
    parkings = get_parking_for_current_shift(db)
    queue_items = get_queue_for_current_shift(db)

    completed_tasks = [t for t in tasks if t.status == "COMPLETED"]
    stuck_tasks = [t for t in tasks if t.status == "STUCK"]

    response = (
        f"{Emoji.INFO} ИНФОРМАЦИЯ О СМЕНЕ\n\n"
        f"📅 Текущий период: {period_name}\n"
        f"⏰ Период: {start_time.strftime('%H:%M %d.%m')} - {end_time.strftime('%H:%M %d.%m')}\n"
        f"⏱️ Текущее время: {now.strftime('%H:%M %d.%m.%Y')}\n\n"
        f"📊 СТАТИСТИКА ЗА СМЕНУ:\n"
        f"• Задач создано: {len(tasks)}\n"
        f"  {Emoji.COMPLETED} Выполнено: {len(completed_tasks)}\n"
        f"  {Emoji.STUCK} Зависло: {len(stuck_tasks)}\n"
        f"  {Emoji.IN_PROGRESS} В работе: {len([t for t in tasks if t.status == 'IN_PROGRESS'])}\n"
        f"  {Emoji.PENDING} Ожидает: {len([t for t in tasks if t.status == 'PENDING'])}\n\n"
        f"• Парковка: {len([p for p in parkings if p.departure_time is None])} ТС сейчас\n"
        f"• Очередь: {len([q for q in queue_items if q.status == 'waiting'])} в ожидании\n"
    )

    builder = InlineKeyboardBuilder()
    builder.button(text=f"{Emoji.BACK} Назад", callback_data="back_to_statuses")
    builder.adjust(1)

    await callback.message.edit_text(response, reply_markup=builder.as_markup())


@router.callback_query(F.data == "back_to_statuses")
@with_db
async def process_back_to_statuses(callback: CallbackQuery, db: Session):
    """Возврат в меню статусов"""
    user = await get_user(db, callback.from_user.id)
    start_time, end_time, period_name = get_current_shift_period()

    await callback.message.edit_text(
        f"{Emoji.STATUS} МЕНЮ СТАТУСОВ\n\n"
        f"📅 Текущий период: {period_name}\n"
        f"⏰ Период: {start_time.strftime('%H:%M %d.%m')} - {end_time.strftime('%H:%M %d.%m')}\n\n"
        f"Выберите тип статуса для просмотра:",
        reply_markup=get_statuses_menu_keyboard()
    )

# ==================== ОБРАБОТЧИКИ ДЛЯ АДМИНИСТРАТОРОВ ====================
@router.message(F.text == f"{Emoji.SETTINGS} Выдать роли")
@with_db
async def process_grant_roles(message: Message, db: Session):
    """Просмотр и выдача ролей администратором"""
    user = await get_user(db, message.from_user.id)
    if not user or "ADMIN" not in get_user_roles(user):
        await message.answer("❌ Доступ запрещен.")
        return

    requests = db.query(RoleRequest).filter(
        RoleRequest.status == "ожидает"
    ).order_by(RoleRequest.created_at).all()

    if not requests:
        await message.answer(f"{Emoji.INFO} Нет запросов на выдачу ролей.")
        return

    requests_by_role = {}
    for req in requests:
        requests_by_role.setdefault(req.requested_role, []).append(req)

    builder = InlineKeyboardBuilder()
    role_names = {
        "OPERATOR": f"{Emoji.OPERATOR} Оператор",
        "DRIVER_TRANSFER": f"{Emoji.DRIVER_TRANSFER} Водитель перегона",
        "ADMIN": f"{Emoji.ADMIN} Администратор",
        "DEB_EMPLOYEE": f"{Emoji.DEB} Сотрудник ДЭБ"
    }

    for role, role_requests in requests_by_role.items():
        if role in role_names:
            builder.button(
                text=f"{role_names[role]} ({len(role_requests)})",
                callback_data=f"show_requests_{role}"
            )

    if requests_by_role:
        builder.button(
            text=f"📋 Все запросы ({len(requests)})",
            callback_data="show_all_requests"
        )

    builder.button(text=f"{Emoji.BACK} Назад", callback_data="menu_main")
    builder.adjust(1)

    await message.answer(
        f"📋 Запросы на выдачу ролей:\n"
        f"Всего запросов: {len(requests)}\n\n"
        f"Выберите роль для просмотра:",
        reply_markup=builder.as_markup()
    )


@router.callback_query(F.data == "show_all_requests")
@with_db
async def process_show_all_requests(callback: CallbackQuery, db: Session):
    """Показать все запросы на роли"""
    admin = await get_user(db, callback.from_user.id)
    if not admin or "ADMIN" not in get_user_roles(admin):
        await callback.message.edit_text("❌ Доступ запрещен.")
        return

    requests = db.query(RoleRequest).filter(
        RoleRequest.status == "ожидает"
    ).order_by(RoleRequest.created_at).all()

    if not requests:
        await callback.message.edit_text(f"{Emoji.INFO} Нет запросов на выдачу ролей.")
        return

    role_names = {
        "OPERATOR": "Оператор",
        "DRIVER_TRANSFER": "Водитель перегона",
        "ADMIN": "Администратор",
        "DEB_EMPLOYEE": "Сотрудник ДЭБ"
    }

    response = "📋 Все запросы на роли:\n\n"
    builder = InlineKeyboardBuilder()

    for i, req in enumerate(requests, 1):
        full_name = f"{req.first_name or ''} {req.last_name or ''}".strip()
        if not full_name:
            full_name = f"{req.user.first_name or ''} {req.user.last_name or ''}".strip() or "Не указано"

        response += (
            f"{i}. {full_name}\n"
            f"   📝 Роль: {role_names.get(req.requested_role, req.requested_role)}\n"
            f"   💼 Должность: {req.position or 'Не указана'}\n"
            f"   👤 @{req.user.username or 'нет'}\n"
            f"   🆔 {req.user.telegram_id}\n"
            f"   ⏰ {req.created_at.strftime('%d.%m.%Y %H:%M')}\n\n"
        )

        builder.button(
            text=f"✅ Выдать {i}",
            callback_data=f"grant_{req.requested_role}_{req.user.telegram_id}"
        )
        builder.button(
            text=f"❌ Отклонить {i}",
            callback_data=f"reject_{req.requested_role}_{req.user.telegram_id}"
        )

    builder.button(
        text=f"{Emoji.BACK} Назад",
        callback_data="back_to_role_list"
    )
    builder.adjust(2)

    await callback.message.edit_text(response[:4000], reply_markup=builder.as_markup())


@router.callback_query(F.data.startswith("show_requests_"))
@with_db
async def process_show_requests(callback: CallbackQuery, db: Session):
    """Показать запросы на конкретную роль"""
    role_str = callback.data.replace("show_requests_", "")
    admin = await get_user(db, callback.from_user.id)

    if not admin or "ADMIN" not in get_user_roles(admin):
        await callback.message.edit_text("❌ Доступ запрещен.")
        return

    requests = db.query(RoleRequest).filter(
        RoleRequest.requested_role == role_str,
        RoleRequest.status == "ожидает"
    ).order_by(RoleRequest.created_at).all()

    if not requests:
        await callback.message.edit_text(f"✅ Нет запросов на роль '{role_str}'.")
        return

    role_names = {
        "OPERATOR": "Оператор",
        "DRIVER_TRANSFER": "Водитель перегона",
        "ADMIN": "Администратор",
        "DEB_EMPLOYEE": "Сотрудник ДЭБ"
    }

    response = f"📋 Запросы на роль '{role_names.get(role_str, role_str)}':\n\n"
    builder = InlineKeyboardBuilder()

    for i, req in enumerate(requests, 1):
        full_name = f"{req.first_name or ''} {req.last_name or ''}".strip()
        if not full_name:
            full_name = f"{req.user.first_name or ''} {req.user.last_name or ''}".strip() or "Не указано"

        response += (
            f"{i}. {full_name}\n"
            f"   💼 {req.position or 'Не указана'}\n"
            f"   👤 @{req.user.username or 'нет'}\n"
            f"   🆔 {req.user.telegram_id}\n"
            f"   ⏰ {req.created_at.strftime('%d.%m.%Y %H:%M')}\n\n"
        )

        builder.button(
            text=f"✅ Выдать {i}",
            callback_data=f"grant_{role_str}_{req.user.telegram_id}"
        )
        builder.button(
            text=f"❌ Отклонить {i}",
            callback_data=f"reject_{role_str}_{req.user.telegram_id}"
        )

    builder.button(
        text=f"{Emoji.BACK} Назад",
        callback_data="back_to_role_list"
    )
    builder.adjust(2)

    await callback.message.edit_text(response, reply_markup=builder.as_markup())


@router.callback_query(F.data.startswith("grant_"))
@with_db
async def process_grant_request(callback: CallbackQuery, db: Session):
    """Выдача роли пользователю"""
    callback_data = callback.data.replace("grant_", "")
    last_underscore = callback_data.rfind('_')

    if last_underscore == -1:
        await callback.message.edit_text("❌ Неверный формат данных.")
        return

    role_str = callback_data[:last_underscore]
    target_id = int(callback_data[last_underscore + 1:])

    admin = await get_user(db, callback.from_user.id)
    if not admin or "ADMIN" not in get_user_roles(admin):
        await callback.message.edit_text("❌ Доступ запрещен.")
        return

    target_user = await get_user(db, target_id)
    if not target_user:
        await callback.message.edit_text("❌ Пользователь не найден.")
        return

    role_model = db.query(RoleModel).filter(RoleModel.name == role_str).first()
    if not role_model:
        await callback.message.edit_text("❌ Роль не найдена.")
        return

    user_roles = get_user_roles(target_user)
    if role_str in user_roles:
        await callback.message.edit_text("❌ У пользователя уже есть эта роль.")
        return

    target_user.roles.append(role_model)

    request = db.query(RoleRequest).filter(
        RoleRequest.user_id == target_user.id,
        RoleRequest.requested_role == role_str,
        RoleRequest.status == "ожидает"
    ).first()

    if request:
        request.status = "approved"
        request.processed_at = get_timezone_aware_now()
        request.processed_by = admin.telegram_id

    db.commit()

    role_names = {
        "OPERATOR": "Оператор",
        "DRIVER_TRANSFER": "Водитель перегона",
        "ADMIN": "Администратор",
        "DEB_EMPLOYEE": "Сотрудник ДЭБ"
    }
    role_name = role_names.get(role_str, role_str)

    try:
        await bot.send_message(
            target_id,
            f"✅ Ваша заявка одобрена!\n\n"
            f"📋 Информация:\n"
            f"👤 Администратор: {admin.first_name} {admin.last_name}\n"
            f"📝 Выдана роль: {role_name}\n"
            f"⏰ Время: {get_timezone_aware_now().strftime('%d.%m.%Y %H:%M')}\n\n"
            f"Используйте /start для обновления меню."
        )
    except Exception as e:
        logger.error(f"Ошибка уведомления пользователя: {e}")

    await callback.message.edit_text(
        f"✅ Роль '{role_name}' выдана пользователю "
        f"{target_user.first_name} {target_user.last_name} (ID: {target_id})"
    )


@router.callback_query(F.data.startswith("reject_"))
@with_db
async def process_reject_request(callback: CallbackQuery, db: Session):
    """Отклонение запроса на роль"""
    callback_data = callback.data.replace("reject_", "")
    last_underscore = callback_data.rfind('_')

    if last_underscore == -1:
        await callback.message.edit_text("❌ Неверный формат данных.")
        return

    role_str = callback_data[:last_underscore]
    target_id = int(callback_data[last_underscore + 1:])

    admin = await get_user(db, callback.from_user.id)
    if not admin or "ADMIN" not in get_user_roles(admin):
        await callback.message.edit_text("❌ Доступ запрещен.")
        return

    target_user = await get_user(db, target_id)
    if not target_user:
        await callback.message.edit_text("❌ Пользователь не найден.")
        return

    request = db.query(RoleRequest).filter(
        RoleRequest.user_id == target_user.id,
        RoleRequest.requested_role == role_str,
        RoleRequest.status == "ожидает"
    ).first()

    if request:
        request.status = "rejected"
        request.processed_at = get_timezone_aware_now()
        request.processed_by = admin.telegram_id
        db.commit()

    role_names = {
        "OPERATOR": "Оператор",
        "DRIVER_TRANSFER": "Водитель перегона",
        "ADMIN": "Администратор",
        "DEB_EMPLOYEE": "Сотрудник ДЭБ"
    }
    role_name = role_names.get(role_str, role_str)

    try:
        await bot.send_message(
            target_id,
            f"❌ Ваша заявка отклонена.\n\n"
            f"📋 Информация:\n"
            f"👤 Администратор: {admin.first_name} {admin.last_name}\n"
            f"📝 Запрошенная роль: {role_name}\n"
            f"⏰ Время: {get_timezone_aware_now().strftime('%d.%m.%Y %H:%M')}\n\n"
            f"Вы можете повторно запросить роль через меню бота."
        )
    except Exception as e:
        logger.error(f"Ошибка уведомления пользователя: {e}")

    await callback.message.edit_text(
        f"❌ Запрос на роль '{role_name}' отклонен для пользователя "
        f"{target_user.first_name} {target_user.last_name} (ID: {target_id})"
    )


@router.message(F.text == f"{Emoji.SETTINGS} Забрать роли")
@with_db
async def process_revoke_roles(message: Message, db: Session):
    """Управление отзывом ролей"""
    user = await get_user(db, message.from_user.id)
    if not user or "ADMIN" not in get_user_roles(user):
        await message.answer("❌ Доступ запрещен.")
        return

    users_with_roles = db.query(User).filter(User.roles.any()).all()
    if not users_with_roles:
        await message.answer(f"{Emoji.INFO} В системе нет пользователей с ролями.")
        return

    builder = InlineKeyboardBuilder()
    for user_obj in users_with_roles:
        if user_obj.telegram_id == user.telegram_id:
            continue

        full_name = f"{user_obj.first_name} {user_obj.last_name}".strip()
        if not full_name:
            full_name = f"Пользователь {user_obj.telegram_id}"

        builder.button(
            text=f"{full_name} (ID: {user_obj.telegram_id})",
            callback_data=f"show_user_roles_{user_obj.telegram_id}"
        )

    builder.button(text=f"{Emoji.BACK} Назад", callback_data="menu_main")
    builder.adjust(1)

    await message.answer(
        "👥 Выберите пользователя для управления ролями:",
        reply_markup=builder.as_markup()
    )


@router.callback_query(F.data.startswith("show_user_roles_"))
@with_db
async def process_show_user_roles(callback: CallbackQuery, db: Session):
    """Показать роли пользователя для отзыва"""
    target_id = int(callback.data.replace("show_user_roles_", ""))
    admin = await get_user(db, callback.from_user.id)

    if not admin or "ADMIN" not in get_user_roles(admin):
        await callback.message.edit_text("❌ Доступ запрещен.")
        return

    target_user = await get_user(db, target_id)
    if not target_user:
        await callback.message.edit_text("❌ Пользователь не найден.")
        return

    user_roles = get_user_roles(target_user)
    if not user_roles:
        await callback.message.edit_text("❌ У пользователя нет ролей.")
        return

    role_names = {
        "DRIVER": "Водитель",
        "DRIVER_TRANSFER": "Водитель перегона",
        "OPERATOR": "Оператор",
        "ADMIN": "Администратор",
        "DEB_EMPLOYEE": "Сотрудник ДЭБ"
    }

    response = (
        f"👤 Пользователь: {target_user.first_name} {target_user.last_name}\n"
        f"🆔 ID: {target_user.telegram_id}\n"
        f"👤 @{target_user.username or 'нет'}\n\n"
        f"📋 Текущие роли:\n"
    )

    builder = InlineKeyboardBuilder()
    for role_key in user_roles:
        role_name = role_names.get(role_key, role_key)
        response += f"• {role_name}\n"

        if role_key != "DRIVER" and not (role_key == "ADMIN" and target_id == admin.telegram_id):
            builder.button(
                text=f"❌ Отозвать {role_name}",
                callback_data=f"revoke_role_{role_key}_{target_id}"
            )

    if not builder.buttons:
        response += "\n⚠️ Нет ролей для отзыва"
        builder.button(text=f"{Emoji.BACK} Назад", callback_data="back_to_user_list")
    else:
        builder.button(text=f"{Emoji.BACK} Назад", callback_data="back_to_user_list")
        builder.adjust(1)

    await callback.message.edit_text(response, reply_markup=builder.as_markup())


@router.callback_query(F.data.startswith("revoke_role_"))
@with_db
async def process_revoke_role(callback: CallbackQuery, db: Session):
    """Отзыв роли у пользователя"""
    callback_data = callback.data.replace("revoke_role_", "")
    last_underscore = callback_data.rfind('_')

    if last_underscore == -1:
        await callback.message.edit_text("❌ Неверный формат данных.")
        return

    role_str = callback_data[:last_underscore]
    target_id = int(callback_data[last_underscore + 1:])

    admin = await get_user(db, callback.from_user.id)
    if not admin or "ADMIN" not in get_user_roles(admin):
        await callback.message.edit_text("❌ Доступ запрещен.")
        return

    if target_id == admin.telegram_id and role_str == "ADMIN":
        await callback.message.edit_text("❌ Нельзя забрать роль администратора у самого себя.")
        return

    target_user = await get_user(db, target_id)
    if not target_user:
        await callback.message.edit_text("❌ Пользователь не найден.")
        return

    role_model = db.query(RoleModel).filter(RoleModel.name == role_str).first()
    if not role_model:
        await callback.message.edit_text("❌ Роль не найдена.")
        return

    user_roles = get_user_roles(target_user)
    if role_str not in user_roles:
        await callback.message.edit_text("❌ У пользователя нет этой роли.")
        return

    target_user.roles.remove(role_model)

    if role_str == "DRIVER_TRANSFER":
        target_user.is_on_shift = False

    db.commit()

    role_names = {
        "OPERATOR": "Оператор",
        "DRIVER_TRANSFER": "Водитель перегона",
        "ADMIN": "Администратор",
        "DEB_EMPLOYEE": "Сотрудник ДЭБ",
        "DRIVER": "Водитель"
    }
    role_name = role_names.get(role_str, role_str)

    try:
        await bot.send_message(
            target_id,
            f"⚠️ У вас была отозвана роль.\n\n"
            f"📋 Информация:\n"
            f"👤 Администратор: {admin.first_name} {admin.last_name}\n"
            f"📝 Отозвана роль: {role_name}\n"
            f"⏰ Время: {get_timezone_aware_now().strftime('%d.%m.%Y %H:%M')}\n\n"
            f"Используйте /start для обновления меню."
        )
    except Exception as e:
        logger.error(f"Ошибка уведомления пользователя: {e}")

    await callback.message.edit_text(
        f"✅ Роль '{role_name}' отозвана у пользователя "
        f"{target_user.first_name} {target_user.last_name} (ID: {target_id})"
    )


@router.callback_query(F.data == "back_to_role_list")
@with_db
async def process_back_to_role_list(callback: CallbackQuery, db: Session):
    """Возврат к списку ролей"""
    user = await get_user(db, callback.from_user.id)
    if not user or "ADMIN" not in get_user_roles(user):
        await callback.message.edit_text("❌ Доступ запрещен.")
        return

    requests = db.query(RoleRequest).filter(RoleRequest.status == "ожидает").all()
    if not requests:
        await callback.message.edit_text(f"{Emoji.INFO} Нет запросов на выдачу ролей.")
        return

    requests_by_role = {}
    for req in requests:
        requests_by_role.setdefault(req.requested_role, []).append(req)

    builder = InlineKeyboardBuilder()
    role_names = {
        "OPERATOR": f"{Emoji.OPERATOR} Оператор",
        "DRIVER_TRANSFER": f"{Emoji.DRIVER_TRANSFER} Водитель перегона",
        "ADMIN": f"{Emoji.ADMIN} Администратор",
        "DEB_EMPLOYEE": f"{Emoji.DEB} Сотрудник ДЭБ"
    }

    for role, role_requests in requests_by_role.items():
        if role in role_names:
            builder.button(
                text=f"{role_names[role]} ({len(role_requests)})",
                callback_data=f"show_requests_{role}"
            )

    builder.button(text=f"{Emoji.BACK} Назад", callback_data="menu_main")
    builder.adjust(1)

    await callback.message.edit_text(
        f"📋 Запросы на выдачу ролей:\n"
        f"Всего запросов: {len(requests)}\n\n"
        f"Выберите роль для просмотра:",
        reply_markup=builder.as_markup()
    )


@router.callback_query(F.data == "back_to_user_list")
@with_db
async def process_back_to_user_list(callback: CallbackQuery, db: Session):
    """Возврат к списку пользователей"""
    user = await get_user(db, callback.from_user.id)
    if not user or "ADMIN" not in get_user_roles(user):
        await callback.message.edit_text("❌ Доступ запрещен.")
        return

    users_with_roles = db.query(User).filter(User.roles.any()).all()
    if not users_with_roles:
        await callback.message.edit_text(f"{Emoji.INFO} В системе нет пользователей с ролями.")
        return

    builder = InlineKeyboardBuilder()
    for user_obj in users_with_roles:
        user_obj_roles = get_user_roles(user_obj)
        if "ADMIN" in user_obj_roles and user_obj.telegram_id != user.telegram_id:
            continue

        full_name = f"{user_obj.first_name} {user_obj.last_name}".strip()
        if not full_name:
            full_name = f"Пользователь {user_obj.telegram_id}"

        builder.button(
            text=f"{full_name} (ID: {user_obj.telegram_id})",
            callback_data=f"show_user_roles_{user_obj.telegram_id}"
        )

    builder.button(text=f"{Emoji.BACK} Назад", callback_data="menu_main")
    builder.adjust(1)

    await callback.message.edit_text(
        "👥 Выберите пользователя для управления ролями:",
        reply_markup=builder.as_markup()
    )


# ==================== ОБРАБОТЧИКИ ДЛЯ СОТРУДНИКОВ ДЭБ ====================
@router.message(F.text == f"{Emoji.DEPARTURE} Зарегистрировать убытие")
@with_db
async def process_register_departure_deb(message: Message, state: FSMContext, db: Session):
    """Регистрация убытия ТС сотрудником ДЭБ"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer("❌ Сначала используйте /start для регистрации.")
        return

    if "DEB_EMPLOYEE" not in get_user_roles(user):
        await message.answer("❌ Доступ запрещен.")
        return

    await state.set_state(DEBStates.waiting_for_departure_registration)
    await message.answer(
        "🚗 Введите номер ТС для регистрации убытия:\n\n"
        f"{VehicleNumberValidator.get_valid_letters_info()}\n\n"
        f"{VehicleNumberValidator.get_examples()}",
        reply_markup=get_cancel_keyboard()
    )


@router.message(DEBStates.waiting_for_departure_registration)
@with_db
async def process_deb_departure_input(message: Message, state: FSMContext, db: Session):
    """Обработка ввода номера ТС для убытия"""
    if message.text == f"{Emoji.CANCEL} Отмена":
        await state.clear()
        user = await get_user(db, message.from_user.id)
        await message.answer("❌ Отменено", reply_markup=get_main_menu_keyboard(user))
        return

    vehicle_number = message.text.upper().strip()
    is_valid, error = await validate_vehicle_number_with_explanation(vehicle_number)

    if not is_valid:
        await message.answer(f"{error}\n\nПожалуйста, введите правильный номер ТС:")
        return

    normalized = await normalize_vehicle_number(vehicle_number)

    parking = db.query(Parking).filter(
        Parking.vehicle_number.in_([normalized, vehicle_number]),
        Parking.departure_time == None
    ).first()

    if not parking:
        await message.answer(
            f"❌ ТС с номером {vehicle_number} не найдено на парковке.\n\n"
            "Возможные причины:\n"
            "• ТС уже убыло\n"
            "• Неправильно введен номер\n"
            "• ТС не зарегистрировано\n\n"
            "Попробуйте еще раз:"
        )
        return

    parking.departure_time = get_timezone_aware_now()
    duration = parking.departure_time - ensure_timezone_aware(parking.arrival_time)

    db.commit()

    driver_name = f"{parking.user.first_name} {parking.user.last_name}".strip() or "Водитель"

    await message.answer(
        f"✅ Убытие зарегистрировано!\n\n"
        f"📋 Информация:\n"
        f"📍 Место #{parking.spot_number}\n"
        f"🚗 ТС: {parking.vehicle_number}\n"
        f"👤 Водитель: {driver_name}\n"
        f"⏰ Прибытие: {parking.arrival_time.strftime('%H:%M %d.%m.%Y')}\n"
        f"⏰ Убытие: {parking.departure_time.strftime('%H:%M %d.%m.%Y')}\n"
        f"⏱️ Время стоянки: {format_duration(int(duration.total_seconds()))}",
        reply_markup=get_main_menu_keyboard(user)
    )

    try:
        await bot.send_message(
            parking.user.telegram_id,
            f"📢 Уведомление от ДЭБ:\n\n"
            f"✅ Ваше ТС {parking.vehicle_number} зарегистрировано как убывшее.\n"
            f"📍 Место #{parking.spot_number} освобождено.\n"
            f"⏰ Время убытия: {parking.departure_time.strftime('%H:%M %d.%m.%Y')}\n"
            f"⏱️ Время стоянки: {format_duration(int(duration.total_seconds()))}"
        )
    except Exception as e:
        logger.error(f"Ошибка уведомления водителя: {e}")

    await state.clear()


@router.message(F.text == f"{Emoji.REPORT} Отчет по парковке")
@with_db
async def process_parking_report_deb(message: Message, db: Session):
    """Отчет по парковке для ДЭБ"""
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.answer("❌ Сначала используйте /start для регистрации.")
        return

    if "DEB_EMPLOYEE" not in get_user_roles(user):
        await message.answer("❌ Доступ запрещен.")
        return

    now = get_timezone_aware_now()
    total_spots = config.PARKING_SPOTS
    active_parkings = db.query(Parking).filter(Parking.departure_time == None).all()
    occupied_spots = len(active_parkings)

    time_stats = {"1h": 0, "2h": 0, "3h": 0, "6h": 0, "12h": 0, "24h": 0}
    long_parking = []

    for parking in active_parkings:
        hours = (now - ensure_timezone_aware(parking.arrival_time)).total_seconds() / 3600
        driver_name = f"{parking.user.first_name} {parking.user.last_name}".strip() or "Водитель"

        if hours > 24:
            time_stats["24h"] += 1
            long_parking.append({
                'spot': parking.spot_number,
                'vehicle': parking.vehicle_number,
                'driver': driver_name,
                'hours': int(hours),
                'arrival': parking.arrival_time.strftime('%H:%M %d.%m')
            })
        elif hours > 12:
            time_stats["12h"] += 1
        elif hours > 6:
            time_stats["6h"] += 1
        elif hours > 3:
            time_stats["3h"] += 1
        elif hours > 2:
            time_stats["2h"] += 1
        elif hours > 1:
            time_stats["1h"] += 1

    response = (
        f"📊 Отчет по парковке (ДЭБ)\n\n"
        f"📅 Дата: {now.strftime('%d.%m.%Y')}\n"
        f"⏰ Время: {now.strftime('%H:%M')}\n\n"
        f"🅿️ Общая статистика:\n"
        f"• Всего мест: {total_spots}\n"
        f"• Занято: {occupied_spots}\n"
        f"• Свободно: {total_spots - occupied_spots}\n\n"
        f"⏰ Время стоянки:\n"
        f"• До 1 часа: {time_stats['1h']}\n"
        f"• 1-2 часа: {time_stats['2h']}\n"
        f"• 2-3 часа: {time_stats['3h']}\n"
        f"• 3-6 часов: {time_stats['6h']}\n"
        f"• 6-12 часов: {time_stats['12h']}\n"
        f"• Более 12 часов: {time_stats['24h']}\n"
    )

    if long_parking:
        response += f"\n⚠️ Длительная стоянка (более 24 часов):\n"
        for p in long_parking[:5]:
            response += (
                f"• Место #{p['spot']}: {p['vehicle']}\n"
                f"  👤 {p['driver']}\n"
                f"  ⏰ {p['hours']} часов (с {p['arrival']})\n"
            )
        if len(long_parking) > 5:
            response += f"• ... и еще {len(long_parking) - 5} ТС\n"

    await message.answer(response)



# ==================== ОБЩИЕ ОБРАБОТЧИКИ ====================
@router.message(F.text == f"{Emoji.CANCEL} Отмена")
@with_db
async def process_cancel(message: Message, state: FSMContext, db: Session):
    """Отмена текущего действия"""
    user = await get_user(db, message.from_user.id) or await get_or_create_user(db, message)
    current_state = await state.get_state()

    if current_state:
        await state.clear()
        await message.answer("❌ Действие отменено.", reply_markup=get_main_menu_keyboard(user))
    else:
        await message.answer("Главное меню:", reply_markup=get_main_menu_keyboard(user))


@router.callback_query(F.data == "menu_main")
@with_db
async def process_menu_main(callback: CallbackQuery, db: Session):
    """Возврат в главное меню"""
    user = await get_user(db, callback.from_user.id)
    await callback.message.edit_text("Возврат в главное меню...")
    await callback.message.answer(
        "Главное меню:",
        reply_markup=get_main_menu_keyboard(user)
    )


@router.message(F.text == f"{Emoji.BACK} Вернуться в меню")
@router.message(F.text == f"{Emoji.UPDATE} Обновить меню")
@with_db
async def process_refresh_menu(message: Message, db: Session):
    """Обновление главного меню"""
    user = await get_user(db, message.from_user.id)
    await message.answer(
        "🔄 Меню обновлено!",
        reply_markup=get_main_menu_keyboard(user)
    )


@router.message(Command("help"))
async def cmd_help(message: Message):
    """Справка по командам"""
    help_text = (
        "📋 Справка по боту управления парковкой\n\n"
        "👤 Основные функции:\n"
        "• /start - Регистрация и главное меню\n"
        "• /help - Эта справка\n\n"
        "🚗 Для водителей:\n"
        "• Прибытие - Зафиксировать прибытие ТС\n"
        "• Убытие - Зафиксировать убытие ТС\n"
        "• Встать на ворота - Выполнить задание\n"
        "• Мои задачи - Просмотр задач для вашего ТС\n"
        "• Сменить роль - Переключиться между ролями\n\n"
        "🦑 Для водителей перегона:\n"
        "• Начать/закончить смену - Управление сменой\n"
        "• Взять задачу - Взять задачу из пула\n"
        "• Текущая задача - Информация о текущей задаче\n"
        "• Завершить задачу - Завершение текущей задачи\n"
        "• Уйти/вернуться с обеда - Управление обедом\n"
        "• Статистика за смену - Просмотр статистики\n\n"
        "🎛️ Для операторов:\n"
        "• Дать задание - Создать новое задание\n"
        "• Пул задач - Просмотр пула задач\n"
        "• Очистить пул - Очистка пула задач\n"
        "• Статус заданий - Активные задания\n"
        "• Занятые ворота - Просмотр занятых ворот\n"
        "• Статус парковки - Информация о парковке\n"
        "• Статус очереди - Информация об очереди\n"
        "• Отчет - Получить отчеты\n"
        "• Зависшие задачи - Управление зависшими задачами\n\n"
        "👑 Для администраторов:\n"
        "• Выдать роли - Управление запросами на роли\n"
        "• Забрать роли - Управление ролями пользователей\n"
        "• Переключить роль - Сменить текущую роль\n"
        "• Обновить меню - Обновить кнопки меню\n\n"
        "👮 Для сотрудников ДЭБ:\n"
        "• Зарегистрировать убытие - Фиксация убытия ТС\n"
        "• Отчет по парковке - Статистика парковки\n\n"
        "📝 Для всех:\n"
        "• Сменить роль - Переключиться между ролями\n"
        "• Запросить роль - Запросить новую роль\n"
        "• Обновить меню - Обновить кнопки меню\n"
        "• Вернуться в меню - Вернуться в главное меню"
    )
    await message.answer(help_text)


# ==================== ФОНОВАЯ ЗАДАЧА ПРОВЕРКИ ЗАДАЧ ====================
async def check_and_notify_unassigned_tasks():
    """
    Фоновая задача проверки задач
    - Каждую минуту проверяет задачи в пуле > 15 мин
    - Каждую минуту проверяет зависшие задачи > 30 мин
    - Отправляет уведомления и повышает приоритеты
    """
    while True:
        try:
            await asyncio.sleep(60)
            logger.info("🔄 Фоновая проверка задач...")

            db = SessionLocal()
            try:
                now = get_timezone_aware_now()

                # 1. Проверка долго ожидающих задач в пуле
                threshold_time = now - timedelta(minutes=15)
                pool_tasks = db.query(Task).join(Parking).filter(
                    Task.status == "PENDING",
                    Task.is_in_pool == True,
                    Task.created_at <= threshold_time,
                    Parking.departure_time == None
                ).all()

                for task in pool_tasks:
                    task_created = ensure_timezone_aware(task.created_at)
                    minutes_ago = int((now - task_created).total_seconds() / 60)
                    task.priority += 1

                    # Уведомление оператора
                    if task.operator_id:
                        try:
                            await bot.send_message(
                                task.operator_id,
                                f"⚠️ Задача #{task.id} в пуле уже {minutes_ago} минут!\n\n"
                                f"🚗 ТС: {task.parking.vehicle_number}\n"
                                f"📍 Место: #{task.parking.spot_number}\n"
                                f"🚪 Ворота: #{task.gate_number}\n"
                                f"📊 Приоритет: {task.priority}\n\n"
                                f"❗️ Ни один водитель не взял задачу."
                            )
                        except Exception as e:
                            logger.error(f"Ошибка уведомления оператора: {e}")

                    # Уведомление активных водителей
                    active_drivers = await get_active_transfer_drivers(db)
                    for driver in active_drivers:
                        try:
                            await bot.send_message(
                                driver.telegram_id,
                                f"⚠️ СРОЧНАЯ ЗАДАЧА! (ожидает {minutes_ago} мин, приоритет {task.priority})\n\n"
                                f"🆔 Задача: #{task.id}\n"
                                f"📍 Место: #{task.parking.spot_number}\n"
                                f"🚗 ТС: {task.parking.vehicle_number}\n"
                                f"🚪 Ворота: #{task.gate_number}\n\n"
                                f'Используйте кнопку "{Emoji.TASK} Взять задачу".'
                            )
                        except Exception as e:
                            logger.error(f"Ошибка уведомления водителя: {e}")

                    await asyncio.sleep(0.5)

                # 2. Проверка зависших задач (в работе > 30 мин)
                stuck_threshold = now - timedelta(minutes=30)
                stuck_tasks = db.query(Task).filter(
                    Task.status == "IN_PROGRESS",
                    Task.started_at <= stuck_threshold
                ).all()

                for task in stuck_tasks:
                    task_started = ensure_timezone_aware(task.started_at)
                    minutes_ago = int((now - task_started).total_seconds() / 60)

                    task.status = "STUCK"
                    task.is_stuck = True
                    task.stuck_reason = f"Автоматический таймаут ({minutes_ago} мин)"

                    if task.parking and task.parking.is_hitch:
                        driver_id = task.driver_id
                        task.driver_id = None
                        task.is_in_pool = True
                        task.priority += 10

                        if driver_id:
                            try:
                                await bot.send_message(
                                    driver_id,
                                    f"⚠️ Задача #{task.id} автоматически снята с вас!\n\n"
                                    f"Причина: превышено время выполнения ({minutes_ago} мин)\n"
                                    f"📍 Место: #{task.parking.spot_number}\n"
                                    f"🚗 ТС: {task.parking.vehicle_number}\n"
                                    f"🚪 Ворота: #{task.gate_number}\n\n"
                                    f"✅ Задача возвращена в пул."
                                )
                            except Exception as e:
                                logger.error(f"Ошибка уведомления водителя: {e}")

                    if task.operator_id:
                        try:
                            await bot.send_message(
                                task.operator_id,
                                f"⚠️ Задача #{task.id} автоматически помечена как зависшая!\n\n"
                                f"Причина: превышено время выполнения ({minutes_ago} мин)\n"
                                f"🚗 ТС: {task.parking.vehicle_number}\n"
                                f"📍 Место: #{task.parking.spot_number}\n"
                                f"🚪 Ворота: #{task.gate_number}\n"
                                f"{'✅ Задача возвращена в пул' if task.parking and task.parking.is_hitch else '❌ Задача закрыта'}\n\n"
                                f"Требуется вмешательство оператора."
                            )
                        except Exception as e:
                            logger.error(f"Ошибка уведомления оператора: {e}")

                    await asyncio.sleep(0.5)

                db.commit()

            except Exception as e:
                logger.error(f"Ошибка в проверке задач: {e}", exc_info=True)
            finally:
                db.close()

        except Exception as e:
            logger.error(f"Критическая ошибка в фоновой задаче: {e}", exc_info=True)
            await asyncio.sleep(30)


# ==================== ФУНКЦИИ ЗАПУСКА ====================
async def on_startup():
    """Действия при запуске бота"""
    print("✅ Бот запущен!")
    init_db()

    if config.ADMIN_IDS:
        db = SessionLocal()
        try:
            for admin_id in config.ADMIN_IDS:
                admin = await get_user(db, admin_id)
                if not admin:
                    admin_user = User(
                        telegram_id=admin_id,
                        first_name="Администратор",
                        last_name="Системы",
                        current_role="ADMIN",
                        created_at=get_timezone_aware_now(),
                        is_on_shift=False,
                        is_on_break=False,
                        total_break_time=0
                    )
                    db.add(admin_user)
                    db.flush()

                    all_roles = db.query(RoleModel).all()
                    for role in all_roles:
                        if role not in admin_user.roles:
                            admin_user.roles.append(role)

                    admin_user.current_role = "ADMIN"
                    print(f"✅ Создан администратор {admin_id}")
                else:
                    admin_roles = get_user_roles(admin)
                    all_roles = db.query(RoleModel).all()

                    for role in all_roles:
                        if role.name not in admin_roles:
                            admin.roles.append(role)
                            print(f"✅ Добавлена роль {role.name} администратору {admin_id}")

                    if not admin.current_role:
                        admin.current_role = "ADMIN"

            db.commit()
            print("✅ Администраторы настроены")

        except Exception as e:
            print(f"❌ Ошибка при создании администраторов: {e}")
            db.rollback()
        finally:
            db.close()


async def main():
    """Основная функция запуска"""
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        print("✅ Вебхук удален")
    except Exception as e:
        logger.error(f"Ошибка удаления вебхука: {e}")

    await on_startup()
    print("🚀 Бот начал работу...")

    asyncio.create_task(check_and_notify_unassigned_tasks())
    print("✅ Фоновая задача запущена")

    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())
