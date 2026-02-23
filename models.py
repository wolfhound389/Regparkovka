# models.py
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, Text, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import enum
from datetime import datetime
import pytz

Base = declarative_base()
moscow_tz = pytz.timezone('Europe/Moscow')

# Таблица связи многие-ко-многим для пользователей и ролей
user_roles = Table('user_roles', Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id'), primary_key=True),
    Column('role_id', Integer, ForeignKey('roles.id'), primary_key=True)
)

class RoleEnum(str, enum.Enum):
    """Типы ролей в системе"""
    DRIVER = "DRIVER"
    DRIVER_TRANSFER = "DRIVER_TRANSFER"
    OPERATOR = "OPERATOR"
    ADMIN = "ADMIN"
    DEB_EMPLOYEE = "DEB_EMPLOYEE"

class VehicleType(str, enum.Enum):
    """Типы транспортных средств"""
    NON_HITCH = "NON_HITCH"
    HITCH = "HITCH"

class TaskStatus(str, enum.Enum):
    """Статусы задач"""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    STUCK = "STUCK"
    CANCELLED = "CANCELLED"

class Role(Base):
    """Модель роли в системе"""
    __tablename__ = 'roles'

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    description = Column(String(200))
    created_at = Column(DateTime, default=lambda: datetime.now(moscow_tz))

    users = relationship("User", secondary=user_roles, back_populates="roles")

class Break(Base):
    """Модель перерыва (обеда)"""
    __tablename__ = 'breaks'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    break_type = Column(String(50), default="LUNCH")
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=True)
    duration = Column(Integer, default=0)
    created_at = Column(DateTime, default=lambda: datetime.now(moscow_tz))

    # Связь будет установлена после определения User
    user = relationship("User", back_populates="breaks")

class User(Base):
    """Модель пользователя"""
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    telegram_id = Column(Integer, unique=True, nullable=False)
    username = Column(String(100))
    first_name = Column(String(100))
    last_name = Column(String(100))
    position = Column(String(200), nullable=True)
    is_on_shift = Column(Boolean, default=False)
    current_role = Column(String(50), nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(moscow_tz))

    # Поля для обеда
    is_on_break = Column(Boolean, default=False)
    break_start_time = Column(DateTime, nullable=True)
    total_break_time = Column(Integer, default=0)

    # Связи
    roles = relationship("Role", secondary=user_roles, back_populates="users")
    parkings = relationship("Parking", back_populates="user")
    tasks_assigned = relationship("Task", foreign_keys="Task.driver_id", back_populates="driver")
    tasks_operator = relationship("Task", foreign_keys="Task.operator_id", back_populates="operator")
    role_requests = relationship("RoleRequest", back_populates="user")
    breaks = relationship("Break", back_populates="user", cascade="all, delete-orphan")
    parking_queue = relationship("ParkingQueue", back_populates="user", cascade="all, delete-orphan")  # Новая связь

    def has_role(self, role_name: str) -> bool:
        """Проверяет, есть ли у пользователя указанная роль"""
        return any(role.name == role_name for role in self.roles)

class RoleRequest(Base):
    """Модель запроса на получение роли"""
    __tablename__ = 'role_requests'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    requested_role = Column(String(50), nullable=False)
    first_name = Column(String(100))
    last_name = Column(String(100))
    position = Column(String(200))
    status = Column(String(50), default="pending")
    created_at = Column(DateTime, default=lambda: datetime.now(moscow_tz))
    processed_at = Column(DateTime, nullable=True)
    processed_by = Column(Integer, nullable=True)

    user = relationship("User", back_populates="role_requests")

class Parking(Base):
    """Модель парковки транспортного средства"""
    __tablename__ = 'parkings'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    vehicle_number = Column(String(20), nullable=False)
    vehicle_type = Column(String(20), nullable=False)
    spot_number = Column(Integer, nullable=False)
    arrival_time = Column(DateTime, default=lambda: datetime.now(moscow_tz))
    departure_time = Column(DateTime, nullable=True)
    is_hitch = Column(Boolean, default=False)
    gate_number = Column(Integer, nullable=True)

    user = relationship("User", back_populates="parkings")
    tasks = relationship("Task", back_populates="parking")

class Task(Base):
    """Модель задачи"""
    __tablename__ = 'tasks'

    id = Column(Integer, primary_key=True)
    parking_id = Column(Integer, ForeignKey('parkings.id'), nullable=False)
    driver_id = Column(Integer, ForeignKey('users.id'), nullable=True)
    operator_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    gate_number = Column(Integer, nullable=False)
    status = Column(String(50), default="PENDING")
    priority = Column(Integer, default=0)
    created_at = Column(DateTime, default=lambda: datetime.now(moscow_tz))
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    is_stuck = Column(Boolean, default=False)
    stuck_reason = Column(String(200), nullable=True)

    # Поля для пула задач
    assigned_driver_id = Column(Integer, ForeignKey('users.id'), nullable=True)
    is_in_pool = Column(Boolean, default=True)

    parking = relationship("Parking", back_populates="tasks")
    driver = relationship("User", foreign_keys=[driver_id], back_populates="tasks_assigned")
    assigned_driver = relationship("User", foreign_keys=[assigned_driver_id])
    operator = relationship("User", foreign_keys=[operator_id], back_populates="tasks_operator")

class ParkingQueue(Base):
    """Модель очереди на парковку"""
    __tablename__ = 'parking_queue'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    vehicle_number = Column(String(20), nullable=False)
    is_hitch = Column(Boolean, default=False)
    vehicle_type = Column(String(20), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(moscow_tz))
    notified_at = Column(DateTime, nullable=True)
    status = Column(String(50), default="waiting")  # waiting, notified, assigned, cancelled
    spot_number = Column(Integer, nullable=True)

    user = relationship("User", back_populates="parking_queue")
