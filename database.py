from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from config import config
from models import Base, Role


# Создаем движок для SQLite
engine = create_engine(
    config.DATABASE_URL,
    connect_args={"check_same_thread": False},
    echo=False
)

# Создаем фабрику сессий
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """Инициализация базы данных и создание ролей"""
    try:
        # Создаем все таблицы
        Base.metadata.create_all(bind=engine)
        print("✅ Таблицы созданы/проверены")

        db = SessionLocal()
        try:
            roles_to_create = [
                ("DRIVER", "Водитель ТС (базовая роль)"),
                ("DRIVER_TRANSFER", "Водитель перегона"),
                ("OPERATOR", "Оператор"),
                ("ADMIN", "Администратор"),
                ("DEB_EMPLOYEE", "Сотрудник ДЭБ"),
            ]

            for role_name, description in roles_to_create:
                existing_role = db.query(Role).filter(Role.name == role_name).first()
                if not existing_role:
                    db.add(Role(name=role_name, description=description))
                    print(f"✅ Добавлена роль: {role_name}")

            db.commit()
            print("✅ Роли созданы/проверены")

        except Exception as e:
            print(f"❌ Ошибка при создании ролей: {e}")
            db.rollback()
        finally:
            db.close()
    except Exception as e:
        print(f"❌ Ошибка при создании таблиц: {e}")


@contextmanager
def get_db_context():
    """Контекстный менеджер для работы с БД"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
