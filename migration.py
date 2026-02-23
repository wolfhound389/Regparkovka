import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from config import config

def run_full_migration():
    """
    Полная миграция базы данных
    Объединяет все миграции в одну для удобства
    """
    print("🔄 Запуск полной миграции базы данных...")

    engine = create_engine(
        config.DATABASE_URL,
        connect_args={"check_same_thread": False},
        echo=True
    )

    # Используем connection для SQLite операций
    with engine.connect() as conn:
        try:
            # ============ 1. МИГРАЦИЯ ПОЛЕЙ ПОЛЬЗОВАТЕЛЯ ============
            print("\n📋 Проверка таблицы users...")
            result = conn.execute(text("PRAGMA table_info(users)"))
            columns = [row[1] for row in result]

            # Поле current_role
            if 'current_role' not in columns:
                print("➕ Добавляем поле current_role...")
                conn.execute(text("ALTER TABLE users ADD COLUMN current_role VARCHAR(50)"))

            # Поля для обеда
            if 'is_on_break' not in columns:
                print("➕ Добавляем поле is_on_break...")
                conn.execute(text("ALTER TABLE users ADD COLUMN is_on_break BOOLEAN DEFAULT 0"))

            if 'break_start_time' not in columns:
                print("➕ Добавляем поле break_start_time...")
                conn.execute(text("ALTER TABLE users ADD COLUMN break_start_time TIMESTAMP"))

            if 'total_break_time' not in columns:
                print("➕ Добавляем поле total_break_time...")
                conn.execute(text("ALTER TABLE users ADD COLUMN total_break_time INTEGER DEFAULT 0"))

            # ============ 2. СОЗДАНИЕ ТАБЛИЦЫ BREAKS ============
            print("\n📋 Создание таблицы breaks...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS breaks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    break_type VARCHAR(50) DEFAULT 'LUNCH',
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    duration INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                )
            """))

            # Создаем индекс для быстрого поиска
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_breaks_user_id ON breaks(user_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_breaks_start_time ON breaks(start_time)
            """))

            # ============ 3. МИГРАЦИЯ ПОЛЕЙ ЗАДАЧ ============
            print("\n📋 Проверка таблицы tasks...")
            result = conn.execute(text("PRAGMA table_info(tasks)"))
            columns = [row[1] for row in result]

            if 'assigned_driver_id' not in columns:
                print("➕ Добавляем поле assigned_driver_id...")
                conn.execute(text("ALTER TABLE tasks ADD COLUMN assigned_driver_id INTEGER REFERENCES users(id)"))

            if 'is_in_pool' not in columns:
                print("➕ Добавляем поле is_in_pool...")
                conn.execute(text("ALTER TABLE tasks ADD COLUMN is_in_pool BOOLEAN DEFAULT 1"))

            # Добавляем индекс для пула задач
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_tasks_pool ON tasks(status, is_in_pool, priority, created_at)
            """))

            # ============ 4. ОБНОВЛЕНИЕ СУЩЕСТВУЮЩИХ ЗАПИСЕЙ ============
            print("\n📋 Обновление существующих записей...")

            # Устанавливаем is_in_pool = TRUE для всех перецепных задач
            conn.execute(text("""
                UPDATE tasks
                SET is_in_pool = 1
                WHERE id IN (
                    SELECT t.id FROM tasks t
                    JOIN parkings p ON t.parking_id = p.id
                    WHERE p.is_hitch = 1 AND t.status = 'PENDING'
                )
            """))

            # Устанавливаем is_in_pool = FALSE для неперецепных задач
            conn.execute(text("""
                UPDATE tasks
                SET is_in_pool = 0
                WHERE id IN (
                    SELECT t.id FROM tasks t
                    JOIN parkings p ON t.parking_id = p.id
                    WHERE p.is_hitch = 0
                )
            """))

            # ============ 5. ПРОВЕРКА И СОЗДАНИЕ РОЛЕЙ ============
            print("\n📋 Проверка наличия ролей...")

            # Создаем сессию для работы с ORM
            Session = sessionmaker(bind=engine)
            db = Session()

            try:
                from models import Role

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
                        print(f"➕ Создана роль: {role_name}")
                    else:
                        print(f"✓ Роль {role_name} уже существует")

                db.commit()
                print("✅ Роли проверены/созданы")

            except Exception as e:
                print(f"❌ Ошибка при создании ролей: {e}")
                db.rollback()
            finally:
                db.close()

            # ============ 6. ФИНАЛЬНЫЙ КОММИТ ============
            conn.commit()
            print("\n✅ ПОЛНАЯ МИГРАЦИЯ УСПЕШНО ЗАВЕРШЕНА!")
            print("   Обновлены таблицы: users, breaks, tasks")
            print("   Созданы индексы для оптимизации запросов")
            print("   Обновлены существующие записи")

        except Exception as e:
            print(f"\n❌ ОШИБКА ПРИ МИГРАЦИИ: {e}")
            import traceback
            traceback.print_exc()
            conn.rollback()
            print("❌ Миграция отменена (rollback)")
            return False

    return True


def check_database():
    """Проверка состояния базы данных"""
    print("\n🔍 ПРОВЕРКА СОСТОЯНИЯ БАЗЫ ДАННЫХ...")

    engine = create_engine(
        config.DATABASE_URL,
        connect_args={"check_same_thread": False},
        echo=False
    )

    with engine.connect() as conn:
        try:
            # Проверяем таблицы
            tables = conn.execute(text("""
                SELECT name FROM sqlite_master
                WHERE type='table'
                ORDER BY name
            """)).fetchall()

            print(f"\n📊 Существующие таблицы:")
            for table in tables:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {table[0]}")).scalar()
                print(f"   • {table[0]}: {count} записей")

            # Проверяем роли
            roles = conn.execute(text("SELECT name FROM roles")).fetchall()
            if roles:
                print(f"\n👥 Роли в системе:")
                for role in roles:
                    print(f"   • {role[0]}")

            # Проверяем наличие новых полей
            print(f"\n🔧 Проверка полей users:")
            columns = conn.execute(text("PRAGMA table_info(users)")).fetchall()
            required_fields = ['is_on_break', 'break_start_time', 'total_break_time', 'current_role']
            for field in required_fields:
                exists = any(col[1] == field for col in columns)
                print(f"   • {field}: {'✅' if exists else '❌'}")

            print(f"\n🔧 Проверка полей tasks:")
            columns = conn.execute(text("PRAGMA table_info(tasks)")).fetchall()
            required_fields = ['assigned_driver_id', 'is_in_pool']
            for field in required_fields:
                exists = any(col[1] == field for col in columns)
                print(f"   • {field}: {'✅' if exists else '❌'}")

            print(f"\n🔧 Проверка таблицы breaks:")
            exists = conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='breaks'"
            )).fetchone()
            print(f"   • breaks: {'✅' if exists else '❌'}")

            if exists:
                count = conn.execute(text("SELECT COUNT(*) FROM breaks")).scalar()
                print(f"   • записей: {count}")

            print("\n✅ Проверка завершена")

        except Exception as e:
            print(f"❌ Ошибка при проверке: {e}")

def add_parking_queue_table():
    """Добавление таблицы очереди на парковку"""
    engine = create_engine(
        config.DATABASE_URL,
        connect_args={"check_same_thread": False},
        echo=True
    )

    with engine.connect() as conn:
        try:
            # Создаем таблицу очереди
            print("📋 Создаем таблицу parking_queue...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS parking_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    vehicle_number VARCHAR(20) NOT NULL,
                    is_hitch BOOLEAN DEFAULT 0,
                    vehicle_type VARCHAR(20) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    notified_at TIMESTAMP,
                    status VARCHAR(50) DEFAULT 'waiting',
                    spot_number INTEGER,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                )
            """))

            # Создаем индексы
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_queue_user_id ON parking_queue(user_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_queue_status ON parking_queue(status)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_queue_created ON parking_queue(created_at)
            """))

            conn.commit()
            print("✅ Таблица parking_queue успешно создана")

        except Exception as e:
            print(f"❌ Ошибка при создании таблицы: {e}")
            conn.rollback()

if __name__ == '__main__':
    add_parking_queue_table()

def run_selected_migration(migration_name=None):
    """
    Запуск выбранной миграции
    Args:
        migration_name: Имя миграции ('all', 'current_role', 'break', 'task_pool', 'check')
    """
    if migration_name == 'check' or not migration_name:
        check_database()
        return

    if migration_name == 'all':
        run_full_migration()
    elif migration_name == 'current_role':
        # Только для обратной совместимости
        print("⚠️ Рекомендуется использовать полную миграцию: run_full_migration()")
        from migrate_add_current_role import migrate_add_current_role
        migrate_add_current_role()
    elif migration_name == 'break':
        print("⚠️ Рекомендуется использовать полную миграцию: run_full_migration()")
        from migration_add_break_fields import migrate_add_break_fields
        migrate_add_break_fields()
    elif migration_name == 'task_pool':
        print("⚠️ Рекомендуется использовать полную миграцию: run_full_migration()")
        from migration_add_task_pool_fields import migrate_add_task_pool_fields
        migrate_add_task_pool_fields()
    else:
        print(f"❌ Неизвестная миграция: {migration_name}")
        print("   Доступные миграции: all, current_role, break, task_pool, check")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Управление миграциями базы данных')
    parser.add_argument(
        'migration',
        nargs='?',
        default='check',
        choices=['all', 'current_role', 'break', 'task_pool', 'check'],
        help='Тип миграции (по умолчанию: check)'
    )

    args = parser.parse_args()

    print("=" * 60)
    print("🗄️  СИСТЕМА УПРАВЛЕНИЯ МИГРАЦИЯМИ БАЗЫ ДАННЫХ")
    print("=" * 60)

    run_selected_migration(args.migration)

    print("\n" + "=" * 60)
