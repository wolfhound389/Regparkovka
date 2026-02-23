"""
Сервис для работы с изображениями ворот и парковки
"""

import os
import random
from pathlib import Path
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)

class ImageService:
    """Сервис для получения изображений по номерам ворот/мест"""

    # Базовые пути к папкам с изображениями
    BASE_PATH = Path("gates_images")

    # Соответствие типов папкам
    FOLDER_MAPPING = {
        "ABK1": "АБК-1",
        "ABK2": "АБК-2",
        "PARKING": "Парковка"
    }

    @classmethod
    def get_image_path(cls, folder_type: str, number: int) -> Optional[Path]:
        """
        Получить путь к изображению по номеру

        Args:
            folder_type: Тип папки ("ABK1", "ABK2", "PARKING")
            number: Номер изображения (ворот или места)

        Returns:
            Path к изображению или None, если не найдено
        """
        try:
            folder_name = cls.FOLDER_MAPPING.get(folder_type)
            if not folder_name:
                logger.error(f"Неизвестный тип папки: {folder_type}")
                return None

            folder_path = cls.BASE_PATH / folder_name

            if not folder_path.exists():
                logger.error(f"Папка не существует: {folder_path}")
                return None

            # Ищем файл с любым расширением изображения
            extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']
            for ext in extensions:
                file_path = folder_path / f"{number}{ext}"
                if file_path.exists():
                    return file_path

                # Также пробуем с ведущим нулем
                file_path = folder_path / f"{number:02d}{ext}"
                if file_path.exists():
                    return file_path

            logger.warning(f"Изображение #{number} не найдено в {folder_path}")
            return None

        except Exception as e:
            logger.error(f"Ошибка при получении изображения: {e}")
            return None

    @classmethod
    def get_random_image(cls, folder_type: str) -> Optional[Path]:
        """
        Получить случайное изображение из папки

        Args:
            folder_type: Тип папки ("ABK1", "ABK2", "PARKING")

        Returns:
            Path к случайному изображению или None
        """
        try:
            folder_name = cls.FOLDER_MAPPING.get(folder_type)
            if not folder_name:
                return None

            folder_path = cls.BASE_PATH / folder_name

            if not folder_path.exists():
                return None

            # Получаем все файлы изображений
            images = []
            for ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']:
                images.extend(folder_path.glob(f"*{ext}"))

            if not images:
                return None

            return random.choice(images)

        except Exception as e:
            logger.error(f"Ошибка при получении случайного изображения: {e}")
            return None

    @classmethod
    def get_available_numbers(cls, folder_type: str) -> List[int]:
        """
        Получить список доступных номеров в папке

        Args:
            folder_type: Тип папки ("ABK1", "ABK2", "PARKING")

        Returns:
            Список доступных номеров
        """
        try:
            folder_name = cls.FOLDER_MAPPING.get(folder_type)
            if not folder_name:
                return []

            folder_path = cls.BASE_PATH / folder_name

            if not folder_path.exists():
                return []

            numbers = []
            for file_path in folder_path.iterdir():
                if file_path.is_file():
                    # Извлекаем номер из имени файла
                    name = file_path.stem
                    try:
                        # Убираем ведущие нули и конвертируем в int
                        number = int(name.lstrip('0') or '0')
                        numbers.append(number)
                    except ValueError:
                        continue

            return sorted(numbers)

        except Exception as e:
            logger.error(f"Ошибка при получении списка номеров: {e}")
            return []
