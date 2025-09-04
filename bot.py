import logging
import re
import os
import base64
import json
import time  # Для sleep при необходимости
from datetime import datetime, timedelta, timezone  # Добавлено timezone
from typing import List, Optional, Dict, Any
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import openpyxl
from openpyxl.worksheet.worksheet import Worksheet
import warnings

# Подавить предупреждения от openpyxl о Data Validation
warnings.filterwarnings("ignore", message="Data Validation extension is not supported and will be removed", category=UserWarning, module="openpyxl.worksheet._reader")

# --- Настройка логирования ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
# Уменьшаем уровень логов для httpx
logging.getLogger("httpx").setLevel(logging.WARNING)
# Создаем логгер для нашего приложения
logger = logging.getLogger(__name__)

# --- Конфигурация ---
CITY = 'Воронеж'
SCOPES = [
    'https://www.googleapis.com/auth/drive'  # Убран доступ к Sheets
]
# Директория для хранения временных файлов
LOCAL_CACHE_DIR = "./local_cache"

# --- Глобальные переменные (инициализируются в main) ---
CREDENTIALS_FILE: str = ""
TELEGRAM_TOKEN: str = ""
PARENT_FOLDER_ID: str = ""
TEMP_FOLDER_ID: str = ""  # Может не использоваться, но оставим
ROOT_FOLDER_YEAR: str = ""


def get_credentials_path() -> str:
    """Декодирует Google Credentials из переменной окружения и сохраняет во временный файл."""
    encoded = os.getenv("GOOGLE_CREDS_BASE64")
    if not encoded:
        raise RuntimeError("Переменная GOOGLE_CREDS_BASE64 не найдена!")
    try:
        decoded = base64.b64decode(encoded).decode('utf-8')
        creds = json.loads(decoded)
        temp_path = "temp_google_creds.json"
        with open(temp_path, 'w') as f:
            json.dump(creds, f)
        logger.info(f"✅ Учетные данные Google сохранены во временный файл: {temp_path}")
        return temp_path
    except Exception as e:
        logger.error(f"❌ Ошибка декодирования GOOGLE_CREDS_BASE64: {e}")
        raise


def init_config():
    """Инициализирует глобальные переменные конфигурации."""
    global CREDENTIALS_FILE, TELEGRAM_TOKEN, PARENT_FOLDER_ID, TEMP_FOLDER_ID, ROOT_FOLDER_YEAR
    CREDENTIALS_FILE = get_credentials_path()
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
    PARENT_FOLDER_ID = os.getenv("PARENT_FOLDER_ID", "")
    TEMP_FOLDER_ID = os.getenv("TEMP_FOLDER_ID", "")  # Не используется напрямую, но оставим
    ROOT_FOLDER_YEAR = str(datetime.now().year)
    if not all([TELEGRAM_TOKEN, PARENT_FOLDER_ID]):  # TEMP_FOLDER_ID больше не обязательна
        missing = [k for k, v in {"TELEGRAM_TOKEN": TELEGRAM_TOKEN, "PARENT_FOLDER_ID": PARENT_FOLDER_ID}.items() if not v]
        raise RuntimeError(f"❌ Отсутствуют обязательные переменные окружения: {', '.join(missing)}")
    # Создаем директорию для кэша, если её нет
    os.makedirs(LOCAL_CACHE_DIR, exist_ok=True)
    logger.info(f"📁 Директория для локального кэша: {os.path.abspath(LOCAL_CACHE_DIR)}")


class GoogleServices:
    """Инкапсуляция Google API сервисов."""
    def __init__(self):
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
        self.drive = build('drive', 'v3', credentials=creds)
        # self.sheets = build('sheets', 'v4', credentials=creds) # Не используется


class FileManager:
    """Работа с файлами и папками на Google Диске."""
    def __init__(self, drive_service):
        self.drive = drive_service

    def find_folder(self, parent_id: str, name: str) -> Optional[str]:
        """Найти папку по имени."""
        query = f"mimeType='application/vnd.google-apps.folder' and name='{name}' and '{parent_id}' in parents and trashed=false"
        try:
            result = self.drive.files().list(q=query, fields="files(id, name)").execute()
            files = result.get('files', [])
            if files:
                logger.debug(f"📁 Найдена папка '{name}' (ID: {files[0]['id']}) внутри родителя {parent_id}")
                return files[0]['id']
            else:
                logger.debug(f"📁 Папка '{name}' НЕ найдена внутри родителя {parent_id}")
                return None
        except Exception as e:
            logger.error(f"❌ Ошибка поиска папки '{name}' в {parent_id}: {e}")
            return None

    def find_file(self, folder_id: str, filename: str) -> Optional[str]:
        """Найти файл в папке."""
        query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
        try:
            result = self.drive.files().list(q=query, fields="files(id, name, mimeType)").execute()
            files = result.get('files', [])
            if files:
                file_info = files[0]
                logger.debug(f"📄 Найден файл '{filename}' (ID: {file_info['id']}) в папке {folder_id}")
                return file_info['id']
            else:
                logger.debug(f"📄 Файл '{filename}' НЕ найден в папке {folder_id}")
                return None
        except Exception as e:
            logger.error(f"❌ Ошибка поиска файла '{filename}' в {folder_id}: {e}")
            return None

    def get_file_modified_time(self, file_id: str) -> Optional[datetime]:
        """Получает время последнего изменения файла на Google Drive."""
        try:
            file_info = self.drive.files().get(fileId=file_id, fields="modifiedTime").execute()
            modified_time_str = file_info.get('modifiedTime')
            if modified_time_str:
                # Парсим строку времени в объект datetime с временной зоной UTC
                modified_time = datetime.strptime(modified_time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                modified_time = modified_time.replace(tzinfo=timezone.utc)
                logger.debug(f"🕒 Время изменения файла на Drive {file_id}: {modified_time}")
                return modified_time
            else:
                logger.warning(f"⚠️ Время изменения не найдено для файла {file_id}")
                return None
        except Exception as e:
            logger.error(f"❌ Ошибка получения времени изменения файла {file_id}: {e}")
            return None

    def download_file(self, file_id: str, local_filename: str) -> bool:
        """Скачивает файл с Google Drive в локальный файл."""
        try:
            logger.info(f"⬇️ Начинаю скачивание файла {file_id} в {local_filename}")
            request = self.drive.files().get_media(fileId=file_id)
            with open(local_filename, 'wb') as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                    progress = int(status.progress() * 100)
                    logger.debug(f"⬇️ Прогресс скачивания {file_id}: {progress}%")
            logger.info(f"✅ Файл {file_id} успешно скачан как {local_filename}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка скачивания файла {file_id} в {local_filename}: {e}")
            return False

    def list_files_in_folder(self, folder_id: str, max_results: int = 100) -> List[Dict[str, Any]]:
        """Получить список файлов и папок в указанной папке Google Drive."""
        try:
            query = f"'{folder_id}' in parents and trashed=false"
            results = self.drive.files().list(
                q=query,
                pageSize=max_results,
                fields="nextPageToken, files(id, name, mimeType, size)"
            ).execute()
            items = results.get('files', [])
            logger.debug(f"📁 Получен список из {len(items)} элементов из папки {folder_id}")
            return items
        except Exception as e:
            logger.error(f"❌ Ошибка получения списка файлов из папки {folder_id}: {e}")
            return []


class LocalDataSearcher:
    """Поиск данных в локальном Excel файле."""
    @staticmethod
    def search_by_number(local_filepath: str, target_number: str, sheet_name: str = "Терминалы") -> List[str]:
        """
        Ищет строки в локальном .xlsm файле, где столбец F (индекс 5) == target_number.
        """
        logger.info(f"🔍 Начинаю поиск номера '{target_number}' в локальном файле {local_filepath}, лист '{sheet_name}'")
        target_number = target_number.strip().upper()
        results = []
        try:
            # Открываем книгу Excel
            workbook = openpyxl.load_workbook(local_filepath, read_only=True, data_only=True)
            if sheet_name not in workbook.sheetnames:
                logger.warning(f"⚠️ Лист '{sheet_name}' не найден в файле {local_filepath}. Доступные листы: {workbook.sheetnames}")
                workbook.close()
                return results
            sheet: Worksheet = workbook[sheet_name]
            logger.debug(f"📄 Обработка листа '{sheet_name}' из файла {local_filepath}")
            # Получаем значения заголовков (первая строка)
            # Предполагаем, что заголовки находятся в первой строке
            header_row = None
            try:
                header_row = list(sheet.iter_rows(min_row=1, max_row=1, values_only=True))[0]
                # Преобразуем значения заголовков в строки, заменяя None на пустые строки
                header_names = [str(cell) if cell is not None else "" for cell in header_row]
                logger.debug(f"🏷️ Заголовки листа '{sheet_name}': {header_names[:10]}...")  # Логируем первые 10
            except Exception as e:
                logger.warning(f"⚠️ Ошибка получения заголовков из первой строки: {e}")
                header_names = None  # Если не удалось получить заголовки
            # Предполагаем, что данные начинаются со второй строки (первая - заголовок)
            for row_num, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
                if len(row) > 0:  # Проверяем, что в строке есть хотя бы один столбец
                    # Исправлено: теперь ищем по столбцу F (индекс 5)
                    cell_f_value = str(row[5]).strip().upper() if len(row) > 5 and row[5] is not None else ""
                    if cell_f_value == target_number:
                        logger.info(f"🔍 Совпадение найдено в файле '{local_filepath}', лист '{sheet_name}', строка {row_num}")
                        # Берём A-Z (первые 26 столбцов), убираем пустые
                        cleaned_data = []
                        for col_index, cell in enumerate(row[:26]):
                            cell_value = str(cell).strip() if cell is not None else ""
                            if cell_value:
                                # Получаем имя столбца (A, B, C, ...)
                                column_letter = openpyxl.utils.get_column_letter(col_index + 1)  # +1 потому что индексация с 1
                                # Получаем имя заголовка, если доступно
                                header_name = header_names[col_index] if header_names and col_index < len(header_names) and header_names[col_index] else "N/A"
                                cleaned_data.append(f"{column_letter}({header_name}):'{cell_value}'")
                                logger.debug(f"    📄 [{column_letter}({header_name})] = '{cell_value}'")
                        results.append(" | ".join(cleaned_data))
            workbook.close()
            logger.info(f"✅ Поиск завершен. Найдено {len(results)} совпадений.")
        except Exception as e:
            logger.error(f"❌ Ошибка при поиске в локальном файле {local_filepath}: {e}", exc_info=True)  # Добавлен exc_info для трассировки
        return results


# --- Команды бота ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Приветствие (работает в личке и группах)."""
    if update.message:
        logger.info(f"📤 Отправка приветствия пользователю {update.effective_user.id}")
        await update.message.reply_text(
            "🤖 Привет! Я могу найти данные по номеру.\n"
            "Используй:\n"
            "• `/s 123456` - поиск по номеру\n"
            "• `/path` - показать содержимое корневой папки\n"
            "• `/test ДДММГГ` - тест формирования пути (например, `/test 010125`)\n"
            "• `@ваш_бот 123456` - упоминание в группах/каналах"
        )


async def show_path(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает содержимое каталога на Google Drive по PARENT_FOLDER_ID."""
    if not update.message:
        return
    user_id = update.effective_user.id
    logger.info(f"📤 Пользователь {user_id} запросил команду /path")
    try:
        await update.message.reply_text("🔍 Получаю содержимое корневой папки на Google Drive...")
        gs = GoogleServices()
        fm = FileManager(gs.drive)
        root_folder_id = PARENT_FOLDER_ID
        try:
            root_folder_info = gs.drive.files().get(fileId=root_folder_id, fields="name").execute()
            root_folder_name = root_folder_info.get('name', 'Без названия')
        except Exception:
            root_folder_name = 'Неизвестная корневая папка'
            logger.warning(f"⚠️ Не удалось получить имя корневой папки с ID {root_folder_id}")
        path_info = f"📂 Корневая папка Google Drive: `{root_folder_name}` (ID: `{root_folder_id}`)\n"
        try:
            items = fm.list_files_in_folder(root_folder_id, max_results=100)
            if not items:
                path_info += "Папка пуста или не содержит файлов/папок."
            else:
                path_info += f"Содержимое ({len(items)} элементов):\n"
                folders = sorted([item for item in items if item.get('mimeType') == 'application/vnd.google-apps.folder'],
                                 key=lambda x: x.get('name', '').lower())
                files = sorted([item for item in items if item.get('mimeType') != 'application/vnd.google-apps.folder'],
                               key=lambda x: x.get('name', '').lower())
                for folder in folders:
                    name = folder.get('name', 'Без названия')
                    fid = folder.get('id', 'N/A')
                    path_info += f"📁 `{name}/` (ID: `{fid}`)\n"
                for file in files:
                    name = file.get('name', 'Без названия')
                    fid = file.get('id', 'N/A')
                    mime_type = file.get('mimeType', 'Неизвестный тип')
                    size = file.get('size', None)
                    size_str = f" ({int(size)} байт)" if size and size.isdigit() else ""
                    path_info += f"📄 `{name}`{size_str} (ID: `{fid}`, Тип: `{mime_type}`)\n"
        except Exception as e:
            path_info += f"❌ Ошибка при получении содержимого корневой папки: {e}\n"
            logger.error(f"❌ Ошибка при получении содержимого корневой папки {root_folder_id}: {e}")
        if len(path_info) > 4096:
            lines = path_info.split('\n')
            current_part = ""
            for line in lines:
                if len(current_part + line + '\n') > 4000:
                    await update.message.reply_text(current_part, parse_mode='Markdown')
                    current_part = "Продолжение `/path`:\n" + line + '\n'
                else:
                    current_part += line + '\n'
            if current_part:
                await update.message.reply_text(current_part, parse_mode='Markdown')
        else:
            await update.message.reply_text(path_info, parse_mode='Markdown')
        logger.info(f"📤 Ответ на /path отправлен пользователю {user_id}")
    except Exception as e:
        error_msg = f"❌ Произошла ошибка при получении структуры папок Google Drive: {e}"
        logger.error(error_msg, exc_info=True)
        if update.message:
            await update.message.reply_text(error_msg)


async def test_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /test для формирования пути и имени файла по дате."""
    if not update.message:
        return
    user_id = update.effective_user.id
    logger.info(f"📤 Пользователь {user_id} запросил команду /test")
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "❌ Неверный формат. Используй: `/test ДДММГГ`\n"
            "Пример: `/test 010125`",
            parse_mode='Markdown'
        )
        return
    date_str = context.args[0].strip()
    if not (len(date_str) == 6 and date_str.isdigit()):
        await update.message.reply_text(
            "❌ Неверный формат даты. Нужно 6 цифр: ДДММГГ\n"
            "Пример: `010125` для 1 января 2025 года",
            parse_mode='Markdown'
        )
        return
    try:
        day = date_str[:2]
        month = date_str[2:4]
        year_short = date_str[4:]
        year_full = f"20{year_short}"
        month_names = ["январь", "февраль", "март", "апрель", "май", "июнь",
                       "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]
        try:
            month_index = int(month) - 1
            month_name = month_names[month_index] if 0 <= month_index <= 11 else "???"
        except (ValueError, IndexError):
            month_name = "???"
        filename = f"АПП_Склад_{date_str}_{CITY}.xlsm"
        path_structure = (
            f"{year_full}\n"
            f"  └── акты\n"
            f"      └── {month} - {month_name}\n"
            f"          └── {date_str}\n"
            f"              └── {filename}"
        )
        response = (
            f"📅 Дата: `{day}.{month}.20{year_short}`\n"
            f"📂 Сформированный путь и файл:\n"
            f"```\n"
            f"{path_structure}\n"
            f"```"
        )
        await update.message.reply_text(response, parse_mode='Markdown')
        logger.info(f"📤 Ответ на /test ({date_str}) отправлен пользователю {user_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка в команде /test: {e}")
        await update.message.reply_text("❌ Произошла ошибка при обработке даты.", parse_mode='Markdown')


async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик неизвестных команд."""
    if update.message:
        user_id = update.effective_user.id
        command = update.message.text.split()[0] if update.message.text else "N/A"
        logger.info(f"📤 Пользователь {user_id} отправил неизвестную команду: {command}")
        help_text = (
            "Кожаный, я понимаю тол
