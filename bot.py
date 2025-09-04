import logging
import re
import os
import base64
import json
import time # Для sleep при необходимости
from datetime import datetime, timedelta, timezone # Добавлено timezone
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
    'https://www.googleapis.com/auth/drive' # Убран доступ к Sheets
]
# Директория для хранения временных файлов
LOCAL_CACHE_DIR = "./local_cache"

# --- Глобальные переменные (инициализируются в main) ---
CREDENTIALS_FILE: str = ""
TELEGRAM_TOKEN: str = ""
PARENT_FOLDER_ID: str = ""
TEMP_FOLDER_ID: str = "" # Может не использоваться, но оставим
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
    TEMP_FOLDER_ID = os.getenv("TEMP_FOLDER_ID", "") # Не используется напрямую, но может быть полезно
    ROOT_FOLDER_YEAR = str(datetime.now().year)
    if not all([TELEGRAM_TOKEN, PARENT_FOLDER_ID]): # TEMP_FOLDER_ID больше не обязательна
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
        Ищет строки в локальном .xlsm файле, где столбец A (индекс 0) == target_number.
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
            # Предполагаем, что данные начинаются со второй строки (первая - заголовок)
            for row_num, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
                if len(row) > 0: # Проверяем, что в строке есть хотя бы один столбец
                    cell_a_value = str(row[0]).strip().upper() if row[0] is not None else ""
                    if cell_a_value == target_number:
                        logger.debug(f"🔍 Совпадение найдено в строке {row_num}")
                        # Берём A-Z (первые 26 столбцов), убираем пустые
                        cleaned = [str(cell).strip() for cell in row[:26] if cell is not None and str(cell).strip()]
                        results.append(" | ".join(cleaned))
            workbook.close()
            logger.info(f"✅ Поиск завершен. Найдено {len(results)} совпадений.")
        except Exception as e:
            logger.error(f"❌ Ошибка при поиске в локальном файле {local_filepath}: {e}")
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
            f"📂 Сформированный путь и файл:\n```\n{path_structure}\n```"
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
            "Кожаный, я понимаю только следующие команды:\n"
            "• `/start` - начать работу со мной\n"
            "• `/s 123456` - найти данные по номеру\n"
            "• `/path` - показать содержимое корневой папки\n"
            "• `/test ДДММГГ` - тест формирования пути\n"
            "Также ты можешь упомянуть меня в группе или канале: `@ваш_бот 123456`"
        )
        await update.message.reply_text(help_text, parse_mode='Markdown')

def extract_number(query: str) -> Optional[str]:
    """
    Извлекает номер (СН) из строки. Поддерживает буквы и цифры.
    Возвращает строку с номером или None.
    """
    if not query:
        return None
    clean_query = query.strip()
    # Проверяем, состоит ли строка только из букв и цифр
    if clean_query.isalnum():
        return clean_query
    # Если есть другие символы, можно добавить дополнительную логику,
    # например, извлекать подстроку до пробела или другого разделителя
    # Пока просто возвращаем None для "некорректных" форматов
    return None

async def handle_query(update: Update, context: ContextTypes.DEFAULT_TYPE, query: str) -> None:
    """
    Общая логика обработки запроса с улучшенным управлением локальным кэшем.
    """
    if not update.message:
        logger.warning("⚠️ Получено обновление без сообщения для handle_query")
        return
    message = update.message
    user_id = message.from_user.id if message.from_user else "N/A"
    logger.info(f"📥 Получен запрос от пользователя {user_id}: '{query}'")
    number = extract_number(query)
    if not number:
        await message.reply_text("❌ Не указан корректный номер. Пример: `123456` или `АВ123456`", parse_mode='Markdown')
        logger.info(f"📤 Ответ на некорректный номер отправлен пользователю {user_id}")
        return
    await message.reply_text(f"🔍 Поиск по номеру: `{number}`", parse_mode='Markdown')
    logger.info(f"📤 Подтверждение поиска отправлено пользователю {user_id}")
    try:
        gs = GoogleServices()
        fm = FileManager(gs.drive)
        lds = LocalDataSearcher()
        current_year = str(datetime.now().year)
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        dates_to_try = [today, yesterday]
        file_id = None
        used_date = None
        logger.info(f"🔍 Начинаю поиск файла для номера: {number}")
        logger.info(f"📁 PARENT_FOLDER_ID (корневая папка): {PARENT_FOLDER_ID}")
        for target_date in dates_to_try:
            filename = f"АПП_Склад_{target_date.strftime('%d%m%y')}_{CITY}.xlsm"
            logger.info(f"🔍 Попытка поиска файла: {filename}")
            root_folder = PARENT_FOLDER_ID
            acts_folder = fm.find_folder(root_folder, "акты")
            if not acts_folder:
                logger.warning(f"⚠️ Папка 'акты' не найдена в корневой папке (ID: {root_folder})")
                continue
            month_names = ["январь", "февраль", "март", "апрель", "май", "июнь",
                           "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]
            month_num = target_date.month
            month_folder_name = f"{target_date.strftime('%m')} - {month_names[month_num - 1]}"
            month_folder = fm.find_folder(acts_folder, month_folder_name)
            if not month_folder:
                logger.warning(f"⚠️ Папка месяца '{month_folder_name}' не найдена в 'акты' (ID: {acts_folder})")
                continue
            date_folder_name = target_date.strftime('%d%m%y')
            date_folder = fm.find_folder(month_folder, date_folder_name)
            if not date_folder:
                logger.warning(f"⚠️ Папка с датой '{date_folder_name}' не найдена в папке месяца (ID: {month_folder})")
                continue
            file_id = fm.find_file(date_folder, filename)
            if file_id:
                logger.info(f"✅ Файл найден: ID={file_id}")
                used_date = target_date
                break
        if not file_id:
            await message.reply_text("❌ Файл за сегодня или вчера не найден.")
            logger.info(f"📤 Сообщение 'файл не найден' отправлено пользователю {user_id}")
            return
        # --- Удалена отправка даты и пути до файла ---

        # --- Новая логика управления локальным файлом ---
        # 1. Определяем имя локального файла
        local_filename = f"local_cache_{used_date.strftime('%Y-%m-%d')}.xlsm"
        local_filepath = os.path.join(LOCAL_CACHE_DIR, local_filename)
        logger.info(f"📁 Путь к локальному файлу: {local_filepath}")
        # 2. Получаем время изменения файла на Google Drive
        drive_modified_time = fm.get_file_modified_time(file_id)
        if not drive_modified_time:
             error_message_for_user = f"❌ Не удалось получить время изменения файла '{filename}' на Google Drive."
             logger.error(f"❌ Не удалось получить время изменения файла {file_id}")
             await message.reply_text(error_message_for_user)
             return
        # 3. Проверяем, нужно ли скачивать файл
        download_needed = True
        if os.path.exists(local_filepath):
            # Получаем время последнего изменения локального файла
            local_modified_time = datetime.fromtimestamp(os.path.getmtime(local_filepath), tz=timezone.utc)
            logger.debug(f"🕒 Время изменения локального файла {local_filepath}: {local_modified_time}")
            logger.debug(f"🕒 Время изменения файла на Drive: {drive_modified_time}")
            # Сравниваем времена
            if drive_modified_time <= local_modified_time:
                logger.info(f"✅ Локальный файл {local_filepath} актуален. Используем его.")
                download_needed = False
            else:
                logger.info(f"🔄 Файл на Drive новее. Нужно скачать заново.")
        else:
            logger.info(f"🆕 Локальный файл {local_filepath} не найден. Нужно скачать.")
        # 4. Скачиваем файл, если необходимо
        if download_needed:
             logger.info(f"⬇️ Скачивание файла {file_id} в локальный файл {local_filepath}")
             download_success = fm.download_file(file_id, local_filepath)
             if not download_success:
                 error_message_for_user = f"❌ Не удалось скачать файл '{filename}'."
                 logger.error(f"❌ Не удалось скачать файл {file_id}")
                 await message.reply_text(error_message_for_user)
                 return # Важно выйти, если скачивание не удалось
             else:
                 # Обновляем время модификации локального файла до времени Drive файла
                 # os.utime(local_filepath, (time.time(), drive_modified_time.timestamp()))
                 pass
        # 5. Обрабатываем локальный файл
        logger.debug(f"🔍 Чтение данных из локального файла {local_filepath}")
        results = lds.search_by_number(local_filepath, number)
        if not results:
            await message.reply_text(f"❌ Запись с номером `{number}` не найдена.")
            logger.info(f"📤 Сообщение 'запись не найдена' отправлено пользователю {user_id}")
            return

        # --- Изменённая часть: формирование красивого ответа ---
        response_lines = []
        for i, result in enumerate(results, start=1): # Добавляем нумерацию, если результатов > 1
            # Разделяем строку по разделителю " | "
            parts = result.split(" | ")
            if len(parts) >= 15:  # Убедимся, что есть достаточно столбцов (A-O)
                # Извлекаем нужные данные
                sn = parts[0]  # СН (столбец A)
                # Проверяем, соответствует ли СН из файла тому, что искали, на всякий случай
                # (если логика поиска в openpyxl строгая, это может быть избыточно)
                type_terminal = parts[4] if len(parts) > 4 else "N/A"  # Тип терминала (E)
                model = parts[6] if len(parts) > 6 else "N/A"  # Модель (G)
                status = parts[8] if len(parts) > 8 else "N/A"  # Статус (I)
                # Изменено: Место хранения теперь столбец O (индекс 14)
                storage = parts[14] if len(parts) > 14 else "N/A"  # Место хранения (O)

                # Формируем красивый ответ
                # Начинаем с СН
                line = f"<b>СН {sn}</b>\n"
                # Добавляем "облако" информации
                line += "☁️ <b>Информация:</b>\n"
                line += f"    • Тип терминала: <code>{type_terminal}</code>\n"
                line += f"    • Модель терминала: <code>{model}</code>\n"
                line += f"    • Статус терминала: <code>{status}</code>\n"
                line += f"    • Место хранения терминала: <code>{storage}</code>"

                # Если результатов несколько, добавляем разделитель
                if len(results) > 1:
                     line = f"<b>--- Результат {i} ---</b>\n{line}\n"

                response_lines.append(line)
            else:
                # Если данных недостаточно, выводим как есть
                response_lines.append(f"<pre>{result}</pre>")

        # Объединяем строки
        # Используем parse_mode='HTML'
        full_response = "\n".join(response_lines)

        # Проверка длины ответа
        if len(full_response) > 4096:
             # Можно разбить на несколько сообщений или обрезать
             # Здесь просто обрежем и добавим уведомление
             full_response = full_response[:4050] + "\n<i>... (результаты обрезаны)</i>"

        await message.reply_text(full_response, parse_mode='HTML')
        logger.info(f"📤 Результаты поиска ({len(results)} совпадений) отправлены пользователю {user_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка обработки запроса '{query}' от пользователя {user_id}: {e}", exc_info=True)
        if update.message:
            await update.message.reply_text("❌ Произошла ошибка при поиске данных.")

# --- Новый обработчик для любого текста ---
async def handle_any_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик любого текстового сообщения, не являющегося командой или упоминанием."""
    if not update.message or not update.message.text:
        return
    user_id = update.effective_user.id
    text = update.message.text.strip()
    # Игнорируем команды и упоминания, так как они обрабатываются отдельно
    if text.startswith('/') or re.match(rf'@{re.escape(context.bot.username)}\b', text, re.IGNORECASE):
        return # Пусть другие обработчики этим займутся
    logger.info(f"📥 Пользователь {user_id} отправил текст: '{text}'")
    # Отправляем ответ "Кожаный ублюдок..."
    response = (
        "Кожаный ублюдок, ты что-то не то ввел.\n"
        "Я понимаю только следующие команды:\n"
        "• `/start` - начать работу со мной\n"
        "• `/s 123456` - найти данные по номеру\n"
        "• `/path` - показать содержимое корневой папки\n"
        "• `/test ДДММГГ` - тест формирования пути\n"
        "Также ты можешь упомянуть меня в группе или канале: `@ваш_бот 123456`"
    )
    await update.message.reply_text(response, parse_mode='Markdown')
    logger.info(f"📤 Ответ 'Кожаный ублюдок...' отправлен пользователю {user_id}")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка сообщений: команды и упоминания."""
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()
    bot_username = context.bot.username
    is_command_s = text.startswith("/s")
    is_command_path = text.startswith("/path")
    is_command_test = text.startswith("/test")
    is_mention = re.match(rf'@{re.escape(bot_username)}\b', text, re.IGNORECASE)
    if is_command_path:
        await show_path(update, context)
    elif is_command_test:
        command_parts = text.split(' ', 1)
        args = command_parts[1:] if len(command_parts) > 1 else []
        context.args = args
        await test_command(update, context)
    elif is_command_s or is_mention:
        if is_command_s:
            query = ' '.join(context.args) if context.args else ''
        else:
            query = re.sub(rf'@{re.escape(bot_username)}\s*', '', text, flags=re.IGNORECASE).strip()
        await handle_query(update, context, query)
    elif text.startswith('/'):
        await unknown_command(update, context)
    # Обработка любого другого текста перемещена в handle_any_text

def main() -> None:
    """Главная функция запуска бота."""
    try:
        init_config()
    except RuntimeError as e:
        logger.critical(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        print(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        return
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    # Обработчики команд
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("path", show_path))
    app.add_handler(CommandHandler("test", test_command))
    app.add_handler(CommandHandler("s", lambda u, c: handle_query(u, c, ' '.join(c.args) if c.args else '')))
    # Обработчик для неизвестных команд
    app.add_handler(MessageHandler(filters.COMMAND, unknown_command))
    # Обработчик для любого текста (должен быть ниже обработчиков команд)
    # Обрабатывает личные сообщения, группы и каналы
    app.add_handler(MessageHandler(
        filters.TEXT & (filters.ChatType.CHANNEL | filters.ChatType.GROUPS | filters.ChatType.PRIVATE),
        handle_any_text # Используем новый обработчик
    ))
    # Обработчик для команд/упоминаний (тот же, что и раньше, но без части логики)
    app.add_handler(MessageHandler(
        filters.TEXT & (filters.ChatType.CHANNEL | filters.ChatType.GROUPS | filters.ChatType.PRIVATE),
        handle_message
    ))
    logger.info("🚀 Бот запущен. Поддержка: личка, группы, каналы (при упоминании).")
    logger.info(f"⚙️ Конфигурация: ROOT_FOLDER_YEAR={ROOT_FOLDER_YEAR}, CITY={CITY}, LOCAL_CACHE_DIR={LOCAL_CACHE_DIR}")
    app.run_polling()

if __name__ == '__main__':
    main()
