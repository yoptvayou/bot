import logging
import re
import os
import base64
import json
import time
from datetime import datetime, timedelta, timezone
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
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --- Конфигурация ---
CITY = 'Воронеж'
SCOPES = [
    'https://www.googleapis.com/auth/drive'
]
LOCAL_CACHE_DIR = "./local_cache"

# --- Глобальные переменные ---
CREDENTIALS_FILE: str = ""
TELEGRAM_TOKEN: str = ""
PARENT_FOLDER_ID: str = ""
TEMP_FOLDER_ID: str = ""
ROOT_FOLDER_YEAR: str = ""


def get_credentials_path() -> str:
    """Декодирует Google Credentials из переменной окружения."""
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
    """Инициализирует глобальные переменные."""
    global CREDENTIALS_FILE, TELEGRAM_TOKEN, PARENT_FOLDER_ID, TEMP_FOLDER_ID, ROOT_FOLDER_YEAR
    CREDENTIALS_FILE = get_credentials_path()
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
    PARENT_FOLDER_ID = os.getenv("PARENT_FOLDER_ID", "")
    TEMP_FOLDER_ID = os.getenv("TEMP_FOLDER_ID", "")
    ROOT_FOLDER_YEAR = str(datetime.now().year)
    if not all([TELEGRAM_TOKEN, PARENT_FOLDER_ID]):
        missing = [k for k, v in {"TELEGRAM_TOKEN": TELEGRAM_TOKEN, "PARENT_FOLDER_ID": PARENT_FOLDER_ID}.items() if not v]
        raise RuntimeError(f"❌ Отсутствуют обязательные переменные окружения: {', '.join(missing)}")
    os.makedirs(LOCAL_CACHE_DIR, exist_ok=True)
    logger.info(f"📁 Директория для кэша: {os.path.abspath(LOCAL_CACHE_DIR)}")


class GoogleServices:
    def __init__(self):
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
        self.drive = build('drive', 'v3', credentials=creds)


class FileManager:
    def __init__(self, drive_service):
        self.drive = drive_service

    def find_folder(self, parent_id: str, name: str) -> Optional[str]:
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
        try:
            file_info = self.drive.files().get(fileId=file_id, fields="modifiedTime").execute()
            modified_time_str = file_info.get('modifiedTime')
            if modified_time_str:
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
        try:
            logger.info(f"⬇️ Скачивание файла {file_id} в {local_filename}")
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
    @staticmethod
    def search_by_number(local_filepath: str, target_number: str, sheet_name: str = "Терминалы") -> List[str]:
        """Ищет строки по номеру в столбце F."""
        logger.info(f"🔍 Поиск номера '{target_number}' в файле {local_filepath}, лист '{sheet_name}'")
        target_number = target_number.strip().upper()
        results = []
        try:
            workbook = openpyxl.load_workbook(local_filepath, read_only=True, data_only=True)
            if sheet_name not in workbook.sheetnames:
                logger.warning(f"⚠️ Лист '{sheet_name}' не найден. Доступные: {workbook.sheetnames}")
                workbook.close()
                return results
            sheet: Worksheet = workbook[sheet_name]
            # Получаем заголовки
            header_row = list(sheet.iter_rows(min_row=1, max_row=1, values_only=True))[0]
            header_names = [str(cell) if cell is not None else "" for cell in header_row]
            logger.debug(f"🏷️ Заголовки: {header_names[:10]}...")
            # Поиск по строкам
            for row_num, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
                if len(row) > 5:
                    cell_f_value = str(row[5]).strip().upper()
                    if cell_f_value == target_number:
                        cleaned_data = []
                        for col_index, cell in enumerate(row[:26]):
                            cell_value = str(cell).strip() if cell is not None else ""
                            if cell_value:
                                column_letter = openpyxl.utils.get_column_letter(col_index + 1)
                                header_name = header_names[col_index] if col_index < len(header_names) else "N/A"
                                cleaned_data.append(f"{column_letter}({header_name}):'{cell_value}'")
                        results.append(" | ".join(cleaned_data))
            workbook.close()
            logger.info(f"✅ Найдено {len(results)} совпадений.")
        except Exception as e:
            logger.error(f"❌ Ошибка при поиске в {local_filepath}: {e}", exc_info=True)
        return results

    @staticmethod
    def get_row_by_index(local_filepath: str, row_index: int, sheet_name: str = "Терминалы") -> Optional[List[str]]:
        """Получает строку по индексу (начиная с 1)."""
        try:
            workbook = openpyxl.load_workbook(local_filepath, read_only=True, data_only=True)
            if sheet_name not in workbook.sheetnames:
                logger.warning(f"⚠️ Лист '{sheet_name}' не найден.")
                workbook.close()
                return None
            sheet: Worksheet = workbook[sheet_name]
            # Получаем заголовки
            header_row = list(sheet.iter_rows(min_row=1, max_row=1, values_only=True))[0]
            header_names = [str(cell) if cell is not None else "" for cell in header_row]
            # Проверяем наличие строки
            if row_index < 1 or row_index >= len(list(sheet.rows)):
                logger.warning(f"⚠️ Строка {row_index} не существует.")
                workbook.close()
                return None
            # Получаем строку
            row = list(sheet.iter_rows(min_row=row_index, max_row=row_index, values_only=True))[0]
            # Формируем результат
            result = []
            for col_index, cell in enumerate(row):
                cell_value = str(cell).strip() if cell is not None else ""
                if cell_value:
                    column_letter = openpyxl.utils.get_column_letter(col_index + 1)
                    header_name = header_names[col_index] if col_index < len(header_names) else "N/A"
                    result.append(f"{column_letter}({header_name}):'{cell_value}'")
            workbook.close()
            return result
        except Exception as e:
            logger.error(f"❌ Ошибка чтения строки {row_index} из {local_filepath}: {e}")
            return None


# --- Команды бота ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(
            "🤖 Привет! Я могу найти данные по номеру.\n"
            "Используй:\n"
            "• `/s 123456` - поиск по номеру\n"
            "• `/path` - содержимое корневой папки\n"
            "• `/test <номер_строки>` - показать строку по номеру\n"
            "• `@ваш_бот 123456` - упоминание"
        )


async def show_path(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    user_id = update.effective_user.id
    logger.info(f"📤 Пользователь {user_id} запросил /path")
    try:
        await update.message.reply_text("🔍 Получаю содержимое корневой папки...")
        gs = GoogleServices()
        fm = FileManager(gs.drive)
        root_folder_id = PARENT_FOLDER_ID
        try:
            root_folder_info = gs.drive.files().get(fileId=root_folder_id, fields="name").execute()
            root_folder_name = root_folder_info.get('name', 'Без названия')
        except Exception:
            root_folder_name = 'Неизвестная корневая папка'
        path_info = f"📂 Корневая папка Google Drive: `{root_folder_name}` (ID: `{root_folder_id}`)\n"
        try:
            items = fm.list_files_in_folder(root_folder_id, max_results=100)
            if not items:
                path_info += "Папка пуста."
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
            path_info += f"❌ Ошибка: {e}\n"
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
        error_msg = f"❌ Ошибка при получении структуры папок: {e}"
        logger.error(error_msg, exc_info=True)
        if update.message:
            await update.message.reply_text(error_msg)


async def test_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает строку по номеру."""
    if not update.message:
        return
    user_id = update.effective_user.id
    logger.info(f"📤 Пользователь {user_id} запросил команду /test")
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "❌ Неверный формат. Используй: `/test <номер_строки>`\n"
            "Пример: `/test 5`",
            parse_mode='Markdown'
        )
        return
    try:
        row_num = int(context.args[0])
        if row_num <= 0:
            await update.message.reply_text("❌ Номер строки должен быть положительным числом.", parse_mode='Markdown')
            return
    except ValueError:
        await update.message.reply_text("❌ Номер строки должен быть целым числом.", parse_mode='Markdown')
        return

    # Ищем последний файл за сегодня или вчера
    gs = GoogleServices()
    fm = FileManager(gs.drive)
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    dates_to_try = [today, yesterday]
    file_id = None
    used_date = None

    for target_date in dates_to_try:
        filename = f"АПП_Склад_{target_date.strftime('%d%m%y')}_{CITY}.xlsm"
        root_folder = PARENT_FOLDER_ID
        acts_folder = fm.find_folder(root_folder, "акты")
        if not acts_folder:
            continue
        month_names = ["январь", "февраль", "март", "апрель", "май", "июнь",
                       "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]
        month_num = target_date.month
        month_folder_name = f"{target_date.strftime('%m')} - {month_names[month_num - 1]}"
        month_folder = fm.find_folder(acts_folder, month_folder_name)
        if not month_folder:
            continue
        date_folder_name = target_date.strftime('%d%m%y')
        date_folder = fm.find_folder(month_folder, date_folder_name)
        if not date_folder:
            continue
        file_id = fm.find_file(date_folder, filename)
        if file_id:
            used_date = target_date
            break

    if not file_id:
        await update.message.reply_text("❌ Файл за сегодня или вчера не найден.")
        return

    # Скачиваем и читаем файл
    local_filename = f"local_cache_{used_date.strftime('%Y-%m-%d')}.xlsm"
    local_filepath = os.path.join(LOCAL_CACHE_DIR, local_filename)

    # Получаем время модификации
    drive_modified_time = fm.get_file_modified_time(file_id)
    if not drive_modified_time:
        await update.message.reply_text("❌ Не удалось получить время изменения файла.")
        return

    # Скачиваем, если нужно
    download_needed = True
    if os.path.exists(local_filepath):
        local_modified_time = datetime.fromtimestamp(os.path.getmtime(local_filepath), tz=timezone.utc)
        if drive_modified_time <= local_modified_time:
            download_needed = False
    if download_needed:
        download_success = fm.download_file(file_id, local_filepath)
        if not download_success:
            await update.message.reply_text("❌ Не удалось скачать файл.")
            return

    # Получаем строку
    lds = LocalDataSearcher()
    row_data = lds.get_row_by_index(local_filepath, row_num)
    if not row_data:
        await update.message.reply_text(f"❌ Строка {row_num} не найдена.")
        return

    # Формируем красивый ответ
    response_lines = []
    for item in row_data:
        parts = item.split(":", 1)
        if len(parts) == 2:
            key = parts[0].strip()
            value = parts[1].strip().replace("'", "")
            response_lines.append(f"• {key}: {value}")
    full_response = "\n".join(response_lines)
    await update.message.reply_text(f"📋 Строка {row_num}:\n{full_response}", parse_mode='Markdown')


async def handle_query(update: Update, context: ContextTypes.DEFAULT_TYPE, query: str) -> None:
    """Обработка запроса по номеру."""
    if not update.message:
        return
    message = update.message
    user_id = message.from_user.id if message.from_user else "N/A"
    number = extract_number(query)
    if not number:
        await message.reply_text("❌ Не указан корректный номер. Пример: `123456`", parse_mode='Markdown')
        return
    await message.reply_text(f"🔍 Поиск по номеру: `{number}`", parse_mode='Markdown')

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

        for target_date in dates_to_try:
            filename = f"АПП_Склад_{target_date.strftime('%d%m%y')}_{CITY}.xlsm"
            root_folder = PARENT_FOLDER_ID
            acts_folder = fm.find_folder(root_folder, "акты")
            if not acts_folder:
                continue
            month_names = ["январь", "февраль", "март", "апрель", "май", "июнь",
                           "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]
            month_num = target_date.month
            month_folder_name = f"{target_date.strftime('%m')} - {month_names[month_num - 1]}"
            month_folder = fm.find_folder(acts_folder, month_folder_name)
            if not month_folder:
                continue
            date_folder_name = target_date.strftime('%d%m%y')
            date_folder = fm.find_folder(month_folder, date_folder_name)
            if not date_folder:
                continue
            file_id = fm.find_file(date_folder, filename)
            if file_id:
                used_date = target_date
                break

        if not file_id:
            await message.reply_text("❌ Файл за сегодня или вчера не найден.")
            return

        local_filename = f"local_cache_{used_date.strftime('%Y-%m-%d')}.xlsm"
        local_filepath = os.path.join(LOCAL_CACHE_DIR, local_filename)

        drive_modified_time = fm.get_file_modified_time(file_id)
        if not drive_modified_time:
            await message.reply_text("❌ Не удалось получить время изменения файла.")
            return

        download_needed = True
        if os.path.exists(local_filepath):
            local_modified_time = datetime.fromtimestamp(os.path.getmtime(local_filepath), tz=timezone.utc)
            if drive_modified_time <= local_modified_time:
                download_needed = False
        if download_needed:
            download_success = fm.download_file(file_id, local_filepath)
            if not download_success:
                await message.reply_text("❌ Не удалось скачать файл.")
                return

        results = lds.search_by_number(local_filepath, number)
        if not results:
            await message.reply_text(f"❌ Запись с номером `{number}` не найдена.")
            return

        # Формируем красивый ответ без обозначений столбцов
        response_lines = []
        for i, result in enumerate(results, start=1):
            parts = result.split(" | ")
            if len(parts) >= 15:
                sn = parts[5]  # СН (F)
                type_terminal = parts[4] if len(parts) > 4 else "N/A"  # Тип (E)
                model = parts[6] if len(parts) > 6 else "N/A"  # Модель (G)
                status = parts[8] if len(parts) > 8 else "N/A"  # Статус (I)
                storage = parts[13] if len(parts) > 13 else "N/A"  # Место хранения (N)
                line = f"<b>СН {sn}</b>\n"
                line += "☁️ <b>Информация:</b>\n"
                line += f"    • Тип терминала: <code>{type_terminal}</code>\n"
                line += f"    • Модель терминала: <code>{model}</code>\n"
                line += f"    • Статус терминала: <code>{status}</code>\n"
                line += f"    • Место хранения терминала: <code>{storage}</code>"
                if len(results) > 1:
                    line = f"<b>--- Результат {i} ---</b>\n{line}\n"
                response_lines.append(line)
            else:
                response_lines.append(f"<pre>{result}</pre>")
        full_response = "\n".join(response_lines)
        if len(full_response) > 4096:
            full_response = full_response[:4050] + "\n<i>... (обрезано)</i>"
        await message.reply_text(full_response, parse_mode='HTML')
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке запроса '{query}': {e}", exc_info=True)
        await message.reply_text("❌ Произошла ошибка при поиске данных.")


def extract_number(query: str) -> Optional[str]:
    if not query:
        return None
    clean_query = query.strip()
    if re.fullmatch(r'[A-Za-z0-9\-]+', clean_query):
        return clean_query
    return None


async def handle_any_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик любого текста."""
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()
    if text.startswith('/') or re.match(rf'@{re.escape(context.bot.username)}\b', text, re.IGNORECASE):
        return
    response = (
        "Кожаный ублюдок, ты что-то не то ввел.\n"
        "Я понимаю только:\n"
        "• `/start` - начать работу\n"
        "• `/s 123456` - поиск по номеру\n"
        "• `/path` - содержимое папки\n"
        "• `/test <номер_строки>` - показать строку\n"
        "• `@ваш_бот 123456` - упоминание"
    )
    await update.message.reply_text(response, parse_mode='Markdown')


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка сообщений."""
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
    else:
        await handle_any_text(update, context)


def main() -> None:
    try:
        init_config()
    except RuntimeError as e:
        logger.critical(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        print(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        return
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("path", show_path))
    app.add_handler(CommandHandler("test", test_command))
    app.add_handler(CommandHandler("s", lambda u, c: handle_query(u, c, ' '.join(c.args) if c.args else '')))
    app.add_handler(MessageHandler(filters.COMMAND, unknown_command))
    app.add_handler(MessageHandler(
        filters.TEXT & (filters.ChatType.CHANNEL | filters.ChatType.GROUPS | filters.ChatType.PRIVATE),
        handle_message
    ))
    logger.info("🚀 Бот запущен.")
    app.run_polling()


if __name__ == '__main__':
    main()
