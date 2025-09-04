import logging
import re
import os
import base64
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import openpyxl
import warnings

# Подавление предупреждений от openpyxl
warnings.filterwarnings("ignore", message="Data Validation extension is not supported", category=UserWarning)

# --- Настройка логирования ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --- Конфигурация ---
CITY = 'Воронеж'
SCOPES = ['https://www.googleapis.com/auth/drive']
LOCAL_CACHE_DIR = "./local_cache"

# --- Глобальные переменные ---
CREDENTIALS_FILE: str = ""
TELEGRAM_TOKEN: str = ""
PARENT_FOLDER_ID: str = ""
TEMP_FOLDER_ID: str = ""
ROOT_FOLDER_YEAR: str = ""

# --- Разрешённые пользователи ---
ALLOWED_USERS = {'tupikin_ik', 'yoptvayou'}

def get_credentials_path() -> str:
    """Декодирует Google Credentials из переменной окружения."""
    encoded = os.getenv("GOOGLE_CREDS_BASE64")
    if not encoded:
        raise RuntimeError("GOOGLE_CREDS_BASE64 не найдена!")
    try:
        decoded = base64.b64decode(encoded).decode('utf-8')
        creds = json.loads(decoded)
        temp_path = "temp_google_creds.json"
        with open(temp_path, 'w') as f:
            json.dump(creds, f)
        logger.info(f"✅ Учетные данные сохранены: {temp_path}")
        return temp_path
    except Exception as e:
        logger.error(f"❌ Ошибка декодирования GOOGLE_CREDS_BASE64: {e}")
        raise

def init_config():
    """Инициализация конфигурации."""
    global CREDENTIALS_FILE, TELEGRAM_TOKEN, PARENT_FOLDER_ID, TEMP_FOLDER_ID, ROOT_FOLDER_YEAR
    CREDENTIALS_FILE = get_credentials_path()
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
    PARENT_FOLDER_ID = os.getenv("PARENT_FOLDER_ID", "")
    TEMP_FOLDER_ID = os.getenv("TEMP_FOLDER_ID", "")
    ROOT_FOLDER_YEAR = str(datetime.now().year)
    if not TELEGRAM_TOKEN or not PARENT_FOLDER_ID:
        missing = []
        if not TELEGRAM_TOKEN: missing.append("TELEGRAM_TOKEN")
        if not PARENT_FOLDER_ID: missing.append("PARENT_FOLDER_ID")
        raise RuntimeError(f"❌ Отсутствуют переменные окружения: {', '.join(missing)}")
    os.makedirs(LOCAL_CACHE_DIR, exist_ok=True)
    logger.info(f"📁 Локальный кэш: {os.path.abspath(LOCAL_CACHE_DIR)}")

class GoogleServices:
    """Одиночка для Google API."""
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
            cls._instance.drive = build('drive', 'v3', credentials=creds)
        return cls._instance

def extract_number(query: str) -> Optional[str]:
    """
    Извлекает номер: только буквы, цифры и тире.
    Возвращает очищенную строку или None.
    """
    if not query:
        return None
    clean = re.sub(r'\s+', '', query.strip())  # Убираем все пробелы
    if re.fullmatch(r'[A-Za-z0-9\-]+', clean):
        return clean
    return None

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Приветствие — нейтральный стиль."""
    if not update.message:
        return
    user = update.effective_user
    chat_type = update.message.chat.type
    if chat_type == 'private' and (not user.username or user.username not in ALLOWED_USERS):
        await update.message.reply_text(
            "Доступ ограничен.\n"
            "Обратитесь к администратору для получения прав."
        )
        return
    await update.message.reply_text(
        "Добро пожаловать.\n"
        "Доступные команды:\n"
        "• <code>/s 123456</code> — найти терминал по серийному номеру\n"
        "• <code>/path</code> — показать содержимое корневой папки\n"
        "• <code>@ваш_бот 123456</code> — вызвать поиск по упоминанию"
    )

async def show_path(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать содержимое папки — нейтральный стиль."""
    if not update.message:
        return
    user = update.effective_user
    if update.message.chat.type == 'private' and (not user.username or user.username not in ALLOWED_USERS):
        await update.message.reply_text(
            "Доступ ограничен.\n"
            "Обратитесь к администратору для получения прав."
        )
        return
    try:
        gs = GoogleServices()
        fm = FileManager(gs.drive)
        root_id = PARENT_FOLDER_ID
        items = fm.list_files_in_folder(root_id, max_results=100)
        text = f"🗂 <b>Корневая папка</b> (ID: <code>{root_id}</code>)\n"
        if not items:
            text += "Содержимое отсутствует."
        else:
            folders = [i for i in items if i['mimeType'] == 'application/vnd.google-apps.folder']
            files = [i for i in items if i['mimeType'] != 'application/vnd.google-apps.folder']
            if folders:
                text += "<b>Подпапки:</b>\n"
                for f in sorted(folders, key=lambda x: x['name'].lower()):
                    text += f"📁 <code>{f['name']}/</code>\n"
                text += "\n"
            if files:
                text += "<b>Файлы:</b>\n"
                for f in sorted(files, key=lambda x: x['name'].lower()):
                    size = f" ({f['size']} байт)" if f.get('size') else ""
                    text += f"📄 <code>{f['name']}</code>{size}\n"
        await update.message.reply_text(text, parse_mode='HTML')
    except Exception as e:
        logger.error(f"❌ Ошибка /path: {e}")
        await update.message.reply_text(
            "Произошла ошибка при получении списка файлов.\n"
            "Попробуйте позже."
        )

class FileManager:
    """Работа с Google Drive."""
    def __init__(self, drive):
        self.drive = drive

    def find_folder(self, parent_id: str, name: str) -> Optional[str]:
        query = f"mimeType='application/vnd.google-apps.folder' and name='{name}' and '{parent_id}' in parents and trashed=false"
        try:
            res = self.drive.files().list(q=query, fields="files(id)").execute()
            return res['files'][0]['id'] if res['files'] else None
        except Exception as e:
            logger.error(f"❌ Ошибка поиска папки '{name}': {e}")
            return None

    def find_file(self, folder_id: str, filename: str) -> Optional[str]:
        query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
        try:
            res = self.drive.files().list(q=query, fields="files(id)").execute()
            return res['files'][0]['id'] if res['files'] else None
        except Exception as e:
            logger.error(f"❌ Ошибка поиска файла '{filename}': {e}")
            return None

    def get_file_modified_time(self, file_id: str) -> Optional[datetime]:
        try:
            info = self.drive.files().get(fileId=file_id, fields="modifiedTime").execute()
            t = info['modifiedTime']
            dt = datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ")
            return dt.replace(tzinfo=timezone.utc)
        except Exception as e:
            logger.error(f"❌ Ошибка получения времени файла {file_id}: {e}")
            return None

    def download_file(self, file_id: str, local_path: str) -> bool:
        try:
            request = self.drive.files().get_media(fileId=file_id)
            with open(local_path, 'wb') as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
            logger.info(f"✅ Файл {file_id} скачан в {local_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка скачивания {file_id}: {e}")
            return False

    def list_files_in_folder(self, folder_id: str, max_results: int = 100) -> List[Dict]:
        try:
            query = f"'{folder_id}' in parents and trashed=false"
            res = self.drive.files().list(q=query, pageSize=max_results, fields="files(id, name, mimeType, size)").execute()
            return res.get('files', [])
        except Exception as e:
            logger.error(f"❌ Ошибка списка файлов в папке {folder_id}: {e}")
            return []

class LocalDataSearcher:
    """Поиск в Excel по СН и формирование ответа по статусу."""
    @staticmethod
    def search_by_number(filepath: str, number: str) -> List[str]:
        number_upper = number.strip().upper()
        results = []
        try:
            wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
            sheet = wb["Терминалы"] if "Терминалы" in wb.sheetnames else None
            if not sheet:
                logger.warning(f"⚠️ Лист 'Терминалы' не найден в {filepath}")
                wb.close()
                return results

            # Индексы столбцов (A=1): E=5, G=7, H=8, I=9, N=14, O=15, P=16, Q=17
            for row in sheet.iter_rows(min_row=2, values_only=True):
                if len(row) < 17 or not row[5]:  # СН в F (6)
                    continue
                sn = str(row[5]).strip().upper()
                if sn != number_upper:
                    continue

                equipment_type = str(row[4]).strip() if row[4] else "Не указано"
                model = str(row[6]).strip() if row[6] else "Не указано"
                request_num = str(row[7]).strip() if row[7] else "Не указано"
                status = str(row[8]).strip() if row[8] else "Не указано"
                storage = str(row[13]).strip() if row[13] else "Не указано"
                issue_status = str(row[14]).strip() if row[14] else ""
                engineer = str(row[15]).strip() if row[15] else "Не указано"
                issue_date = str(row[16]).strip() if row[16] else "Не указано"

                # Формируем базовый ответ
                response_parts = [
                    f"<b>СН:</b> <code>{sn}</code>",
                    f"<b>Тип оборудования:</b> <code>{equipment_type}</code>",
                    f"<b>Модель терминала:</b> <code>{model}</code>",
                    f"<b>Статус оборудования:</b> <code>{status}</code>"
                ]

                # Дополнительные поля по условиям
                if status == "На складе":
                    if storage != "Не указано":
                        response_parts.append(f"<b>Место на складе:</b> <code>{storage}</code>")

                elif status == "Зарезервировано" and issue_status == "Выдан":
                    response_parts.extend([
                        f"<b>Номер заявки:</b> <code>{request_num}</code>",
                        f"<b>Выдан инженеру:</b> <code>{engineer}</code>",
                        f"<b>Дата выдачи:</b> <code>{issue_date}</code>"
                    ])

                elif status in ["Не работоспособно", "Выведено из эксплуатации"]:
                    if storage != "Не указано":
                        response_parts.append(f"<b>Место на складе:</b> <code>{storage}</code>")

                # Остальные статусы — только базовые данные

                result_text = "ℹ️ <b>Информация о терминале</b>\n" + "\n".join(response_parts)
                results.append(result_text)

            wb.close()
        except Exception as e:
            logger.error(f"❌ Ошибка чтения Excel {filepath}: {e}", exc_info=True)
        return results

async def handle_search(update: Update, query: str):
    """Общая логика поиска — нейтральный стиль."""
    if not update.message:
        return
    user = update.effective_user
    if update.message.chat.type == 'private' and (not user.username or user.username not in ALLOWED_USERS):
        await update.message.reply_text(
            "Доступ ограничен.\n"
            "Обратитесь к администратору для получения прав."
        )
        return

    number = extract_number(query)
    if not number:
        await update.message.reply_text(
            "Некорректный формат номера.\n"
            "Введите серийный номер в формате: AB123456 (без пробелов).",
            parse_mode='HTML'
        )
        return

    await update.message.reply_text(f"🔍 Поиск терминала: <code>{number}</code>...", parse_mode='HTML')

    try:
        gs = GoogleServices()
        fm = FileManager(gs.drive)
        lds = LocalDataSearcher()
        today = datetime.now()
        file_id = None
        used_date = None

        # Поиск файла за последние 30 дней
        for days_back in range(31):
            target_date = today - timedelta(days=days_back)
            filename = f"АПП_Склад_{target_date.strftime('%d%m%y')}_{CITY}.xlsm"

            acts = fm.find_folder(PARENT_FOLDER_ID, "акты")
            if not acts: continue

            month_num = target_date.month
            month_name = ["январь", "февраль", "март", "апрель", "май", "июнь",
                          "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"][month_num - 1]
            month_folder = fm.find_folder(acts, f"{target_date.strftime('%m')} - {month_name}")
            if not month_folder: continue

            date_folder = fm.find_folder(month_folder, target_date.strftime('%d%m%y'))
            if not date_folder: continue

            file_id = fm.find_file(date_folder, filename)
            if file_id:
                used_date = target_date
                break

        if not file_id:
            await update.message.reply_text(
                "Файл с данными не найден.\n"
                "Возможно, данные ещё не загружены за указанный период."
            )
            return

        local_file = os.path.join(LOCAL_CACHE_DIR, f"cache_{used_date.strftime('%Y%m%d')}.xlsm")
        drive_time = fm.get_file_modified_time(file_id)
        if not drive_time:
            await update.message.reply_text("Не удалось получить дату изменения файла.")
            return

        # Проверка необходимости скачивания
        download_needed = True
        if os.path.exists(local_file):
            local_time = datetime.fromtimestamp(os.path.getmtime(local_file), tz=timezone.utc)
            if drive_time <= local_time:
                download_needed = False

        if download_needed:
            if not fm.download_file(file_id, local_file):
                await update.message.reply_text("Не удалось скачать файл с данными.")
                return

        results = lds.search_by_number(local_file, number)
        if not results:
            await update.message.reply_text(
                f"Терминал с СН <code>{number}</code> не найден в базе данных.\n"
                "Проверьте правильность ввода или обратитесь к администратору.",
                parse_mode='HTML'
            )
            return

        for result in results:
            if len(result) > 4096:
                result = result[:4050] + "\n<i>... (обрезано)</i>"
            await update.message.reply_text(result, parse_mode='HTML')

    except Exception as e:
        logger.error(f"❌ Ошибка поиска: {e}", exc_info=True)
        await update.message.reply_text(
            "Произошла ошибка при выполнении поиска.\n"
            "Повторите попытку позже."
        )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка всех сообщений — нейтральный стиль."""
    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()
    bot_username = context.bot.username.lower()

    if text.startswith("/s"):
        query = text[2:].strip()
        if not query:
            await update.message.reply_text(
                "Укажите серийный номер после команды.\n"
                "Пример: <code>/s AB123456</code>",
                parse_mode='HTML'
            )
            return
        await handle_search(update, query)
        return

    mention_match = re.match(rf'@{re.escape(bot_username)}\s*(.+)', text, re.IGNORECASE)
    if mention_match:
        query = mention_match.group(1).strip()
        if not query:
            await update.message.reply_text(
                "Укажите серийный номер после упоминания бота.\n"
                "Пример: @ваш_бот AB123456",
                parse_mode='HTML'
            )
            return
        await handle_search(update, query)
        return

    if text.startswith('/'):
        await update.message.reply_text(
            "Доступные команды:\n"
            "• <code>/s 123456</code> — поиск терминала\n"
            "• <code>/path</code> — просмотр папки\n"
            "• <code>@ваш_бот 123456</code> — быстрый поиск",
            parse_mode='HTML'
        )

def main():
    try:
        init_config()
    except Exception as e:
        logger.critical(f"❌ Критическая ошибка: {e}")
        return

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("path", show_path))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.COMMAND, handle_message))

    logger.info("🚀 Бот запущен. Готов к работе.")
    app.run_polling()

if __name__ == '__main__':
    main()
