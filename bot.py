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
import io
import openpyxl
from openpyxl.worksheet.worksheet import Worksheet
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
    """Приветствие."""
    if not update.message:
        return
    user = update.effective_user
    chat_type = update.message.chat.type

    if chat_type == 'private' and (not user.username or user.username not in ALLOWED_USERS):
        await update.message.reply_text("Слышь, кожаный мешок, я переписываюсь в личке только с батей.")
        return

    await update.message.reply_text(
        "🤖 Привет! Я могу найти данные по номеру.\n"
        "Используй:\n"
        "• `/s 123456` — поиск по номеру\n"
        "• `/path` — показать содержимое папки\n"
        "• `@ваш_бот 123456` — упоминание в группе"
    )

async def show_path(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать содержимое корневой папки."""
    if not update.message:
        return
    user = update.effective_user
    if update.message.chat.type == 'private' and (not user.username or user.username not in ALLOWED_USERS):
        await update.message.reply_text("Слышь, кожаный мешок, я переписываюсь в личке только с батей.")
        return

    try:
        gs = GoogleServices()
        fm = FileManager(gs.drive)
        root_id = PARENT_FOLDER_ID
        items = fm.list_files_in_folder(root_id, max_results=100)

        text = f"📂 Корневая папка (ID: `{root_id}`)\n"
        if not items:
            text += "Пусто."
        else:
            folders = [i for i in items if i['mimeType'] == 'application/vnd.google-apps.folder']
            files = [i for i in items if i['mimeType'] != 'application/vnd.google-apps.folder']

            for f in sorted(folders, key=lambda x: x['name'].lower()):
                text += f"📁 `{f['name']}/` (ID: `{f['id']}`)\n"
            for f in sorted(files, key=lambda x: x['name'].lower()):
                size = f" ({f['size']} байт)" if f.get('size') else ""
                text += f"📄 `{f['name']}`{size} (ID: `{f['id']}`)\n"

        await update.message.reply_text(text, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"❌ Ошибка /path: {e}")
        await update.message.reply_text("❌ Ошибка при получении папки.")

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
    """Поиск в Excel."""
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

        for row in sheet.iter_rows(min_row=2, values_only=True):
            if len(row) < 17 or not row[5]:  # Проверяем, что есть хотя бы 17 столбцов и СН (столбец 6, индекс 5)
                continue

            sn = str(row[5]).strip().upper()
            if sn != number_upper:
                continue

            # Извлекаем нужные поля по индексам (0-based)
            equipment_type = str(row[4]).strip() if row[4] else "N/A"  # E (5)
            model = str(row[6]).strip() if row[6] else "N/A"           # G (7)
            status = str(row[8]).strip() if row[8] else "N/A"          # I (9)
            issue_status = str(row[9]).strip() if row[9] else ""       # J (10) — "Выдан" или пусто
            request_num = str(row[7]).strip() if row[7] else "N/A"     # H (8) — Номер заявки
            engineer = str(row[15]).strip() if row[15] else "N/A"      # P (16) — Выдан инженеру
            issue_date = str(row[16]).strip() if row[16] else "N/A"    # Q (17) — Дата выдачи
            storage = str(row[13]).strip() if row[13] else "N/A"       # N (14) — Место хранения

            # Формируем ответ в зависимости от статуса
            response_parts = [
                f"    • Тип оборудования: <code>{equipment_type}</code>",
                f"    • Модель оборудования: <code>{model}</code>",
                f"    • Статус: <code>{status}</code>"
            ]

            if status == "На складе":
                response_parts.append(f"    • Место хранения: <code>{storage}</code>")

            elif status == "Зарезервировано":
                if issue_status == "Выдан":
                    response_parts.extend([
                        f"    • Номер заявки: <code>{request_num}</code>",
                        f"    • Выдан инженеру: <code>{engineer}</code>",
                        f"    • Дата выдачи: <code>{issue_date}</code>"
                    ])
                # Если "Зарезервировано", но не "Выдан" — ничего дополнительно не добавляем

            else:
                # Для всех остальных статусов — только базовые 3 поля (уже добавлены)
                pass

            # Формируем итоговый текст
            result_text = "<b>СН " + str(row[5]) + "</b>\n☁️ <b>Информация:</b>\n" + "\n".join(response_parts)
            results.append(result_text)

        wb.close()
    except Exception as e:
        logger.error(f"❌ Ошибка чтения Excel {filepath}: {e}", exc_info=True)
    return results

async def handle_search(update: Update, query: str):
    """Общая логика поиска."""
    if not update.message:
        return
    user = update.effective_user
    if update.message.chat.type == 'private' and (not user.username or user.username not in ALLOWED_USERS):
        await update.message.reply_text("Слышь, кожаный мешок, я переписываюсь в личке только с батей.")
        return

    number = extract_number(query)
    if not number:
        await update.message.reply_text(
            "❌ Укажите корректный номер. Пример: `123456` или `AB123456`",
            parse_mode='Markdown'
        )
        return

    await update.message.reply_text(f"🔍 Поиск по номеру: `{number}`", parse_mode='Markdown')

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

            # Путь: PARENT_FOLDER_ID → "акты" → "01 - январь" → "010124" → файл
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
            await update.message.reply_text("Сорян, файл не найден, искать негде.")
            return

        # Подготовка локального файла
        local_file = os.path.join(LOCAL_CACHE_DIR, f"cache_{used_date.strftime('%Y%m%d')}.xlsm")
        drive_time = fm.get_file_modified_time(file_id)

        if not drive_time:
            await update.message.reply_text("❌ Не удалось получить время файла на Drive.")
            return

        download_needed = True
        if os.path.exists(local_file):
            local_time = datetime.fromtimestamp(os.path.getmtime(local_file), tz=timezone.utc)
            if drive_time <= local_time:
                download_needed = False

        if download_needed:
            if not fm.download_file(file_id, local_file):
                await update.message.reply_text("❌ Не удалось скачать файл.")
                return

        # Поиск в файле
        results = lds.search_by_number(local_file, number)
        if not results:
            await update.message.reply_text("Кожаный мешок, проверь СН. Я не могу его найти.")
            return

        # Формирование ответа
        lines = []
        for i, r in enumerate(results, 1):
            line = (
                f"<b>СН {r['sn']}</b>\n"
                "☁️ <b>Информация:</b>\n"
                f"    • Тип терминала: <code>{r['type']}</code>\n"
                f"    • Модель: <code>{r['model']}</code>\n"
                f"    • Статус: <code>{r['status']}</code>\n"
                f"    • Место хранения: <code>{r['storage']}</code>"
            )
            if len(results) > 1:
                line = f"<b>--- Результат {i} ---</b>\n{line}"
            lines.append(line)

        response = "\n\n".join(lines)
        if len(response) > 4096:
            response = response[:4050] + "\n<i>... (обрезано)</i>"

        await update.message.reply_text(response, parse_mode='HTML')
    except Exception as e:
        logger.error(f"❌ Ошибка поиска: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка при поиске.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка всех сообщений."""
    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()
    bot_username = context.bot.username.lower()

    # Парсим команду /s
    if text.startswith("/s"):
        # Извлекаем всё после /s
        query = text[2:].strip()
        if not query:
            await update.message.reply_text(
                "❌ Укажите номер после `/s`. Пример: `/s 123456`",
                parse_mode='Markdown'
            )
            return
        await handle_search(update, query)
        return

    # Обработка упоминания
    mention_match = re.match(rf'@{re.escape(bot_username)}\s*(.+)', text, re.IGNORECASE)
    if mention_match:
        query = mention_match.group(1).strip()
        if not query:
            await update.message.reply_text(
                "❌ Укажите номер после упоминания. Пример: `@ваш_бот 123456`",
                parse_mode='Markdown'
            )
            return
        await handle_search(update, query)
        return

    # Неизвестная команда
    if text.startswith('/'):
        await update.message.reply_text(
            "Кожаный, я понимаю только:\n"
            "• `/start`\n"
            "• `/s 123456`\n"
            "• `@ваш_бот 123456`",
            parse_mode='Markdown'
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
    app.add_handler(MessageHandler(filters.COMMAND, handle_message))  # Для /s

    logger.info("🚀 Бот запущен.")
    app.run_polling()

if __name__ == '__main__':
    main()
