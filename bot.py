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
BLACKLIST_FILE_ID: str = ""
WHITELIST_FILE_ID: str = ""

# --- Разрешённые пользователи (администраторы) ---
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
    global CREDENTIALS_FILE, TELEGRAM_TOKEN, PARENT_FOLDER_ID, TEMP_FOLDER_ID, ROOT_FOLDER_YEAR, BLACKLIST_FILE_ID, WHITELIST_FILE_ID
    CREDENTIALS_FILE = get_credentials_path()
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
    PARENT_FOLDER_ID = os.getenv("PARENT_FOLDER_ID", "")
    TEMP_FOLDER_ID = os.getenv("TEMP_FOLDER_ID", "")
    BLACKLIST_FILE_ID = os.getenv("BLACKLIST_FILE_ID", "")
    WHITELIST_FILE_ID = os.getenv("WHITELIST_FILE_ID", "")
    ROOT_FOLDER_YEAR = str(datetime.now().year)

    if not TELEGRAM_TOKEN or not PARENT_FOLDER_ID or not BLACKLIST_FILE_ID or not WHITELIST_FILE_ID:
        missing = []
        if not TELEGRAM_TOKEN: missing.append("TELEGRAM_TOKEN")
        if not PARENT_FOLDER_ID: missing.append("PARENT_FOLDER_ID")
        if not BLACKLIST_FILE_ID: missing.append("BLACKLIST_FILE_ID")
        if not WHITELIST_FILE_ID: missing.append("WHITELIST_FILE_ID")
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

import io  # Убедитесь, что импортирован

class AccessManager:
    """Управление доступом: чёрный/белый списки по username."""
    def __init__(self, drive_service):
        self.drive = drive_service
        self.blacklist = set()
        self.whitelist = set()

    def download_list(self, file_id: str) -> List[str]:
        """Скачивает файл и возвращает список username (без @, в нижнем регистре)."""
        try:
            request = self.drive.files().get_media(fileId=file_id)
            file_data = io.BytesIO()
            downloader = MediaIoBaseDownload(file_data, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            file_data.seek(0)
            content = file_data.read().decode('utf-8')
            # Очищаем: удаляем @, приводим к нижнему регистру, убираем пробелы
            usernames = []
            for line in content.splitlines():
                cleaned = line.strip().lower().replace('@', '')
                if cleaned:
                    usernames.append(cleaned)
            return usernames
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки списка из файла {file_id}: {e}")
            return []

    def update_lists(self):
        """Загружает чёрный и белый списки."""
        if WHITELIST_FILE_ID:
            self.whitelist = set(self.download_list(WHITELIST_FILE_ID))
            logger.info(f"✅ Загружен белый список: {len(self.whitelist)} пользователей")
        else:
            logger.warning("⚠️ WHITELIST_FILE_ID не задан — белый список пуст")

        if BLACKLIST_FILE_ID:
            self.blacklist = set(self.download_list(BLACKLIST_FILE_ID))
            logger.info(f"✅ Загружен чёрный список: {len(self.blacklist)} пользователей")
        else:
            logger.warning("⚠️ BLACKLIST_FILE_ID не задан — чёрный список пуст")

    def is_allowed(self, username: str) -> bool:
        """
        Проверка доступа по username:
        - Администраторы (ALLOWED_USERS) всегда допущены
        - Чёрный список: приоритет выше
        - Белый список: если задан — только он решает
        """
        if not username:
            return False

        username_lower = username.lower()

        # Администраторы всегда имеют доступ
        if username_lower in {u.lower() for u in ALLOWED_USERS}:
            return True

        # Чёрный список — запрещает доступ, даже если в белом
        if username_lower in self.blacklist:
            return False

        # Если белый список активен — только он определяет доступ
        if self.whitelist and username_lower not in self.whitelist:
            return False

        # Если белый список пуст — разрешаем всех, кроме чёрного
        return True

# Глобальные переменные
access_manager: Optional[AccessManager] = None

def extract_number(query: str) -> Optional[str]:
    """
    Извлекает номер: только буквы, цифры и тире.
    Возвращает очищенную строку или None.
    """
    if not query:
        return None
    # Удаляем все пробелы и лишние символы
    clean = re.sub(r'[^A-Za-z0-9\-]', '', query.strip())
    if clean and re.fullmatch(r'[A-Za-z0-9\-]+', clean):
        return clean.upper()  # Приводим к верхнему регистру для единообразия
    return None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user = update.effective_user
    chat_type = update.message.chat.type
    if chat_type == 'private' and (not user.username or user.username not in ALLOWED_USERS):
        await update.message.reply_text(
            "Ты кто такой, а?\n"
            "Не в списке — не входи.\n"
            "Хочешь доступ — плати бабки или лежи в багажнике до утра."
        )
        return

    await update.message.reply_text(
            "О, смотри-ка — гость на складе!\n"
            "Только не стой как лох у контейнера — говори, что надо.\n"
            "\n"
            "• <code>/s 123456</code> — найти терминал по СН, если не боишься\n"
            "• <code>/path</code> — глянуть, что у нас в папке завалялось\n"
            "• <code>/reload_lists</code> — обновить список предателей и своих\n"
            "• <code>@Sklad_bot 123456</code> — крикни в чатике, я найду\n",
            parse_mode='HTML'
    )


async def show_path(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать содержимое папки — нейтральный стиль."""
    if update.message.chat.type == 'private':
        user = update.effective_user
        if not user.username or not access_manager.is_allowed(user.username):
            await update.message.reply_text(
                "Ты кто такой, а?\n"
                "Не в списке — не входи.\n"
                "Хочешь доступ — плати бабки или лежи в багажнике до утра."
            )
            return

    try:
        gs = GoogleServices()
        fm = FileManager(gs.drive)
        root_id = PARENT_FOLDER_ID
        items = fm.list_files_in_folder(root_id, max_results=100)

        text = f"🗂 <b>Корневая папка</b> (ID: <code>{root_id}</code>)\n"
        if not items:
            text += "Здесь даже паук не селится — пусто."
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

async def reload_lists(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Перезагрузка чёрного и белого списков (только для администраторов)."""
    if not update.message or not update.effective_user:
        return

    user = update.effective_user
    if not user.username or user.username.lower() not in {u.lower() for u in ALLOWED_USERS}:
        await update.message.reply_text("❌ Доступ запрещён.")
        return

    if not access_manager:
        await update.message.reply_text("❌ Система доступа не инициализирована.")
        return

    access_manager.update_lists()
    await update.message.reply_text(
        f"✅ Списки успешно перезагружены.\n"
        f"Белый список: {len(access_manager.whitelist)} пользователей\n"
        f"Чёрный список: {len(access_manager.blacklist)} пользователей"
    )
    logger.info(f"🔄 Администратор {user.username} перезагрузил списки доступа.")


class FileManager:
    """Работа с Google Drive."""
    def __init__(self, drive):
        self.drive = drive

    def find_folder(self, parent_id: str, name: str) -> Optional[str]:
        query = f"mimeType='application/vnd.google-apps.folder' and name='{name}' and '{parent_id}' in parents and trashed=false"
        try:
            res = self.drive.files().list(q=query, fields="files(id)").execute()
            folder_id = res['files'][0]['id'] if res['files'] else None
            if folder_id:
                logger.info(f"🔍 Найдена папка: '{name}' (ID: {folder_id})")
            else:
                logger.debug(f"📁 Папка не найдена: '{name}' в родителе {parent_id}")
            return folder_id
        except Exception as e:
            logger.error(f"❌ Ошибка поиска папки '{name}': {e}")
            return None

    def find_file(self, folder_id: str, filename: str) -> Optional[str]:
        query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
        try:
            res = self.drive.files().list(q=query, fields="files(id)").execute()
            file_id = res['files'][0]['id'] if res['files'] else None
            if file_id:
                logger.info(f"📎 Найден файл: '{filename}' (ID: {file_id})")
            else:
                logger.debug(f"📄 Файл не найден: '{filename}' в папке {folder_id}")
            return file_id
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
            logger.info(f"✅ Файл успешно скачан: ID={file_id}, путь={local_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка при скачивании файла ID={file_id} в {local_path}: {e}")
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

            for row in sheet.iter_rows(min_row=2, values_only=True):
                if len(row) < 17 or not row[5]:  # СН в столбце F (индекс 5)
                    continue

                # Извлечение данных
                sn = str(row[5]).strip().upper()
                if sn != number_upper:
                    continue

                equipment_type = str(row[4]).strip() if row[4] else "Не указано"
                model = str(row[6]).strip() if row[6] else "Не указано"
                request_num = str(row[7]).strip() if row[7] else "Не указано"

                # Регистронезависимая обработка статусов
                raw_status = str(row[8]) if row[8] else ""
                status = raw_status.strip()
                status_lower = status.lower()

                storage = str(row[13]).strip() if row[13] else "Не указано"

                raw_issue_status = str(row[14]) if row[14] else ""
                issue_status = raw_issue_status.strip()
                issue_status_lower = issue_status.lower()

                engineer = str(row[15]).strip() if row[15] else "Не указано"
                issue_date = str(row[16]).strip() if row[16] else "Не указано"

                # Логируем для отладки
                logger.info(f"Найден СН {sn}: статус='{status}', выдан='{issue_status}', место='{storage}'")

                # Формируем базовый ответ
                response_parts = [
                    f"<b>СН:</b> <code>{sn}</code>",
                    f"<b>Тип оборудования:</b> <code>{equipment_type}</code>",
                    f"<b>Модель терминала:</b> <code>{model}</code>",
                    f"<b>Статус оборудования:</b> <code>{status}</code>"
                ]

                # Показываем место на складе, если:
                # - На складе
                # - Не работоспособно / Выведено из эксплуатации
                # - Зарезервировано и выдан — тоже показываем место
                if status_lower == "на складе":
                    response_parts.append(f"<b>Место на складе:</b> <code>{storage}</code>")
                elif status_lower in ["не работоспособно", "выведено из эксплуатации"]:
                    response_parts.append(f"<b>Место на складе:</b> <code>{storage}</code>")
                elif status_lower == "зарезервировано" and issue_status_lower == "выдан":
                    response_parts = [
                        f"💀 <b>СН:</b> <code>{sn}</code>",
                        f"<b>Тип:</b> <code>{equipment_type}</code>",
                        f"<b>Модель:</b> <code>{model}</code>",
                        f"<b>Статус:</b> <code>{status}</code> — как труп в багажнике",
                        f"<b>Место:</b> <code>{storage}</code> — можно разобрать на запчасти"
                    ]
                    result_text = "🗑 <b>Отработал своё</b>" + "".join(response_parts)

                result_text = "ℹ️ <b>Информация о терминале</b>\n" + "\n".join(response_parts)
                results.append(result_text)

            wb.close()
        except Exception as e:
            logger.error(f"❌ Ошибка чтения Excel {filepath}: {e}", exc_info=True)
        return results


async def handle_search(update: Update, query: str):
    """Общая логика поиска — нейтральный стиль."""
    if update.message.chat.type == 'private':
        user = update.effective_user
        if not user.username or not access_manager.is_allowed(user.username):
            await update.message.reply_text(
                "Ты кто такой, а?\n"
                "Не в списке — не входи.\n"
                "Хочешь доступ — плати бабки или лежи в багажнике до утра."
            )
            return

    number = extract_number(query)
    if not number:
        await update.message.reply_text(
                "Ты чё, братан, по пьяни печатаешь?\n"
                "СН — это типа <code>AB123456</code>, без пробелов, без носков в клавиатуре.\n"
                "Попробуй ещё раз, а то выкину в реку.\n",
                parse_mode='HTML'            
        )
        return

    await update.message.reply_text(f"🔍 Копаю в архивах... Где-то был этот <code>{number}</code>...\n"
                                     "Если не спёрли, как в прошлый раз — найду.", parse_mode='HTML')

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
            if not acts:
                continue

            month_num = target_date.month
            month_name = ["январь", "февраль", "март", "апрель", "май", "июнь",
                          "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"][month_num - 1]
            month_folder = fm.find_folder(acts, f"{target_date.strftime('%m')} - {month_name}")
            if not month_folder:
                continue

            date_folder = fm.find_folder(month_folder, target_date.strftime('%d%m%y'))
            if not date_folder:
                continue

            file_id = fm.find_file(date_folder, filename)
            if file_id:
                used_date = target_date
                break

        if not file_id:
            await update.message.reply_text(
                "Архивы пусты, брат.\n"
                "Либо файл сожгли, либо его ещё не подкинули.\n"
                "Приходи завтра — может, кто-нибудь не сдохнет и загрузит.\n"
                )
            return

        logger.info(f"📁 Найден файл: {filename} (ID: {file_id}) от {used_date.strftime('%d.%m.%Y')}")
        local_file = os.path.join(LOCAL_CACHE_DIR, f"cache_{used_date.strftime('%Y%m%d')}.xlsm")
        drive_time = fm.get_file_modified_time(file_id)
        if not drive_time:
            await update.message.reply_text("Не удалось получить дату изменения файла.")
            return

        # Логирование проверки кэша
        logger.info(f"🕒 Время изменения файла в Google Drive: {drive_time.isoformat()}")
        download_needed = True
        if os.path.exists(local_file):
            local_time = datetime.fromtimestamp(os.path.getmtime(local_file), tz=timezone.utc)
            logger.info(f"🕒 Локальное время файла: {local_time.isoformat()}")
            if drive_time <= local_time:
                logger.info(f"✅ Кэш актуален. Скачивание не требуется: {local_file}")
                download_needed = False
            else:
                logger.info(f"⚠️ Файл устарел. Требуется перезагрузка: {local_file}")
        else:
            logger.info(f"📥 Файл не найден в кэше. Будет загружен: {local_file}")

        if download_needed:
            if not fm.download_file(file_id, local_file):
                await update.message.reply_text("Не удалось скачать файл с данными.")
                return
            logger.info(f"📥 Успешно загружен файл: {filename} → {local_file}")
        else:
            logger.info(f"📂 Используется кэшированный файл: {local_file}")

        results = lds.search_by_number(local_file, number)
        if not results:
            await update.message.reply_text(
                f"Терминал с СН <code>{number}</code>?\n"
                "Нету. Ни в базе, ни в подвале, ни в багажнике 'Весты'.\n"
                "Может, он уже в металлоломе... или ты втираешь очки?\n",
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
            "Блять, опять глючит!\n"
            "То сервер падает, то бот тупит...\n"
            "Повтори запрос, а не то закрою тебя в контейнере на сутки."
            )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка сообщений: только команды и упоминания в чатах."""
    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()
    bot_username = context.bot.username.lower()
    chat_type = update.message.chat.type

    # В личных чатах — обрабатываем всё (если доступ разрешён)
    if chat_type == 'private':
        user = update.effective_user
        if not user.username or not access_manager.is_allowed(user.username.lower()):
            await update.message.reply_text(
                "Ты кто такой, а?\n"
                "Не в списке — не входи.\n"
                "Хочешь доступ — плати бабки или лежи в багажнике до утра."
            )
            return
        # Обработка как раньше
        if text.startswith("/s"):
            query = text[2:].strip()
            if not query:
                await update.message.reply_text(
                    "Укажи серийный номер после команды.\n"
                    "Пример: <code>/s AB123456</code>",
                    parse_mode='HTML'
                )
                return
            await handle_search(update, query)
            return
        elif text.startswith('/'):
            await update.message.reply_text(
                "Неизвестная команда.\n"
                "Доступные команды:\n"
                "• <code>/s СН</code> — найти терминал по серийному номеру\n"
                "• <code>/path</code> — показать содержимое корневой папки\n"
                "• <code>/reload_lists</code> — перезагрузить списки доступа",
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                "Используйте:\n"
                "• <code>/s СН</code> — найти терминал по серийному номеру\n"
                "• <code>/path</code> — показать содержимое корневой папки\n"
                "• <code>/reload_lists</code> — перезагрузить списки доступа",
                parse_mode='HTML'
            )
        return

    # В групповых чатах (group/supergroup) — только команды и упоминания
    if chat_type in ['group', 'supergroup']:
        # Проверяем, является ли сообщение командой (всё ещё нужно, чтобы /s работал)
        if text.startswith("/s"):
            # Проверим, адресована ли команда именно этому боту: /s@Sklad_bot
            if f"@{bot_username}" in text.split()[0] or not ' ' in text:  # /s@bot или /s текст
                query = re.sub(r'^/s(?:@[\w_]+)?\s*', '', text).strip()
                if not query:
                    await update.message.reply_text(
                        "Укажи серийный номер после команды.\n"
                        "Пример: <code>/s AB123456</code>",
                        parse_mode='HTML'
                    )
                    return
                await handle_search(update, query)
                return
            else:
                # Это команда /s, но не для нас — игнорируем
                return

        # Проверяем упоминание: @Sklad_bot ...
        mention_match = re.match(rf'@{re.escape(bot_username)}\s*(.+)', text, re.IGNORECASE)
        if mention_match:
            query = mention_match.group(1).strip()
            if not query:
                await update.message.reply_text(
                    "Укажи серийный номер после упоминания бота.\n"
                    "Пример: @Sklad_bot AB123456",
                    parse_mode='HTML'
                )
                return
            await handle_search(update, query)
            return

        # Все остальные сообщения — игнорируем
        return


def main():
    try:
        init_config()
    except Exception as e:
        logger.critical(f"❌ Критическая ошибка: {e}")
        return

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Инициализация AccessManager
    global access_manager
    gs = GoogleServices()
    access_manager = AccessManager(gs.drive)
    access_manager.update_lists()  # Загружаем списки при старте

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("path", show_path))
    app.add_handler(CommandHandler("reload_lists", reload_lists))  # Новая команда
    app.add_handler(MessageHandler(filters.TEXT, handle_message))

    logger.info("🚀 Бот запущен. Готов к работе.")
    app.run_polling()


if __name__ == '__main__':
    main()
