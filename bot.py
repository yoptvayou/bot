import logging
import re
import os
import base64
import json
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ——— КОНФИГУРАЦИЯ ——————————————————————————————————————
def get_credentials_path():
    """Декодируем Google Credentials из переменной окружения и сохраняем во временный файл"""
    try:
        encoded = os.getenv("GOOGLE_CREDS_BASE64")
        if not encoded:
            raise RuntimeError("Переменная GOOGLE_CREDS_BASE64 не найдена!")
        
        # Декодируем base64 → JSON
        decoded = base64.b64decode(encoded).decode('utf-8')
        creds = json.loads(decoded)
        
        # Сохраняем временный файл (нужен для Google API)
        temp_path = "temp_google_creds.json"
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(creds, f, ensure_ascii=False, indent=2)
        
        return temp_path
    except base64.binascii.Error as e:
        logger.error(f"Ошибка декодирования base64: {e}")
        raise RuntimeError("Некорректная переменная GOOGLE_CREDS_BASE64 - не является валидной base64 строкой")
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка парсинга JSON: {e}")
        raise RuntimeError("Некорректная переменная GOOGLE_CREDS_BASE64 - не является валидным JSON")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при создании файла учетных данных: {e}")
        raise RuntimeError(f"Не удалось создать файл учетных данных: {e}")

# Используем временный файл
try:
    CREDENTIALS_FILE = get_credentials_path()           # Ключ сервисного аккаунта
except Exception as e:
    logger.error(f"Критическая ошибка при инициализации учетных данных: {e}")
    raise

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "ВСТАВЬ_ТОКЕН")                 # Токен от @BotFather
PARENT_FOLDER_ID = os.getenv("PARENT_FOLDER_ID", "ID_папки_актов")             # Папка "акты"
TEMP_FOLDER_ID = os.getenv("TEMP_FOLDER_ID", "ID_папки_Bot_Temp_Copies")     # Папка для временных копий
ROOT_FOLDER_YEAR = '2025'
CITY = 'Воронеж'

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
# ————————————————————————————————————————————————————

class GoogleServices:
    """Инкапсуляция Google API сервисов"""
    def __init__(self):
        try:
            creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
            self.drive = build('drive', 'v3', credentials=creds)
            self.sheets = build('sheets', 'v4', credentials=creds)
        except Exception as e:
            logger.error(f"Ошибка инициализации Google API: {e}")
            raise RuntimeError(f"Не удалось инициализировать Google API: {e}")

class FileManager:
    """Работа с файлами и папками на Google Диске"""
    def __init__(self, drive_service):
        self.drive = drive_service

    def find_folder(self, parent_id: str, name: str) -> str:
        """Найти папку по имени"""
        try:
            query = f"mimeType='application/vnd.google-apps.folder' and name='{name}' " \
                    f"and '{parent_id}' in parents and trashed=false"
            result = self.drive.files().list(q=query, fields="files(id, name)").execute()
            files = result.get('files', [])
            if files:
                return files[0]['id']
            return None
        except HttpError as e:
            logger.error(f"HTTP ошибка при поиске папки '{name}': {e}")
            raise RuntimeError(f"Ошибка доступа к Google Drive при поиске папки '{name}'")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при поиске папки '{name}': {e}")
            raise RuntimeError(f"Не удалось найти папку '{name}': {e}")

    def find_file(self, folder_id: str, filename: str) -> str:
        """Найти файл в папке"""
        try:
            query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
            result = self.drive.files().list(q=query, fields="files(id, name)").execute()
            files = result.get('files', [])
            if files:
                return files[0]['id']
            return None
        except HttpError as e:
            logger.error(f"HTTP ошибка при поиске файла '{filename}': {e}")
            raise RuntimeError(f"Ошибка доступа к Google Drive при поиске файла '{filename}'")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при поиске файла '{filename}': {e}")
            raise RuntimeError(f"Не удалось найти файл '{filename}': {e}")

    def create_sheets_copy(self, file_id: str, name: str) -> str:
        """Создать копию Excel как Google Таблицу в TEMP_FOLDER_ID"""
        try:
            metadata = {
                'name': name,
                'parents': [TEMP_FOLDER_ID],
                'mimeType': 'application/vnd.google-apps.spreadsheet'
            }
            file = self.drive.files().copy(fileId=file_id, body=metadata).execute()
            logger.info(f"Копия создана: {name} (ID: {file['id']})")
            return file['id']
        except HttpError as e:
            logger.error(f"HTTP ошибка при создании копии файла: {e}")
            raise RuntimeError(f"Ошибка доступа к Google Drive при создании копии файла")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при создании копии файла: {e}")
            raise RuntimeError(f"Не удалось создать копию файла: {e}")

    def safe_delete(self, file_id: str):
        """Удаляет файл, только если он в TEMP_FOLDER_ID"""
        try:
            file_info = self.drive.files().get(fileId=file_id, fields="parents").execute()
            if TEMP_FOLDER_ID in file_info.get('parents', []):
                self.drive.files().delete(fileId=file_id).execute()
                logger.info(f"✅ Временный файл удалён: {file_id}")
            else:
                logger.warning(f"❌ Удаление запрещено (не в TEMP): {file_id}")
        except HttpError as e:
            if e.resp.status == 404:
                logger.warning(f"Файл {file_id} уже удален или не существует")
            else:
                logger.error(f"HTTP ошибка при удалении временного файла: {e}")
                raise RuntimeError(f"Ошибка доступа к Google Drive при удалении файла")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при удалении временного файла: {e}")
            raise RuntimeError(f"Не удалось удалить временный файл: {e}")

class DataSearcher:
    """Поиск данных в Google Таблице"""
    def __init__(self, sheets_service):
        self.sheets = sheets_service

    def read_sheet(self, spreadsheet_id: str, range_name: str) -> list:
        """Читает данные из таблицы"""
        try:
            result = self.sheets.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            return result.get('values', [])
        except HttpError as e:
            if e.resp.status == 404:
                logger.error(f"Таблица с ID {spreadsheet_id} не найдена")
                raise RuntimeError("Файл не найден или не является таблицей Google")
            elif e.resp.status == 403:
                logger.error(f"Нет доступа к таблице с ID {spreadsheet_id}")
                raise RuntimeError("Нет прав доступа к таблице")
            else:
                logger.error(f"HTTP ошибка при чтении таблицы: {e}")
                raise RuntimeError(f"Ошибка доступа к таблице Google Sheets")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при чтении таблицы: {e}")
            raise RuntimeError(f"Не удалось прочитать таблицу: {e}")

    def search_by_sn(self, rows: list, target_sn: str) -> list:
        """
        Ищет строки, где столбец F (индекс 5) == target_sn (регистронезависимо)
        """
        try:
            target_sn = target_sn.strip().upper()
            results = []
            for row in rows[1:]:  # Пропускаем заголовок
                if len(row) > 5 and row[5].strip().upper() == target_sn:
                    # Берём A-Z, убираем пустые
                    cleaned = [cell.strip() for cell in row[:26] if cell.strip()]
                    results.append(" | ".join(cleaned))
            return results
        except Exception as e:
            logger.error(f"Ошибка при поиске по СН: {e}")
            raise RuntimeError(f"Ошибка при обработке данных таблицы: {e}")

# ——— ОСНОВНОЙ БОТ ————————————————————————————————————

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Приветствие (работает в личке и группах)"""
    try:
        if update.message:
            await update.message.reply_text(
                "🤖 Привет! Я могу найти данные по номеру СН.\n"
                "Используй:\n"
                "• `/s СН12345`\n"
                "• `@ваш_бот СН12345`"
            )
    except Exception as e:
        logger.error(f"Ошибка в команде /start: {e}")

def extract_sn(query: str) -> str | None:
    """
    Извлекает номер СН из строки (например, СН12345)
    Возвращает в верхнем регистре или None
    """
    try:
        match = re.search(r'СН[А-Яа-яA-Za-z0-9]+', query, re.IGNORECASE)
        return match.group(0).strip().upper() if match else None
    except Exception as e:
        logger.error(f"Ошибка при извлечении СН из запроса: {e}")
        return None

async def handle_query(update: Update, context: ContextTypes.DEFAULT_TYPE, query: str):
    """
    Общая логика обработки запроса
    """
    try:
        if not update.message:
            logger.warning("Получено обновление без сообщения")
            return
            
        message = update.message
        
        sn = extract_sn(query)
        if not sn:
            await message.reply_text("❌ Не указан номер СН. Пример: `СН12345`", parse_mode='Markdown')
            return

        await message.reply_text(f"🔍 Поиск по номеру: `{sn}`", parse_mode='Markdown')

        # Инициализация сервисов
        try:
            gs = GoogleServices()
            fm = FileManager(gs.drive)
            ds = DataSearcher(gs.sheets)
        except RuntimeError as e:
            await message.reply_text(f"❌ Ошибка инициализации сервисов: {e}")
            return
        except Exception as e:
            logger.error(f"Неожиданная ошибка инициализации сервисов: {e}")
            await message.reply_text("❌ Критическая ошибка инициализации сервисов")
            return

        # Поиск файла: сегодня или вчера
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        dates_to_try = [today, yesterday]
        file_id = None
        used_date = None

        for target_date in dates_to_try:
            try:
                filename = f"АПП_Склад_{target_date.strftime('%d%m%y')}_{CITY}.xlsm"
                
                # Найти папку года (ROOT_FOLDER_YEAR)
                root_folder = fm.find_folder(PARENT_FOLDER_ID, ROOT_FOLDER_YEAR)
                if not root_folder:
                    logger.warning(f"Папка года '{ROOT_FOLDER_YEAR}' не найдена")
                    continue

                # Найти папку месяца: "01 - январь"
                month_names = ["январь", "февраль", "март", "апрель", "май", "июнь",
                               "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]
                month_num = target_date.month
                month_folder_name = f"{target_date.strftime('%m')} - {month_names[month_num - 1]}"
                month_folder = fm.find_folder(root_folder, month_folder_name)
                if not month_folder:
                    logger.warning(f"Папка месяца '{month_folder_name}' не найдена")
                    continue

                # Найти файл в папке месяца
                file_id = fm.find_file(month_folder, filename)
                if file_id:
                    used_date = target_date
                    break
            except RuntimeError as e:
                logger.error(f"Ошибка при поиске файла для даты {target_date}: {e}")
                continue
            except Exception as e:
                logger.error(f"Неожиданная ошибка при поиске файла для даты {target_date}: {e}")
                continue

        if not file_id:
            await message.reply_text("❌ Файл за сегодня или вчера не найден.")
            return

        date_str = used_date.strftime("%d.%m.%Y")
        await message.reply_text(f"✅ Файл найден за {date_str}")

        # Конвертация в Google Таблицу
        temp_name = f"TEMP_{filename.replace('.xlsm', '')}"
        try:
            spreadsheet_id = fm.create_sheets_copy(file_id, temp_name)
        except RuntimeError as e:
            await message.reply_text(f"❌ Ошибка при создании копии файла: {e}")
            return
        except Exception as e:
            logger.error(f"Неожиданная ошибка при создании копии файла: {e}")
            await message.reply_text("❌ Не удалось обработать файл.")
            return

        if not spreadsheet_id:
            await message.reply_text("❌ Не удалось обработать файл.")
            return

        # Чтение данных
        try:
            rows = ds.read_sheet(spreadsheet_id, "Терминалы!A:Z")
        except RuntimeError as e:
            fm.safe_delete(spreadsheet_id)  # Пытаемся удалить временный файл
            await message.reply_text(f"❌ Ошибка при чтении таблицы: {e}")
            return
        except Exception as e:
            logger.error(f"Неожиданная ошибка при чтении таблицы: {e}")
            fm.safe_delete(spreadsheet_id)  # Пытаемся удалить временный файл
            await message.reply_text("❌ Не удалось прочитать таблицу.")
            return

        # Удаляем временный файл сразу после чтения
        try:
            fm.safe_delete(spreadsheet_id)
        except Exception as e:
            logger.error(f"Ошибка при удалении временного файла: {e}")
            # Не прерываем выполнение, так как данные уже получены

        if not rows:
            await message.reply_text("📋 Лист 'Терминалы' пуст.")
            return

        # Поиск
        try:
            results = ds.search_by_sn(rows, sn)
        except RuntimeError as e:
            await message.reply_text(f"❌ Ошибка при обработке данных: {e}")
            return
        except Exception as e:
            logger.error(f"Неожиданная ошибка при обработке данных: {e}")
            await message.reply_text("❌ Ошибка при обработке данных таблицы.")
            return

        if results:
            response = f"✅ Найдено по `{sn}`:\n\n" + "\n\n".join(results)
            if len(response) > 4096:
                response = response[:4090] + "\n..."
        else:
            response = f"❌ Запись с `{sn}` не найдена."

        await message.reply_text(response, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Критическая ошибка обработки запроса: {e}", exc_info=True)
        try:
            if update.message:
                await update.message.reply_text("❌ Произошла критическая ошибка при обработке запроса.")
        except:
            pass

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка сообщений: команды и упоминания"""
    try:
        if not update.message or not update.message.text:
            return

        text = update.message.text.strip()
        bot_username = context.bot.username

        # Проверяем типы запросов
        is_command = text.startswith("/s")
        is_mention = re.match(rf'@{re.escape(bot_username)}\b', text, re.IGNORECASE)

        if not (is_command or is_mention):
            return  # Не наше

        # Извлекаем запрос
        if is_command:
            query = ' '.join(context.args) if context.args else ''
        else:
            query = re.sub(rf'@{re.escape(bot_username)}\s*', '', text, flags=re.IGNORECASE).strip()

        await handle_query(update, context, query)
    except Exception as e:
        logger.error(f"Ошибка в обработчике сообщений: {e}", exc_info=True)

def main():
    try:
        # Проверка обязательных переменных окружения
        required_vars = ["TELEGRAM_TOKEN", "PARENT_FOLDER_ID", "TEMP_FOLDER_ID"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise RuntimeError(f"Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}")

        app = Application.builder().token(TELEGRAM_TOKEN).build()

        # Обработчики
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("s", lambda u, c: handle_query(u, c, ' '.join(c.args) if c.args else '')))
        app.add_handler(MessageHandler(
            filters.TEXT & (filters.ChatType.CHANNEL | filters.ChatType.GROUPS | filters.ChatType.PRIVATE),
            handle_message
        ))

        logger.info("🚀 Бот запущен. Поддержка: личка, группы, каналы (при упоминании).")
        app.run_polling()
    except RuntimeError as e:
        logger.error(f"Критическая ошибка при запуске бота: {e}")
        print(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при запуске бота: {e}", exc_info=True)
        print(f"НЕОЖИДАННАЯ ОШИБКА: {e}")

if __name__ == '__main__':
    main()
