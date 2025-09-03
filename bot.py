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

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Отдельно настраиваем логирование для httpx, чтобы уменьшить verbosity
logging.getLogger("httpx").setLevel(logging.WARNING)

# ——— КОНФИГУРАЦИЯ ——————————————————————————————————————
# Декодируем Google Credentials из переменной окружения
def get_credentials_path():
    encoded = os.getenv("GOOGLE_CREDS_BASE64")
    if not encoded:
        raise RuntimeError("Переменная GOOGLE_CREDS_BASE64 не найдена!")
    # Декодируем base64 → JSON
    decoded = base64.b64decode(encoded).decode('utf-8')
    creds = json.loads(decoded)
    # Сохраняем временный файл (нужен для Google API)
    temp_path = "temp_google_creds.json"
    with open(temp_path, 'w') as f:
        json.dump(creds, f)
    return temp_path

# Используем временный файл и переменные окружения
CREDENTIALS_FILE = get_credentials_path()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")  # Токен от @BotFather
PARENT_FOLDER_ID = os.getenv("PARENT_FOLDER_ID")  # Папка, где лежит "2025"
TEMP_FOLDER_ID = os.getenv("TEMP_FOLDER_ID")  # Папка для временных копий
# ROOT_FOLDER_YEAR теперь формируется динамически
ROOT_FOLDER_YEAR = str(datetime.now().year)
CITY = 'Воронеж'

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
# ————————————————————————————————————————————————————

class GoogleServices:
    """Инкапсуляция Google API сервисов"""
    def __init__(self):
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
        self.drive = build('drive', 'v3', credentials=creds)
        self.sheets = build('sheets', 'v4', credentials=creds)

class FileManager:
    """Работа с файлами и папками на Google Диске"""
    def __init__(self, drive_service):
        self.drive = drive_service

    def find_folder(self, parent_id: str, name: str) -> str:
        """Найти папку по имени"""
        query = f"mimeType='application/vnd.google-apps.folder' and name='{name}' " \
                f"and '{parent_id}' in parents and trashed=false"
        result = self.drive.files().list(q=query, fields="files(id, name)").execute()
        files = result.get('files', [])
        if files:
            logger.debug(f"Найдена папка '{name}' (ID: {files[0]['id']}) внутри родителя {parent_id}")
            return files[0]['id']
        else:
            logger.debug(f"Папка '{name}' НЕ найдена внутри родителя {parent_id}")
            return None

    def find_file(self, folder_id: str, filename: str) -> str:
        """Найти файл в папке"""
        query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
        result = self.drive.files().list(q=query, fields="files(id, name)").execute()
        files = result.get('files', [])
        if files:
            logger.debug(f"Найден файл '{filename}' (ID: {files[0]['id']}) в папке {folder_id}")
            return files[0]['id']
        else:
            logger.debug(f"Файл '{filename}' НЕ найден в папке {folder_id}")
            return None

    def create_sheets_copy(self, file_id: str, name: str) -> str:
        """Создать копию Excel как Google Таблицу в TEMP_FOLDER_ID"""
        metadata = {
            'name': name,
            'parents': [TEMP_FOLDER_ID],
            'mimeType': 'application/vnd.google-apps.spreadsheet'
        }
        try:
            file = self.drive.files().copy(fileId=file_id, body=metadata).execute()
            logger.info(f"Копия создана: {name} (ID: {file['id']})")
            return file['id']
        except Exception as e:
            logger.error(f"Ошибка копирования: {e}")
            return None

    def safe_delete(self, file_id: str):
        """Удаляет файл, только если он в TEMP_FOLDER_ID"""
        try:
            file_info = self.drive.files().get(fileId=file_id, fields="parents").execute()
            if TEMP_FOLDER_ID in file_info.get('parents', []):
                self.drive.files().delete(fileId=file_id).execute()
                logger.info(f"✅ Временный файл удалён: {file_id}")
            else:
                logger.warning(f"❌ Удаление запрещено (не в TEMP): {file_id}")
        except Exception as e:
            logger.error(f"Ошибка удаления временного файла: {e}")

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
        except Exception as e:
            logger.error(f"Ошибка чтения таблицы: {e}")
            return []

    def search_by_sn(self, rows: list, target_sn: str) -> list:
        """
        Ищет строки, где столбец F (индекс 5) == target_sn (регистронезависимо)
        """
        target_sn = target_sn.strip().upper()
        results = []
        for row in rows[1:]:  # Пропускаем заголовок
            if len(row) > 5 and row[5].strip().upper() == target_sn:
                # Берём A-Z, убираем пустые
                cleaned = [cell.strip() for cell in row[:26] if cell.strip()]
                results.append(" | ".join(cleaned))
        return results

# ——— ОСНОВНОЙ БОТ ————————————————————————————————————

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Приветствие (работает в личке и группах)"""
    if update.message: # Проверка на существование сообщения
        await update.message.reply_text(
            "🤖 Привет! Я могу найти данные по номеру СН.\n"
            "Используй:\n"
            "• `/s СН12345` - поиск по номеру СН\n"
            "• `/test ДДММГГ` - тест формирования пути (например, `/test 010125`)\n"
            "• `@ваш_бот СН12345` - упоминание в группах/каналах"
        )

async def test_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /test для формирования пути и имени файла по дате"""
    if not update.message:
        return

    # Получаем аргумент - дату
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "❌ Неверный формат. Используй: `/test ДДММГГ`\n"
            "Пример: `/test 010125`",
            parse_mode='Markdown'
        )
        return

    date_str = context.args[0].strip()
    
    # Проверяем формат даты (6 цифр)
    if not (len(date_str) == 6 and date_str.isdigit()):
        await update.message.reply_text(
            "❌ Неверный формат даты. Нужно 6 цифр: ДДММГГ\n"
            "Пример: `010125` для 1 января 2025 года",
            parse_mode='Markdown'
        )
        return

    try:
        # Извлекаем части даты
        day = date_str[:2]
        month = date_str[2:4]
        year_short = date_str[4:]
        year_full = f"20{year_short}" # Предполагаем, что это 21 век
        
        # Получаем название месяца
        month_names = ["январь", "февраль", "март", "апрель", "май", "июнь",
                       "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]
        try:
            month_index = int(month) - 1
            if 0 <= month_index <= 11:
                month_name = month_names[month_index]
            else:
                month_name = "???"
        except (ValueError, IndexError):
            month_name = "???"
        
        # Формируем имя файла
        filename = f"АПП_Склад_{date_str}_{CITY}.xlsm"
        
        # Формируем путь согласно новой структуре
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
        
    except Exception as e:
        logger.error(f"Ошибка в команде /test: {e}")
        await update.message.reply_text(
            "❌ Произошла ошибка при обработке даты.",
            parse_mode='Markdown'
        )

async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик неизвестных команд"""
    if update.message:
        help_text = (
            "Кожаный, я понимаю только следующие команды:\n"
            "• `/start` - начать работу со мной\n"
            "• `/s СН12345` - найти данные по номеру СН\n"
            "• `/test ДДММГГ` - тест формирования пути\n"
            "Также ты можешь упомянуть меня в группе или канале: `@ваш_бот СН12345`"
        )
        await update.message.reply_text(help_text, parse_mode='Markdown')

def extract_sn(query: str) -> str | None:
    """
    Извлекает номер СН из строки (например, СН12345)
    Возвращает в верхнем регистре или None
    """
    match = re.search(r'СН[А-Яа-яA-Za-z0-9]+', query, re.IGNORECASE)
    return match.group(0).strip().upper() if match else None

async def handle_query(update: Update, context: ContextTypes.DEFAULT_TYPE, query: str):
    """
    Общая логика обработки запроса
    """
    # Проверка на существование сообщения
    if not update.message:
        logger.warning("Получено обновление без сообщения для handle_query")
        return

    message = update.message
    sn = extract_sn(query)
    if not sn:
        await message.reply_text("❌ Не указан номер СН. Пример: `СН12345`", parse_mode='Markdown')
        return

    await message.reply_text(f"🔍 Поиск по номеру: `{sn}`", parse_mode='Markdown')

    try:
        # Инициализация сервисов
        gs = GoogleServices()
        fm = FileManager(gs.drive)
        ds = DataSearcher(gs.sheets)
        
        # Динамически определяем текущий год
        current_year = str(datetime.now().year)

        # Поиск файла: сегодня или вчера
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        dates_to_try = [today, yesterday]
        file_id = None
        used_date = None

        logger.info(f"Начинаю поиск файла для СН: {sn}")
        logger.info(f"PARENT_FOLDER_ID: {PARENT_FOLDER_ID}")

        for target_date in dates_to_try:
            filename = f"АПП_Склад_{target_date.strftime('%d%m%y')}_{CITY}.xlsm"
            logger.info(f"Попытка поиска файла: {filename}")

            # Найти папку года (текущий год)
            logger.debug(f"Поиск папки года '{current_year}' внутри PARENT_FOLDER_ID '{PARENT_FOLDER_ID}'")
            root_folder = fm.find_folder(PARENT_FOLDER_ID, current_year)
            if not root_folder:
                logger.warning(f"Папка года '{current_year}' не найдена в '{PARENT_FOLDER_ID}'")
                continue
            logger.debug(f"Папка года найдена: ID={root_folder}")

            # Найти папку "акты"
            logger.debug(f"Поиск папки 'акты' внутри папки года '{root_folder}'")
            acts_folder = fm.find_folder(root_folder, "акты")
            if not acts_folder:
                logger.warning(f"Папка 'акты' не найдена в папке года (ID: {root_folder})")
                continue
            logger.debug(f"Папка 'акты' найдена: ID={acts_folder}")

            # Найти папку месяца: "01 - январь"
            month_names = ["январь", "февраль", "март", "апрель", "май", "июнь",
                           "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]
            month_num = target_date.month
            month_folder_name = f"{target_date.strftime('%m')} - {month_names[month_num - 1]}"
            logger.debug(f"Поиск папки месяца '{month_folder_name}' внутри папки 'акты' '{acts_folder}'")
            month_folder = fm.find_folder(acts_folder, month_folder_name)
            if not month_folder:
                logger.warning(f"Папка месяца '{month_folder_name}' не найдена в 'акты' (ID: {acts_folder})")
                continue
            logger.debug(f"Папка месяца найдена: ID={month_folder}")

            # Найти папку с датой: "ДДММГГ"
            date_folder_name = target_date.strftime('%d%m%y')
            logger.debug(f"Поиск папки с датой '{date_folder_name}' внутри папки месяца '{month_folder}'")
            date_folder = fm.find_folder(month_folder, date_folder_name)
            if not date_folder:
                logger.warning(f"Папка с датой '{date_folder_name}' не найдена в папке месяца (ID: {month_folder})")
                continue
            logger.debug(f"Папка с датой найдена: ID={date_folder}")

            # Найти файл в папке с датой
            logger.debug(f"Поиск файла '{filename}' внутри папки с датой '{date_folder}'")
            file_id = fm.find_file(date_folder, filename)
            if file_id:
                logger.info(f"Файл найден: ID={file_id}")
                used_date = target_date
                break # Файл найден, выходим из цикла
            else:
                logger.warning(f"Файл '{filename}' не найден в папке с датой (ID: {date_folder})")

        if not file_id:
            await message.reply_text("❌ Файл за сегодня или вчера не найден.")
            return

        date_str = used_date.strftime("%d.%m.%Y")
        await message.reply_text(f"✅ Файл найден за {date_str}")

        # Конвертация в Google Таблицу
        temp_name = f"TEMP_{filename.replace('.xlsm', '')}"
        logger.debug(f"Создание временной копии файла {file_id} с именем {temp_name}")
        spreadsheet_id = fm.create_sheets_copy(file_id, temp_name)
        if not spreadsheet_id:
            await message.reply_text("❌ Не удалось обработать файл.")
            return

        # Чтение данных
        logger.debug(f"Чтение данных из временной таблицы {spreadsheet_id}, лист 'Терминалы!A:Z'")
        rows = ds.read_sheet(spreadsheet_id, "Терминалы!A:Z")
        logger.debug(f"Удаление временной таблицы {spreadsheet_id}")
        fm.safe_delete(spreadsheet_id)  # Удаляем сразу после чтения

        if not rows:
            await message.reply_text("📋 Лист 'Терминалы' пуст.")
            return

        # Поиск
        logger.debug(f"Поиск СН '{sn}' в данных")
        results = ds.search_by_sn(rows, sn)
        if results:
            response = f"✅ Найдено по `{sn}`:\n\n" + "\n\n".join(results)
            if len(response) > 4096:
                response = response[:4090] + "\n..."
        else:
            response = f"❌ Запись с `{sn}` не найдена."

        await message.reply_text(response, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Ошибка обработки: {e}", exc_info=True)
        # Проверка на существование сообщения перед отправкой ошибки
        if update.message:
            await update.message.reply_text("❌ Произошла ошибка при поиске данных.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка сообщений: команды и упоминания"""
    # Проверка на существование сообщения и текста
    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()
    bot_username = context.bot.username

    # Проверяем типы запросов
    is_command_s = text.startswith("/s")
    is_command_test = text.startswith("/test")
    is_mention = re.match(rf'@{re.escape(bot_username)}\b', text, re.IGNORECASE)

    if is_command_test:
        # Обрабатываем команду /test отдельно
        # Извлекаем аргументы после /test
        command_parts = text.split(' ', 1)
        args = command_parts[1:] if len(command_parts) > 1 else []
        # Создаем фейковый context для передачи аргументов
        context.args = args
        await test_command(update, context)
        return
    elif is_command_s or is_mention:
        # Извлекаем запрос
        if is_command_s:
            query = ' '.join(context.args) if context.args else ''
        else:
            query = re.sub(rf'@{re.escape(bot_username)}\s*', '', text, flags=re.IGNORECASE).strip()
        
        await handle_query(update, context, query)
        return
    elif text.startswith('/'):
        # Если это неизвестная команда, отправляем сообщение о поддерживаемых командах
        await unknown_command(update, context)
        return

def main():
    # Проверка обязательных переменных окружения
    required_vars = ["TELEGRAM_TOKEN", "PARENT_FOLDER_ID", "TEMP_FOLDER_ID"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.critical(f"Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}")
        print(f"КРИТИЧЕСКАЯ ОШИБКА: Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}")
        return # Завершаем работу если нет обязательных переменных

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Обработчики
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("test", test_command))
    app.add_handler(CommandHandler("s", lambda u, c: handle_query(u, c, ' '.join(c.args) if c.args else '')))
    # Обработчик для неизвестных команд (все команды, кроме зарегистрированных)
    app.add_handler(MessageHandler(filters.COMMAND, unknown_command))
    app.add_handler(MessageHandler(
        filters.TEXT & (filters.ChatType.CHANNEL | filters.ChatType.GROUPS | filters.ChatType.PRIVATE),
        handle_message
    ))

    logger.info("🚀 Бот запущен. Поддержка: личка, группы, каналы (при упоминании).")
    logger.info(f"Конфигурация: ROOT_FOLDER_YEAR={ROOT_FOLDER_YEAR}, CITY={CITY}")
    app.run_polling()

if __name__ == '__main__':
    main()