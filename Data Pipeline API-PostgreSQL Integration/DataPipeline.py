import json
import psycopg2
import logging
from datetime import datetime, timedelta
import os
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import smtplib
import ssl
from email.message import EmailMessage


class Loader:
    def __init__(self, config_path):
        self.config_path = config_path

    def load_config(self):
        # Загружаем конфигурацию из JSON-файла
        with open(self.config_path, 'r') as config_file:
            config = json.load(config_file)
        return config


class APIClient:
    def __init__(self, base_url, client, client_key):
        self.base_url = base_url
        self.client = client
        self.client_key = client_key

    def fetch_data(self, start, end):
        # Получаем данные от API с использованием переданных параметров
        params = {
            "client": self.client,
            "client_key": self.client_key,
            "start": start,
            "end": end
        }
        response = requests.get(self.base_url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()


class DataProcessor:
    def process_data(self, data):
        processed_data = []

        for record in data:
            # Преобразуем строку JSON в объект
            passback_params = json.loads(record['passback_params'].replace("'", "\""))
            processed_record = {
                "user_id": record["lti_user_id"],
                "oauth_consumer_key": passback_params.get("oauth_consumer_key", None),
                "lis_result_sourcedid": passback_params.get("lis_result_sourcedid", None),
                "lis_outcome_service_url": passback_params.get("lis_outcome_service_url", None),
                "is_correct": self.process_is_correct(record["attempt_type"], record["is_correct"]),
                "attempt_type": record["attempt_type"],
                "created_at": record["created_at"]
            }
            # Валидация данных: добавляем только если есть user_id
            if processed_record["user_id"]:
                processed_data.append(processed_record)

        return processed_data

    def process_is_correct(self, attempt_type, is_correct_value):
        # Определяем значение is_correct в зависимости от типа попытки
        if (attempt_type == "submit"):
            return 1 if is_correct_value else 0
        elif attempt_type == "run":
            return None
        return is_correct_value


class DatabaseManager:
    def __init__(self, db_config):
        # Подключаемся к базе данных с использованием конфигурации
        self.conn = psycopg2.connect(
            dbname=db_config['database'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        # Создаем таблицу grading_data, если она еще не существует
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS grading_data (
                id SERIAL PRIMARY KEY,
                user_id TEXT,
                oauth_consumer_key TEXT,
                lis_result_sourcedid TEXT,
                lis_outcome_service_url TEXT,
                is_correct INTEGER,
                attempt_type TEXT,
                created_at TIMESTAMP
            )
        """)
        self.conn.commit()
        cur.close()

    def insert_data(self, data):
        # Вставляем данные в таблицу grading_data
        cur = self.conn.cursor()

        for record in data:
            cur.execute("""
                INSERT INTO grading_data (user_id, oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url, is_correct, attempt_type, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                record["user_id"],
                record["oauth_consumer_key"],
                record["lis_result_sourcedid"],
                record["lis_outcome_service_url"],
                record["is_correct"],
                record["attempt_type"],
                record["created_at"]
            ))

        self.conn.commit()
        cur.close()

    def close(self):
        # Закрываем соединение с базой данных
        self.conn.close()


class Logger:
    def __init__(self, log_dir="logs"):
        # Создаем директорию для логов, если ее нет
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)
        self._cleanup_old_logs()

        # Настраиваем логирование
        log_filename = datetime.now().strftime("%Y-%m-%d") + ".log"
        logging.basicConfig(
            filename=os.path.join(self.log_dir, log_filename),
            format='%(asctime)s %(levelname)s: %(message)s',
            level=logging.INFO
        )

    def log_info(self, message):
        # Логирование информационного сообщения
        logging.info(message)

    def log_error(self, message):
        # Логирование сообщения об ошибке
        logging.error(message)

    def _cleanup_old_logs(self):
        # Удаляем логи старше 3 дней
        for filename in os.listdir(self.log_dir):
            file_path = os.path.join(self.log_dir, filename)
            file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            if datetime.now() - file_time > timedelta(days=3):
                os.remove(file_path)


class GoogleSheetsUploader:
    def __init__(self, credentials_path, spreadsheet_name):
        self.credentials_path = credentials_path
        self.spreadsheet_name = spreadsheet_name
        self.client = self.authenticate()

    def authenticate(self):
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_path, scope)
        client = gspread.authorize(creds)
        return client

    def create_or_get_worksheet(self, sheet, worksheet_name):
        try:
            worksheet = sheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=worksheet_name, rows="100", cols="20")
        return worksheet

    def upload_data(self, data, worksheet_name=None):
        # Используем текущее имя таблицы как имя листа, если оно не указано
        if worksheet_name is None:
            worksheet_name = datetime.now().strftime("%Y-%m-%d")

        # Открываем таблицу
        sheet = self.client.open(self.spreadsheet_name)

        # Получаем лист или создаем новый с именем по дате
        worksheet = self.create_or_get_worksheet(sheet, worksheet_name)

        # Поиск последней пустой строки для вставки данных
        row_index = len(worksheet.get_all_values()) + 1

        # Запись заголовков
        headers = ["За период с","до","Дата загрузки", "Всего попыток", "Успешные попытки", "Уникальные пользователи"]
        worksheet.insert_row(headers, row_index)

        # Запись данных
        worksheet.insert_row(list(data.values()), row_index + 1)


class EmailNotifier:
    def __init__(self, smtp_server, port, sender_email, sender_password, recipients):
        self.smtp_server = smtp_server
        self.port = port
        self.sender_email = sender_email
        self.sender_password = sender_password
        self.recipients = recipients

    def send_email(self, subject, message):
        msg = EmailMessage()
        msg.set_content(message)
        msg['Subject'] = subject
        msg['From'] = self.sender_email
        msg['To'] = ', '.join(self.recipients)

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(self.smtp_server, self.port, context=context) as server:
            server.login(self.sender_email, self.sender_password)
            server.send_message(msg)


class DataPipeline:
    def __init__(self, api_config_path, start, end, db_config_path, google_credentials_path, spreadsheet_name,
                 email_notifier):
        # Создаем объекты Loader для API и базы данных
        api_loader = Loader(api_config_path)
        db_loader = Loader(db_config_path)

        # Загружаем конфигурации
        self.api_config = api_loader.load_config()
        self.db_config = db_loader.load_config()

        # Создаем необходимые объекты внутри DataPipeline
        self.data_processor = DataProcessor()
        self.db_manager = DatabaseManager(self.db_config)
        self.logger = Logger()
        self.google_sheets_uploader = GoogleSheetsUploader(google_credentials_path, spreadsheet_name)
        self.email_notifier = email_notifier
        self.start = start
        self.end = end

    def run(self):
        self.logger.log_info("Начало загрузки данных")

        try:
            # Инициализация API клиента с конфигурацией
            api_client = APIClient(
                base_url=self.api_config["api_url"],
                client=self.api_config["client"],
                client_key=self.api_config["client_key"]
            )

            # Получение данных от API
            raw_data = api_client.fetch_data(self.start, self.end)
            self.logger.log_info("Данные успешно получены от API")

            if not raw_data:
                raise ValueError("Полученные данные пусты")

            # Обработка данных
            processed_data = self.data_processor.process_data(raw_data)
            self.logger.log_info("Данные успешно обработаны")

            # Загрузка данных в базу данных
            self.db_manager.insert_data(processed_data)
            self.logger.log_info("Данные успешно загружены в базу данных")

            # Агрегация данных
            aggregated_data = {
                "start": self.start,
                "end": self.end,
                "date": datetime.now().strftime("%Y-%m-%d"),
                "total_attempts": len(processed_data),
                "successful_attempts": sum(1 for record in processed_data if record["is_correct"] == 1),
                "unique_users": len(set(record["user_id"] for record in processed_data))
            }

            # Загрузка агрегированных данных в Google Sheets
            self.google_sheets_uploader.upload_data(aggregated_data)
            self.logger.log_info("Агрегированные данные успешно загружены в Google Sheets")

            # Отправка оповещения по электронной почте
            email_subject = "Data Pipeline Completed Successfully"
            email_message = f"""
            Data pipeline has been completed successfully.

            Period: {aggregated_data['start']} to {aggregated_data['end']}
            Date: {aggregated_data['date']}
            Total Attempts: {aggregated_data['total_attempts']}
            Successful Attempts: {aggregated_data['successful_attempts']}
            Unique Users: {aggregated_data['unique_users']}
            """
            self.email_notifier.send_email(email_subject, email_message)

            self.logger.log_info(f"Данные успешно высланы на адреса: {self.email_notifier.recipients}")

        except Exception as e:
            # Логирование ошибки
            self.logger.log_error(f"Произошла ошибка: {e}")

            # Отправка оповещения об ошибке по электронной почте
            email_subject = "Data Pipeline Failed"
            email_message = f"Data pipeline encountered an error: {e}"
            self.email_notifier.send_email(email_subject, email_message)

        finally:
            # Закрытие соединения с базой данных
            self.db_manager.close()
            self.logger.log_info("Скрипт завершен")


# Инициализация и запуск пайплайна с уведомлениями по электронной почте
email_notifier = EmailNotifier(
    smtp_server="smtp.mail.ru",
    port=465,
    sender_email="simulative_otchot@mail.ru",
    sender_password="amfqXuZTXyTMxUXzk5sJ",
    recipients=["chacter@mail.ru", "chacter21@gmail.com"]
)

pipeline = DataPipeline(
    api_config_path="itresume_api.json",
    start="2023-04-01 13:30:00.000000",
    end="2023-04-01 13:35:00.000000",
    db_config_path="db_connection.json",
    google_credentials_path="solid-range-432510-b0-5d5d27d02700.json",
    spreadsheet_name="Статистика по студентам SkillFactory. IT Resume",
    email_notifier=email_notifier
)

pipeline.run()
