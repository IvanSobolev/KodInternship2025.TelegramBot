import os
import json
import logging
import asyncio
import threading
import telebot
import requests
from telebot import types
from aiokafka import AIOKafkaConsumer
from dataclasses import dataclass
from uuid import UUID
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Task data model
@dataclass
class Task:
    task_id: UUID
    title: str
    text: str
    department: str
    tg_id: int = None  # Будет установлено для конкретного пользователя
    assigned_to: Optional[int] = None
    completed: bool = False
    in_review: bool = False  # Флаг для задач, отправленных на ревью

# In-memory storage for tasks
tasks: Dict[str, Task] = {}
user_tasks: Dict[int, List[str]] = {}

# Bot configuration
TOKEN = ""

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "150.241.88.0:29092"
KAFKA_TOPIC = "new_tasks_notifications"  
KAFKA_SIMPLE_NOTIFICATIONS_TOPIC = "simple_notifications" 

# API Configuration
API_BASE_URL = "http://150.241.88.0:8080/api"

# Initialize the bot
bot = telebot.TeleBot(TOKEN)

# В начале файла после импортов добавляю функцию для преобразования id отдела в текстовое название
def get_department_name(department_id):
    """Преобразует числовой идентификатор отдела в текстовое название"""
    departments = {
        0: "Пусто",
        1: "Frontend",
        2: "Backend",
        3: "UI/UX",
    }
    
    # Если передана строка, пытаемся извлечь числовой идентификатор
    if isinstance(department_id, str):
        try:
            # Пытаемся извлечь число из строки
            for key in departments.keys():
                if str(key) in department_id:
                    return departments[key]
            # Если это название отдела, возвращаем его как есть
            return department_id
        except:
            return department_id
    
    # Если передано число, преобразуем его в название отдела
    if isinstance(department_id, int) and department_id in departments:
        return departments[department_id]
    
    # Если не удалось преобразовать, возвращаем исходное значение
    return str(department_id)

# Создаем главное меню с кнопками
def get_main_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    btn1 = types.KeyboardButton('📋 Доступные задачи')
    btn2 = types.KeyboardButton('🔍 Мои задачи')
    btn3 = types.KeyboardButton('ℹ️ Помощь')
    markup.add(btn1, btn2, btn3)
    return markup

# Функция для регистрации пользователя через API
def register_worker(telegram_id, first_name, last_name=None, department_id=1):
    try:
        # Формируем URL API для добавления работника
        url = f"{API_BASE_URL}/workers"
        
        # Структура AddWorkerDto
        worker_data = {
            "telegramId": telegram_id,
            "fullName": first_name + (f" {last_name}" if last_name else ""),
            "department": department_id  # Используем переданный отдел вместо 0
        }
        
        headers = {
            "Content-Type": "application/json"
        }
        
        # Отправляем запрос к API
        logger.info(f"Отправка запроса на регистрацию пользователя: {json.dumps(worker_data)}")
        response = requests.post(url, json=worker_data, headers=headers)
        
        # Проверяем результат
        if response.status_code in [200, 201]:
            logger.info(f"Пользователь успешно зарегистрирован: {telegram_id}, отдел: {department_id}")
            return True
        elif response.status_code == 500 and "уже существует" in response.text.lower():
            logger.info(f"Пользователь уже зарегистрирован: {telegram_id}")
            # Обновляем отдел пользователя, если он уже существует
            update_result = update_worker_department(telegram_id, department_id)
            if update_result:
                logger.info(f"Отдел пользователя {telegram_id} обновлен до {department_id}")
            return True
        else:
            logger.error(f"Ошибка регистрации пользователя {telegram_id}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"Исключение при регистрации пользователя {telegram_id}: {e}")
        return False

# Функция для получения активных задач пользователя
def get_user_active_tasks(telegram_id):
    try:
        url = f"{API_BASE_URL}/ProjectTask/active-for-user/{telegram_id}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            logger.info(f"У пользователя {telegram_id} нет активных задач (404)")
            # Удаляем все локальные задачи пользователя, так как API говорит, что их нет
            if telegram_id in user_tasks:
                # Получаем список ID задач пользователя
                task_ids = user_tasks[telegram_id].copy()
                for task_id in task_ids:
                    handle_task_completion(task_id, telegram_id)
                # Очищаем список после обработки всех задач
                if telegram_id in user_tasks:
                    user_tasks[telegram_id] = []
            return None
        else:
            logger.error(f"Ошибка получения активных задач пользователя {telegram_id}: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Ошибка получения активных задач пользователя {telegram_id}: {e}")
        return None

# Функция для принятия задачи через API
def accept_task(telegram_id, task_id):
    try:
        url = f"{API_BASE_URL}/ProjectTask/{task_id}/accept"
        # Параметр tgId передается как query parameter
        response = requests.post(url, params={"tgId": telegram_id})
        if response.status_code == 200:
            response_data = response.json()
            # Проверяем, не завершена ли задача
            if isinstance(response_data, dict) and response_data.get("status") == 3:
                # Задача уже завершена
                handle_task_completion(task_id, telegram_id)
                return True, "Задача уже завершена"
                
            # Обновляем информацию о задаче, включая отдел, если она вернулась из API
            if task_id in tasks and isinstance(response_data, dict):
                if "department" in response_data:
                    tasks[task_id].department = response_data["department"]
            return True, response_data
        return False, f"Ошибка: {response.status_code} - {response.text}"
    except Exception as e:
        logger.error(f"Ошибка принятия задачи {task_id} пользователем {telegram_id}: {e}")
        return False, f"Ошибка соединения: {e}"

# Функция для завершения задачи через API
def complete_task(task_id):
    try:
        url = f"{API_BASE_URL}/ProjectTask/{task_id}/complete"
        response = requests.post(url)
        
        # Если 404, значит задача не найдена (возможно уже выполнена)
        if response.status_code == 404:
            logger.info(f"Задача {task_id} не найдена в API при попытке завершения (404)")
            return True, "Задача уже завершена"
            
        if response.status_code == 200:
            # Пытаемся получить данные о задаче из ответа
            try:
                response_data = response.json()
                if isinstance(response_data, dict) and response_data.get("status") == 3:
                    # Если статус = 3 (Выполнена), задача успешно завершена
                    logger.info(f"Задача {task_id} успешно завершена, статус = 3")
                    return True, "Задача успешно завершена"
            except:
                pass
                
            return True, "Задача отправлена на ревью"
        return False, f"Ошибка: {response.status_code} - {response.text}"
    except Exception as e:
        logger.error(f"Ошибка завершения задачи {task_id}: {e}")
        return False, f"Ошибка соединения: {e}"

# Функция для отправки задачи на ревью через API
def send_to_review(task_id):
    try:
        url = f"{API_BASE_URL}/ProjectTask/{task_id}/review"
        response = requests.post(url)
        if response.status_code == 200:
            return True, "Задача отправлена на ревью"
        return False, f"Ошибка: {response.status_code} - {response.text}"
    except Exception as e:
        logger.error(f"Ошибка отправки задачи {task_id} на ревью: {e}")
        return False, f"Ошибка соединения: {e}"

@bot.message_handler(commands=['start'])
def start(message):
    """Send a welcome message when the command /start is issued."""
    user_first_name = message.from_user.first_name
    user_last_name = message.from_user.last_name
    username = message.from_user.username
    user_id = message.from_user.id
    
    # Логирование ID пользователя
    logger.info(f"Новый пользователь: {user_first_name}, Telegram ID: {user_id}")
    
    # Создаем приветствие с просьбой выбрать отдел
    welcome_text = f"""
🤖 *Привет, {user_first_name}!* 

Я бот для отслеживания задач. Для начала работы выберите ваш отдел.

🆔 Ваш Telegram ID: `{user_id}`
"""
    
    # Отправляем сообщение с приветствием
    bot.send_message(
        message.chat.id, 
        welcome_text,
        parse_mode="Markdown"
    )
    
    # Отправляем запрос на выбор отдела
    select_department_message = "📋 *Выберите ваш отдел:*"
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("Frontend", callback_data="register_dept_1"),
        types.InlineKeyboardButton("Backend", callback_data="register_dept_2"),
        types.InlineKeyboardButton("UI/UX", callback_data="register_dept_3")
    )
    
    bot.send_message(
        message.chat.id,
        select_department_message,
        parse_mode="Markdown",
        reply_markup=markup
    )

# Функция для получения доступных задач пользователя
def get_available_tasks(telegram_id):
    try:
        url = f"{API_BASE_URL}/ProjectTask/avaible-to-user/{telegram_id}"
        response = requests.get(url)
        if response.status_code == 200:
            tasks_data = response.json()
            logger.info(f"Получено {len(tasks_data)} доступных задач для пользователя {telegram_id} из API")
            return tasks_data
        elif response.status_code == 404:
            logger.info(f"Для пользователя {telegram_id} нет доступных задач (404)")
            return []
        else:
            logger.error(f"Ошибка получения доступных задач пользователя {telegram_id}: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Ошибка получения доступных задач пользователя {telegram_id}: {e}")
        return None

@bot.message_handler(func=lambda message: message.text == '📋 Доступные задачи')
def list_tasks(message):
    """List available tasks."""
    user_id = message.from_user.id
    logger.info(f"Пользователь {user_id} запросил доступные задачи")
    
    # Получаем доступные задачи из API
    available_tasks_data = get_available_tasks(user_id)
    
    # Если возникла ошибка при получении задач из API, используем локальные задачи как резервный вариант
    if available_tasks_data is None:
        logger.warning(f"Не удалось получить доступные задачи из API для пользователя {user_id}. Используем локальные данные.")
        
        # Получаем отдел пользователя из API
        user_department = None
        try:
            url = f"{API_BASE_URL}/workers/{user_id}"
            response = requests.get(url)
            if response.status_code == 200:
                worker_data = response.json()
                user_department = worker_data.get("department")
                logger.info(f"User {user_id} department: {user_department}")
        except Exception as e:
            logger.error(f"Error getting user department: {e}")
        
        # Добавляем логирование для всех задач в системе
        logger.info(f"Всего задач в системе: {len(tasks)}")
        for task_id, task in tasks.items():
            logger.info(f"Задача {task_id}: отдел={task.department}, назначена={task.assigned_to}, завершена={task.completed}")
        
        # Filter tasks that are not assigned or assigned to this user
        # AND belong to the user's department
        available_tasks = []
        for task in tasks.values():
            # Проверка назначения задачи
            assignment_condition = (task.assigned_to is None or task.assigned_to == user_id) and not task.completed
            
            # Проверка соответствия отдела
            department_match = False
            if user_department is None:  # Если отдел пользователя не указан, показываем все задачи
                department_match = True
            elif isinstance(task.department, str) and isinstance(user_department, (int, float)):
                # Если отдел задачи - строка, а отдел пользователя - число
                # Проверяем, содержит ли название отдела числовой id
                department_match = str(user_department) in task.department.lower()
                logger.info(f"Сравнение отделов (строка-число): {task.department} и {user_department}, результат: {department_match}")
            elif isinstance(user_department, str) and isinstance(task.department, (int, float)):
                # Если отдел пользователя - строка, а отдел задачи - число
                department_match = str(task.department) in user_department.lower()
                logger.info(f"Сравнение отделов (число-строка): {user_department} и {task.department}, результат: {department_match}")
            else:
                # Стандартное сравнение, приведенное к строкам
                try:
                    user_dept_str = str(user_department).lower()
                    task_dept_str = str(task.department).lower()
                    department_match = user_dept_str == task_dept_str or task_dept_str in user_dept_str or user_dept_str in task_dept_str
                    logger.info(f"Сравнение отделов (строковое): '{task_dept_str}' и '{user_dept_str}', результат: {department_match}")
                except Exception as e:
                    logger.error(f"Ошибка при сравнении отделов: {e}")
                    department_match = True  # В случае ошибки показываем задачу
            
            # Добавляем задачу, если она соответствует условиям
            if assignment_condition and department_match:
                available_tasks.append(task)
    else:
        # Используем данные, полученные из API
        available_tasks = []
        
        for task_data in available_tasks_data:
            task_id = task_data.get('id')
            
            # Проверяем статус задачи
            status_code = task_data.get('status')
            status_info = get_task_status_from_code(status_code)
            
            # Пропускаем завершенные задачи
            if status_info["is_completed"]:
                continue
                
            # Создаём объект Task для отображения
            new_task = Task(
                task_id=UUID(task_id) if task_id else UUID('00000000-0000-0000-0000-000000000000'),
                title=task_data.get('title', 'Без названия'),
                text=task_data.get('description', task_data.get('text', 'Описание отсутствует')),
                department=task_data.get('department', 0),
                assigned_to=None if status_code == 0 else user_id,
                completed=status_info["is_completed"],
                in_review=status_info["is_review"]
            )
            
            # Сохраняем задачу в локальное хранилище для дальнейшего использования
            tasks[str(new_task.task_id)] = new_task
            
            # Добавляем в список доступных задач
            available_tasks.append(new_task)
            
        logger.info(f"Получено {len(available_tasks)} доступных задач для пользователя {user_id}")
    
    # Если нет доступных задач
    if not available_tasks:
        bot.send_message(
            message.chat.id,
            "📭 *Нет доступных задач*\n\nВ данный момент нет доступных задач.",
            parse_mode="Markdown",
            reply_markup=get_main_menu()
        )
        return
    
    bot.send_message(
        message.chat.id,
        "📋 *Доступные задачи:*\n\nНиже представлен список доступных задач:",
        parse_mode="Markdown"
    )
    
    for task in available_tasks:
        # Определяем статус задачи
        status_emoji = "✅" if task.completed else "🔄" if task.in_review else "⏳" if task.assigned_to else "🆕"
        status_text = "Выполнена" if task.completed else "На ревью" if task.in_review else "В процессе" if task.assigned_to else "Новая"
        assigned = f"Назначена на вас" if task.assigned_to == user_id else "Не назначена"
        
        # Получаем название отдела вместо числового кода
        department_name = get_department_name(task.department)
        
        markup = types.InlineKeyboardMarkup(row_width=1)
        if not task.completed:
            if task.assigned_to is None:
                markup.add(types.InlineKeyboardButton("✋ Взять задачу", callback_data=f"take_{task.task_id}"))
            elif task.assigned_to == user_id:
                # Always show Complete button regardless of review status
                markup.add(types.InlineKeyboardButton("✅ Завершить задачу", callback_data=f"complete_{task.task_id}"))
        
        task_card = f"""
🔹 *{task.title}*
{task.text}

📁 Отдел: *{department_name}*
🏷️ Статус: *{status_emoji} {status_text}*
👤 {assigned}
"""
        
        bot.send_message(
            message.chat.id,
            task_card,
            reply_markup=markup if markup.keyboard else None,
            parse_mode="Markdown"
        )

# Функция для определения статуса задачи на основе числового кода
def get_task_status_from_code(status_code):
    """
    Определяет статус задачи на основе числового кода:
    0 - todo (нужно сделать)
    1 - в процессе (в работе)
    2 - на ревью (ожидает)
    3 - выполнено
    """
    try:
        # Конвертируем в int, если передано как строка
        if isinstance(status_code, str):
            status_code = int(status_code)
        
        status_map = {
            0: {"is_completed": False, "is_review": False, "text": "В очереди", "emoji": "🆕"},
            1: {"is_completed": False, "is_review": False, "text": "В процессе", "emoji": "⏳"},
            2: {"is_completed": False, "is_review": True, "text": "На ревью", "emoji": "🔄"},
            3: {"is_completed": True, "is_review": False, "text": "Выполнена", "emoji": "✅"}
        }
        
        return status_map.get(status_code, {"is_completed": False, "is_review": False, "text": "Неизвестно", "emoji": "❓"})
    except (ValueError, TypeError):
        logger.error(f"Невозможно определить статус из кода: {status_code}")
        return {"is_completed": False, "is_review": False, "text": "Неизвестно", "emoji": "❓"}

# Функция для обработки успешного завершения задачи
def handle_task_completion(task_id, user_id):
    """Обрабатывает успешное завершение задачи и очищает её из локального хранилища."""
    logger.info(f"Обработка успешного завершения задачи {task_id} для пользователя {user_id}")
    
    try:
        # Если задача найдена в локальном хранилище
        if task_id in tasks:
            # Отмечаем её как выполненную
            tasks[task_id].completed = True
            tasks[task_id].in_review = False
            
            # Обновляем отображение у всех пользователей
            update_task_for_all_users(task_id, completed=True, in_review=False)
            
        # Проверяем списки задач пользователей и удаляем ссылки на завершенную задачу
        if user_id in user_tasks and task_id in user_tasks[user_id]:
            user_tasks[user_id].remove(task_id)
            logger.info(f"Задача {task_id} удалена из списка задач пользователя {user_id}")
    
    except Exception as e:
        logger.error(f"Ошибка при обработке завершения задачи {task_id}: {e}")

# Функция для синхронизации статусов задачи между API и локальным хранилищем
def sync_task_status(task_id, user_id):
    """Синхронизирует статус задачи между API и локальным хранилищем."""
    try:
        # Проверяем наличие задачи в локальном хранилище
        if task_id not in tasks:
            logger.info(f"Задача {task_id} не найдена в локальном хранилище для синхронизации")
            return
            
        # Получаем статус задачи из API
        url = f"{API_BASE_URL}/ProjectTask/status/{task_id}"
        response = requests.get(url)
        
        # Если API вернул 404, значит задача завершена и больше не активна
        if response.status_code == 404:
            logger.info(f"Задача {task_id} не найдена в API (404). Вероятно, она завершена.")
            handle_task_completion(task_id, user_id)
            return
            
        if response.status_code != 200:
            logger.error(f"Не удалось получить статус задачи {task_id} из API: {response.status_code}")
            return
            
        # Парсим ответ от API
        status_data = response.json()
        status_code = status_data.get('status')
        status_string = status_data.get('statusString', '')
        
        # Определяем статус задачи по числовому коду
        status_info = get_task_status_from_code(status_code)
        is_review = status_info["is_review"]
        is_completed = status_info["is_completed"]
        
        logger.info(f"Синхронизация статуса задачи {task_id}: API статус код={status_code}, статус={status_string}, is_review={is_review}, is_completed={is_completed}")
        
        # Обновляем локальную задачу
        tasks[task_id].in_review = is_review
        tasks[task_id].completed = is_completed
        
        # Если задача выполнена, удаляем её из списка задач пользователя
        if is_completed:
            handle_task_completion(task_id, user_id)
        else:
            # Обновляем отображение для всех пользователей
            update_task_for_all_users(task_id, completed=is_completed, in_review=is_review)
        
        logger.info(f"Статус задачи {task_id} синхронизирован: completed={tasks[task_id].completed}, in_review={tasks[task_id].in_review}")
        
    except Exception as e:
        logger.error(f"Ошибка при синхронизации статуса задачи {task_id}: {e}")

@bot.message_handler(func=lambda message: message.text == '🔍 Мои задачи')
def my_tasks(message):
    """List tasks assigned to the user."""
    user_id = message.from_user.id
    
    # Создаем словарь для отслеживания задач по их идентификаторам, чтобы избежать дубликатов
    task_dict = {}
    
    # Получаем задачу из API
    api_task = get_user_active_tasks(user_id)
    
    # Если API вернул 404, значит у пользователя нет активных задач
    # В этом случае локальное хранилище уже очищено в функции get_user_active_tasks
    
    # Сначала обрабатываем задачу из API, если она есть
    if api_task:
        task_id = api_task.get('id')
        
        # Синхронизируем статус задачи с API
        sync_task_status(task_id, user_id)
        
        # Проверяем числовой статус задачи
        status_code = api_task.get('status')
        status_info = get_task_status_from_code(status_code)
        
        is_review = status_info["is_review"]
        is_completed = status_info["is_completed"]
        status_text = status_info["text"]
        status_emoji = status_info["emoji"]
        
        # Для отладки также проверяем статус по строке
        task_status = api_task.get('statusString', '').lower()
        is_cancelled = 'cancel' in task_status or 'отмен' in task_status
        
        # Логируем статус задачи для отладки
        logger.info(f"Статус задачи {task_id} из API: код={status_code}, текст={task_status}")
        logger.info(f"Анализ статуса: is_review={is_review}, is_completed={is_completed}, is_cancelled={is_cancelled}")
        
        # Добавляем задачу в словарь
        task_dict[task_id] = {
            'source': 'api',
            'title': api_task['title'],
            'text': api_task.get('description', api_task.get('text', 'Описание задачи отсутствует')),
            'department': get_department_name(api_task.get('department', 'Не указан')),
            'status_emoji': status_emoji,
            'status_text': status_text,
            'is_completed': is_completed,
            'is_review': is_review,
            'is_cancelled': is_cancelled,
            'is_rejected': False,
            'id': task_id
        }
        
        # Обновляем локальную задачу, если она существует, для синхронизации статусов
        if task_id in tasks:
            tasks[task_id].in_review = is_review
            tasks[task_id].completed = is_completed
    
    # Затем проверяем локальные задачи - но только если их нет в API
    # Это помогает избежать показа локальных задач, которые уже завершены в API
    if not api_task and user_id in user_tasks and user_tasks[user_id]:
        for task_id in user_tasks[user_id].copy():  # Используем copy() чтобы избежать изменения списка при итерации
            if task_id in tasks:
                task = tasks[task_id]
                if task.assigned_to == user_id:  # Убедимся, что задача назначена на этого пользователя
                    # Проверяем статус через API
                    sync_task_status(task_id, user_id)
                    
                    # Если после синхронизации задача все еще существует
                    if task_id in tasks and task_id in user_tasks.get(user_id, []):
                        # Определяем статус задачи
                        status_emoji = "✅" if task.completed else "🔄" if task.in_review else "⏳"
                        status_text = "Выполнена" if task.completed else "На ревью" if task.in_review else "В процессе"
                        
                        # Добавляем задачу в словарь
                        task_dict[task_id] = {
                            'source': 'local',
                            'title': task.title,
                            'text': task.text,
                            'department': get_department_name(task.department),
                            'status_emoji': status_emoji,
                            'status_text': status_text,
                            'is_completed': task.completed,
                            'is_review': task.in_review,
                            'is_cancelled': False,
                            'is_rejected': False,
                            'id': task_id
                        }
    
    # Разделяем задачи на активные и выполненные
    active_tasks = []
    completed_tasks = []
    
    for task_info in task_dict.values():
        # Исключаем задачи в статусе отмены
        if task_info['is_cancelled']:
            continue
            
        # Распределяем задачи по категориям
        if task_info['is_completed']:
            completed_tasks.append(task_info)
        else:
            active_tasks.append(task_info)
    
    # Отображаем задачи пользователю
    if active_tasks or completed_tasks:
        # Показываем активные задачи
        if active_tasks:
            bot.send_message(
                message.chat.id,
                "🔍 *Ваши активные задачи:*\n\nНиже представлены задачи, назначенные на вас:",
                parse_mode="Markdown"
            )
            
            for task_info in active_tasks:
                markup = types.InlineKeyboardMarkup(row_width=1)
                
                # Добавляем кнопку в зависимости от статуса
                if not task_info['is_review']:  # Показываем кнопку только для задач не на ревью
                    markup.add(types.InlineKeyboardButton("✅ Завершить задачу", 
                        callback_data=f"complete_api_{task_info['id']}" if task_info['source'] == 'api' else f"complete_{task_info['id']}"))
                
                task_card = f"""
🔹 *{task_info['title']}*
{task_info['text']}

📁 Отдел: *{task_info['department']}*
🏷️ Статус: *{task_info['status_emoji']} {task_info['status_text']}*
👤 Назначена на вас
"""
                
                bot.send_message(
                    message.chat.id,
                    task_card,
                    reply_markup=markup if markup.keyboard else None,
                    parse_mode="Markdown"
                )
        
        # Показываем выполненные задачи
        if completed_tasks:
            bot.send_message(
                message.chat.id,
                "✅ *Выполненные задачи:*",
                parse_mode="Markdown"
            )
            
            for task_info in completed_tasks:
                task_card = f"""
🔹 *{task_info['title']}*
{task_info['text']}

📁 Отдел: *{task_info['department']}*
🏷️ Статус: *✅ Выполнена*
👤 Выполнена вами
"""
                
                bot.send_message(
                    message.chat.id,
                    task_card,
                    parse_mode="Markdown"
                )
    else:
        # Если нет задач - сообщаем об этом
        bot.send_message(
            message.chat.id,
            "📭 *У вас нет назначенных задач*\n\nВы пока не взяли ни одной задачи на выполнение.",
            parse_mode="Markdown",
            reply_markup=get_main_menu()
        )

@bot.message_handler(func=lambda message: message.text == 'ℹ️ Помощь')
def help_command(message):
    """Show help information."""
    help_text = """
ℹ️ *Информация о боте*

Этот бот предназначен для отслеживания задач. Вот что вы можете делать:

• Просматривать доступные задачи
• Брать задачи на выполнение
• Отмечать задачи как выполненные
• Просматривать свои назначенные задачи

*Как использовать:*
1. Нажмите на кнопку "📋 Доступные задачи" для просмотра всех задач
2. Нажмите "✋ Взять задачу" под интересующей вас задачей
3. Для просмотра ваших задач нажмите "🔍 Мои задачи"
4. Когда задача выполнена, нажмите "✅ Завершить задачу"

*Примечание:*
Ваш Telegram ID: `{user_id}`
Этот ID необходим для получения задач от системы.
"""
    user_id = message.from_user.id
    bot.send_message(message.chat.id, help_text.format(user_id=user_id), parse_mode="Markdown", reply_markup=get_main_menu())

@bot.callback_query_handler(func=lambda call: True)
def button_handler(call):
    """Handle button presses."""
    try:
        # Извлекаем информацию из callback_data
        callback_data = call.data
        user_id = call.from_user.id
        user_first_name = call.from_user.first_name
        user_last_name = call.from_user.last_name
        
        # Проверяем, если это запрос на регистрацию с выбором отдела
        if callback_data.startswith("register_dept_"):
            department_id = int(callback_data.split("_")[-1])
            department_name = get_department_name(department_id)
            
            # Регистрируем пользователя в системе с выбранным отделом
            registration_result = register_worker(user_id, user_first_name, user_last_name, department_id)
            
            if registration_result:
                # Отправляем подтверждение
                bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text=f"✅ *Вы успешно зарегистрированы!*\n\nВаш отдел: *{department_name}*",
                    parse_mode="Markdown"
                )
                
                # Отправляем главное меню
                bot.send_message(
                    call.message.chat.id,
                    "Используйте кнопки ниже для навигации:\n\n• *📋 Доступные задачи* - просмотр всех доступных задач\n• *🔍 Мои задачи* - просмотр задач, назначенных на вас\n• *ℹ️ Помощь* - информация о боте",
                    parse_mode="Markdown",
                    reply_markup=get_main_menu()
                )
            else:
                # Сообщение об ошибке
                bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text="❌ *Ошибка регистрации*\n\nНе удалось зарегистрироваться. Пожалуйста, попробуйте позже или обратитесь к администратору.",
                    parse_mode="Markdown"
                )
                
                # Предлагаем попробовать снова
                markup = types.InlineKeyboardMarkup(row_width=1)
                markup.add(
                    types.InlineKeyboardButton("Frontend", callback_data="register_dept_1"),
                    types.InlineKeyboardButton("Backend", callback_data="register_dept_2"),
                    types.InlineKeyboardButton("UI/UX", callback_data="register_dept_3")
                )
                
                bot.send_message(
                    call.message.chat.id,
                    "📋 *Попробуйте выбрать отдел снова:*",
                    parse_mode="Markdown",
                    reply_markup=markup
                )
            
            return
            
        # Проверяем, является ли это выбором отдела для существующего пользователя
        elif callback_data.startswith("select_dept_"):
            department_id = int(callback_data.split("_")[-1])
            department_name = get_department_name(department_id)
            
            # Обновляем отдел пользователя
            success = update_worker_department(user_id, department_id)
            
            if success:
                # Отправляем подтверждение
                bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text=f"✅ *Отдел успешно выбран*\n\nВаш отдел: *{department_name}*",
                    parse_mode="Markdown"
                )
                
                # Отправляем главное меню
                bot.send_message(
                    call.message.chat.id,
                    "Теперь вы можете использовать основные функции бота:",
                    reply_markup=get_main_menu()
                )
            else:
                # Сообщение об ошибке
                bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text="❌ *Ошибка при выборе отдела*\n\nПожалуйста, попробуйте позже или обратитесь к администратору.",
                    parse_mode="Markdown"
                )
                
                # Отправляем главное меню
                bot.send_message(
                    call.message.chat.id,
                    "Вы можете использовать основные функции бота:",
                    reply_markup=get_main_menu()
                )
            
            return
        
        # Проверяем, есть ли префикс в callback_data
        if "_" in callback_data:
            action, task_id = callback_data.split("_", 1)
            
            # Находим задачу в памяти
            if task_id not in tasks:
                bot.answer_callback_query(call.id, text="Задача не найдена или устарела. Перезагрузите список задач.", show_alert=True)
                return
            
            task = tasks[task_id]
            
            if action == "take":
                # Проверяем, не назначена ли уже задача
                if task.assigned_to is not None:
                    # Задача уже назначена
                    assigned_user_name = "другому пользователю"
                    try:
                        url = f"{API_BASE_URL}/workers/{task.assigned_to}"
                        response = requests.get(url)
                        if response.status_code == 200:
                            worker_data = response.json()
                            assigned_user_name = worker_data.get("fullName", f"пользователю ID:{task.assigned_to}")
                    except Exception as e:
                        logger.error(f"Ошибка при получении данных пользователя {task.assigned_to}: {e}")
                    
                    # Уведомление для пользователя, что задача уже занята
                    bot.edit_message_text(
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id,
                        text=f"❌ *Задача уже назначена*\n\nЭта задача уже назначена на {assigned_user_name}.",
                        parse_mode="Markdown"
                    )
                    bot.answer_callback_query(
                        call.id, 
                        text=f"Задача уже назначена на {assigned_user_name}.", 
                        show_alert=True
                    )
                    return
                
                # Проверяем, есть ли у пользователя уже взятые задачи
                api_task = get_user_active_tasks(user_id)
                has_active_tasks_in_api = api_task is not None
                
                has_active_tasks_locally = False
                if user_id in user_tasks and user_tasks[user_id]:
                    for existing_task_id in user_tasks[user_id]:
                        if existing_task_id in tasks:
                            existing_task = tasks[existing_task_id]
                            if not existing_task.completed:
                                has_active_tasks_locally = True
                                break
                
                # Если у пользователя уже есть активная задача, не разрешаем взять новую
                if has_active_tasks_in_api or has_active_tasks_locally:
                    bot.edit_message_text(
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id,
                        text="❌ *Нельзя взять несколько задач*\n\nУ вас уже есть активная задача. Завершите текущую задачу перед тем, как взять новую.",
                        parse_mode="Markdown"
                    )
                    bot.answer_callback_query(
                        call.id, 
                        text="Нельзя взять несколько задач. Завершите текущую задачу сначала.", 
                        show_alert=True
                    )
                    return
                
                # Пытаемся принять задачу через API
                success, result = accept_task(user_id, task_id)
                
                if success:
                    # API успешно назначил задачу
                    logger.info(f"Задача {task_id} назначена пользователю {user_id} через API")
                    
                    # Обновляем локальные данные
                    if user_id not in user_tasks:
                        user_tasks[user_id] = []
                    if task_id not in user_tasks[user_id]:
                        user_tasks[user_id].append(task_id)
                        
                    # После принятия задачи, обновляем её статус через API
                    sync_task_status(task_id, user_id)
                else:
                    # API не доступен или вернул ошибку
                    # Проверяем, не взята ли задача кем-то еще пока мы ожидали ответа API
                    if task.assigned_to is not None:
                        bot.edit_message_text(
                            chat_id=call.message.chat.id,
                            message_id=call.message.message_id,
                            text="❌ *Задача уже назначена*\n\nЭта задача была только что назначена другому пользователю.",
                            parse_mode="Markdown"
                        )
                        bot.answer_callback_query(call.id, text="Задача уже назначена.")
                        return
                    
                    # Назначаем локально
                    task.assigned_to = user_id
                    if user_id not in user_tasks:
                        user_tasks[user_id] = []
                    user_tasks[user_id].append(task_id)
                
                # Обновляем отображение задачи у всех пользователей
                update_task_for_all_users(task_id, assigned_to=user_id)
                
                # Получаем актуальный статус задачи после назначения
                task_status_code = 1  # По умолчанию "В процессе"
                
                # Пытаемся получить актуальный статус из API
                try:
                    url = f"{API_BASE_URL}/ProjectTask/status/{task_id}"
                    response = requests.get(url)
                    if response.status_code == 200:
                        status_data = response.json()
                        task_status_code = status_data.get('status', 1)
                except Exception as e:
                    logger.error(f"Ошибка получения статуса задачи после назначения: {e}")
                
                # Получаем статус по коду
                status_info = get_task_status_from_code(task_status_code)
                
                # Показываем обновленную карточку задачи пользователю, который взял задачу
                markup = types.InlineKeyboardMarkup(row_width=1)
                
                # Если задача не на ревью и не завершена, показываем кнопку завершения
                if not status_info["is_review"] and not status_info["is_completed"]:
                    markup.add(types.InlineKeyboardButton("✅ Завершить задачу", callback_data=f"complete_{task.task_id}"))
                
                task_card = f"""
🔹 *{task.title}*
{task.text}

📁 Отдел: *{get_department_name(task.department)}*
🏷️ Статус: *{status_info["emoji"]} {status_info["text"]}*
👤 Назначена на вас
"""
                
                bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text=task_card,
                    reply_markup=markup if markup.keyboard else None,
                    parse_mode="Markdown"
                )
                
                # Отправим уведомление об успешном взятии задачи
                bot.answer_callback_query(
                    call.id, 
                    text="✅ Вы успешно взяли задачу на выполнение!", 
                    show_alert=True
                )
                
            elif action == "complete":
                # Проверяем, назначена ли задача текущему пользователю
                if task.assigned_to != user_id:
                    bot.edit_message_text(
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id,
                        text="❌ *Ошибка*\n\nВы не можете завершить эту задачу, так как она не назначена на вас.",
                        parse_mode="Markdown"
                    )
                    bot.answer_callback_query(call.id, text="Задача не назначена на вас.")
                    return
                
                # Пытаемся завершить задачу через API
                success, message_text = complete_task(task_id)
                
                if success:
                    # Синхронизируем статус с API
                    try:
                        sync_task_status(task_id, user_id)
                    except Exception as e:
                        logger.error(f"Ошибка синхронизации статуса: {e}")
                        
                    # Всегда отмечаем задачу как отправленную на ревью, вне зависимости от предыдущего состояния
                    task.completed = False
                    task.in_review = True
                    
                    # Получаем статус "На ревью" (код 2)
                    status_info = get_task_status_from_code(2)
                    status_text = status_info["text"]
                    status_emoji = status_info["emoji"]
                    
                    notification_text = "📝 Задача отправлена на ревью!"
                    
                    # Обновляем отображение задачи у всех пользователей
                    update_task_for_all_users(task_id, in_review=True, completed=False)
                    
                    # Показываем обновленную карточку задачи пользователю
                    task_card = f"""
🔹 *{task.title}*
{task.text}

📁 Отдел: *{task.department}*
🏷️ Статус: *{status_emoji} {status_text}*
👤 Назначена на вас
"""
                    
                    bot.edit_message_text(
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id,
                        text=task_card,
                        parse_mode="Markdown"
                    )
                    
                    # Отправим уведомление
                    bot.answer_callback_query(
                        call.id, 
                        text=notification_text, 
                        show_alert=True
                    )
                else:
                    # Ограничиваем длину сообщения об ошибке до 200 символов
                    error_text = f"❌ Ошибка: {message_text}"
                    if len(error_text) > 200:
                        error_text = error_text[:197] + "..."
                        
                    bot.answer_callback_query(
                        call.id, 
                        text=error_text, 
                        show_alert=True
                    )
            
            elif action == "review":
                # Проверяем, назначена ли задача текущему пользователю и находится ли она на ревью
                if task.assigned_to != user_id or not task.in_review:
                    bot.edit_message_text(
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id,
                        text="❌ *Ошибка*\n\nВы не можете выполнить задачу, которая не назначена на вас или не находится на ревью.",
                        parse_mode="Markdown"
                    )
                    bot.answer_callback_query(call.id, text="Невозможно выполнить операцию.")
                    return
                
                # Пытаемся отметить задачу как выполненную через API
                success, message_text = complete_task(task_id)
                
                if success:
                    # Синхронизируем статус с API
                    try:
                        sync_task_status(task_id, user_id)
                    except Exception as e:
                        logger.error(f"Ошибка синхронизации статуса: {e}")
                    
                    # Отмечаем задачу как выполненную в локальном хранилище
                    task.completed = True
                    task.in_review = False
                    
                    # Получаем статус "Выполнена" (код 3)
                    status_info = get_task_status_from_code(3)
                    status_text = status_info["text"]
                    status_emoji = status_info["emoji"]
                    
                    # Обновляем отображение задачи у всех пользователей
                    update_task_for_all_users(task_id, completed=True, in_review=False)
                    
                    # Показываем обновленную карточку задачи пользователю
                    task_card = f"""
🔹 *{task.title}*
{task.text}

📁 Отдел: *{task.department}*
🏷️ Статус: *{status_emoji} {status_text}*
👤 Выполнена вами
"""
                    
                    bot.edit_message_text(
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id,
                        text=task_card,
                        parse_mode="Markdown"
                    )
                    
                    # Отправим уведомление об успешном выполнении задачи
                    bot.answer_callback_query(
                        call.id, 
                        text="🎉 Задача успешно выполнена после ревью!", 
                        show_alert=True
                    )
                else:
                    # Ограничиваем длину сообщения об ошибке до 200 символов
                    error_text = f"❌ Ошибка: {message_text}"
                    if len(error_text) > 200:
                        error_text = error_text[:197] + "..."
                        
                    bot.answer_callback_query(
                        call.id, 
                        text=error_text, 
                        show_alert=True
                    )
    except Exception as e:
        logger.error(f"Ошибка обработки callback_query: {e}")
        bot.answer_callback_query(call.id, text="Произошла ошибка при обработке запроса. Пожалуйста, попробуйте еще раз.", show_alert=True)

# Обработчик для всех остальных сообщений
@bot.message_handler(func=lambda message: True)
def echo_all(message):
    bot.send_message(
        message.chat.id, 
        "Пожалуйста, используйте кнопки меню для навигации.",
        reply_markup=get_main_menu()
    )

# Функция для отправки уведомления всем пользователям о новой задаче
def notify_all_about_new_task(task_data):
    try:
        # Получаем список всех зарегистрированных пользователей
        url = f"{API_BASE_URL}/workers"
        response = requests.get(url)
        
        if response.status_code != 200:
            logger.error(f"Не удалось получить список пользователей: {response.status_code} - {response.text}")
            return
        
        users = response.json()
        
        # Получаем название отдела вместо числового кода
        department_name = get_department_name(task_data['Department'])
        
        # Формируем сообщение о новой задаче
        notification = f"""
🆕 *Новая задача в системе!*

🔹 *{task_data['Title']}*
{task_data['Text']}

📁 Отдел: *{department_name}*

Нажмите "✋ Взять задачу", чтобы принять её на выполнение.
"""
        
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(types.InlineKeyboardButton("✋ Взять задачу", callback_data=f"take_{task_data['TaskId']}"))
        
        task_department = task_data.get('Department')
        logger.info(f"Отправка уведомлений о новой задаче для отдела: {task_department}")
        
        # Отправляем уведомление только пользователям из того же отдела
        for user in users:
            try:
                telegram_id = user.get('telegramId')
                user_department = user.get('department')
                
                if not telegram_id:
                    continue
                
                # Проверка соответствия отдела
                department_match = False
                if user_department is None or task_department is None:  # Если отдел не указан, показываем всем
                    department_match = True
                elif isinstance(task_department, str) and isinstance(user_department, (int, float)):
                    # Если отдел задачи - строка, а отдел пользователя - число
                    department_match = str(user_department) in task_department.lower()
                    logger.info(f"Сравнение отделов (строка-число): {task_department} и {user_department}, результат: {department_match}")
                elif isinstance(user_department, str) and isinstance(task_department, (int, float)):
                    # Если отдел пользователя - строка, а отдел задачи - число
                    department_match = str(task_department) in user_department.lower()
                    logger.info(f"Сравнение отделов (число-строка): {user_department} и {task_department}, результат: {department_match}")
                else:
                    # Стандартное сравнение, приведенное к строкам
                    try:
                        user_dept_str = str(user_department).lower()
                        task_dept_str = str(task_department).lower()
                        department_match = user_dept_str == task_dept_str or task_dept_str in user_dept_str or user_dept_str in task_dept_str
                        logger.info(f"Сравнение отделов (строковое): '{task_dept_str}' и '{user_dept_str}', результат: {department_match}")
                    except Exception as e:
                        logger.error(f"Ошибка при сравнении отделов: {e}")
                        department_match = True  # В случае ошибки показываем задачу
                
                # Отправлять только если отделы совпадают
                if department_match:
                    bot.send_message(
                        telegram_id,
                        notification,
                        reply_markup=markup,
                        parse_mode="Markdown"
                    )
                    logger.info(f"Уведомление о задаче {task_data['TaskId']} отправлено пользователю {telegram_id} (отдел: {user_department})")
                else:
                    logger.info(f"Уведомление не отправлено пользователю {telegram_id} (отдел: {user_department}), так как отдел не соответствует {task_department}")
            except Exception as e:
                logger.error(f"Ошибка отправки уведомления пользователю {telegram_id}: {e}")
    
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомлений о новой задаче: {e}")

async def kafka_consumer():
    """Consume messages from Kafka topic."""
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                # Получаем данные из Kafka
                task_data = msg.value
                logger.info(f"Получено сообщение от Kafka: {task_data}")
                
                # Проверяем наличие получателей
                if not task_data.get("RecipientTelegramIds") or len(task_data["RecipientTelegramIds"]) == 0:
                    logger.warning(f"Сообщение не содержит получателей: {task_data}")
                    continue
                
                # Создаем задачу из данных формата C# сервиса
                base_task = Task(
                    task_id=UUID(task_data["TaskId"]),
                    title=task_data["Title"],
                    text=task_data["Text"],
                    department=task_data["Department"]
                )
                
                task_id_str = str(base_task.task_id)
                logger.info(f"Обработка новой задачи {task_id_str} для отдела {base_task.department}")
                
                # Сохраняем задачу в хранилище для каждого получателя
                for recipient_id in task_data.get("RecipientTelegramIds", []):
                    # Проверяем отдел пользователя перед отправкой уведомления
                    try:
                        recipient_department = None
                        try:
                            url = f"{API_BASE_URL}/workers/{recipient_id}"
                            response = requests.get(url)
                            if response.status_code == 200:
                                worker_data = response.json()
                                recipient_department = worker_data.get("department")
                                logger.info(f"Отдел пользователя {recipient_id}: {recipient_department}")
                        except Exception as e:
                            logger.error(f"Ошибка получения отдела пользователя {recipient_id}: {e}")
                        
                        # Создаем задачу для каждого получателя, вне зависимости от отдела
                        # Отправку уведомления контролируем отдельно
                        recipient_task = Task(
                            task_id=base_task.task_id,
                            title=base_task.title,
                            text=base_task.text,
                            department=base_task.department,
                            tg_id=recipient_id
                        )
                        
                        # Сохраняем задачу в хранилище
                        tasks[task_id_str] = recipient_task
                        
                        # Создаем кнопку "Взять задачу"
                        markup = types.InlineKeyboardMarkup(row_width=1)
                        markup.add(types.InlineKeyboardButton("✋ Взять задачу", callback_data=f"take_{task_id_str}"))
                        
                        # Получаем название отдела вместо числового кода
                        department_name = get_department_name(recipient_task.department)
                        
                        notification = f"""
🔔 *Новая задача!*

🔹 *{recipient_task.title}*
{recipient_task.text}

📁 Отдел: *{department_name}*
"""
                        
                        # Отправляем уведомление всем из списка получателей
                        # Фильтрация по отделам будет происходить при запросе списка доступных задач
                        try:
                            bot.send_message(
                                recipient_id,
                                notification,
                                reply_markup=markup,
                                parse_mode="Markdown"
                            )
                            logger.info(f"Уведомление о задаче {task_id_str} отправлено пользователю {recipient_id}")
                        except Exception as e:
                            logger.error(f"Ошибка отправки уведомления пользователю {recipient_id}: {e}")
                    except Exception as e:
                        logger.error(f"Ошибка обработки получателя {recipient_id}: {e}")

                
                logger.info(f"Обработка задачи {task_id_str} завершена")
            except Exception as e:
                logger.error(f"Ошибка обработки сообщения от Kafka: {e}")
    finally:
        await consumer.stop()

# Функция для обработки простых уведомлений
async def process_simple_notification(notification_data):
    """Process simple notification messages from Kafka."""
    try:
        # Получаем данные уведомления
        correlation_id = notification_data.get("CorrelationId")
        message_type = notification_data.get("MessageType", "Info")
        content = notification_data.get("Content", "")
        timestamp = notification_data.get("Timestamp")
        target_user_ids = notification_data.get("TargetUserTelegramIds", [])
        task_id = notification_data.get("TaskId")  # ID задачи, если это уведомление о ревью
        status_code = notification_data.get("StatusCode")  # Числовой код статуса задачи
        
        if not content:
            logger.warning(f"Получено пустое уведомление: {notification_data}")
            return
        
        # Ищем фразы, указывающие на завершение задачи
        completion_phrases = [
            "успешно завершена и принята", 
            "задача успешно выполнена",
            "задача выполнена", 
            "задача завершена",
            "принята на проверку",
            "принято"
        ]
        
        # Проверяем на явное указание о завершении/принятии задачи в тексте
        is_task_accepted = any(phrase in content.lower() for phrase in completion_phrases)
        
        # Проверяем статус задачи через API, если есть ID задачи
        if task_id:
            try:
                # Проверяем статус через API
                url = f"{API_BASE_URL}/ProjectTask/status/{task_id}"
                response = requests.get(url)
                
                # Если статус задачи = 3 (выполнена) или 404 (не найдена, удалена), считаем её завершенной
                if response.status_code == 404 or (response.status_code == 200 and response.json().get('status') == 3):
                    is_task_accepted = True
                    logger.info(f"Задача {task_id} помечена как завершенная по ответу API")
            except Exception as e:
                logger.error(f"Ошибка при проверке статуса задачи через API: {e}")
        
        # Если это сообщение о завершении задачи
        if (is_task_accepted or status_code == 3) and task_id:
            logger.info(f"Получено уведомление о принятии задачи: {task_id}, содержание: {content}")
            
            # Находим всех пользователей, у которых есть эта задача
            affected_users = []
            for user_id, task_list in user_tasks.items():
                if task_id in task_list:
                    affected_users.append(user_id)
            
            # Обрабатываем завершение задачи для каждого пользователя
            for user_id in affected_users:
                handle_task_completion(task_id, user_id)
                
            # Если есть указанные получатели, отправляем им явное уведомление о завершении
            if target_user_ids and len(target_user_ids) > 0:
                notification_text = f"""
✅ *Задача завершена*

{content}

_Время: {timestamp}_
"""
                for user_id in target_user_ids:
                    try:
                        bot.send_message(
                            user_id,
                            notification_text,
                            parse_mode="Markdown"
                        )
                        logger.info(f"Отправлено уведомление о завершении задачи {task_id} пользователю {user_id}")
                    except Exception as e:
                        logger.error(f"Ошибка отправки уведомления о завершении пользователю {user_id}: {e}")
                        
            # Выходим, так как уже обработали завершение задачи
            return
            
        # Определяем emoji в зависимости от типа сообщения
        type_emoji = {
            "Info": "ℹ️",
            "Warning": "⚠️",
            "SystemAlert": "🔔",
            "Success": "✅",
            "Error": "❌",
            "Review": "🔄",  # Добавляем тип для уведомлений о ревью
            "Completed": "✅"  # Для уведомлений о завершении задачи
        }.get(message_type, "ℹ️")
        
        # Проверяем, является ли это уведомление о ревью задачи
        is_review_notification = message_type == "Review" and task_id
        is_completion_notification = (message_type == "Completed" or message_type == "Success" or message_type == "Complete") and task_id
        
        # Дополнительно проверяем содержимое сообщения на признаки уведомления о выполнении
        if task_id and not is_completion_notification and not is_review_notification:
            if ("выполнена" in content.lower() or 
                "завершена" in content.lower() or 
                "completed" in content.lower() or 
                "успешно" in content.lower() or
                "принята" in content.lower()):
                is_completion_notification = True
                logger.info(f"Обнаружено уведомление о выполнении задачи по тексту: {content}")
        
        # Определяем, это принятие, отклонение ревью или завершение задачи
        is_rejection = "отклонена" in content.lower() or "отклонен" in content.lower() or "reject" in content.lower()
        is_approval = ("принята" in content.lower() or "принят" in content.lower() or 
                        "accept" in content.lower() or "approved" in content.lower() or
                        "выполнена" in content.lower() or "завершена" in content.lower() or 
                        "успешно завершена" in content.lower() or "успешно выполнена" in content.lower())
        
        # Форматируем сообщение
        notification_text = f"""
{type_emoji} *{message_type}*

{content}

_Время: {timestamp}_
"""
        
        # Если указан status_code, используем его для определения статуса
        status_info = None
        if status_code is not None:
            status_info = get_task_status_from_code(status_code)
            logger.info(f"Определен статус задачи из кода: {status_code} -> {status_info}")

        # Если это уведомление о ревью/завершении и у нас есть ID задачи, обновляем статус
        markup = None
        if (is_review_notification or is_completion_notification) and task_id:
            # Обновляем статус задачи в локальном хранилище, если она существует
            if task_id in tasks:
                task = tasks[task_id]
                
                # Если у нас есть числовой статус, используем его
                if status_info:
                    is_completed = status_info["is_completed"]
                    is_review = status_info["is_review"]
                    status_text = status_info["text"]
                    status_emoji = status_info["emoji"]
                else:
                    # Иначе определяем по тексту сообщения
                    if is_completion_notification or is_approval:
                        # Если это уведомление о завершении или принятии задачи
                        is_completed = True
                        is_review = False
                        status_text = "Выполнена"
                        status_emoji = "✅"
                    elif is_rejection:
                        # Если задача отклонена, меняем статус на "в процессе"
                        is_completed = False
                        is_review = False
                        status_text = "Отклонена и возвращена в работу"
                        status_emoji = "⏳"
                    else:
                        # Если задача отправлена на ревью
                        is_completed = False
                        is_review = True
                        status_text = "На ревью"
                        status_emoji = "🔄"
                
                # Добавляем логирование
                logger.info(f"Задача {task_id} изменила статус: is_completed={is_completed}, is_review={is_review}")
                logger.info(f"Текст уведомления: {content}")
                
                # Обновляем задачу в локальном хранилище
                task.completed = is_completed
                task.in_review = is_review
                        
                # Обновляем задачу для всех пользователей
                update_task_for_all_users(task_id, completed=is_completed, in_review=is_review)
                
                # Создаем кнопку для соответствующего действия
                markup = types.InlineKeyboardMarkup(row_width=1)
                
                if is_completed:
                    # Для завершенных задач не показываем кнопок
                    pass
                elif is_rejection:
                    # Для отклоненных задач показываем кнопку "Завершить задачу"
                    markup.add(types.InlineKeyboardButton("✅ Завершить задачу", callback_data=f"complete_{task_id}"))
                
                # Получаем название отдела вместо числового кода
                department_name = get_department_name(task.department)
                
                # Обновляем текст уведомления, добавляя информацию о задаче
                notification_text = f"""
{type_emoji} *Статус задачи изменен*

{content}

🔹 *{task.title}*
{task.text}

📁 Отдел: *{department_name}*
🏷️ Статус: *{status_emoji} {status_text}*

_Время: {timestamp}_
"""
        
        # Если есть конкретные получатели, отправляем только им
        if target_user_ids and len(target_user_ids) > 0:
            for user_id in target_user_ids:
                try:
                    # Ограничиваем длину сообщения
                    if len(notification_text) > 4000:
                        notification_text = notification_text[:3997] + "..."
                        
                    bot.send_message(
                        user_id,
                        notification_text,
                        parse_mode="Markdown",
                        reply_markup=markup
                    )
                    logger.info(f"Уведомление {correlation_id} отправлено пользователю {user_id}")
                except Exception as e:
                    logger.error(f"Ошибка отправки уведомления пользователю {user_id}: {e}")
        else:
            # Если получатели не указаны, пытаемся получить всех пользователей и отправить им
            try:
                url = f"{API_BASE_URL}/workers"
                response = requests.get(url)
                
                if response.status_code == 200:
                    users = response.json()
                    for user in users:
                        try:
                            telegram_id = user.get('telegramId')
                            if telegram_id:
                                # Ограничиваем длину сообщения
                                if len(notification_text) > 4000:
                                    notification_text = notification_text[:3997] + "..."
                                    
                                bot.send_message(
                                    telegram_id,
                                    notification_text,
                                    parse_mode="Markdown",
                                    reply_markup=markup
                                )
                                logger.info(f"Широковещательное уведомление {correlation_id} отправлено пользователю {telegram_id}")
                        except Exception as e:
                            logger.error(f"Ошибка отправки широковещательного уведомления пользователю {telegram_id}: {e}")
                else:
                    logger.error(f"Не удалось получить список пользователей для широковещательного уведомления: {response.status_code}")
            except Exception as e:
                logger.error(f"Ошибка при попытке широковещательной рассылки уведомления {correlation_id}: {e}")
    
    except Exception as e:
        logger.error(f"Ошибка обработки простого уведомления: {e}")

async def simple_notifications_consumer():
    """Consume messages from SimpleNotifications Kafka topic."""
    consumer = AIOKafkaConsumer(
        KAFKA_SIMPLE_NOTIFICATIONS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    await consumer.start()
    try:
        logger.info(f"Запущен прослушиватель простых уведомлений в топике {KAFKA_SIMPLE_NOTIFICATIONS_TOPIC}")
        async for msg in consumer:
            try:
                # Получаем данные из Kafka
                notification_data = msg.value
                logger.info(f"Получено простое уведомление от Kafka: {notification_data}")
                
                # Обрабатываем уведомление
                await process_simple_notification(notification_data)
                
            except Exception as e:
                logger.error(f"Ошибка обработки сообщения от Kafka (SimpleNotifications): {e}")
    finally:
        await consumer.stop()

def run_bot():
    """Run the Telegram bot."""
    bot.infinity_polling()

def run_kafka_consumer():
    """Run the Kafka consumer in the event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Запускаем оба консьюмера параллельно
    tasks = [
        loop.create_task(kafka_consumer()),
        loop.create_task(simple_notifications_consumer())
    ]
    
    try:
        # Запускаем асинхронные задачи и ждем их завершения
        loop.run_until_complete(asyncio.gather(*tasks))
    except Exception as e:
        logger.error(f"Ошибка в Kafka консьюмерах: {e}")
    finally:
        # Закрываем цикл событий
        loop.close()

# Функция для обновления отображения задачи для всех пользователей
def update_task_for_all_users(task_id, assigned_to=None, completed=False, in_review=None):
    """Update task display for all users when its status changes."""
    try:
        if task_id not in tasks:
            logger.error(f"Попытка обновить несуществующую задачу: {task_id}")
            return
        
        task = tasks[task_id]
        
        # Обновляем статус задачи в нашем локальном хранилище
        if assigned_to is not None:
            task.assigned_to = assigned_to
        
        if completed is not None:
            task.completed = completed
            
        if in_review is not None:
            task.in_review = in_review
        
        # Мы больше не отправляем уведомления всем пользователям об обновлении статуса задачи
        logger.info(f"Задача {task_id} обновлена: assigned_to={task.assigned_to}, completed={task.completed}, in_review={task.in_review}")
    
    except Exception as e:
        logger.error(f"Ошибка при обновлении отображения задачи: {e}")

# Функция для обновления отдела пользователя через API
def update_worker_department(telegram_id, department_id):
    try:
        # Формируем URL API для обновления работника
        url = f"{API_BASE_URL}/workers/{telegram_id}/department"
        
        # Формируем правильные данные для API - объект JSON с полем department
        data = {"department": department_id}
        
        # Отправляем запрос к API
        logger.info(f"Отправка запроса на обновление отдела пользователя {telegram_id}: {department_id}")
        response = requests.put(url, json=data, headers={"Content-Type": "application/json"})
        
        # Проверяем результат
        if response.status_code in [200, 204]:
            logger.info(f"Отдел пользователя успешно обновлен: {telegram_id} -> {department_id}")
            return True
        else:
            logger.error(f"Ошибка обновления отдела пользователя {telegram_id}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"Исключение при обновлении отдела пользователя {telegram_id}: {e}")
        return False

if __name__ == "__main__":
    # Start Kafka consumer in a separate thread
    kafka_thread = threading.Thread(target=run_kafka_consumer, daemon=True)
    kafka_thread.start()
    
    # Start the bot in the main thread
    logger.info("Telegram бот запущен")
    run_bot()
