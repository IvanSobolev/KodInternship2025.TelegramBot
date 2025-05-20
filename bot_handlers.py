import telebot
from telebot import types
from typing import Dict, List

from config import TOKEN
from api import client
from ui import keyboards
from utils.status import get_task_status_from_code

bot = telebot.TeleBot(TOKEN)

@bot.message_handler(commands=['start'])
def start(message):
    user_first_name = message.from_user.first_name
    user_id = message.from_user.id
    
    welcome_text = f"""
🤖 *Привет, {user_first_name}!* 

Я бот для отслеживания задач. Для начала работы выберите ваш отдел.

🆔 Ваш Telegram ID: `{user_id}`
"""
    
    bot.send_message(
        message.chat.id, 
        welcome_text,
        parse_mode="Markdown"
    )
    
    select_department_message = "📋 *Выберите ваш отдел:*"
    markup = keyboards.get_department_selection_markup()
    
    bot.send_message(
        message.chat.id,
        select_department_message,
        parse_mode="Markdown",
        reply_markup=markup
    )

@bot.message_handler(func=lambda message: message.text == '📋 Доступные задачи')
def list_tasks(message):
    user_id = message.from_user.id
    
    available_tasks_data = client.get_available_tasks(user_id)
    
    if not available_tasks_data:
        bot.send_message(
            message.chat.id,
            "📭 *Нет доступных задач*\n\nВ данный момент нет доступных задач.",
            parse_mode="Markdown",
            reply_markup=keyboards.get_main_menu()
        )
        return
    
    bot.send_message(
        message.chat.id,
        "📋 *Доступные задачи:*\n\nНиже представлен список доступных задач:",
        parse_mode="Markdown"
    )
    
    for task_data in available_tasks_data:
        task_id = task_data.get('id')
        
        status_code = task_data.get('status')
        status_info = get_task_status_from_code(status_code)
        
        if status_info["is_completed"]:
            continue
            
        department_name = client.get_department_name(task_data.get('department', 0))
        
        status_emoji = status_info["emoji"]
        status_text = status_info["text"]
        assigned = f"Назначена на вас" if status_code in [1, 2] else "Не назначена"
        
        is_assigned = status_code in [1, 2]
        is_review = status_info["is_review"]
        completed = status_info["is_completed"]
        markup = keyboards.get_task_action_markup(task_id, is_assigned, is_review, completed)
        
        task_card = f"""
🔹 *{task_data.get('title', 'Без названия')}*
{task_data.get('description', task_data.get('text', 'Описание отсутствует'))}

📁 Отдел: *{department_name}*
🏷️ Статус: *{status_emoji} {status_text}*
👤 {assigned}
"""
        
        bot.send_message(
            message.chat.id,
            task_card,
            reply_markup=markup,
            parse_mode="Markdown"
        )

@bot.message_handler(func=lambda message: message.text == '🔍 Мои задачи')
def my_tasks(message):
    user_id = message.from_user.id
    
    api_task = client.get_user_active_tasks(user_id)
    
    if not api_task:
        bot.send_message(
            message.chat.id,
            "📭 *У вас нет назначенных задач*\n\nВы пока не взяли ни одной задачи на выполнение.",
            parse_mode="Markdown",
            reply_markup=keyboards.get_main_menu()
        )
        return
    
    task_id = api_task.get('id')
    
    status_code = api_task.get('status')
    status_info = get_task_status_from_code(status_code)
    
    is_review = status_info["is_review"]
    is_completed = status_info["is_completed"]
    status_text = status_info["text"]
    status_emoji = status_info["emoji"]
    
    task_status = api_task.get('statusString', '').lower()
    is_cancelled = 'cancel' in task_status or 'отмен' in task_status
    
    bot.send_message(
        message.chat.id,
        "🔍 *Ваши активные задачи:*\n\nНиже представлены задачи, назначенные на вас:",
        parse_mode="Markdown"
    )
    
    markup = types.InlineKeyboardMarkup(row_width=1)
    
    if not is_review and not is_completed and not is_cancelled:
        markup.add(types.InlineKeyboardButton("✅ Завершить задачу", 
            callback_data=f"complete_{task_id}"))
    
    department_name = client.get_department_name(api_task.get('department', 'Не указан'))
    
    task_card = f"""
🔹 *{api_task['title']}*
{api_task.get('description', api_task.get('text', 'Описание задачи отсутствует'))}

📁 Отдел: *{department_name}*
🏷️ Статус: *{status_emoji} {status_text}*
👤 Назначена на вас
"""
    
    bot.send_message(
        message.chat.id,
        task_card,
        reply_markup=markup if markup.keyboard else None,
        parse_mode="Markdown"
    )

@bot.message_handler(func=lambda message: message.text == 'ℹ️ Помощь')
def help_command(message):
    user_id = message.from_user.id
    help_text = f"""
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
    bot.send_message(message.chat.id, help_text, parse_mode="Markdown", reply_markup=keyboards.get_main_menu())

@bot.callback_query_handler(func=lambda call: True)
def button_handler(call):
    try:
        callback_data = call.data
        user_id = call.from_user.id
        user_first_name = call.from_user.first_name
        
        if callback_data.startswith("register_dept_"):
            department_id = int(callback_data.split("_")[-1])
            department_name = client.get_department_name(department_id)
            
            registration_result = client.register_worker(user_id, user_first_name, department_id)
            
            if registration_result:
                bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text=f"✅ *Вы успешно зарегистрированы!*\n\nВаш отдел: *{department_name}*",
                    parse_mode="Markdown"
                )
                
                bot.send_message(
                    call.message.chat.id,
                    "Используйте кнопки ниже для навигации:\n\n• *📋 Доступные задачи* - просмотр всех доступных задач\n• *🔍 Мои задачи* - просмотр задач, назначенных на вас\n• *ℹ️ Помощь* - информация о боте",
                    parse_mode="Markdown",
                    reply_markup=keyboards.get_main_menu()
                )
            else:
                bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text="❌ *Ошибка регистрации*\n\nНе удалось зарегистрироваться. Пожалуйста, попробуйте позже или обратитесь к администратору.",
                    parse_mode="Markdown"
                )
                
                markup = keyboards.get_department_selection_markup()
                
                bot.send_message(
                    call.message.chat.id,
                    "📋 *Попробуйте выбрать отдел снова:*",
                    parse_mode="Markdown",
                    reply_markup=markup
                )
            
            return
            
        elif callback_data.startswith("select_dept_"):
            department_id = int(callback_data.split("_")[-1])
            department_name = client.get_department_name(department_id)
            
            success = client.update_worker_department(user_id, department_id)
            
            if success:
                bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text=f"✅ *Отдел успешно выбран*\n\nВаш отдел: *{department_name}*",
                    parse_mode="Markdown"
                )
                
                bot.send_message(
                    call.message.chat.id,
                    "Теперь вы можете использовать основные функции бота:",
                    reply_markup=keyboards.get_main_menu()
                )
            else:
                bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text="❌ *Ошибка при выборе отдела*\n\nПожалуйста, попробуйте позже или обратитесь к администратору.",
                    parse_mode="Markdown"
                )
                
                bot.send_message(
                    call.message.chat.id,
                    "Вы можете использовать основные функции бота:",
                    reply_markup=keyboards.get_main_menu()
                )
            
            return
        
        if "_" in callback_data:
            action, task_id = callback_data.split("_", 1)
            
            if action == "complete_api":
                # Удаляем префикс api_ из task_id
                if task_id.startswith("api_"):
                    task_id = task_id[4:]
                
                api_task = client.get_user_active_tasks(user_id)
                
                if api_task:
                    task_matches = str(api_task.get('id')) == str(task_id)
                    
                    if task_matches:
                        success, message_text = client.complete_task(task_id)
                        
                        if success:
                            status_info = get_task_status_from_code(2)
                            status_text = status_info["text"]
                            status_emoji = status_info["emoji"]
                            
                            department = client.get_department_name(api_task.get('department', 'Не указан'))
                            
                            notification_text = "📝 Задача отправлена на ревью!"
                            
                            task_card = f"""
🔹 *{api_task['title']}*
{api_task.get('description', api_task.get('text', 'Описание задачи отсутствует'))}

📁 Отдел: *{department}*
🏷️ Статус: *{status_emoji} {status_text}*
👤 Назначена на вас
"""
                            
                            bot.edit_message_text(
                                chat_id=call.message.chat.id,
                                message_id=call.message.message_id,
                                text=task_card,
                                parse_mode="Markdown"
                            )
                            
                            bot.answer_callback_query(
                                call.id, 
                                text=notification_text, 
                                show_alert=True
                            )
                        else:
                            error_text = f"❌ Ошибка: {message_text}"
                            if len(error_text) > 200:
                                error_text = error_text[:197] + "..."
                                
                            bot.answer_callback_query(
                                call.id, 
                                text=error_text, 
                                show_alert=True
                            )
                    else:
                        success, message_text = client.complete_task(task_id)
                        
                        if success:
                            bot.answer_callback_query(
                                call.id, 
                                text="📝 Задача отправлена на ревью!", 
                                show_alert=True
                            )
                            bot.send_message(
                                call.message.chat.id, 
                                "✅ Задача успешно отправлена на ревью!",
                                reply_markup=keyboards.get_main_menu()
                            )
                        else:
                            error_text = f"❌ Ошибка: {message_text}"
                            if len(error_text) > 200:
                                error_text = error_text[:197] + "..."
                                
                            bot.answer_callback_query(
                                call.id, 
                                text=error_text, 
                                show_alert=True
                            )
                else:
                    success, message_text = client.complete_task(task_id)
                    
                    if success:
                        bot.answer_callback_query(
                            call.id, 
                            text="📝 Задача отправлена на ревью!", 
                            show_alert=True
                        )
                        bot.send_message(
                            call.message.chat.id, 
                            "✅ Задача успешно отправлена на ревью!",
                            reply_markup=keyboards.get_main_menu()
                        )
                    else:
                        error_text = f"❌ Ошибка: {message_text}"
                        if len(error_text) > 200:
                            error_text = error_text[:197] + "..."
                            
                        bot.answer_callback_query(
                            call.id, 
                            text=error_text, 
                            show_alert=True
                        )
                return
            
            if action == "take":
                api_task = client.get_user_active_tasks(user_id)
                has_active_tasks = api_task is not None
                
                if has_active_tasks:
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
                
                success, result = client.accept_task(user_id, task_id)
                
                if success:
                    task_data = client.get_task_by_id(task_id)
                    if not task_data:
                        bot.answer_callback_query(call.id, text="Не удалось получить информацию о задаче", show_alert=True)
                        return
                    
                    task_status_code = task_data.get('status', 1)
                    status_info = get_task_status_from_code(task_status_code)
                    
                    markup = types.InlineKeyboardMarkup(row_width=1)
                    
                    if not status_info["is_review"] and not status_info["is_completed"]:
                        markup.add(types.InlineKeyboardButton("✅ Завершить задачу", callback_data=f"complete_{task_id}"))
                    
                    task_card = f"""
🔹 *{task_data.get('title', 'Без названия')}*
{task_data.get('description', task_data.get('text', 'Описание отсутствует'))}

📁 Отдел: *{client.get_department_name(task_data.get('department', 0))}*
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
                    
                    bot.answer_callback_query(
                        call.id, 
                        text="✅ Вы успешно взяли задачу на выполнение!", 
                        show_alert=True
                    )
                else:
                    error_text = f"❌ Ошибка: {result}"
                    if len(error_text) > 200:
                        error_text = error_text[:197] + "..."
                        
                    bot.answer_callback_query(
                        call.id, 
                        text=error_text, 
                        show_alert=True
                    )
                    
            elif action == "complete":
                success, message_text = client.complete_task(task_id)
                
                if success:
                    task_data = client.get_task_by_id(task_id)
                    if not task_data:
                        bot.answer_callback_query(
                            call.id, 
                            text="📝 Задача отправлена на ревью!", 
                            show_alert=True
                        )
                        return
                    
                    status_info = get_task_status_from_code(2)
                    status_text = status_info["text"]
                    status_emoji = status_info["emoji"]
                    
                    notification_text = "📝 Задача отправлена на ревью!"
                    
                    task_card = f"""
🔹 *{task_data.get('title', 'Без названия')}*
{task_data.get('description', task_data.get('text', 'Описание отсутствует'))}

📁 Отдел: *{client.get_department_name(task_data.get('department', 0))}*
🏷️ Статус: *{status_emoji} {status_text}*
👤 Назначена на вас
"""
                    
                    bot.edit_message_text(
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id,
                        text=task_card,
                        parse_mode="Markdown"
                    )
                    
                    bot.answer_callback_query(
                        call.id, 
                        text=notification_text, 
                        show_alert=True
                    )
                else:
                    error_text = f"❌ Ошибка: {message_text}"
                    if len(error_text) > 200:
                        error_text = error_text[:197] + "..."
                        
                    bot.answer_callback_query(
                        call.id, 
                        text=error_text, 
                        show_alert=True
                    )
    except Exception as e:
        bot.answer_callback_query(call.id, text="Произошла ошибка при обработке запроса. Пожалуйста, попробуйте еще раз.", show_alert=True)

@bot.message_handler(func=lambda message: True)
def echo_all(message):
    bot.send_message(
        message.chat.id, 
        "Пожалуйста, используйте кнопки меню для навигации.",
        reply_markup=keyboards.get_main_menu()
    )