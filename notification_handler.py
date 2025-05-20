from typing import Dict
import telebot
from api import client
from utils.status import get_task_status_from_code

class NotificationHandler:
    def __init__(self, bot: telebot.TeleBot):
        self.bot = bot
    
    async def handle_new_task(self, task_data: Dict):
        try:
            if not task_data.get("RecipientTelegramIds") or len(task_data["RecipientTelegramIds"]) == 0:
                return
            
            task_id = task_data["TaskId"]
            title = task_data["Title"]
            text = task_data["Text"]
            department = task_data["Department"]
            
            department_name = client.get_department_name(department)
            
            notification = f"""
🔔 *Новая задача!*

🔹 *{title}*
{text}

📁 Отдел: *{department_name}*
"""
            
            markup = telebot.types.InlineKeyboardMarkup(row_width=1)
            markup.add(telebot.types.InlineKeyboardButton("✋ Взять задачу", callback_data=f"take_{task_id}"))
            
            for recipient_id in task_data.get("RecipientTelegramIds", []):
                try:
                    self.bot.send_message(
                        recipient_id,
                        notification,
                        reply_markup=markup,
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass
                    
        except Exception:
            pass
    
    async def handle_simple_notification(self, notification_data: Dict):
        try:
            correlation_id = notification_data.get("CorrelationId")
            message_type = notification_data.get("MessageType", "Info")
            content = notification_data.get("Content", "")
            timestamp = notification_data.get("Timestamp")
            target_user_ids = notification_data.get("TargetUserTelegramIds", [])
            task_id = notification_data.get("TaskId")
            status_code = notification_data.get("StatusCode")
            
            if not content:
                return
            
            completion_phrases = [
                "успешно завершена и принята", 
                "задача успешно выполнена",
                "задача выполнена", 
                "задача завершена",
                "принята на проверку",
                "принято"
            ]
            
            is_task_accepted = any(phrase in content.lower() for phrase in completion_phrases)
            
            if task_id:
                try:
                    task_status = client.get_task_status(task_id)
                    
                    if not task_status or (task_status and task_status.get('status') == 3):
                        is_task_accepted = True
                except Exception:
                    pass
            
            if (is_task_accepted or status_code == 3) and task_id:
                if target_user_ids and len(target_user_ids) > 0:
                    notification_text = f"""
✅ *Задача завершена*

{content}

_Время: {timestamp}_
"""
                    for user_id in target_user_ids:
                        try:
                            self.bot.send_message(
                                user_id,
                                notification_text,
                                parse_mode="Markdown"
                            )
                        except Exception:
                            pass
                            
                return
                
            type_emoji = {
                "Info": "ℹ️",
                "Warning": "⚠️",
                "SystemAlert": "🔔",
                "Success": "✅",
                "Error": "❌",
                "Review": "🔄",
                "Completed": "✅"
            }.get(message_type, "ℹ️")
            
            is_review_notification = message_type == "Review" and task_id
            is_completion_notification = (message_type == "Completed" or message_type == "Success" or message_type == "Complete") and task_id
            
            if task_id and not is_completion_notification and not is_review_notification:
                if ("выполнена" in content.lower() or 
                    "завершена" in content.lower() or 
                    "completed" in content.lower() or 
                    "успешно" in content.lower() or
                    "принята" in content.lower()):
                    is_completion_notification = True
            
            is_rejection = ("отклонена" in content.lower() or "отклонен" in content.lower() or 
                          "reject" in content.lower() or "возвращена" in content.lower() or 
                          "возвращен" in content.lower() or "отправлена на доработку" in content.lower())
            is_approval = ("принята" in content.lower() or "принят" in content.lower() or 
                            "accept" in content.lower() or "approved" in content.lower() or
                            "выполнена" in content.lower() or "завершена" in content.lower() or 
                            "успешно завершена" in content.lower() or "успешно выполнена" in content.lower())
            
            notification_text = f"""
{type_emoji} *{message_type}*

{content}

_Время: {timestamp}_
"""
            
            status_info = None
            if status_code is not None:
                status_info = get_task_status_from_code(status_code)
            
            markup = None
            if (is_review_notification or is_completion_notification) and task_id:
                task_data = client.get_task_by_id(task_id)
                
                if task_data:
                    if status_info:
                        is_completed = status_info["is_completed"]
                        is_review = status_info["is_review"]
                        status_text = status_info["text"]
                        status_emoji = status_info["emoji"]
                    else:
                        if is_completion_notification or is_approval:
                            is_completed = True
                            is_review = False
                            status_text = "Выполнена"
                            status_emoji = "✅"
                        elif is_rejection:
                            is_completed = False
                            is_review = False
                            status_text = "Отклонена и возвращена в работу"
                            status_emoji = "⏳"
                        else:
                            is_completed = False
                            is_review = True
                            status_text = "На ревью"
                            status_emoji = "🔄"
                    
                    markup = telebot.types.InlineKeyboardMarkup(row_width=1)
                    
                    if is_completed:
                        pass
                    elif is_rejection:
                        markup.add(telebot.types.InlineKeyboardButton("✅ Завершить задачу", callback_data=f"complete_{task_id}"))
                    
                    department_name = client.get_department_name(task_data.get('department', 0))
                    
                    notification_text = f"""
{type_emoji} *Статус задачи изменен*

{content}

🔹 *{task_data.get('title', 'Без названия')}*
{task_data.get('description', task_data.get('text', 'Описание отсутствует'))}

📁 Отдел: *{department_name}*
🏷️ Статус: *{status_emoji} {status_text}*

_Время: {timestamp}_
"""
            
            if target_user_ids and len(target_user_ids) > 0:
                for user_id in target_user_ids:
                    try:
                        if len(notification_text) > 4000:
                            notification_text = notification_text[:3997] + "..."
                            
                        self.bot.send_message(
                            user_id,
                            notification_text,
                            parse_mode="Markdown",
                            reply_markup=markup
                        )
                    except Exception:
                        pass
            else:
                try:
                    users = client.get_all_workers()
                    if users:
                        for user in users:
                            try:
                                telegram_id = user.get('telegramId')
                                if telegram_id:
                                    if len(notification_text) > 4000:
                                        notification_text = notification_text[:3997] + "..."
                                        
                                    self.bot.send_message(
                                        telegram_id,
                                        notification_text,
                                        parse_mode="Markdown",
                                        reply_markup=markup
                                    )
                            except Exception:
                                pass
                except Exception:
                    pass
        
        except Exception:
            pass