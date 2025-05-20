from telebot import types

def get_main_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    btn1 = types.KeyboardButton('📋 Доступные задачи')
    btn2 = types.KeyboardButton('🔍 Мои задачи')
    btn3 = types.KeyboardButton('ℹ️ Помощь')
    markup.add(btn1, btn2, btn3)
    return markup

def get_department_selection_markup():
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("Frontend", callback_data="register_dept_1"),
        types.InlineKeyboardButton("Backend", callback_data="register_dept_2"),
        types.InlineKeyboardButton("UI/UX", callback_data="register_dept_3")
    )
    return markup

def get_task_action_markup(task_id, is_assigned=False, is_review=False, completed=False):
    markup = types.InlineKeyboardMarkup(row_width=1)
    
    if not completed:
        if not is_assigned:
            markup.add(types.InlineKeyboardButton("✋ Взять задачу", callback_data=f"take_{task_id}"))
        elif not is_review:
            markup.add(types.InlineKeyboardButton("✅ Завершить задачу", callback_data=f"complete_{task_id}"))
            
    return markup if markup.keyboard else None