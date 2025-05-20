def get_task_status_from_code(status_code):
    try:
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
        return {"is_completed": False, "is_review": False, "text": "Неизвестно", "emoji": "❓"}