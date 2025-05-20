import requests
from typing import Dict, List, Optional, Tuple, Any
from config import API_BASE_URL, DEPARTMENTS

def get_department_name(department_id):
    if isinstance(department_id, str):
        try:
            for key in DEPARTMENTS.keys():
                if str(key) in department_id:
                    return DEPARTMENTS[key]
            return department_id
        except:
            return department_id
    
    if isinstance(department_id, int) and department_id in DEPARTMENTS:
        return DEPARTMENTS[department_id]
    
    return str(department_id)

def register_worker(telegram_id: int, first_name: str, department_id: int = 1) -> bool:
    try:
        url = f"{API_BASE_URL}/workers"
        
        worker_data = {
            "telegramId": telegram_id,
            "fullName": first_name,
            "department": department_id
        }
        
        headers = {"Content-Type": "application/json"}
        
        response = requests.post(url, json=worker_data, headers=headers)
        
        if response.status_code in [200, 201]:
            return True
        elif response.status_code == 500 and "уже существует" in response.text.lower():
            update_result = update_worker_department(telegram_id, department_id)
            return True
        else:
            return False
    except Exception:
        return False

def update_worker_department(telegram_id: int, department_id: int) -> bool:
    try:
        url = f"{API_BASE_URL}/workers/{telegram_id}/department"
        data = {"department": department_id}
        
        response = requests.put(url, json=data, headers={"Content-Type": "application/json"})
        
        return response.status_code in [200, 204]
    except Exception:
        return False

def get_user_active_tasks(telegram_id: int) -> Optional[Dict]:
    try:
        url = f"{API_BASE_URL}/ProjectTask/active-for-user/{telegram_id}"
        response = requests.get(url)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return None
        else:
            return None
    except Exception:
        return None

def get_available_tasks(telegram_id: int) -> Optional[List[Dict]]:
    try:
        url = f"{API_BASE_URL}/ProjectTask/avaible-to-user/{telegram_id}"
        response = requests.get(url)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return []
        else:
            return None
    except Exception:
        return None

def accept_task(telegram_id: int, task_id: str) -> Tuple[bool, Any]:
    try:
        url = f"{API_BASE_URL}/ProjectTask/{task_id}/accept"
        response = requests.post(url, params={"tgId": telegram_id})
        
        if response.status_code == 200:
            response_data = response.json()
            if isinstance(response_data, dict) and response_data.get("status") == 3:
                return True, "Задача уже завершена"
            return True, response_data
        
        return False, f"Ошибка: {response.status_code} - {response.text}"
    except Exception as e:
        return False, f"Ошибка соединения: {e}"

def complete_task(task_id: str) -> Tuple[bool, str]:
    try:
        url = f"{API_BASE_URL}/ProjectTask/{task_id}/complete"
        response = requests.post(url)
        
        if response.status_code == 200:
            return True, "Задача успешно завершена"
        elif response.status_code == 404:
            return False, "Задача не найдена"
        
        return False, f"Ошибка: {response.status_code} - {response.text}"
    except Exception as e:
        return False, f"Ошибка соединения: {e}"

def send_to_review(task_id: str) -> Tuple[bool, str]:
    try:
        url = f"{API_BASE_URL}/ProjectTask/{task_id}/review"
        response = requests.post(url)
        
        if response.status_code == 200:
            return True, "Задача отправлена на ревью"
        
        return False, f"Ошибка: {response.status_code} - {response.text}"
    except Exception as e:
        return False, f"Ошибка соединения: {e}"

def get_task_status(task_id: str) -> Optional[Dict]:
    try:
        url = f"{API_BASE_URL}/ProjectTask/status/{task_id}"
        response = requests.get(url)
        
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None

def get_task_by_id(task_id: str) -> Optional[Dict]:
    try:
        url = f"{API_BASE_URL}/ProjectTask/{task_id}"
        response = requests.get(url)
        
        if response.status_code == 200:
            return response.json()
        
        if response.status_code == 404:
            review_url = f"{API_BASE_URL}/ProjectTask/review/{task_id}"
            review_response = requests.get(review_url)
            
            if review_response.status_code == 200:
                return review_response.json()
                
        return None
    except Exception:
        return None

def get_all_workers() -> Optional[List[Dict]]:
    try:
        url = f"{API_BASE_URL}/workers"
        response = requests.get(url)
        
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None

def get_worker_by_id(telegram_id: int) -> Optional[Dict]:
    try:
        url = f"{API_BASE_URL}/workers/{telegram_id}"
        response = requests.get(url)
        
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None