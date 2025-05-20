from dataclasses import dataclass
from uuid import UUID
from typing import Optional

@dataclass
class Task:
    task_id: UUID
    title: str
    text: str
    department: str
    tg_id: int = None
    assigned_to: Optional[int] = None
    completed: bool = False
    in_review: bool = False