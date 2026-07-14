"""每次 Wiki 调用必须携带的租户与权限范围。"""

from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True, slots=True)
class WikiScope:
    tenant_id: int
    knowledge_base_id: UUID
    actor_id: str
    can_write: bool = False
