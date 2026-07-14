"""Wiki 目录应用服务。"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID, uuid4

from app.schemas.wiki.folders import (
    WikiFolderCreateRequest,
    WikiFolderResponse,
    WikiFolderUpdateRequest,
)
from app.wiki.errors import WikiNotFoundError, WikiPermissionError, WikiValidationError
from app.wiki.models import WikiFolder
from app.wiki.scope import WikiScope


class FolderStore(Protocol):
    async def find_folder(self, scope: WikiScope, folder_id: UUID) -> WikiFolder | None: ...

    async def list_folders(
        self, scope: WikiScope, parent_id: UUID | None
    ) -> list[WikiFolder]: ...

    async def insert_folder(
        self, scope: WikiScope, folder: WikiFolder
    ) -> WikiFolder: ...

    async def max_descendant_depth(
        self, scope: WikiScope, folder: WikiFolder
    ) -> int: ...

    async def move_folder_tree(
        self,
        scope: WikiScope,
        folder: WikiFolder,
        *,
        parent_id: UUID | None,
        name: str,
        sort_order: int,
        new_path: str,
        new_depth: int,
    ) -> WikiFolder: ...

    async def delete_empty_folder(
        self, scope: WikiScope, folder: WikiFolder
    ) -> None: ...


class WikiFolderService:
    MAX_DEPTH = 3

    def __init__(self, store: FolderStore) -> None:
        self._store = store

    @staticmethod
    def _require_write(scope: WikiScope) -> None:
        if not scope.can_write:
            raise WikiPermissionError()

    @staticmethod
    def _name(value: str) -> str:
        name = value.strip()
        if not name or "/" in name:
            raise WikiValidationError("INVALID_FOLDER_NAME", "目录名称不能为空或包含斜杠")
        return name

    async def create_folder(
        self, scope: WikiScope, request: WikiFolderCreateRequest
    ) -> WikiFolderResponse:
        self._require_write(scope)
        parent = await self._parent(scope, request.parent_id)
        depth = (parent.depth if parent else 0) + 1
        if depth > self.MAX_DEPTH:
            raise WikiValidationError("FOLDER_DEPTH_EXCEEDED", "Wiki 目录最多允许 3 层")
        name = self._name(request.name)
        now = datetime.now(UTC)
        folder = WikiFolder(
            id=uuid4(),
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            parent_id=request.parent_id,
            name=name,
            path=f"{parent.path if parent else ''}/{name}",
            depth=depth,
            sort_order=request.sort_order,
            created_at=now,
            updated_at=now,
            deleted_at=None,
        )
        return WikiFolderResponse.model_validate(
            await self._store.insert_folder(scope, folder)
        )

    async def list_folders(
        self, scope: WikiScope, parent_id: UUID | None = None
    ) -> list[WikiFolderResponse]:
        if parent_id is not None and await self._store.find_folder(scope, parent_id) is None:
            raise WikiNotFoundError("FOLDER_NOT_FOUND", "Wiki 目录不存在")
        return [
            WikiFolderResponse.model_validate(folder)
            for folder in await self._store.list_folders(scope, parent_id)
        ]

    async def update_folder(
        self,
        scope: WikiScope,
        folder_id: UUID,
        request: WikiFolderUpdateRequest,
    ) -> WikiFolderResponse:
        self._require_write(scope)
        folder = await self._store.find_folder(scope, folder_id)
        if folder is None:
            raise WikiNotFoundError("FOLDER_NOT_FOUND", "Wiki 目录不存在")

        parent_id = request.parent_id if "parent_id" in request.model_fields_set else folder.parent_id
        if parent_id == folder.id:
            raise WikiValidationError("FOLDER_CYCLE", "目录不能移动到自己的子树")
        parent = await self._parent(scope, parent_id)
        if parent is not None and (
            parent.path == folder.path or parent.path.startswith(folder.path + "/")
        ):
            raise WikiValidationError("FOLDER_CYCLE", "目录不能移动到自己的子树")

        name = self._name(request.name) if "name" in request.model_fields_set and request.name is not None else folder.name
        sort_order = request.sort_order if request.sort_order is not None else folder.sort_order
        new_depth = (parent.depth if parent else 0) + 1
        descendant_depth = await self._store.max_descendant_depth(scope, folder)
        if new_depth + descendant_depth > self.MAX_DEPTH:
            raise WikiValidationError("FOLDER_DEPTH_EXCEEDED", "移动后 Wiki 目录会超过 3 层")
        new_path = f"{parent.path if parent else ''}/{name}"
        moved = await self._store.move_folder_tree(
            scope,
            folder,
            parent_id=parent_id,
            name=name,
            sort_order=sort_order,
            new_path=new_path,
            new_depth=new_depth,
        )
        return WikiFolderResponse.model_validate(moved)

    async def delete_folder(self, scope: WikiScope, folder_id: UUID) -> None:
        self._require_write(scope)
        folder = await self._store.find_folder(scope, folder_id)
        if folder is None:
            raise WikiNotFoundError("FOLDER_NOT_FOUND", "Wiki 目录不存在")
        await self._store.delete_empty_folder(scope, folder)

    async def _parent(
        self, scope: WikiScope, parent_id: UUID | None
    ) -> WikiFolder | None:
        if parent_id is None:
            return None
        parent = await self._store.find_folder(scope, parent_id)
        if parent is None:
            raise WikiNotFoundError("FOLDER_NOT_FOUND", "父目录不存在")
        return parent
