from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from app.schemas.wiki.folders import WikiFolderCreateRequest, WikiFolderUpdateRequest
from app.wiki.errors import WikiConflictError, WikiValidationError
from app.wiki.folder_service import WikiFolderService
from app.wiki.models import WikiFolder
from app.wiki.scope import WikiScope


class MemoryFolderStore:
    def __init__(self) -> None:
        self.folders: dict[UUID, WikiFolder] = {}
        self.page_caches: dict[UUID, tuple[list[str], str]] = {}

    async def find_folder(self, scope: WikiScope, folder_id: UUID) -> WikiFolder | None:
        folder = self.folders.get(folder_id)
        if folder is None or folder.deleted_at is not None:
            return None
        if folder.tenant_id != scope.tenant_id or folder.knowledge_base_id != scope.knowledge_base_id:
            return None
        return folder

    async def list_folders(self, scope: WikiScope, parent_id: UUID | None) -> list[WikiFolder]:
        return sorted(
            [
                folder
                for folder in self.folders.values()
                if folder.tenant_id == scope.tenant_id
                and folder.knowledge_base_id == scope.knowledge_base_id
                and folder.parent_id == parent_id
                and folder.deleted_at is None
            ],
            key=lambda folder: (folder.sort_order, folder.name),
        )

    async def insert_folder(self, scope: WikiScope, folder: WikiFolder) -> WikiFolder:
        if any(
            item.knowledge_base_id == scope.knowledge_base_id
            and item.parent_id == folder.parent_id
            and item.name == folder.name
            and item.deleted_at is None
            for item in self.folders.values()
        ):
            raise WikiConflictError("FOLDER_NAME_CONFLICT", "同级目录重名")
        self.folders[folder.id] = folder
        return folder

    async def max_descendant_depth(self, scope: WikiScope, folder: WikiFolder) -> int:
        return max(
            (item.depth - folder.depth for item in self.folders.values() if item.path.startswith(folder.path + "/")),
            default=0,
        )

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
    ) -> WikiFolder:
        old_path = folder.path
        depth_delta = new_depth - folder.depth
        affected = [item for item in self.folders.values() if item.path == old_path or item.path.startswith(old_path + "/")]
        for item in affected:
            item.path = new_path + item.path[len(old_path) :]
            item.depth += depth_delta
        folder.parent_id = parent_id
        folder.name = name
        folder.sort_order = sort_order
        for page_id, (category_path, slug) in list(self.page_caches.items()):
            old_parts = [part for part in old_path.split("/") if part]
            if category_path[: len(old_parts)] == old_parts:
                suffix = category_path[len(old_parts) :]
                new_parts = [part for part in new_path.split("/") if part] + suffix
                self.page_caches[page_id] = (new_parts, "/" + "/".join([*new_parts, slug]))
        return folder

    async def delete_empty_folder(self, scope: WikiScope, folder: WikiFolder) -> None:
        has_child = any(item.parent_id == folder.id and item.deleted_at is None for item in self.folders.values())
        if has_child:
            raise WikiConflictError("FOLDER_NOT_EMPTY", "目录包含子目录")
        folder.deleted_at = datetime.now(UTC)


@pytest.fixture
def scope() -> WikiScope:
    return WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="editor", can_write=True)


@pytest.mark.asyncio
async def test_create_folder_derives_path_and_depth(scope: WikiScope) -> None:
    service = WikiFolderService(store := MemoryFolderStore())
    root = await service.create_folder(scope, WikiFolderCreateRequest(name=" 技术 "))
    child = await service.create_folder(
        scope, WikiFolderCreateRequest(name="架构", parent_id=root.id)
    )

    assert root.path == "/技术"
    assert root.depth == 1
    assert child.path == "/技术/架构"
    assert child.depth == 2
    assert [item.id for item in await service.list_folders(scope, root.id)] == [child.id]


@pytest.mark.asyncio
async def test_create_folder_rejects_fourth_level(scope: WikiScope) -> None:
    service = WikiFolderService(MemoryFolderStore())
    first = await service.create_folder(scope, WikiFolderCreateRequest(name="一"))
    second = await service.create_folder(scope, WikiFolderCreateRequest(name="二", parent_id=first.id))
    third = await service.create_folder(scope, WikiFolderCreateRequest(name="三", parent_id=second.id))

    with pytest.raises(WikiValidationError, match="3 层"):
        await service.create_folder(scope, WikiFolderCreateRequest(name="四", parent_id=third.id))


@pytest.mark.asyncio
async def test_move_folder_rejects_own_descendant(scope: WikiScope) -> None:
    service = WikiFolderService(MemoryFolderStore())
    root = await service.create_folder(scope, WikiFolderCreateRequest(name="根"))
    child = await service.create_folder(scope, WikiFolderCreateRequest(name="子", parent_id=root.id))

    with pytest.raises(WikiValidationError, match="子树"):
        await service.update_folder(
            scope,
            root.id,
            WikiFolderUpdateRequest.model_validate({"parent_id": child.id}),
        )


@pytest.mark.asyncio
async def test_move_folder_updates_descendant_and_page_caches(scope: WikiScope) -> None:
    service = WikiFolderService(store := MemoryFolderStore())
    source = await service.create_folder(scope, WikiFolderCreateRequest(name="旧"))
    child = await service.create_folder(scope, WikiFolderCreateRequest(name="子", parent_id=source.id))
    target = await service.create_folder(scope, WikiFolderCreateRequest(name="新"))
    page_id = uuid4()
    store.page_caches[page_id] = (["旧", "子"], "entity/acme")

    moved = await service.update_folder(
        scope,
        source.id,
        WikiFolderUpdateRequest(parent_id=target.id, name="迁移"),
    )

    assert moved.path == "/新/迁移"
    assert (await store.find_folder(scope, child.id)).path == "/新/迁移/子"
    assert store.page_caches[page_id] == (
        ["新", "迁移", "子"],
        "/新/迁移/子/entity/acme",
    )


@pytest.mark.asyncio
async def test_delete_non_empty_folder_is_rejected(scope: WikiScope) -> None:
    service = WikiFolderService(store := MemoryFolderStore())
    root = await service.create_folder(scope, WikiFolderCreateRequest(name="根"))
    await service.create_folder(scope, WikiFolderCreateRequest(name="子", parent_id=root.id))

    with pytest.raises(WikiConflictError, match="子目录"):
        await service.delete_folder(scope, root.id)
