"""Wiki 目录 PostgreSQL 仓储。"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import Select, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import load_only

from app.wiki.errors import WikiConflictError, WikiNotFoundError, WikiValidationError
from app.wiki.models import WikiFolder, WikiLogEntry, WikiPage
from app.wiki.scope import WikiScope


def _folder_scope(scope: WikiScope):
    return (
        WikiFolder.tenant_id == scope.tenant_id,
        WikiFolder.knowledge_base_id == scope.knowledge_base_id,
        WikiFolder.deleted_at.is_(None),
    )


def build_folder_lookup_statement(
    scope: WikiScope, folder_id: UUID, *, for_update: bool = False
) -> Select[tuple[WikiFolder]]:
    statement = select(WikiFolder).where(*_folder_scope(scope), WikiFolder.id == folder_id)
    return statement.with_for_update() if for_update else statement


def build_folder_subtree_statement(
    scope: WikiScope, path: str
) -> Select[tuple[WikiFolder]]:
    return select(WikiFolder).where(
        *_folder_scope(scope),
        or_(WikiFolder.path == path, WikiFolder.path.startswith(f"{path}/", autoescape=True)),
    )


class SqlAlchemyFolderStore:
    """原子维护目录子树及页面目录缓存。"""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def find_folder(self, scope: WikiScope, folder_id: UUID) -> WikiFolder | None:
        result = await self._session.execute(
            build_folder_lookup_statement(scope, folder_id, for_update=False)
        )
        return result.scalar_one_or_none()

    async def list_folders(
        self, scope: WikiScope, parent_id: UUID | None
    ) -> list[WikiFolder]:
        parent_filter = (
            WikiFolder.parent_id.is_(None)
            if parent_id is None
            else WikiFolder.parent_id == parent_id
        )
        result = await self._session.execute(
            select(WikiFolder)
            .where(*_folder_scope(scope), parent_filter)
            .order_by(WikiFolder.sort_order, WikiFolder.name, WikiFolder.id)
        )
        return list(result.scalars())

    async def insert_folder(
        self, scope: WikiScope, folder: WikiFolder
    ) -> WikiFolder:
        if folder.parent_id is not None:
            parent_result = await self._session.execute(
                build_folder_lookup_statement(scope, folder.parent_id, for_update=True)
            )
            parent = parent_result.scalar_one_or_none()
            if parent is None:
                raise WikiNotFoundError("FOLDER_NOT_FOUND", "父目录不存在或已删除")
            folder.path = f"{parent.path}/{folder.name}"
            folder.depth = parent.depth + 1
            if folder.depth > 3:
                raise WikiValidationError("FOLDER_DEPTH_EXCEEDED", "Wiki 目录最多允许 3 层")
        self._session.add(folder)
        self._append_log(scope, "folder_created", f"创建目录 {folder.path}")
        try:
            await self._session.flush()
        except IntegrityError as exc:
            await self._session.rollback()
            raise WikiConflictError("FOLDER_NAME_CONFLICT", "同级目录名称已存在") from exc
        return folder

    async def max_descendant_depth(
        self, scope: WikiScope, folder: WikiFolder
    ) -> int:
        result = await self._session.execute(
            select(func.max(WikiFolder.depth)).where(
                *_folder_scope(scope),
                WikiFolder.path.startswith(f"{folder.path}/", autoescape=True),
            )
        )
        maximum = result.scalar_one_or_none()
        return max((maximum or folder.depth) - folder.depth, 0)

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
        locked_result = await self._session.execute(
            build_folder_lookup_statement(scope, folder.id, for_update=True)
        )
        locked = locked_result.scalar_one_or_none()
        if locked is None:
            raise WikiNotFoundError("FOLDER_NOT_FOUND", "Wiki 目录不存在")

        parent: WikiFolder | None = None
        if parent_id is not None:
            parent_result = await self._session.execute(
                build_folder_lookup_statement(scope, parent_id, for_update=True)
            )
            parent = parent_result.scalar_one_or_none()
            if parent is None:
                raise WikiNotFoundError("FOLDER_NOT_FOUND", "目标父目录不存在")
            if parent.path == locked.path or parent.path.startswith(locked.path + "/"):
                raise WikiValidationError("FOLDER_CYCLE", "目录不能移动到自己的子树")

        conflict_result = await self._session.execute(
            select(WikiFolder.id).where(
                *_folder_scope(scope),
                WikiFolder.parent_id.is_(None)
                if parent_id is None
                else WikiFolder.parent_id == parent_id,
                WikiFolder.name == name,
                WikiFolder.id != locked.id,
            )
        )
        if conflict_result.scalar_one_or_none() is not None:
            raise WikiConflictError("FOLDER_NAME_CONFLICT", "同级目录名称已存在")

        subtree_result = await self._session.execute(
            build_folder_subtree_statement(scope, locked.path)
            .order_by(WikiFolder.depth)
            .with_for_update()
        )
        subtree = list(subtree_result.scalars())
        actual_new_depth = (parent.depth if parent else 0) + 1
        relative_max = max(item.depth - locked.depth for item in subtree)
        if actual_new_depth + relative_max > 3:
            raise WikiValidationError("FOLDER_DEPTH_EXCEEDED", "移动后 Wiki 目录会超过 3 层")
        actual_new_path = f"{parent.path if parent else ''}/{name}"
        old_path = locked.path
        depth_delta = actual_new_depth - locked.depth

        locked.parent_id = parent_id
        locked.name = name
        locked.sort_order = sort_order
        for item in subtree:
            item.path = actual_new_path + item.path[len(old_path) :]
            item.depth += depth_delta
            item.updated_at = datetime.now(UTC)

        folder_paths = {item.id: self._path_parts(item.path) for item in subtree}
        page_result = await self._session.execute(
            select(WikiPage)
            .options(
                load_only(
                    WikiPage.id,
                    WikiPage.folder_id,
                    WikiPage.slug,
                    WikiPage.category_path,
                    WikiPage.wiki_path,
                    WikiPage.depth,
                    WikiPage.updated_at,
                )
            )
            .where(
                WikiPage.tenant_id == scope.tenant_id,
                WikiPage.knowledge_base_id == scope.knowledge_base_id,
                WikiPage.deleted_at.is_(None),
                WikiPage.folder_id.in_(folder_paths),
            )
            .with_for_update()
        )
        for page in page_result.scalars():
            category_path = folder_paths[page.folder_id]
            page.category_path = category_path
            page.depth = len(category_path)
            page.wiki_path = "/" + "/".join([*category_path, page.slug])
            page.updated_at = datetime.now(UTC)

        self._append_log(scope, "folder_moved", f"移动目录 {old_path} 到 {actual_new_path}")
        try:
            await self._session.flush()
        except IntegrityError as exc:
            await self._session.rollback()
            raise WikiConflictError("FOLDER_NAME_CONFLICT", "同级目录名称已存在") from exc
        return locked

    async def delete_empty_folder(
        self, scope: WikiScope, folder: WikiFolder
    ) -> None:
        locked_result = await self._session.execute(
            build_folder_lookup_statement(scope, folder.id, for_update=True)
        )
        locked = locked_result.scalar_one_or_none()
        if locked is None:
            raise WikiNotFoundError("FOLDER_NOT_FOUND", "Wiki 目录不存在")
        child_count = int(
            (
                await self._session.execute(
                    select(func.count(WikiFolder.id)).where(
                        *_folder_scope(scope), WikiFolder.parent_id == locked.id
                    )
                )
            ).scalar_one()
        )
        page_count = int(
            (
                await self._session.execute(
                    select(func.count(WikiPage.id)).where(
                        WikiPage.tenant_id == scope.tenant_id,
                        WikiPage.knowledge_base_id == scope.knowledge_base_id,
                        WikiPage.folder_id == locked.id,
                        WikiPage.deleted_at.is_(None),
                    )
                )
            ).scalar_one()
        )
        if child_count or page_count:
            raise WikiConflictError("FOLDER_NOT_EMPTY", "目录包含子目录或页面，不能删除")
        locked.deleted_at = datetime.now(UTC)
        locked.updated_at = datetime.now(UTC)
        self._append_log(scope, "folder_deleted", f"删除目录 {locked.path}")
        await self._session.flush()

    @staticmethod
    def _path_parts(path: str) -> list[str]:
        return [part for part in path.split("/") if part]

    def _append_log(self, scope: WikiScope, action: str, message: str) -> None:
        self._session.add(
            WikiLogEntry(
                tenant_id=scope.tenant_id,
                knowledge_base_id=scope.knowledge_base_id,
                operation_id=uuid4(),
                action=action,
                message=message,
                pages_affected=[],
                actor_id=scope.actor_id,
            )
        )
