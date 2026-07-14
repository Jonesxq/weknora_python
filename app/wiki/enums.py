"""Wiki 领域枚举。"""

from enum import StrEnum


class WikiPageType(StrEnum):
    SUMMARY = "summary"
    ENTITY = "entity"
    CONCEPT = "concept"
    INDEX = "index"
    LOG = "log"
    SYNTHESIS = "synthesis"
    COMPARISON = "comparison"


class WikiPageStatus(StrEnum):
    DRAFT = "draft"
    PUBLISHED = "published"
    ARCHIVED = "archived"


class WikiIssueStatus(StrEnum):
    PENDING = "pending"
    IGNORED = "ignored"
    RESOLVED = "resolved"
