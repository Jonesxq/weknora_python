"""基于结构化 Wiki 链接的确定性图算法。"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class GraphPage:
    slug: str
    title: str
    page_type: str
    link_count: int = 0


@dataclass(frozen=True, slots=True)
class WikiGraphEdge:
    source: str
    target: str


@dataclass(frozen=True, slots=True)
class WikiGraph:
    nodes: list[GraphPage]
    edges: list[WikiGraphEdge]


def _filtered_graph(
    pages: list[GraphPage],
    edges: list[WikiGraphEdge],
    allowed_types: set[str] | None,
) -> tuple[dict[str, GraphPage], list[WikiGraphEdge], dict[str, set[str]]]:
    page_map = {
        page.slug: page
        for page in pages
        if allowed_types is None or page.page_type in allowed_types
    }
    valid_edges = sorted(
        {
            edge
            for edge in edges
            if edge.source in page_map and edge.target in page_map and edge.source != edge.target
        },
        key=lambda edge: (edge.source, edge.target),
    )
    adjacency = {slug: set() for slug in page_map}
    for edge in valid_edges:
        adjacency[edge.source].add(edge.target)
        adjacency[edge.target].add(edge.source)
    return page_map, valid_edges, adjacency


def _node(page: GraphPage, adjacency: dict[str, set[str]]) -> GraphPage:
    return GraphPage(
        slug=page.slug,
        title=page.title,
        page_type=page.page_type,
        link_count=len(adjacency[page.slug]),
    )


def build_overview_graph(
    pages: list[GraphPage],
    edges: list[WikiGraphEdge],
    *,
    limit: int,
    allowed_types: set[str] | None = None,
) -> WikiGraph:
    """按连接度和 slug 稳定选出 overview 节点及其内部边。"""

    page_map, valid_edges, adjacency = _filtered_graph(pages, edges, allowed_types)
    nodes = sorted(
        (_node(page, adjacency) for page in page_map.values()),
        key=lambda page: (-page.link_count, page.slug),
    )[:limit]
    selected = {node.slug for node in nodes}
    return WikiGraph(
        nodes=nodes,
        edges=[
            edge for edge in valid_edges if edge.source in selected and edge.target in selected
        ],
    )


def build_ego_graph(
    pages: list[GraphPage],
    edges: list[WikiGraphEdge],
    *,
    center: str,
    hops: int,
    limit: int,
    allowed_types: set[str] | None = None,
) -> WikiGraph:
    """从中心页执行无向 BFS，类型过滤同时阻止继续扩展。"""

    page_map, valid_edges, adjacency = _filtered_graph(pages, edges, allowed_types)
    if center not in page_map or limit <= 0:
        return WikiGraph(nodes=[], edges=[])

    distance = {center: 0}
    queue = deque([center])
    while queue:
        current = queue.popleft()
        if distance[current] >= hops:
            continue
        for neighbor in sorted(adjacency[current]):
            if neighbor not in distance:
                distance[neighbor] = distance[current] + 1
                queue.append(neighbor)

    remaining = sorted(
        (slug for slug in distance if slug != center),
        key=lambda slug: (distance[slug], -len(adjacency[slug]), slug),
    )
    selected_order = [center, *remaining][:limit]
    selected = set(selected_order)
    return WikiGraph(
        nodes=[_node(page_map[slug], adjacency) for slug in selected_order],
        edges=[
            edge for edge in valid_edges if edge.source in selected and edge.target in selected
        ],
    )
