from __future__ import annotations

from app.wiki.graph import GraphPage, WikiGraphEdge, build_ego_graph, build_overview_graph


PAGES = [
    GraphPage(slug="entity/a", title="A", page_type="entity"),
    GraphPage(slug="entity/b", title="B", page_type="entity"),
    GraphPage(slug="concept/c", title="C", page_type="concept"),
    GraphPage(slug="summary/hidden", title="Hidden", page_type="summary"),
]
EDGES = [
    WikiGraphEdge(source="entity/a", target="entity/b"),
    WikiGraphEdge(source="entity/a", target="concept/c"),
    WikiGraphEdge(source="entity/b", target="concept/c"),
    WikiGraphEdge(source="entity/b", target="summary/hidden"),
    WikiGraphEdge(source="summary/hidden", target="concept/c"),
]


def test_overview_keeps_only_edges_between_stably_ranked_top_nodes() -> None:
    graph = build_overview_graph(PAGES, EDGES, limit=2)

    assert [node.slug for node in graph.nodes] == ["concept/c", "entity/b"]
    assert graph.edges == [WikiGraphEdge(source="entity/b", target="concept/c")]
    assert [node.link_count for node in graph.nodes] == [3, 3]


def test_ego_graph_treats_links_as_undirected() -> None:
    graph = build_ego_graph(PAGES, EDGES, center="concept/c", hops=1, limit=20)

    assert {node.slug for node in graph.nodes} == {
        "concept/c",
        "entity/a",
        "entity/b",
        "summary/hidden",
    }


def test_type_filter_blocks_hidden_nodes_and_traversal_through_them() -> None:
    graph = build_ego_graph(
        PAGES,
        EDGES,
        center="entity/b",
        hops=2,
        limit=20,
        allowed_types={"entity"},
    )

    assert [node.slug for node in graph.nodes] == ["entity/b", "entity/a"]
    assert graph.edges == [WikiGraphEdge(source="entity/a", target="entity/b")]


def test_graph_hard_limit_is_applied_deterministically() -> None:
    graph = build_ego_graph(PAGES, EDGES, center="entity/a", hops=3, limit=2)

    assert [node.slug for node in graph.nodes] == ["entity/a", "concept/c"]
