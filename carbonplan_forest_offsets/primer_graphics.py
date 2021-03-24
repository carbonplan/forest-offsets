from pygraphviz import AGraph


def norcal_png():
    G = AGraph()
    G.add_node("s", label="Northern California Coast")
    assessment_areas = {
        "a_hilo": "Redwood/Douglas-fir\nMixed Conifer",
        "a_all": "Oak Woodland",
    }
    for node, label in assessment_areas.items():
        G.add_node(node, label=label)
        G.add_edge("s", node)

    site_classes = {"sc_1": "low", "sc_2": "high", "sc_3": "all"}

    for node, label in site_classes.items():
        G.add_node(node, label=label)
        if label == "all":
            G.add_edge("a_all", node)
        else:
            G.add_edge("a_hilo", node)
    G.add_subgraph(
        [node for node in site_classes.keys()],
        name="cluster_x",
        label="Site Class",
        labelloc="b",
        labeljust="b",
        style="dotted",
    )

    # G.add_edge('s', 'a_1')
    png = G.draw(prog="dot", format="png")
    return png


def overview_png():
    G = AGraph()
    G.add_node("s", label="Supersection")
    assessment_areas = {"a_hilo": "Assessment Area 1", "a_all": "Assessment Area 2"}
    for node, label in assessment_areas.items():
        G.add_node(node, label=label)
        G.add_edge("s", node)

    site_classes = {"sc_1": "low", "sc_2": "high", "sc_3": "all"}

    for node, label in site_classes.items():
        G.add_node(node, label=label)
        if label == "all":
            G.add_edge("a_all", node)
        else:
            G.add_edge("a_hilo", node)
    G.add_subgraph(
        [node for node in site_classes.keys()],
        name="cluster_x",
        label="Site Class",
        labelloc="b",
        labeljust="b",
        style="dotted",
    )

    # G.add_edge('s', 'a_1')
    png = G.draw(prog="dot", format="png")
    return png
