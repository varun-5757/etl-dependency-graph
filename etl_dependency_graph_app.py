# etl_dependency_graph_streamlit_app.py
import pandas as pd
import networkx as nx
from pyvis.network import Network
import streamlit as st
import streamlit.components.v1 as components
import tempfile
import os
import json  # ✅ Added missing import


def main():
    st.set_page_config(layout="wide")
    st.title("ETL Dependency Graph Viewer")

    # ------------------------
    # Load dataset from uploaded CSV file
    # ------------------------
    try:
        df = pd.read_csv("New_Data_FMS.csv")
        df = df.rename(columns={
            "JOB_NAME": "job",
            "SOURCE_OBJECT_NAME": "source",
            "TARGET_OBJECT_NAME": "target"
        })
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return

    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

    df = df.dropna(subset=["source", "target", "job"])
    df = df[(df['source'] != '') & (df['target'] != '') & (df['job'] != '')]
    edges = [(row["source"], row["target"], row["job"]) for _, row in df.iterrows()]
    nodes = sorted(set(df["source"]).union(set(df["target"])))

    @st.cache_data(show_spinner=False)
    def get_valid_nodes():
        return [n for n in nodes if isinstance(n, str) and n.strip() != '']

    def render_sidebar():
        st.sidebar.header("Explore Dependencies")
        selected_node = st.sidebar.selectbox("Select a table/job/report:", get_valid_nodes(), key="node_select")
        direction = st.sidebar.radio("Dependency Direction", ["Downstream (Impact)", "Upstream (Lineage)"], key="direction_radio")
        return selected_node.strip(), direction

    selected_node, direction = render_sidebar()

    g = nx.DiGraph()
    for src, tgt, job in edges:
        g.add_edge(src, tgt, label=job)

    def get_subgraph(graph, start_node, direction="downstream"):
        visited = set()
        to_visit = [start_node.strip()]
        sub_edges = []
        while to_visit:
            current = to_visit.pop()
            if current in visited:
                continue
            visited.add(current)
            try:
                neighbors = graph.successors(current) if direction == "downstream" else graph.predecessors(current)
                for n in neighbors:
                    sub_edges.append((current, n)) if direction == "downstream" else sub_edges.append((n, current))
                    to_visit.append(n)
            except nx.NetworkXError:
                continue
        return sub_edges

    selected_raw_edges = get_subgraph(g, selected_node, direction="downstream" if direction.startswith("Down") else "upstream")

    filtered_nodes = set()
    for src, tgt in selected_raw_edges:
        filtered_nodes.add(src)
        filtered_nodes.add(tgt)

    selected_edges = [
        (src, tgt, job)
        for (src, tgt, job) in edges
        if (src, tgt) in selected_raw_edges or (tgt, src) in selected_raw_edges
    ]

    net = Network(height="700px", width="100%", directed=True, notebook=False)
    net.set_options(json.dumps({
        "nodes": {"size": 18, "font": {"size": 14, "multi": "html"}},
        "edges": {
            "arrows": {"to": {"enabled": True}},
            "font": {"size": 12, "align": "middle", "multi": "html"},
            "smooth": {"enabled": False}
        },
        "layout": {"improvedLayout": True},
        "physics": {"enabled": False},
        "interaction": {
            "navigationButtons": True,
            "keyboard": True,
            "dragNodes": True,
            "zoomView": True
        }
    }, indent=2))

    for node in filtered_nodes:
        if node and node.lower() != 'none':
            net.add_node(node, label=node)

    for src, tgt, job in selected_edges:
        if all([src, tgt, job]) and all(x.lower() != 'none' for x in [src, tgt, job]):
            net.add_edge(src, tgt, label=job, color="red")

    html_content = ""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
        net.save_graph(tmp_file.name)
        with open(tmp_file.name, 'r', encoding='utf-8') as f:
            html_content = f.read()
        os.unlink(tmp_file.name)

    if html_content:
        components.html(html_content, height=750, scrolling=True)

    if not filtered_nodes:
        st.warning("No connected nodes found for the selected input.")
    else:
        filtered_df = df[df.apply(lambda row: row['source'] in filtered_nodes and row['target'] in filtered_nodes, axis=1)]
        st.subheader("Filtered ETL Mapping Table")
        st.dataframe(filtered_df[["JOBID", "PROJECT_NAME", "job", "source", "target"]])

    st.write("✅ App finished rendering.")


if __name__ == "__main__":
    main()
