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

    # Strip whitespace and filter out empty or 'None' strings
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    df = df.dropna(subset=["source", "target", "job"])
    df = df[~df['source'].str.lower().eq('none')]
    df = df[~df['target'].str.lower().eq('none')]
    df = df[~df['job'].str.lower().eq('none')]
    df = df[(df['source'] != '') & (df['target'] != '') & (df['job'] != '')]

    # Create 2 types of edges: source -> job and job -> target
    edges = []
    for _, row in df.iterrows():
        edges.append((row["source"], row["job"]))
        edges.append((row["job"], row["target"]))

    all_nodes = set(df["source"]) | set(df["target"]) | set(df["job"])

    @st.cache_data(show_spinner=False)
    def get_valid_nodes():
        return sorted([n for n in all_nodes if isinstance(n, str) and n.strip() and n.lower() != 'none'])

    def render_sidebar():
        st.sidebar.header("Explore Dependencies")
        selected_node = st.sidebar.selectbox(
            "Select a table/job/report:",
            options=get_valid_nodes(),
            index=0,
            key="node_select",
            format_func=lambda x: x,
        )
        direction = st.sidebar.radio(
            "Dependency Direction",
            ["Downstream (Impact)", "Upstream (Lineage)"],
            key="direction_radio"
        )
        return selected_node.strip(), direction

    selected_node, direction = render_sidebar()

    # Build graph
    g = nx.DiGraph()
    for src, tgt in edges:
        g.add_edge(src, tgt)

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
                nbrs = graph.successors(current) if direction == "downstream" else graph.predecessors(current)
                for n in nbrs:
                    edge = (current, n) if direction == "downstream" else (n, current)
                    sub_edges.append(edge)
                    to_visit.append(n)
            except nx.NetworkXError:
                continue
        return sub_edges

    selected_raw_edges = get_subgraph(
        g,
        selected_node,
        direction="downstream" if direction.startswith("Down") else "upstream"
    )

    # Filter nodes
    filtered_nodes = set()
    for src, tgt in selected_raw_edges:
        filtered_nodes.add(src)
        filtered_nodes.add(tgt)

    # Render network
    net = Network(height="750px", width="100%", directed=True, notebook=False)
    net.set_options(json.dumps({
        "nodes": {"size": 18, "font": {"size": 14, "multi": "html"}},
        "edges": {
            "arrows": {"to": {"enabled": True}},
            "smooth": {"enabled": False},
            "color": {"color": "#A9A9A9"}
        },
        "layout": {
            "hierarchical": {
                "enabled": True,
                "direction": "UD",
                "sortMethod": "directed"
            }
        },
        "physics": {"enabled": False},
        "interaction": {
            "navigationButtons": True,
            "keyboard": True,
            "dragNodes": True,
            "dragView": True,
            "zoomView": True
        }
    }, indent=2))

    # Add nodes
    for node in filtered_nodes:
        node_type = "job" if node in df["job"].values else "table"
        color = "lightblue" if node_type == "job" else "lightgreen"
        net.add_node(node, label=node, color=color)

    # Add edges
    for src, tgt in selected_raw_edges:
        net.add_edge(src, tgt)

    # Generate HTML
    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
        net.save_graph(tmp_file.name)
        html_content = open(tmp_file.name, 'r', encoding='utf-8').read()
        os.unlink(tmp_file.name)

    if html_content:
        components.html(html_content, height=800, scrolling=True)
        with st.expander("Legend", expanded=True):
            st.markdown("""
            - 🟦 Jobs  
            - 🟩 Tables
            """)

    if not filtered_nodes:
        st.warning("No connected nodes found for the selected input.")
    else:
        filtered_df = df[df.apply(
            lambda row: (
                row['source'] in filtered_nodes and
                row['target'] in filtered_nodes and
                row['job'] in filtered_nodes
            ), axis=1
        )]
        st.subheader("ETL Mapping Table")
        st.dataframe(filtered_df[["JOBID", "PROJECT_NAME", "job", "source", "target"]])

    st.write("✅ App finished rendering.")


if __name__ == "__main__":
    main()
