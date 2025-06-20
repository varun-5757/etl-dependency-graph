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

    # Create 2 types of edges: source -> job and job -> target
    edges = []
    edge_styles = {}  # for bolding selected node's edges
    for _, row in df.iterrows():
        edges.append((row["source"], row["job"]))
        edge_styles[(row["source"], row["job"])] = row["job"]
        edges.append((row["job"], row["target"]))
        edge_styles[(row["job"], row["target"])] = row["job"]

    all_nodes = set()
    all_nodes.update(df["source"].tolist())
    all_nodes.update(df["target"].tolist())
    all_nodes.update(df["job"].tolist())

    @st.cache_data(show_spinner=False)
    def get_valid_nodes():
        return sorted([n for n in all_nodes if isinstance(n, str) and n.strip() != ''])

    def render_sidebar():
        st.sidebar.header("Explore Dependencies")
        selected_node = st.sidebar.selectbox(
            "Select a table/job/report:",
            options=get_valid_nodes(),
            index=0,
            key="node_select",
            format_func=lambda x: x,
        )
        direction = st.sidebar.radio("Dependency Direction", ["Downstream (Impact)", "Upstream (Lineage)"], key="direction_radio")
        return selected_node.strip(), direction

    selected_node, direction = render_sidebar()

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
                neighbors = graph.successors(current) if direction == "downstream" else graph.predecessors(current)
                for n in neighbors:
                    sub_edges.append((current, n)) if direction == "downstream" else sub_edges.append((n, current))
                    to_visit.append(n)
            except nx.NetworkXError:
                continue
        return sub_edges

    selected_raw_edges = get_subgraph(g, selected_node, direction="downstream" if direction.startswith("Down") else "upstream")

    filtered_nodes = set()
    directly_connected = set()
    for src, tgt in selected_raw_edges:
        filtered_nodes.add(src)
        filtered_nodes.add(tgt)
        if selected_node in (src, tgt):
            directly_connected.add(tgt if src == selected_node else src)

    net = Network(height="750px", width="100%", directed=True, notebook=False)
    net.set_options(json.dumps({
        "nodes": {"size": 18, "font": {"size": 14, "multi": "html"}},
        "edges": {
            "arrows": {"to": {"enabled": True}},
            "smooth": {"enabled": False},
            "color": {"color": "#848484"}
        },
        "layout": {
            "hierarchical": {
                "enabled": True,
                "direction": "UD",
                "sortMethod": "directed",
                "nodeSpacing": 150,
                "treeSpacing": 300,
                "levelSeparation": 200
            }
        },
        "physics": {
            "enabled": False
        },
        "interaction": {
            "navigationButtons": True,
            "keyboard": True,
            "dragNodes": True,
            "dragView": True,
            "zoomView": True,
            "tooltipDelay": 200,
            "multiselect": True
        }
    }, indent=2))

    for node in filtered_nodes:
        if node and node.lower() != 'none':
            node_type = "job" if node in df["job"].values else "table"
            color = "#1f77b4" if node_type == "job" else "#2ca02c"  # darker blue and green
            font = {"size": 14, "bold": True} if node in directly_connected or node == selected_node else {"size": 14}
            net.add_node(node, label=node, color=color, font=font)

    for src, tgt in selected_raw_edges:
        if all([src, tgt]) and all(x.lower() != 'none' for x in [src, tgt]):
            edge_color = "#333333" if selected_node in (src, tgt) else "#848484"
            edge_width = 3 if selected_node in (src, tgt) else 1
            net.add_edge(src, tgt, color=edge_color, width=edge_width)

    # Inject JavaScript to auto-adjust zoom if labels overlap
    zoom_script = """
    <script type="text/javascript">
    window.addEventListener('load', function() {
        let attempts = 0;
        const interval = setInterval(function() {
            let labels = document.querySelectorAll('div.vis-network canvas');
            if (labels.length > 0 || attempts > 10) {
                let container = document.querySelector('div.vis-network');
                if (container && container.network && container.network.fit) {
                    container.network.fit();
                }
                clearInterval(interval);
            }
            attempts++;
        }, 1000);
    });
    </script>
    """

    html_content = ""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
        net.save_graph(tmp_file.name)
        with open(tmp_file.name, 'r', encoding='utf-8') as f:
            html_content = f.read()
        os.unlink(tmp_file.name)

    if html_content:
        components.html(html_content + zoom_script, height=800, scrolling=True)

        with st.expander("Legend", expanded=True):
            st.markdown("""
            - 🟦 Jobs  
            - 🟩 Tables
            """)

    if not filtered_nodes:
        st.warning("No connected nodes found for the selected input.")
    else:
        filtered_df = df[df.apply(
            lambda row: row['source'] in filtered_nodes and row['target'] in filtered_nodes and row['job'] in filtered_nodes,
            axis=1)]
        st.subheader("ETL Mapping Table")
        st.dataframe(filtered_df[["JOBID", "PROJECT_NAME", "job", "source", "target"]])

    st.write("✅ App finished rendering.")


if __name__ == "__main__":
    main()
