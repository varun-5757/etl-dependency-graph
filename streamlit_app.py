# etl_dependency_graph_streamlit_app.py
import pandas as pd
import networkx as nx
from pyvis.network import Network
import streamlit as st
import streamlit.components.v1 as components
import tempfile
import os
import json


def main():
    st.set_page_config(layout="wide")
    st.title("ETL Dependency Graph Viewer")

    # —— 1. Load and tidy data ——
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
    df = df[(df["source"] != "") & (df["target"] != "") & (df["job"] != "")]

    # —— 2. Build *deduplicated* edge set + lookup ——
    edges_set: set[tuple[str, str]] = set()
    edge_map: dict[tuple[str, str], str] = {}

    for _, row in df.iterrows():
        src_job = (row["source"], row["job"])
        job_tgt = (row["job"], row["target"])
        edges_set.add(src_job)
        edges_set.add(job_tgt)
        edge_map[src_job] = row["job"]
        edge_map[job_tgt] = row["job"]

    edges = list(edges_set)  # guaranteed unique – fixes unintended bold duplicates

    all_nodes = set(df["source"]).union(df["target"]).union(df["job"])  # for dropdown

    @st.cache_data(show_spinner=False)
    def get_valid_nodes():
        return sorted([n for n in all_nodes if isinstance(n, str) and n.strip().lower() != "none"])

    # —— 3. Sidebar controls ——
    st.sidebar.header("Explore Dependencies")
    selected_node = st.sidebar.selectbox(
        "Select a table/job/report:",
        options=get_valid_nodes(),
        index=0,
        key="node_select",
    ).strip()
    direction_choice = st.sidebar.radio(
        "Dependency Direction", ["Downstream (Impact)", "Upstream (Lineage)"], key="direction_radio"
    )
    downstream = direction_choice.startswith("Down")

    # —— 4. Build NetworkX sub‑graph ——
    g = nx.DiGraph()
    g.add_edges_from(edges)

    def get_subgraph(start_node: str, downstream_mode: bool = True):
        visited, to_visit, sub_edges = set(), [start_node], []
        while to_visit:
            current = to_visit.pop()
            if current in visited:
                continue
            visited.add(current)
            nbrs_iter = g.successors(current) if downstream_mode else g.predecessors(current)
            for n in nbrs_iter:
                edge = (current, n) if downstream_mode else (n, current)
                if edge in edge_map:
                    sub_edges.append(edge)
                to_visit.append(n)
        return sub_edges

    selected_raw_edges = get_subgraph(selected_node, downstream)

    # —— 5. Derive node/edge styling info ——
    filtered_nodes = {src for src, _ in selected_raw_edges}.union({tgt for _, tgt in selected_raw_edges})
    direct_edges = {(s, t) for s, t in selected_raw_edges if s == selected_node or t == selected_node}
    directly_connected = {t if s == selected_node else s for s, t in direct_edges}

    # —— 6. Build PyVis network ——
    net = Network(height="750px", width="100%", directed=True, notebook=False)
    net.set_options(json.dumps({
        "nodes": {"size": 18, "font": {"size": 14, "multi": "html"}},
        "edges": {"arrows": {"to": {"enabled": True}}, "smooth": {"enabled": False}, "color": {"color": "#A9A9A9"}},
        "layout": {"hierarchical": {"enabled": True, "direction": "UD", "sortMethod": "directed", "nodeSpacing": 150, "treeSpacing": 300, "levelSeparation": 200}},
        "physics": {"enabled": False},
        "interaction": {"navigationButtons": True, "keyboard": True, "dragNodes": True, "dragView": True, "zoomView": True, "tooltipDelay": 200, "multiselect": True}
    }, indent=2))

    for node in filtered_nodes:
        node_type = "job" if node in df["job"].values else "table"
        color = "lightblue" if node_type == "job" else "lightgreen"
        font = {"size": 14, "bold": node in directly_connected or node == selected_node}
        net.add_node(node, label=node, color=color, font=font)

        for src, tgt in selected_raw_edges:
        # base style for every edge
        style_kwargs = {"color": "#A9A9A9", "width": 1}
        # highlight only if directly connected to the currently‑selected node
        if (src, tgt) in direct_edges:
            style_kwargs.update({"color": "#4B4B4B", "width": 3})
        net.add_edge(src, tgt, **style_kwargs)

    # —— 7. Auto‑fit once HTML loads ——
    zoom_script = """
    <script type=\"text/javascript\">
    window.addEventListener('load', () => {
        let tries = 0;
        const intv = setInterval(() => {
            const container = document.querySelector('div.vis-network');
            if (container && container.network && container.network.fit) {
                container.network.fit();
                clearInterval(intv);
            }
            if (++tries > 10) clearInterval(intv);
        }, 1000);
    });
    </script>
    """

    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
        net.save_graph(tmp_file.name)
        html_content = open(tmp_file.name, encoding="utf-8").read()
        os.unlink(tmp_file.name)

    if html_content:
        components.html(html_content + zoom_script, height=800, scrolling=True)
        with st.expander("Legend", expanded=True):
            st.markdown("""
            - 🟦 Jobs  
            - 🟩 Tables
            """)

    if filtered_nodes:
        view_df = df[df.apply(lambda r: r['source'] in filtered_nodes and r['target'] in filtered_nodes and r['job'] in filtered_nodes, axis=1)]
        st.subheader("ETL Mapping Table")
        st.dataframe(view_df[["JOBID", "PROJECT_NAME", "job", "source", "target"]])
    else:
        st.warning("No connected nodes found for the selected input.")

    st.write("✅ App finished rendering.")


if __name__ == "__main__":
    main()
