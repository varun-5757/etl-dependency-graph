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
        df = df.rename(
            columns={
                "JOB_NAME": "job",
                "SOURCE_OBJECT_NAME": "source",
                "TARGET_OBJECT_NAME": "target",
            }
        )
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return

    # strip whitespace
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

    # drop empty rows
    df = df.dropna(subset=["source", "target", "job"])
    df = df[(df["source"] != "") & (df["target"] != "") & (df["job"] != "")]

    # —— 2. Build *deduplicated* edge set + lookup ——
    edges_set: set[tuple[str, str]] = set()
    edge_owner: dict[tuple[str, str], str] = {}  # maps edge → job name (for table view)

    for _, row in df.iterrows():
        src_job = (row["source"], row["job"])
        job_tgt = (row["job"], row["target"])
        edges_set.add(src_job)
        edges_set.add(job_tgt)
        edge_owner[src_job] = row["job"]
        edge_owner[job_tgt] = row["job"]

    edges = list(edges_set)  # unique edges only – fixes phantom bold issue

    # —— 3. Build node universe ——
    all_nodes = set(df["source"]).union(df["target"]).union(df["job"])

    @st.cache_data(show_spinner=False)
    def get_valid_nodes():
        return sorted([n for n in all_nodes if isinstance(n, str) and n.strip().lower() != "none"])

    # —— 4. Sidebar controls ——
    st.sidebar.header("Explore Dependencies")
    selected_node: str = st.sidebar.selectbox(
        "Select a table/job/report:", options=get_valid_nodes(), index=0, key="node_select"
    ).strip()
    downstream = st.sidebar.radio(
        "Dependency Direction", ["Downstream (Impact)", "Upstream (Lineage)"], key="direction_radio"
    ).startswith("Down")

    # —— 5. Build NetworkX graph ——
    g = nx.DiGraph()
    g.add_edges_from(edges)

    def get_subgraph(start: str, downstream_mode: bool = True):
        """Return list of edges reachable from *start* in chosen direction."""
        visited, to_visit, sub_edges = set(), [start], set()
        while to_visit:
            cur = to_visit.pop()
            if cur in visited:
                continue
            visited.add(cur)
            nbrs = g.successors(cur) if downstream_mode else g.predecessors(cur)
            for n in nbrs:
                edge = (cur, n) if downstream_mode else (n, cur)
                if edge in edge_owner:
                    sub_edges.add(edge)
                to_visit.append(n)
        return list(sub_edges)

    selected_edges = get_subgraph(selected_node, downstream)

    # —— 6. Derive styling helpers ——
    filtered_nodes = {s for s, _ in selected_edges}.union({t for _, t in selected_edges})
    direct_edges = {(s, t) for s, t in selected_edges if s == selected_node or t == selected_node}
    directly_connected_nodes = {t if s == selected_node else s for s, t in direct_edges}

    # —— 7. Build PyVis network ——
    net = Network(height="750px", width="100%", directed=True, notebook=False)
    net.set_options(
        json.dumps(
            {
                "nodes": {"size": 18, "font": {"size": 14, "multi": "html"}},
                "edges": {
                    "arrows": {"to": {"enabled": True}},
                    "smooth": {"enabled": False},
                    "color": {"color": "#A9A9A9"},
                },
                "layout": {
                    "hierarchical": {
                        "enabled": True,
                        "direction": "UD",
                        "sortMethod": "directed",
                        "nodeSpacing": 150,
                        "treeSpacing": 300,
                        "levelSeparation": 200,
                    }
                },
                "physics": {"enabled": False},
                "interaction": {
                    "navigationButtons": True,
                    "keyboard": True,
                    "dragNodes": True,
                    "dragView": True,
                    "zoomView": True,
                    "tooltipDelay": 200,
                    "multiselect": True,
                },
            },
            indent=2,
        )
    )

    # add nodes first
    for n in filtered_nodes:
        n_type = "job" if n in df["job"].values else "table"
        n_color = "lightblue" if n_type == "job" else "lightgreen"
        n_font = {"size": 14, "bold": n == selected_node or n in directly_connected_nodes}
        net.add_node(n, label=n, color=n_color, font=n_font)

    # add edges once (no duplication) – highlight only those touching selected node
    for src, tgt in selected_edges:
        e_kwargs = {"color": "#A9A9A9", "width": 1}
        if (src, tgt) in direct_edges:
            e_kwargs.update({"color": "#4B4B4B", "width": 3})
        net.add_edge(src, tgt, **e_kwargs)

    # —— 8. Auto‑fit graph once rendered ——
    fit_js = """
    <script type=\"text/javascript\">
    window.addEventListener('load', () => {
        const intv = setInterval(() => {
            const container = document.querySelector('div.vis-network');
            if (container && container.network && container.network.fit) {
                container.network.fit();
                clearInterval(intv);
            }
        }, 800);
    });
    </script>
    """

    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp:
        net.save_graph(tmp.name)
        html = open(tmp.name, encoding="utf-8").read()
        os.unlink(tmp.name)

    if html:
        components.html(html + fit_js, height=800, scrolling=True)
        with st.expander("Legend", expanded=True):
            st.markdown("""
            - 🟦 Jobs  
            - 🟩 Tables
            """)

    # table below graph
    if filtered_nodes:
        view = df[df.apply(lambda r: r["source"] in filtered_nodes and r["target"] in filtered_nodes and r["job"] in filtered_nodes, axis=1)]
        st.subheader("ETL Mapping Table")
        st.dataframe(view[["JOBID", "PROJECT_NAME", "job", "source", "target"]])
    else:
        st.warning("No connected nodes found for the selected input.")

    st.write("✅ App finished rendering.")


if __name__ == "__main__":
    main()
