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
    # Load datasets from two CSV files
    # ------------------------
    try:
        # Read source->job mapping
        df_src = pd.read_csv("Sources to Jobs.csv").rename(
            columns={"SOURCE_OBJECT_NAME": "source", "JOB_NAME": "job"}
        )[["source", "job"]]
        # Read job->target mapping, keep metadata
        df_tgt = pd.read_csv("Job to Targets.csv").rename(
            columns={
                "JOB_NAME": "job",
                "TARGET_OBJECT_NAME": "target",
                "JOBID": "jobid",
                "PROJECT_NAME": "project"
            }
        )[["job", "target", "jobid", "project"]]
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return

    # Clean whitespace in all string columns
    for df_ in (df_src, df_tgt):
        for col in df_.select_dtypes(include="object").columns:
            df_[col] = df_[col].str.strip()

    # Filter out invalid rows
    df_src = df_src[df_src["source"].astype(bool) & df_src["job"].astype(bool)]
    df_tgt = df_tgt[df_tgt["job"].astype(bool) & df_tgt["target"].astype(bool)]

    # Prepare node lists
    all_tables = sorted(set(df_src["source"]).union(df_tgt["target"]))
    all_jobs = sorted(set(df_src["job"]).union(df_tgt["job"]))

    # Sidebar: select type then node
    st.sidebar.header("Explore Dependencies")
    node_type = st.sidebar.radio("Select Type:", ["Table", "Job"], index=0)
    if node_type == "Table":
        selected_node = st.sidebar.selectbox(
            "Select a table:", all_tables, key="table_select", index=0
        ).strip()
    else:
        selected_node = st.sidebar.selectbox(
            "Select a job:", all_jobs, key="job_select", index=0
        ).strip()

    direction = st.sidebar.radio(
        "Dependency Direction:", ["Downstream (Impact)", "Upstream (Lineage)", "Both"], key="direction_radio"
    )

    if not selected_node:
        st.info("Please select a node to view dependencies.")
        return

    # Build directed graph
    g = nx.DiGraph()
    # add source->job edges
    for src, job in df_src[['source', 'job']].itertuples(index=False):
        g.add_edge(src, job)
    # add job->target edges
    for job, tgt in df_tgt[['job', 'target']].itertuples(index=False):
        g.add_edge(job, tgt)

    # Traverse to build subgraph edges
    def get_sub_edges(start, downstream=True):
        visited = set()
        stack = [start]
        sub_edges = []
        while stack:
            cur = stack.pop()
            if cur in visited:
                continue
            visited.add(cur)
            try:
                nbrs = g.successors(cur) if downstream else g.predecessors(cur)
            except nx.NetworkXError:
                continue
            for n in nbrs:
                sub_edges.append((cur, n) if downstream else (n, cur))
                stack.append(n)
        return sub_edges

    # Compute sub_edges based on direction
    if direction == "Both":
        down_edges = get_sub_edges(selected_node, downstream=True)
        up_edges = get_sub_edges(selected_node, downstream=False)
        sub_edges = list({*down_edges, *up_edges})
    else:
        downstream_flag = direction.startswith("Downstream")
        sub_edges = get_sub_edges(selected_node, downstream=downstream_flag)

    sub_nodes = {s for s, _ in sub_edges} | {t for _, t in sub_edges}

    # PyVis network setup
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
                "sortMethod": "directed",
                "levelSeparation": 100,
                "nodeSpacing": 50,
                "treeSpacing": 100
            }
        },
        "physics": {"enabled": False},
        "interaction": {"navigationButtons": True, "keyboard": True, "dragNodes": True, "dragView": True, "zoomView": True, "autoResize": True}
    }, indent=2))

    # Add nodes colored by type
    for n in sub_nodes:
        color = "lightblue" if n in all_jobs else "lightgreen"
        net.add_node(n, label=n, color=color)

    # Add edges
    for s, t in sub_edges:
        net.add_edge(s, t)

    # Render HTML
    path = tempfile.NamedTemporaryFile(delete=False, suffix=".html").name
    net.save_graph(path)
    with open(path, 'r', encoding='utf-8') as f:
        html = f.read()
    os.unlink(path)
    components.html(html, height=800, scrolling=True)

    # Legend
    with st.expander("Legend", expanded=True):
        st.markdown("""
        - 🟦 Jobs  
        - 🟩 Tables
        """)

    # Display mapping table
    if not sub_edges:
        st.warning("No dependencies found for selected node.")
    else:
        df_map = df_src.merge(df_tgt, on="job", how="inner")
        df_display = df_map[df_map.apply(
            lambda r: r["source"] in sub_nodes and r["target"] in sub_nodes and r["job"] in sub_nodes,
            axis=1
        )]
        st.subheader("ETL Mapping Table")
        st.dataframe(df_display[["jobid", "project", "job", "source", "target"]])

    st.write("✅ App finished rendering.")


if __name__ == "__main__":
    main()
