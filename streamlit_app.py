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
        df_src = pd.read_csv("Sources to Jobs.csv").rename(
            columns={"SOURCE_OBJECT_NAME": "source", "JOB_NAME": "job", "JOBID": "jobid", "PROJECT_NAME": "project"}
        )
        df_tgt = pd.read_csv("Job to Targets.csv").rename(
        columns={"JOB_NAME": "job", "TARGET_OBJECT_NAME": "target", "JOBID": "jobid", "PROJECT_NAME": "project"}
    )
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return

    # Clean whitespace
    for df_ in (df_src, df_tgt):
        for col in df_.select_dtypes(include="object").columns:
            df_[col] = df_[col].str.strip()

    # Filter out invalid rows
    df_src = df_src.dropna(subset=["source", "job"]) 
    df_src = df_src[(df_src['source'] != '') & (df_src['job'] != '')]
    df_tgt = df_tgt.dropna(subset=["job", "target"])
    df_tgt = df_tgt[(df_tgt['job'] != '') & (df_tgt['target'] != '')]

    # Combine edges lists
    edges = []
    # source -> job
    for _, row in df_src.iterrows():
        edges.append((row['source'], row['job']))
    # job -> target
    for _, row in df_tgt.iterrows():
        edges.append((row['job'], row['target']))

    # Prepare node lists
    all_tables = sorted(set(df_src['source']).union(df_tgt['target']))
    all_jobs = sorted(set(df_src['job']).union(df_tgt['job']))

    # Sidebar: select type then node
    st.sidebar.header("Explore Dependencies")
    node_type = st.sidebar.radio("Select Type:", ["Table", "Job"], index=0)
    if node_type == "Table":
        selected_node = st.sidebar.selectbox("Select a table:", all_tables, key="table_select").strip()
    else:
        selected_node = st.sidebar.selectbox("Select a job:", all_jobs, key="job_select").strip()

    direction = st.sidebar.radio(
        "Dependency Direction:", ["Downstream (Impact)", "Upstream (Lineage)"], key="direction_radio"
    )
    downstream = direction.startswith("Downstream")

    if not selected_node:
        st.info("Please select a node to view dependencies.")
        return

    # Build directed graph
    g = nx.DiGraph()
    for src, job in edges:
        g.add_edge(src, job)
    for job, tgt in edges:
        # only source->job entries have job as second in tuple; skip duplicates
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

    sub_edges = get_sub_edges(selected_node, downstream)
    sub_nodes = {s for s, t in sub_edges} | {t for s, t in sub_edges}

    # PyVis network setup
    net = Network(height="750px", width="100%", directed=True, notebook=False)
    net.set_options(json.dumps({
        "nodes": {"size": 18, "font": {"size": 14, "multi": "html"}},
        "edges": {
            "arrows": {"to": {"enabled": True}},
            "smooth": {"enabled": False},
            "color": {"color": "#A9A9A9"}
        },
        "layout": {"hierarchical": {"enabled": True, "direction": "UD", "sortMethod": "directed"}},
        "physics": {"enabled": False},
        "interaction": {"navigationButtons": True, "keyboard": True, "dragNodes": True, "dragView": True, "zoomView": True}
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
    html = open(path, 'r', encoding='utf-8').read()
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
        # merge source-job and job-target for display
        df_map = pd.merge(df_src, df_tgt, on='job', how='inner')
        df_display = df_map[df_map.apply(lambda r: r['source'] in sub_nodes and r['job'] in sub_nodes and r['target'] in sub_nodes, axis=1)]
        st.subheader("ETL Mapping Table")
        st.dataframe(df_display[["jobid", "project", "job", "source", "target"]])

    st.write("✅ App finished rendering.")


if __name__ == "__main__":
    main()
