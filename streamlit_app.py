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

        # Strip whitespace and filter out empty or 'None' values
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    # Keep rows with valid source and job; allow blank/null targets
    df = df.dropna(subset=["source", "job"] )
    df = df[~df['source'].str.lower().eq('none')]
    df = df[~df['job'].str.lower().eq('none')]
    df = df[(df['source'] != '') & (df['job'] != '')]

        # Prepare node lists (exclude blank/null targets for table list)
    source_tables = set(df['source'])
    target_tables = {t for t in df['target'] if isinstance(t, str) and t.strip()}
    all_tables = sorted(source_tables.union(target_tables))
    all_jobs = sorted(set(df['job']))

    # Sidebar: select type then node: select type then node
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

    # Build directed graph with conditional edges
    g = nx.DiGraph()
    for _, row in df.iterrows():
        src = row['source']
        job = row['job']
        tgt = row['target']
        # always add source -> job
        g.add_edge(src, job)
        # only add job -> target if target is non-empty
        if isinstance(tgt, str) and tgt.strip():
            g.add_edge(job, tgt)

    # Traverse to build subgraph edges
    def get_sub_edges(start, downstream=True):
        visited = set()
        stack = [start]
        edges = []
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
                edges.append((cur, n) if downstream else (n, cur))
                stack.append(n)
        return edges

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
        col = "lightblue" if n in all_jobs else "lightgreen"
        net.add_node(n, label=n, color=col)

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

    # Display mapping
    if not sub_edges:
        st.warning("No dependencies found for selected node.")
    else:
        df_sub = df[df.apply(lambda r: r['source'] in sub_nodes and r['job'] in sub_nodes and r['target'] in sub_nodes, axis=1)]
        st.subheader("ETL Mapping Table")
        st.dataframe(df_sub[["JOBID", "PROJECT_NAME", "job", "source", "target"]])

    st.write("✅ App finished rendering.")

if __name__ == "__main__":
    main()
