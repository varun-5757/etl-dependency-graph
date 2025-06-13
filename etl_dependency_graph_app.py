# etl_dependency_graph_streamlit_app.py
import pandas as pd
import networkx as nx
from pyvis.network import Network
import streamlit as st
import streamlit.components.v1 as components
import tempfile
import json
import os

st.set_page_config(layout="wide")
st.title("ETL Dependency Graph Viewer")

# ------------------------
# Mocked Up Dataset
# ------------------------
data = [
    {"job": "Job_A", "source": "Raw_Sales", "target": "Stg_Sales"},
    {"job": "Job_B", "source": "Stg_Sales", "target": "Dim_Customer"},
    {"job": "Job_C", "source": "Dim_Customer", "target": "Fact_Sales"},
    {"job": "Job_D", "source": "Fact_Sales", "target": "Sales_Report"},
    {"job": "Job_E", "source": "Raw_Inventory", "target": "Stg_Inventory"},
    {"job": "Job_F", "source": "Stg_Inventory", "target": "Dim_Product"},
    {"job": "Job_G", "source": "Dim_Product", "target": "Fact_Inventory"},
    {"job": "Job_H", "source": "Fact_Inventory", "target": "Inventory_Report"},
    {"job": "Job_I", "source": "Fact_Sales", "target": "Combined_Report"},
    {"job": "Job_J", "source": "Fact_Inventory", "target": "Combined_Report"}
]
df = pd.DataFrame(data)

# Clean any None values just in case
df = df.dropna(subset=["source", "target", "job"])

edges = [(row["source"], row["target"], row["job"]) for _, row in df.iterrows()]
nodes = sorted(set(df["source"]).union(set(df["target"])))

# Sidebar filters (fixed None-label issue by not using 'with st.sidebar')
st.sidebar.header("Explore Dependencies")
selected_node = st.sidebar.selectbox("Select a table/job/report:", [n for n in nodes if n])
direction = st.sidebar.radio("Dependency Direction", ["Downstream (Impact)", "Upstream (Lineage)"])

# Build graph
g = nx.DiGraph()
for src, tgt, job in edges:
    g.add_edge(src, tgt, label=job)

def get_subgraph(graph, start_node, direction="downstream"):
    visited = set()
    to_visit = [start_node]
    sub_edges = []
    while to_visit:
        current = to_visit.pop()
        if current in visited:
            continue
        visited.add(current)
        neighbors = graph.successors(current) if direction == "downstream" else graph.predecessors(current)
        for n in neighbors:
            sub_edges.append((current, n)) if direction == "downstream" else sub_edges.append((n, current))
            to_visit.append(n)
    return sub_edges

selected_raw_edges = get_subgraph(g, selected_node, direction="downstream" if direction.startswith("Down") else "upstream")

# Derive the filtered nodes from selected edges
filtered_nodes = set()
for src, tgt in selected_raw_edges:
    filtered_nodes.add(src)
    filtered_nodes.add(tgt)

# Extract job info for selected edges
selected_edges = [
    (src, tgt, job)
    for (src, tgt, job) in edges
    if (src, tgt) in selected_raw_edges or (tgt, src) in selected_raw_edges
]

# Pyvis graph
net = Network(height="600px", width="100%", directed=True, notebook=False)

# Set options using valid JSON
graph_options = {
    "nodes": {
        "size": 15,
        "font": {"size": 14}
    },
    "edges": {
        "arrows": {"to": {"enabled": True}},
        "font": {"size": 12, "align": "middle"}
    },
    "physics": {
        "enabled": True,
        "solver": "forceAtlas2Based",
        "forceAtlas2Based": {
            "gravitationalConstant": -50,
            "springLength": 100,
            "springConstant": 0.05
        },
        "stabilization": {"iterations": 100}
    }
}
net.set_options(json.dumps(graph_options))

# Add only relevant nodes and edges
for src, tgt, job in selected_edges:
    if all([src, tgt, job]):
        net.add_node(src, label=str(src))
        net.add_node(tgt, label=str(tgt))
        net.add_edge(src, tgt, label=str(job), color="red")

# Render HTML in Streamlit
with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
    net.save_graph(tmp_file.name)
    with open(tmp_file.name, 'r', encoding='utf-8') as f:
        html = f.read()
    os.unlink(tmp_file.name)  # Clean up temp file
    components.html(html, height=650, scrolling=True)

# Show filtered data
filtered_df = df[df.apply(lambda row: row['source'] in filtered_nodes and row['target'] in filtered_nodes, axis=1)]
st.subheader("Filtered ETL Mapping Table")
st.dataframe(filtered_df)
