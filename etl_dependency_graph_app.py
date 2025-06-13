# etl_dependency_graph_streamlit_app.py
import pandas as pd
import networkx as nx
from pyvis.network import Network
import streamlit as st
import streamlit.components.v1 as components
import tempfile

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

edges = [(row["source"], row["target"], row["job"]) for _, row in df.iterrows()]
nodes = sorted(set(df["source"]).union(set(df["target"])))

# Sidebar filters
st.sidebar.header("Explore Dependencies")
selected_node = st.sidebar.selectbox("Select a table/job/report:", nodes)
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

selected_edges = get_subgraph(g, selected_node, direction="downstream" if direction.startswith("Down") else "upstream")

# Pyvis graph
net = Network(height="600px", width="100%", directed=True)
for src, tgt, job in edges:
    net.add_node(src, label=src)
    net.add_node(tgt, label=tgt)
    if (src, tgt) in selected_edges:
        net.add_edge(src, tgt, label=job, color="red")
    else:
        net.add_edge(src, tgt, label=job, color="lightgray")

# Render HTML in Streamlit
with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
    net.save_graph(tmp_file.name)
    components.html(open(tmp_file.name, 'r', encoding='utf-8').read(), height=650, scrolling=True)

# Show data
st.subheader("Underlying ETL Mapping Table")
st.dataframe(df)

# --- Instructions for Running ---
st.markdown("""
### How to Run this App:
1. Ensure Python 3 is installed.
2. Install required libraries:
```bash
pip install streamlit pyvis pandas networkx
```
3. Save this script as `etl_dependency_graph_app.py`
4. Launch the app:
```bash
streamlit run etl_dependency_graph_app.py
```
5. Use the left sidebar to select a job/table and explore dependencies.
""")
