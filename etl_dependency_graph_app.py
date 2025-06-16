# etl_dependency_graph_streamlit_app.py
import pandas as pd
import networkx as nx
from pyvis.network import Network
import streamlit as st
import streamlit.components.v1 as components
import tempfile
import json
import os


def main():
    st.set_page_config(layout="wide")
    st.title("ETL Dependency Graph Viewer")

    # ------------------------
    # Mocked Up Dataset
    # ------------------------
    data = [
        {"job": "ODS_EDW.FIN_PSFT.12.Load_FACT_Ledger", "source": "ODS_P.DSSTG.STG_FACT_LEDGER_SOURCE_INFO", "target": "EDW_P.FIN.FACT_LEDGER"},
        {"job": "ODS_EDW.FIN_PSFT.12.Load_FACT_Ledger", "source": "ODS_P.FIN_PSFT.PS_LEDGER", "target": "ODS_P.DSSTG.STG_FACT_LEDGER_SOURCE_INFO"},
        {"job": "ODS_EDW.FIN_PSFT.12.Load_FACT_Ledger", "source": "EDW_P.FIN.FACT_LEDGER", "target": "ODS_P.DSSTG.STG_FACT_LEDGER_TARGET_INFO"},
        {"job": "ODS_EDW.FIN_PSFT.12.Load_FACT_Ledger", "source": "ODS_P.DSSTG.STG_FACT_LEDGER_SOURCE_INFO", "target": "ODS_P.DSSTG.STG_FACT_LEDGER_TARGET_INFO"},
        {"job": "ODS_EDW.FIN_PSFT.13.Load_FACT_Ledger_Budget", "source": "ODS_P.FIN_PSFT.PS_LEDGER_BUDG", "target": "EDW_P.FIN.FACT_LEDGER_BUDGET"},
        {"job": "ODS_EDW.FIN_PSFT.14.Load_FACT_Journal_Pending", "source": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_INFO", "target": "EDW_P.FIN.FACT_JOURNAL"},
        {"job": "ODS_EDW.FIN_PSFT.14.Load_FACT_Journal_Pending", "source": "EDW_P.FIN.FACT_JOURNAL", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_TARGET_INFO"},
        {"job": "ODS_EDW.FIN_PSFT.14.Load_FACT_Journal_Pending", "source": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_INFO", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_TARGET_INFO"},
        {"job": "ODS_EDW.FIN_PSFT.14.Load_FACT_Journal_Pending", "source": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_FAIL_INFO", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_INFO"},
        {"job": "ODS_EDW.FIN_PSFT.14.Load_FACT_Journal_Pending", "source": "ODS_P.FIN_PSFT.PS_JRNL_HEADER", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_INFO"},
        {"job": "ODS_EDW.FIN_PSFT.14.Load_FACT_Journal_Pending", "source": "ODS_P.FIN_PSFT.PS_JRNL_LN", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_INFO"}
    ]
    df = pd.DataFrame(data)

    df = df.dropna(subset=["source", "target", "job"])
    df = df[(df['source'].astype(str).str.strip() != '') & (df['target'].astype(str).str.strip() != '') & (df['job'].astype(str).str.strip() != '')]
    edges = [(row["source"], row["target"], row["job"]) for _, row in df.iterrows()]
    nodes = sorted(set(df["source"]).union(set(df["target"])))

    @st.cache_data(show_spinner=False)
    def get_valid_nodes():
        return [n for n in nodes if isinstance(n, str) and n.strip() != '']

    def render_sidebar():
        st.sidebar.header("Explore Dependencies")
        selected_node = st.sidebar.selectbox("Select a table/job/report:", get_valid_nodes(), key="node_select")
        direction = st.sidebar.radio("Dependency Direction", ["Downstream (Impact)", "Upstream (Lineage)"], key="direction_radio")
        return selected_node, direction

    selected_node, direction = render_sidebar()

    g = nx.DiGraph()
    for src, tgt, job in edges:
        g.add_edge(str(src).strip(), str(tgt).strip(), label=str(job).strip())

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

    filtered_nodes = set()
    for src, tgt in selected_raw_edges:
        filtered_nodes.add(src)
        filtered_nodes.add(tgt)

    selected_edges = [
        (src, tgt, job)
        for (src, tgt, job) in edges
        if (src, tgt) in selected_raw_edges or (tgt, src) in selected_raw_edges
    ]

    net = Network(height="600px", width="100%", directed=True, notebook=False)
    net.set_options(json.dumps({
        "nodes": {"size": 15, "font": {"size": 10, "multi": "html"}},
        "edges": {
            "arrows": {"to": {"enabled": True}},
            "font": {"size": 10, "align": "middle", "multi": "html"},
            "smooth": False
        },
        "layout": {
            "hierarchical": {
                "enabled": True,
                "direction": "LR",
                "sortMethod": "directed",
                "nodeSpacing": 507,
                "treeSpacing": 845,
                "levelSeparation": 507
            }
        },
        "physics": {
            "enabled": True,
            "hierarchicalRepulsion": {
                "centralGravity": 0.0,
                "springLength": 1014,
                "springConstant": 0.01,
                "nodeDistance": 507,
                "damping": 0.09
            },
            "stabilization": {"iterations": 300}
        },
        "interaction": {
            "navigationButtons": True,
            "keyboard": True
        }
    }))

    for node in filtered_nodes:
        node = str(node).strip()
        if node and node.lower() != 'none':
            net.add_node(node, label=node)

    for src, tgt, job in selected_edges:
        src, tgt, job = str(src).strip(), str(tgt).strip(), str(job).strip()
        if all([src, tgt, job]) and all(x.lower() != 'none' for x in [src, tgt, job]):
            net.add_edge(src, tgt, label=job, color="red")

    html_content = ""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
        net.save_graph(tmp_file.name)
        with open(tmp_file.name, 'r', encoding='utf-8') as f:
            html_content = f.read()
        os.unlink(tmp_file.name)

    if html_content:
        components.html(html_content, height=650, scrolling=True)

    if not filtered_nodes:
        st.warning("No connected nodes found for the selected input.")
    else:
        filtered_df = df[df.apply(lambda row: row['source'] in filtered_nodes and row['target'] in filtered_nodes, axis=1)]
        st.subheader("Filtered ETL Mapping Table")
        st.dataframe(filtered_df)

    st.write("✅ App finished rendering.")


if __name__ == "__main__":
    main()
