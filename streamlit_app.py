import streamlit as st
import streamlit.components.v1 as components
import json

st.set_page_config(layout="wide")
st.title("ETL Tree Viewer (D3.js)")

# Load ETL data
with open("etl_graph_data.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Load HTML template
with open("index.html", "r", encoding="utf-8") as f:
    html = f.read()

# Inject the data directly into the JS block
html = html.replace("// __DATA_PLACEHOLDER__", f"const data = {json.dumps(data)};\ndrawTree(data);")

# Show in Streamlit
components.html(html, height=800, scrolling=True)
