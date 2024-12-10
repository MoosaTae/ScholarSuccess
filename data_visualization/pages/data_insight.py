import os
import tempfile

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyvis.network import Network
from streamlit.components.v1 import html

st.title("Scopus - Data Insight from Cassandra")
CASANDRA_HOST = os.getenv("DB_HOST", "127.0.0.1")
KEYSPACE = os.getenv("KEYSPACE", "scopus_data")
AUTH_PROVIDER = os.getenv("AUTH_PROVIDER", None)

cluster = Cluster([CASANDRA_HOST], auth_provider=AUTH_PROVIDER)
session = cluster.connect(KEYSPACE)

st.subheader("Cassandra Data Retrieval")
rows = session.execute("SELECT * FROM records LIMIT 5000;")

# Convert to DataFrame
df = pd.DataFrame(list(rows))
df["subject_area_list"] = df["subject_area"].apply(
    lambda x: x.strip("[]").split(",") if pd.notnull(x) else []
)
df["main_subject_area"] = df["subject_area_list"].apply(
    lambda lst: lst[0].strip() if len(lst) > 0 else None
)

if df.empty:
    st.warning("No data returned from Cassandra.")
else:
    st.success("Data retrieved successfully!")
    st.write("Sample of the retrieved data:")
    st.dataframe(df.head(10))

    # Visualization Example: Count of records by subject_area
    st.subheader("Records Count by Main Subject Area")
    area_counts = df["main_subject_area"].value_counts().reset_index()
    area_counts.columns = ["subject_area", "count"]

    fig = px.bar(
        area_counts,
        x="subject_area",
        y="count",
        labels={"subject_area": "Subject Area", "count": "Number of Records"},
        title="Number of Records by Main Subject Area",
    )
    st.plotly_chart(fig)

st.subheader("Scatter Plot of Cited-By vs. Reference Count by Main Subject Area")
if not df.empty and "cited_by" in df.columns and "ref_count" in df.columns:
    fig = px.scatter(
        df,
        x="ref_count",
        y="cited_by",
        color="main_subject_area",
        labels={"ref_count": "Reference Count", "cited_by": "Cited By"},
        title="Cited-By vs. Reference Count",
    )
    st.plotly_chart(fig)
else:
    st.info("Not enough data to visualize cited_by vs. ref_count.")

st.subheader("Treemap of Main Subject Areas")
if not df.empty and "main_subject_area" in df.columns:
    area_counts = df["main_subject_area"].value_counts().reset_index()
    area_counts.columns = ["subject_area", "count"]
    fig = px.treemap(
        area_counts,
        path=["subject_area"],
        values="count",
        title="Treemap of Main Subject Areas",
    )
    st.plotly_chart(fig)
else:
    st.info("Not enough data to visualize a treemap of subject areas.")

st.subheader("Distribution of Document Types")
if "document_type" in df.columns:
    doc_type_counts = df["document_type"].value_counts().reset_index()
    doc_type_counts.columns = ["document_type", "count"]
    fig = px.pie(
        doc_type_counts,
        names="document_type",
        values="count",
        title="Proportion of Document Types",
    )
    st.plotly_chart(fig)
else:
    st.info("No document_type column available.")

st.subheader("Top 15 Source Titles by Number of Publications")
if "source_title" in df.columns:
    source_counts = df["source_title"].value_counts().head(15).reset_index()
    source_counts.columns = ["source_title", "count"]
    fig = px.bar(
        source_counts,
        x="count",
        y="source_title",
        orientation="h",
        title="Top 15 Source Titles",
        labels={"count": "Number of Publications", "source_title": "Source Title"},
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig)
else:
    st.info("No source_title column available.")

st.subheader("Source Type Trend Over Time")
if "publication_date" in df.columns and "source_type" in df.columns:
    df["year"] = pd.to_datetime(df["publication_date"], errors="coerce").dt.year
    source_year = (
        df.dropna(subset=["year"])
        .groupby(["year", "source_type"])
        .size()
        .reset_index(name="count")
    )
    fig = px.line(
        source_year,
        x="year",
        y="count",
        color="source_type",
        title="Publication Count by Source Type Over Time",
        labels={"year": "Year", "count": "Count", "source_type": "Source Type"},
    )
    st.plotly_chart(fig)
else:
    st.info("Required columns (publication_date, source_type) not available.")

st.subheader("Network Graph of Subject Areas and Document Types")
if "main_subject_area" in df.columns and "document_type" in df.columns:
    # Create pairs (main_subject_area, document_type) from each record
    pairs = df[["main_subject_area", "document_type"]].dropna()

    if not pairs.empty:
        # Build network
        net = Network(
            notebook=True,
            height="600px",
            width="100%",
            bgcolor="#222222",
            font_color="white",
        )

        # Add nodes for unique subject areas and document types
        subject_areas = pairs["main_subject_area"].unique()
        doc_types = pairs["document_type"].unique()

        # Add subject area nodes (color them differently)
        for sa in subject_areas:
            net.add_node(sa, label=sa, title=sa, color="#1f78b4")

        # Add document type nodes
        for dt in doc_types:
            net.add_node(dt, label=dt, title=dt, color="#33a02c")

        # Add edges for each pair
        for i, row in pairs.iterrows():
            net.add_edge(row["main_subject_area"], row["document_type"])

        # Set network options
        net.set_options("""
        var options = {
        "nodes": {
            "shape": "dot",
            "size": 10
        },
        "edges": {
            "smooth": false
        },
        "physics": {
            "enabled": true,
            "barnesHut": {
            "gravitationalConstant": -8000
            }
        }
        }
        """)

        # Generate and display HTML
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=".html", mode="w", encoding="utf-8"
        ) as tmp:
            net.save_graph(tmp.name)
            with open(tmp.name, "r", encoding="utf-8") as f:
                html_content = f.read()
            st.components.v1.html(html_content, height=600)
            os.unlink(tmp.name)  # Clean up
    else:
        st.info("No pairs of subject_area and document_type available for the network.")
