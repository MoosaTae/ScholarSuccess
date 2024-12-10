import json
import requests
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyvis.network import Network
import os
from streamlit.components.v1 import html
import tempfile
import os
# Load JSON data
try:
    with open("response_1733462178691.json", "r") as file:
        data = json.load(file)
except FileNotFoundError:
    st.error("JSON file not found. Please ensure 'response_1733462178691.json' exists.")
    st.stop()

# Extract data from JSON
classification_report = data["classification_report"]
confusion_matrix = np.array(data["confusion_matrix"])
feature_importance = data["feature_importance"]

# Set up Sidebar Navigation
st.sidebar.title("Navigation")
section = st.sidebar.selectbox("Choose a Section:", ["Visualize Data", "API Request", "Data Insight from Cassandra"])

if section == "Visualize Data":
    st.title("Scholar Success Rate Prediction - Data Visualization")

    # Classification Report Visualization
    st.header("Classification Report")
    classes = [
        cls for cls in classification_report if isinstance(classification_report[cls], dict)
    ]
    metrics = ["precision", "recall", "f1-score"]

    for metric in metrics:
        values = [classification_report[cls].get(metric, 0) for cls in classes]
        fig = px.bar(
            x=classes,
            y=values,
            labels={"x": "Class", "y": metric.capitalize()},
            title=f"{metric.capitalize()} per Class",
            color=values,
            color_continuous_scale="Viridis",
        )
        st.plotly_chart(fig)

    # Confusion Matrix Visualization
    st.header("Confusion Matrix")
    fig = go.Figure(
        data=go.Heatmap(
            z=confusion_matrix,
            x=["Predicted: False", "Predicted: True"],
            y=["True: False", "True: True"],
            colorscale="Blues",
            hoverongaps=False,
        )
    )
    fig.update_layout(
        title="Confusion Matrix",
        xaxis_title="Predicted Label",
        yaxis_title="True Label",
    )
    st.plotly_chart(fig)

    # Feature Importance Visualization
    st.header("Feature Importance")
    sorted_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[:10]
    feature_names, importances = zip(*sorted_features)
    fig = px.bar(
        x=importances,
        y=feature_names,
        orientation="h",
        labels={"x": "Importance", "y": "Feature"},
        title="Top 10 Features by Importance",
        color=importances,
        color_continuous_scale="Aggrnyl",
    )
    fig.update_layout(yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig)

elif section == "API Request":
    st.title("Scholar Success Rate Prediction - API Request")

    # Input Fields for API Request
    st.subheader("Input Parameters")
    title = st.text_input("Title")
    abstract = st.text_area("Abstract")
    ref_count = st.number_input("Reference Count", step=1)

    # API Endpoint
    st.subheader("API Endpoint")
    url = st.text_input("Request URL", value="http://localhost:8000/predict")

    # Make the POST request on button click
    if st.button("Submit Request"):
        payload = {
            "title": title,
            "abstract": abstract,
            "ref_count": ref_count,
        }

        try:
            # Send POST request
            response = requests.post(
                url,
                headers={
                    "accept": "application/json",
                    "Content-Type": "application/json",
                },
                json=payload,
            )

            # Handle the response
            if response.status_code == 200:
                st.success("Request Successful!")
                response_data = response.json()  # Get the JSON response

                st.subheader("Response Visualization")

                # Display response
                if isinstance(response_data, list):
                    st.write("Table View of the Response:")
                    st.table(response_data)
                elif isinstance(response_data, dict):
                    st.write("Key-Value View of the Response:")
                    st.json(response_data)

                # If there's numeric data, visualize it
                numeric_data = {
                    k: v
                    for k, v in response_data.items()
                    if isinstance(v, (int, float))
                }
                if numeric_data:
                    fig = px.bar(
                        x=list(numeric_data.keys()),
                        y=list(numeric_data.values()),
                        labels={"x": "Keys", "y": "Values"},
                        title="Bar Chart of Numeric Data in Response",
                    )
                    st.plotly_chart(fig)

                # Example for nested data (adjust key as needed)
                if "nested_key" in response_data:
                    nested_data = response_data["nested_key"]
                    st.write("Nested Data Example:")
                    st.json(nested_data)
            else:
                st.error(f"Request Failed! Status Code: {response.status_code}")
                st.text(response.text)  # Show error message
        except Exception as e:
            st.error(f"An error occurred: {e}")
        # Radar Chart for Classification Metrics
    st.header("Radar Chart of Classification Metrics")
    # Prepare data for radar chart
    classes = [cls for cls in classification_report if isinstance(classification_report[cls], dict)]
    metrics = ["precision", "recall", "f1-score"]
    # Create a DataFrame from the classification report for easy manipulation
    radar_data = []
    for cls in classes:
        radar_data.append({
            "Class": cls,
            "Precision": classification_report[cls].get("precision", 0),
            "Recall": classification_report[cls].get("recall", 0),
            "F1-score": classification_report[cls].get("f1-score", 0),
        })
    radar_df = pd.DataFrame(radar_data)

    # Radar chart requires repeating the first metric to close the circle
    categories = metrics
    fig = go.Figure()
    for i, row in radar_df.iterrows():
        fig.add_trace(
            go.Scatterpolar(
                r=[row["Precision"], row["Recall"], row["F1-score"], row["Precision"]],
                theta=categories + [categories[0]],
                fill='toself',
                name=row["Class"]
            )
        )

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0,1]
            )
        ),
        showlegend=True,
        title="Radar Chart of Classification Metrics"
    )
    st.plotly_chart(fig)

elif section == "Data Insight from Cassandra":
    st.title("Scopus - Data Insight from Cassandra")

    # Cassandra Connection
    # Adjust these settings as per your cluster configuration
    # For example, if you have username/password, use PlainTextAuthProvider(username='user', password='pass')
    auth_provider = None  # If authentication is not needed, otherwise: PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(['127.0.0.1'], auth_provider=auth_provider)  # Replace '127.0.0.1' with your Cassandra node IP
    session = cluster.connect('scopus_data')  # Keyspace name

    st.subheader("Cassandra Data Retrieval")
    # Example Query: Get a limited set of records
    # For large datasets, consider pagination or filters
    rows = session.execute("SELECT * FROM records LIMIT 5000;")

    # Convert to DataFrame
    df = pd.DataFrame(list(rows))
    df['subject_area_list'] = df['subject_area'].apply(
        lambda x: x.strip("[]").split(",") if pd.notnull(x) else []
    )
    df['main_subject_area'] = df['subject_area_list'].apply(lambda lst: lst[0].strip() if len(lst) > 0 else None)

    if df.empty:
        st.warning("No data returned from Cassandra.")
    else:
        st.success("Data retrieved successfully!")
        st.write("Sample of the retrieved data:")
        st.dataframe(df.head(10))

        # Visualization Example: Count of records by subject_area
        st.subheader("Records Count by Main Subject Area")
        area_counts = df['main_subject_area'].value_counts().reset_index()
        area_counts.columns = ['subject_area', 'count']

        fig = px.bar(
            area_counts,
            x='subject_area',
            y='count',
            labels={"subject_area": "Subject Area", "count": "Number of Records"},
            title="Number of Records by Main Subject Area"
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
            title="Cited-By vs. Reference Count"
        )
        st.plotly_chart(fig)
    else:
        st.info("Not enough data to visualize cited_by vs. ref_count.")


    st.subheader("Treemap of Main Subject Areas")
    if not df.empty and "main_subject_area" in df.columns:
        area_counts = df['main_subject_area'].value_counts().reset_index()
        area_counts.columns = ['subject_area', 'count']
        fig = px.treemap(
            area_counts,
            path=['subject_area'],
            values='count',
            title="Treemap of Main Subject Areas"
        )
        st.plotly_chart(fig)
    else:
        st.info("Not enough data to visualize a treemap of subject areas.")

    st.subheader("Distribution of Document Types")
    print(df.columns)
    if 'document_type' in df.columns:
        doc_type_counts = df['document_type'].value_counts().reset_index()
        doc_type_counts.columns = ['document_type', 'count']
        fig = px.pie(
            doc_type_counts,
            names='document_type',
            values='count',
            title='Proportion of Document Types'
        )
        st.plotly_chart(fig)
    else:
        st.info("No document_type column available.")
    
    st.subheader("Top 15 Source Titles by Number of Publications")
    if 'source_title' in df.columns:
        source_counts = df['source_title'].value_counts().head(15).reset_index()
        source_counts.columns = ['source_title', 'count']
        fig = px.bar(
            source_counts,
            x='count',
            y='source_title',
            orientation='h',
            title='Top 15 Source Titles',
            labels={'count': 'Number of Publications', 'source_title': 'Source Title'}
        )
        fig.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig)
    else:
        st.info("No source_title column available.")
        
    st.subheader("Source Type Trend Over Time")
    if 'publication_date' in df.columns and 'source_type' in df.columns:
        df['year'] = pd.to_datetime(df['publication_date'], errors='coerce').dt.year
        source_year = df.dropna(subset=['year']).groupby(['year', 'source_type']).size().reset_index(name='count')
        fig = px.line(
            source_year,
            x='year',
            y='count',
            color='source_type',
            title='Publication Count by Source Type Over Time',
            labels={'year': 'Year', 'count': 'Count', 'source_type': 'Source Type'}
        )
        st.plotly_chart(fig)
    else:
        st.info("Required columns (publication_date, source_type) not available.")

    st.subheader("Network Graph of Subject Areas and Document Types")
    if 'main_subject_area' in df.columns and 'document_type' in df.columns:
        # Create pairs (main_subject_area, document_type) from each record
        pairs = df[['main_subject_area', 'document_type']].dropna()

        if not pairs.empty:
            # Build network
            net = Network(notebook=True, height='600px', width='100%', bgcolor='#222222', font_color='white')
            
            # Add nodes for unique subject areas and document types
            subject_areas = pairs['main_subject_area'].unique()
            doc_types = pairs['document_type'].unique()

            # Add subject area nodes (color them differently)
            for sa in subject_areas:
                net.add_node(sa, label=sa, title=sa, color='#1f78b4')
            
            # Add document type nodes
            for dt in doc_types:
                net.add_node(dt, label=dt, title=dt, color='#33a02c')
            
            # Add edges for each pair
            for i, row in pairs.iterrows():
                net.add_edge(row['main_subject_area'], row['document_type'])

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
            with tempfile.NamedTemporaryFile(delete=False, suffix='.html', mode='w', encoding='utf-8') as tmp:
                net.save_graph(tmp.name)
                with open(tmp.name, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                st.components.v1.html(html_content, height=600)
                os.unlink(tmp.name)  # Clean up
        else:
            st.info("No pairs of subject_area and document_type available for the network.")
