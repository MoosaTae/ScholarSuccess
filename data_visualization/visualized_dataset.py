import json
import requests
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

# Load JSON data
try:
    with open("response_1733462178691.json", "r") as file:
        data = json.load(file)
except FileNotFoundError:
    st.error("JSON file not found. Please ensure 'response_1733462178691.json' exists.")
    st.stop()

# Extract data
classification_report = data["classification_report"]
confusion_matrix = np.array(data["confusion_matrix"])
feature_importance = data["feature_importance"]

# Streamlit Layout
st.title("Scholar Success Rate Prediction")

# Sidebar Navigation with Dropdown
st.sidebar.title("Navigation")
section = st.sidebar.selectbox("Choose a Section:", ["Visualize Data", "API Request"])

if section == "Visualize Data":
    st.header("Data Visualization")

    # Classification Report
    st.subheader("Classification Report")
    classes = [
        cls
        for cls in classification_report
        if isinstance(classification_report[cls], dict)
    ]
    metrics = ["precision", "recall", "f1-score"]

    # Bar charts for classification report
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

    # Confusion Matrix
    st.subheader("Confusion Matrix")
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

    # Feature Importance
    st.subheader("Feature Importance")
    sorted_features = sorted(
        feature_importance.items(), key=lambda x: x[1], reverse=True
    )[:10]
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
    st.header("API Request Component")

    # Input Fields
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

                # Visualize the response
                st.subheader("Response Visualization")

                # Example 1: Visualize as a Table
                if isinstance(response_data, list):
                    st.write("Table View of the Response:")
                    st.table(
                        response_data
                    )  # Display as a table if it's a list of dictionaries
                elif isinstance(response_data, dict):
                    st.write("Key-Value View of the Response:")
                    st.json(response_data)  # Display JSON for dictionaries

                # Example 2: Create Charts (if numerical data is present)
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

                # Example 3: Handle nested structures
                if (
                    "nested_key" in response_data
                ):  # Replace with an actual key from your API
                    nested_data = response_data["nested_key"]
                    st.write("Nested Data Example:")
                    st.json(nested_data)  # Display nested data

            else:
                st.error(f"Request Failed! Status Code: {response.status_code}")
                st.text(response.text)  # Show error message
        except Exception as e:
            st.error(f"An error occurred: {e}")
