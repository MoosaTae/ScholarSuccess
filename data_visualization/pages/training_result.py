import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import json
import numpy as np
import requests

url = "http://localhost:8000/report_training"


def fetch_report_training_data(mock=False):
    if mock:
        try:
            with open("response_1733462178691.json", "r") as file:
                data = json.load(file)
            return {"success": True, "data": data}
        except FileNotFoundError:
            return {
                "success": False,
                "message": "JSON file not found. Please ensure 'response_1733462178691.json' exists.",
            }
    try:
        response = requests.get(url)
        response.raise_for_status()
        if response.status_code == 200:
            return {"success": True, "data": response.json()}

    except requests.exceptions.RequestException as e:
        print(e)
        return {
            "success": False,
            "message": str(e),
            "status_code": response.status_code,
        }


data = fetch_report_training_data()
if not data["success"]:
    st.error(f"An error occurred: {data['message']}")
    st.stop()
classification_report = data["data"]["classification_report"]
confusion_matrix = np.array(data["data"]["confusion_matrix"])
feature_importance = data["data"]["feature_importance"]

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
sorted_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[
    :10
]
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
