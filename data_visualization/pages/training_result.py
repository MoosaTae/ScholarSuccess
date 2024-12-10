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


def create_metrics_visualization(classification_report):
    """Create and display classification metrics visualization."""
    st.header("Model Performance Metrics")

    # Calculate overall accuracy
    accuracy = classification_report.get("accuracy", 0) * 100
    st.metric("Overall Model Accuracy", f"{accuracy:.1f}%")

    # Prepare data for visualization
    classes = ["False", "True"]  # Non-successful and Successful cases
    metrics = ["precision", "recall", "f1-score"]

    # Create a combined metrics chart
    metrics_data = []
    for cls in classes:
        for metric in metrics:
            metrics_data.append(
                {
                    "Class": "Successful" if cls == "True" else "Not Successful",
                    "Metric": metric.capitalize(),
                    "Value": classification_report[cls][metric] * 100,
                }
            )

    fig = px.bar(
        metrics_data,
        x="Class",
        y="Value",
        color="Metric",
        barmode="group",
        labels={"Value": "Percentage (%)", "Class": "Prediction Class"},
        title="Precision, Recall, and F1-Score by Class",
    )
    st.plotly_chart(fig)

    # Add metric explanations
    st.subheader("Understanding the Metrics")
    st.markdown(f"""
    - **Precision**: 
        - When predicting successful scholars: {classification_report['True']['precision']*100:.1f}% accurate
        - When predicting non-successful scholars: {classification_report['False']['precision']*100:.1f}% accurate
    
    - **Recall**: 
        - Correctly identifies {classification_report['True']['recall']*100:.1f}% of actually successful scholars
        - Correctly identifies {classification_report['False']['recall']*100:.1f}% of non-successful scholars
    
    - **Dataset Distribution**: 
        - Total scholars: {int(classification_report['True']['support'] + classification_report['False']['support'])}
        - Successful: {int(classification_report['True']['support'])} ({classification_report['True']['support']/(classification_report['True']['support'] + classification_report['False']['support'])*100:.1f}%)
        - Non-successful: {int(classification_report['False']['support'])}
    """)


def create_confusion_matrix(confusion_matrix):
    """Create and display enhanced confusion matrix visualization."""
    st.header("Confusion Matrix")

    # Calculate percentages for annotations
    total = confusion_matrix.sum()
    percentages = confusion_matrix / total * 100

    fig = go.Figure(
        data=go.Heatmap(
            z=confusion_matrix,
            x=["Predicted: Not Successful", "Predicted: Successful"],
            y=["Actually: Not Successful", "Actually: Successful"],
            colorscale="Blues",
            text=[
                [f"{val:,d}<br>({pct:.1f}%)" for val, pct in zip(row, pct_row)]
                for row, pct_row in zip(confusion_matrix, percentages)
            ],
            texttemplate="%{text}",
            textfont={"size": 14},
            hoverongaps=False,
        )
    )

    fig.update_layout(
        title="Confusion Matrix (Count and Percentage)",
        xaxis_title="Predicted Outcome",
        yaxis_title="Actual Outcome",
    )
    st.plotly_chart(fig)


def create_feature_importance(feature_importance):
    """Create and display enhanced feature importance visualization."""
    st.header("Top Predictive Features")

    # Sort and prepare feature importance data
    sorted_features = sorted(
        feature_importance.items(), key=lambda x: x[1], reverse=True
    )[:10]
    feature_names, importances = zip(*sorted_features)

    # Clean up feature names for better readability
    clean_names = [name.replace("_", " ").title() for name in feature_names]

    fig = px.bar(
        x=importances,
        y=clean_names,
        orientation="h",
        labels={"x": "Relative Importance", "y": "Feature"},
        title="Top 10 Most Important Features for Prediction",
        color=importances,
        color_continuous_scale="Aggrnyl",
    )

    fig.update_layout(
        yaxis=dict(autorange="reversed"),
        showlegend=False,
    )

    st.plotly_chart(fig)


# Set up page configuration
st.set_page_config(page_title="Scholar Success Prediction Analysis", layout="wide")

st.title("Scholar Success Rate Prediction - Data Visualization")

# Fetch and process data
data = fetch_report_training_data()
if not data["success"]:
    st.error(f"An error occurred: {data['message']}")
    st.stop()

classification_report = data["data"]["classification_report"]
confusion_matrix = np.array(data["data"]["confusion_matrix"])
feature_importance = data["data"]["feature_importance"]

# Create visualizations
create_metrics_visualization(classification_report)
create_confusion_matrix(confusion_matrix)
create_feature_importance(feature_importance)
