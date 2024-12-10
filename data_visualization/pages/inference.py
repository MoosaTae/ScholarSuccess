import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import json
import numpy as np
import requests

url = "http://localhost:8000/predict"


def request_inference(title, abstract, ref_count):
    payload = {
        "title": title,
        "abstract": abstract,
        "ref_count": ref_count,
    }
    try:
        response = requests.post(
            url,
            headers={
                "accept": "application/json",
                "Content-Type": "application/json",
            },
            json=payload,
        )
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


def create_probability_bar_chart(probabilities):
    """Create a bar chart showing prediction probabilities."""
    fig = go.Figure(
        data=[
            go.Bar(
                x=["Unsuccessful (0)", "Successful (1)"],
                y=[probabilities["0"], probabilities["1"]],
                marker_color=["#ff9999", "#99ff99"],
            )
        ]
    )

    fig.update_layout(
        title="Prediction Probabilities",
        yaxis_title="Probability",
        yaxis_range=[0, 1],
        showlegend=False,
    )
    return fig


def create_gauge_chart(probability):
    """Create a gauge chart showing success probability."""
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=probability * 100,
            title={"text": "Success Probability"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": "darkgreen"},
                "steps": [
                    {"range": [0, 33], "color": "#ff9999"},
                    {"range": [33, 66], "color": "#ffff99"},
                    {"range": [66, 100], "color": "#99ff99"},
                ],
            },
        )
    )

    fig.update_layout(height=300)
    return fig


st.title("Scholar Success Rate Prediction - API Request")

# Input Fields for API Request
st.subheader("Input Parameters")
title = st.text_input("Title")
abstract = st.text_area("Abstract")
ref_count = st.number_input("Reference Count", step=1)


# Make the POST request on button click
if st.button("Predict"):
    response_data = request_inference(title, abstract, ref_count)

    st.subheader("Response Visualization")

    # Display response
    if response_data["success"]:
        st.success("Request Successful!")
        data = response_data["data"]
        probabilities = data["probability"]
        prediction = data["prediction"]

        # Display prediction probabilities
        col1, col2 = st.columns(2)

        with col1:
            st.plotly_chart(
                create_probability_bar_chart(probabilities), use_container_width=True
            )

        with col2:
            st.plotly_chart(
                create_gauge_chart(probabilities["1"]), use_container_width=True
            )

        # Display prediction result
        prediction_text = "Successful" if prediction == 1 else "Unsuccessful"
        st.markdown(f"### Prediction: **{prediction_text}**")
        st.markdown(
            f"Confidence: **{max(probabilities['0'], probabilities['1'])*100:.2f}%**"
        )

    else:
        st.error(f"Request Failed! Status Code: {response_data.status_code}")
    # Radar Chart for Classification Metrics
# st.header("Radar Chart of Classification Metrics")
# # Prepare data for radar chart
# classes = [
#     cls for cls in classification_report if isinstance(classification_report[cls], dict)
# ]
# metrics = ["precision", "recall", "f1-score"]
# # Create a DataFrame from the classification report for easy manipulation
# radar_data = []
# for cls in classes:
#     radar_data.append(
#         {
#             "Class": cls,
#             "Precision": classification_report[cls].get("precision", 0),
#             "Recall": classification_report[cls].get("recall", 0),
#             "F1-score": classification_report[cls].get("f1-score", 0),
#         }
#     )
# radar_df = pd.DataFrame(radar_data)

# # Radar chart requires repeating the first metric to close the circle
# categories = metrics
# fig = go.Figure()
# for i, row in radar_df.iterrows():
#     fig.add_trace(
#         go.Scatterpolar(
#             r=[row["Precision"], row["Recall"], row["F1-score"], row["Precision"]],
#             theta=categories + [categories[0]],
#             fill="toself",
#             name=row["Class"],
#         )
#     )

# fig.update_layout(
#     polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
#     showlegend=True,
#     title="Radar Chart of Classification Metrics",
# )
# st.plotly_chart(fig)
