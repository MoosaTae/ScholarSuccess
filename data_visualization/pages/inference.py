import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import json
import numpy as np
import requests

url = "http://localhost:8000/predict"


st.title("Scholar Success Rate Prediction - API Request")

# Input Fields for API Request
st.subheader("Input Parameters")
title = st.text_input("Title")
abstract = st.text_area("Abstract")
ref_count = st.number_input("Reference Count", step=1)


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
                k: v for k, v in response_data.items() if isinstance(v, (int, float))
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
