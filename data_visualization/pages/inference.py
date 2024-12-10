import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import json
import numpy as np
import requests

url = "http://localhost:8000/predict"

EXAMPLES = {
    "Example 1: Chemistry Research": {
        "title": "Lactide Lactone Chain Shuttling Copolymerization Mediated by an Aminobisphenolate Supported Aluminum Complex and Al(O iPr)3: Access to New Polylactide Based Block Copolymers",
        "abstract": "The chain shuttling ring-opening copolymerization of l-lactide with ε-caprolactone has been achieved using two aluminum catalysts presenting different selectivities and benzyl alcohol as chain transfer agent. A newly synthesized aminobisphenolate supported aluminum complex affords the synthesis of lactone rich poly(l-lactide-co-lactone) statistical copolymeric blocks, while Al(OiPr)3 produces semicrystalline poly(l-lactide) rich blocks.",
        "ref_count": 26,
    },
    "Example 2: Computer Conference": {
        "title": "0.01 Cent per Second: Developing a Cloud-based Cost-effective Audio Transcription System for an Online Video Learning Platform",
        "abstract": "Using automatic speech recognition (ASR) to transcribe videos in an online video learning platform can benefit learners in multiple ways. However, existing speech-to-text APIs can be costly to use, especially for long lecture videos commonly found in such platform. In this work, we developed a cloud-based ASR system that is cost-optimized for the workload of online learning platforms. We characterized such workload and applied a combination of techniques from system architecture, including: (1) serverless, (2) preemptible instance, and (3) batching and audio transcription optimization, including: (1) audio segmentation, (2) cost-based segment merging, and (3) locally hosted transcription model. All of which work together to provide a low transcription cost per minute of audio. We experimented and calculated the processing cost, time, and accuracy and showed that our system offers accuracy on par with existing speech-to-text services at a significantly lower cost. We have also integrated this system into an online video learning platform.",
        "ref_count": 15,
    },
}


if "title" not in st.session_state:
    st.session_state.title = ""
if "abstract" not in st.session_state:
    st.session_state.abstract = ""
if "ref_count" not in st.session_state:
    st.session_state.ref_count = 0


def request_inference(title, abstract, ref_count, mock=False):
    if mock:
        return {
            "success": True,
            "data": {
                "probability": {"0": 0.2, "1": 0.8},
                "prediction": 1,
            },
        }
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

st.subheader("Try an Example")
example_buttons = st.columns(len(EXAMPLES))

for i, (name, data) in enumerate(EXAMPLES.items()):
    if example_buttons[i].button(f"Load {name}"):
        # selected_example = data
        st.session_state.title = data["title"]
        st.session_state.abstract = data["abstract"]
        st.session_state.ref_count = data["ref_count"]


# Input Fields for API Request
st.subheader("Input Parameters")
title = st.text_input("Title", value=st.session_state.title)
abstract = st.text_area("Abstract", value=st.session_state.abstract)
ref_count = st.number_input("Reference Count", value=st.session_state.ref_count, step=1)

# Update session state based on input changes
st.session_state.title = title
st.session_state.abstract = abstract
st.session_state.ref_count = ref_count


# Make the POST request on button click
if st.button("Predict"):
    response_data = request_inference(title, abstract, ref_count, mock=True)

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
