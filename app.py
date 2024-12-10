from fastapi import FastAPI, Request, Response
from typing import List, Dict, Any, Union, Optional, Tuple, Literal
import uvicorn
import onnxruntime as rt
import json
import os
import numpy as np
import torch
from transformers import AutoTokenizer, AutoModel
from type import predict_request, predict_response, training_report_response

app = FastAPI()


DEPLOY_MODEL_PATH = "model/XGBoost.onnx"
REPORT_TRAINING_PATH = "model/training_report.json"

model_name = "allenai/scibert_scivocab_uncased"
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"using {device}")
tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=True)
model = AutoModel.from_pretrained(
    model_name, torch_dtype=torch.float16
)

if not os.path.exists(DEPLOY_MODEL_PATH):
    raise Exception("Model not found")
if not os.path.exists(REPORT_TRAINING_PATH):
    raise Exception("Training report not found")

rt_inference = rt.InferenceSession(f"{DEPLOY_MODEL_PATH}")
input_name = rt_inference.get_inputs()[0].name


def process(texts, max_length=512):
    global model, tokenizer, device
    inputs = tokenizer(
        texts,
        return_tensors="pt",
        padding=True,
        truncation=True,
        max_length=max_length,
    )
    with torch.no_grad():
        outputs = model(**inputs)

    last_hidden_state = outputs.last_hidden_state.numpy()
    # A : (batch_size, sequence_length, hidden_size)
    return last_hidden_state.mean(axis=1)


def process_title_abstract(title, abstract):
    title = process(title)
    abstract = process(abstract)
    return np.hstack([title, abstract])


def predict_onnx(data) -> Tuple[int, Dict[int, float]]:
    data = data.astype(np.float32)
    input_data = {input_name: data}
    prediction, prob = rt_inference.run(None, input_data)
    # print(prediction, prob)

    prediction = int(prediction[0])
    prob_dict = {str(k): float(v) for k, v in prob[0].items()}
    return prediction, prob_dict


@app.get("/report_training", response_model=training_report_response)
async def report_training() -> training_report_response:
    train_report = json.loads(open(REPORT_TRAINING_PATH).read())
    return training_report_response(**train_report)


@app.post("/predict", response_model=predict_response)
async def predict(request: predict_request) -> predict_response:
    """
    Predict the outcome based on the title, abstract, and reference count.
    """
    title = request.title
    abstract = request.abstract
    ref_count = request.ref_count

    X = process_title_abstract(title, abstract)
    X = np.column_stack([X, ref_count, len(abstract), len(title)])
    print(X)
    prediction, prob = predict_onnx(X)

    return predict_response(prediction=prediction, probability=prob)


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)
