from dataclasses import dataclass
from typing import List, Dict, Any, Union, Optional, Tuple, Literal, Annotated
from fastapi import Body

@dataclass
class training_report_response:
    classification_report: Dict[str, Any]
    confusion_matrix: List[List[int]]
    feature_importance: Dict[str, float]
    learning_curves: Dict[str, List[float]]


@dataclass
class predict_request:
    title: Annotated[
        str,
        Body(
            examples=[
                "Lactide Lactone Chain Shuttling Copolymerization Mediated by an Aminobisphenolate Supported Aluminum Complex and Al(O iPr)3: Access to New Polylactide Based Block Copolymers",
                "Robust Output-Feedback Stabilization of a Nonlinear Bioreactor: A Matrix Inequality Approach",
            ],
            description="The title of the paper",
        ),
    ]
    abstract: Annotated[
        str,
        Body(
            examples=[
                "The chain shuttling ring-opening copolymerization of l-lactide with ε-caprolactone has been achieved using two aluminum catalysts presenting different selectivities and benzyl alcohol as chain transfer agent. A newly synthesized aminobisphenolate supported aluminum complex affords the synthesis of lactone rich poly(l-lactide-co-lactone) statistical copolymeric blocks, while Al(OiPr)3 produces semicrystalline poly(l-lactide) rich blocks.",
                "This paper deals with an output-feedback stabilization problem of a nonlinear continuous bioreactor, whose vector fields are rational functions. In this problem, the control input must be bounded in some specified range in order to prevent the system reaching into undesired properties, such as bifurcation.",
            ],
            description="The abstract of the paper",
        ),
    ]
    ref_count: Annotated[
        int,
        Body(
            examples=[26, 17], description="Number of references in the paper"
        ),
    ]

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "title": "Lactide Lactone Chain Shuttling Copolymerization Mediated by an Aminobisphenolate Supported Aluminum Complex and Al(O iPr)3: Access to New Polylactide Based Block Copolymers",
                    "abstract": "The chain shuttling ring-opening copolymerization of l-lactide with ε-caprolactone has been achieved using two aluminum catalysts presenting different selectivities and benzyl alcohol as chain transfer agent. A newly synthesized aminobisphenolate supported aluminum complex affords the synthesis of lactone rich poly(l-lactide-co-lactone) statistical copolymeric blocks, while Al(OiPr)3 produces semicrystalline poly(l-lactide) rich blocks.",
                    "ref_count": 26,
                },
                {
                    "title": "Robust Output-Feedback Stabilization of a Nonlinear Bioreactor: A Matrix Inequality Approach",
                    "abstract": "This paper deals with an output-feedback stabilization problem of a nonlinear continuous bioreactor, whose vector fields are rational functions. In this problem, the control input must be bounded in some specified range in order to prevent the system reaching into undesired properties, such as bifurcation.",
                    "ref_count": 17,
                },
            ]
        }
    }


@dataclass
class predict_response:
    prediction: Annotated[
        int,
        Body(examples=[1, 0], description="Predicted classification (0 or 1)"),
    ]
    probability: Annotated[
        Dict[str, float],
        Body(
            examples=[{"0": 0.15, "1": 0.85}, {"0": 0.75, "1": 0.25}],
            description="Probability distribution for each class",
        ),
    ]

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"prediction": 1, "probability": {"0": 0.15, "1": 0.85}},
                {"prediction": 0, "probability": {"0": 0.75, "1": 0.25}},
            ]
        }
    }
