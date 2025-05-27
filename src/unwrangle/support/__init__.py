""" 
Helper functions for support files, such as graphql queries and mutations, YAML data, etc. 
"""

import importlib

from yaml import safe_load

# Mapping the descriptor to the output global ID column.
_study_descriptors = None

_support_details = (
    importlib.resources.files("unwrangle") / "support"
)  # descriptors.yaml

def study_descriptors():
    global _study_descriptors

    if _study_descriptors is None:
        _study_descriptors = safe_load(
            open((_support_details / "study_descriptors.yaml").read_text())
        )
    return _study_descriptors

def descriptor_upsert():
    global _study_descriptors

    if _study_descriptors is None:
        _study_descriptors = safe_load(
            open((_support_details / "descriptor_upsert.yaml").read_text())
        )
    return _study_descriptors
