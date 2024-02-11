OPTIONS = {
    "LOCAL_EXTRACT",
    "SCHEMA_STATISTICS",
    "FORMATTED_CSV",
    # "USE_TYPE_LABEL",
    "USE_PRED_TYPE",
    "PICK_SAMPLED_INST_ONLY",
}

"""
Advanced options (default = off):
- INST_LITERAL_PROPERTY
"""


def hasOption(option: str) -> bool:
    return option in OPTIONS
