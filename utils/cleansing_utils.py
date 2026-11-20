# Allow imports when Airflow accidentally parses this file as a DAG
import sys
from pathlib import Path as _Path
_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import re


def normalize_text(x):
    if x is None:
        return None

    if isinstance(x, (tuple, list)):
        return "".join(map(str, x))

    text = str(x).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def clean_problem_description(text):
    text = normalize_text(text)

    if text:
        text = text.lower().strip()

    return text


def sanitize_problem_description_for_llm(text):
    text = normalize_text(text)

    if not text:
        return ""

    text = text.lower().strip()

    # mask urls
    text = re.sub(r"https?://\S+|www\.\S+", "[LINK]", text)

    # mask emails
    text = re.sub(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b", "[EMAIL]", text)

    # mask long numeric strings
    text = re.sub(r"\b\d{4,}\b", "[NUMBER]", text)

    # mask mixed alphanumeric identifiers only
    text = re.sub(r"\b(?=\w*[A-Za-z])(?=\w*\d)[A-Za-z0-9_-]{6,}\b", "[TOKEN]", text)

    return text