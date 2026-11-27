import pytest
import sys, os

sys.path.append(os.path.abspath("/app"))
from app.labelstudio_utils import *
from datetime import datetime
import logging


def test_get_label_studio_format():
    plaintext = "full plaintext"
    channel_title = "Test TV"
    df = pd.DataFrame(
        [
            {
                "plaintext": plaintext,
                "plaintext_whisper": plaintext,
                "start": datetime(2024, 3, 3, 12, 0, 0),
                "channel_title": channel_title,
                "channel_name": "news123",
                "channel_program": "Morning News",
                "channel_program_type": "Live",
                "model_name": "TestModel",
                "prompt_version": "0.0.1",
                "pipeline_version": "TestModel/0.0.1",
                "model_result": "10",
                "model_reason": "a reason",
                "year": 2024,
                "month": 3,
                "day": 3,
                "channel": "news",
                "country": "country",
            }
        ]
    )

    row = df.iloc[0]

    # Call the function
    output = get_label_studio_format(row)

    # Assertions to validate the expected structure
    assert "data" in output
    assert "item" in output["data"]
    assert output["data"]["item"]["plaintext"] == plaintext
    assert output["data"]["item"]["channel_title"] == channel_title
    assert output["data"]["item"]["year"] == 2024
    assert output["data"]["item"]["prompt_version"] == "0.0.1"
    assert output["data"]["item"]["pipeline_version"] == "TestModel/0.0.1"
    assert output["data"]["item"]["url_mediatree"] == get_url_mediatree(
        row["start"], row["channel_name"]
    )
