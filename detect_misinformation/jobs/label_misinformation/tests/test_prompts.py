import os
import sys

sys.path.append(os.path.abspath("/app"))
from app.prompts import DisinformationPrompt, PROMPTS


def test_production_prompt_defined():
    from app.prompts import PIPELINE_PRODUCTION_PROMPT

    prompt = PROMPTS[PIPELINE_PRODUCTION_PROMPT]
    assert prompt.prod


def test_prompt_versions():
    for version in PROMPTS:
        assert version == PROMPTS[version].version


def test_create_prompt():
    prompt = DisinformationPrompt(
        prompt="This is a test prompt", version="0.0.0", prod=False
    )

    assert str(prompt) == (
        f"DisinformationPrompt Version: {prompt.version} | Production Use: {prompt.prod} | Prompt Text: {prompt.prompt}"
    )
