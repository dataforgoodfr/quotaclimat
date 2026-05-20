"""Prompt loading utilities."""

from pathlib import Path

import yaml
from jinja2 import Environment, TemplateSyntaxError
from pydantic import BaseModel


def _validate_jinja2_syntax(template: str) -> str:
    try:
        Environment().parse(template)
    except TemplateSyntaxError as e:
        raise ValueError(f"Invalid Jinja2 template syntax: {e}") from None
    return template


class Prompt(BaseModel):
    user_prompt: str
    system_prompt: str | None = None


def load_prompt(path: str | Path) -> Prompt:
    """Load a prompt from a YAML file (keys: user_prompt, optional system_prompt) or a plain text file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Prompt file not found: {path}")

    if path.suffix in (".yaml", ".yml"):
        with open(path) as f:
            data = yaml.safe_load(f)
        if "user_prompt" not in data:
            raise ValueError(f"YAML prompt file must have a 'user_prompt' key: {path}")
        return Prompt(
            user_prompt=_validate_jinja2_syntax(data["user_prompt"]),
            system_prompt=data.get("system_prompt"),
        )

    return Prompt(user_prompt=_validate_jinja2_syntax(path.read_text()))
