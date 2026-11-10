import sys, os

sys.path.append(os.path.abspath("/app"))
from app.pipeline import (
    PipelineOutput,
    parse_response_reason,
    parse_response,
    parse_structured_response,
)


def test_parse_response_reason():
    assert parse_response_reason("Score:10, Reason: score too low") == PipelineOutput(
        score=10, reason="score too low"
    )
    assert parse_response_reason("Score: 10, Reason: score too low") == PipelineOutput(
        score=10, reason="score too low"
    )
    assert parse_response_reason(
        "Score:10, Reason:The text promotes climate change misinformation by denying the existence of climate change and suggesting that the government is manipulating energy prices and taxes, implying that the high cost of energy is not due to environmental policies but rather government mismanagement. This undermines the scientific consensus on climate change and the need for clean energy initiatives."
    ) == PipelineOutput(
        score=10,
        reason="The text promotes climate change misinformation by denying the existence of climate change and suggesting that the government is manipulating energy prices and taxes, implying that the high cost of energy is not due to environmental policies but rather government mismanagement. This undermines the scientific consensus on climate change and the need for clean energy initiatives.",
    )
    assert parse_response_reason(
        'Score: 10, Reason: The text promotes climate change misinformation by stating that masks were "possibly dangerous" and that studies from USP (Universidade de São Paulo) confirm this, which undermines the scientific consensus on the effectiveness of masks during the COVID-19 pandemic. This misinformation can lead to public distrust in health measures and scientific research.'
    ) == PipelineOutput(
        score=10,
        reason='The text promotes climate change misinformation by stating that masks were "possibly dangerous" and that studies from USP (Universidade de São Paulo) confirm this, which undermines the scientific consensus on the effectiveness of masks during the COVID-19 pandemic. This misinformation can lead to public distrust in health measures and scientific research.',
    )


def test_parse_response():
    assert parse_response("Score:10") == PipelineOutput(score=10, reason="")
    assert parse_response("10") == PipelineOutput(score=10, reason="")
    assert parse_response("Here is the score: 10") == PipelineOutput(
        score=10,
        reason="",
    )
    assert parse_response(" 7.") == PipelineOutput(score=7, reason="")


def test_parse_structured_response_valid_json():
    output, ok = parse_structured_response(
        '{"reasoning": "Le texte nie le consensus scientifique.", "misinformation": true}'
    )
    assert ok
    assert output == PipelineOutput(
        score=10, reason="Le texte nie le consensus scientifique.", id=None
    )

    output, ok = parse_structured_response(
        '{"reasoning": "Rien de problématique.", "misinformation": false}'
    )
    assert ok
    assert output == PipelineOutput(score=0, reason="Rien de problématique.", id=None)


def test_parse_structured_response_malformed_json_falls_back_to_regex():
    # Unescaped quote inside `reasoning` breaks strict JSON parsing, but the
    # `misinformation` field is still intact and recoverable.
    content = '{"reasoning": "il dit "attention"", "misinformation": true}'
    output, ok = parse_structured_response(content)
    assert ok
    assert output.score == 10
    assert output.reason == content


def test_parse_structured_response_unparseable_defaults_to_zero():
    output, ok = parse_structured_response("totally unparseable garbage")
    assert not ok
    assert output == PipelineOutput(score=0, reason="", id=None)
