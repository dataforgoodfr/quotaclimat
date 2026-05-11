from typing import Optional, Tuple


_ASR_TEXT_TAG = "<asr_text>"
_LANG_PREFIX = "language "


def normalize_language_name(language: str) -> str:
    """
    Normalize language name to the canonical format used by Qwen3-ASR:
    first letter uppercase, the rest lowercase (e.g., 'cHINese' -> 'Chinese').

    Args:
        language (str): Input language name.

    Returns:
        str: Normalized language name.

    Raises:
        ValueError: If language is empty.
    """
    if language is None:
        raise ValueError("language is None")
    s = str(language).strip()
    if not s:
        raise ValueError("language is empty")
    return s[:1].upper() + s[1:].lower()


def detect_and_fix_repetitions(text: str, threshold: int = 20) -> str:
    def fix_char_repeats(s: str, thresh: int) -> str:
        res = []
        i = 0
        n = len(s)
        while i < n:
            count = 1
            while i + count < n and s[i + count] == s[i]:
                count += 1
            if count > thresh:
                res.append(s[i])
            else:
                res.append(s[i:i + count])
            i += count
        return ''.join(res)

    if not text:
        return text
    return fix_char_repeats(text, threshold)


def parse_asr_output(
    raw: str,
    user_language: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Parse Qwen3-ASR raw output into (language, text).

    Cases:
      - With tag: "language Chinese<asr_text>...."
      - With newlines: "language Chinese\\n...\\n<asr_text>...."
      - No tag: treat whole string as text.
      - "language None<asr_text>": treat as empty audio -> ("", "")

    If user_language is provided, language is forced to user_language and raw is treated as text-only
    (the model is expected to output plain transcription without metadata).

    Args:
        raw: Raw decoded string.
        user_language: Canonical language name if user forced language.

    Returns:
        Tuple[str, str]: (language, text)
    """
    if raw is None:
        return "", ""
    s = str(raw).strip()
    if not s:
        return "", ""

    s = detect_and_fix_repetitions(s)

    if user_language:
        # user explicitly forced language => model output is treated as pure text
        return user_language, s

    meta_part = s
    text_part = ""
    has_tag = _ASR_TEXT_TAG in s
    if has_tag:
        meta_part, text_part = s.split(_ASR_TEXT_TAG, 1)
    else:
        # no tag => pure text
        return "", s.strip()

    meta_lower = meta_part.lower()

    # empty audio heuristic
    if "language none" in meta_lower:
        t = text_part.strip()
        if not t:
            return "", ""
        # if model still returned something, keep it but language unknown
        return "", t

    # extract "language xxx" from meta
    lang = ""
    for line in meta_part.splitlines():
        line = line.strip()
        if not line:
            continue
        low = line.lower()
        if low.startswith(_LANG_PREFIX):
            val = line[len(_LANG_PREFIX):].strip()
            if val:
                lang = normalize_language_name(val)
            break

    return lang, text_part.strip()
