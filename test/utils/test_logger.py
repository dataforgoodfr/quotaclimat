import logging

from quotaclimat.utils.logger import CustomFormatter, set_log_prefix


def _record(level=logging.INFO):
    return logging.LogRecord("x", level, "run.py", 12, "hello", None, None)


def test_no_time_by_default_but_the_level(monkeypatch):
    monkeypatch.delenv("LOG_TIME", raising=False)
    assert CustomFormatter().format(_record()) == "\x1b[36mINFO - run.py:12 | hello\x1b[0m"


def test_log_time_puts_the_time_back(monkeypatch):
    monkeypatch.setenv("LOG_TIME", "true")
    line = CustomFormatter().format(_record())
    assert line.startswith("\x1b[36m20")
    assert " - INFO - run.py:12 | hello" in line


def test_prefix_starts_the_line(monkeypatch):
    monkeypatch.delenv("LOG_TIME", raising=False)
    formatter = CustomFormatter()
    set_log_prefix("tf1")
    try:
        assert formatter.format(_record(logging.WARNING)) == "\x1b[33;20mtf1 | WARNING - run.py:12 | hello\x1b[0m"
    finally:
        set_log_prefix(None)
    assert formatter.format(_record()) == "\x1b[36mINFO - run.py:12 | hello\x1b[0m"
