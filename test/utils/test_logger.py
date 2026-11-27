import logging

from quotaclimat.utils.logger import CustomFormatter, set_log_prefix


def _record(level=logging.INFO, msg="hello"):
    return logging.LogRecord("x", level, "run.py", 12, msg, None, None)


def test_no_time_by_default_but_the_level(monkeypatch):
    monkeypatch.delenv("LOG_TIME", raising=False)
    assert CustomFormatter(with_color=False).format(_record()) == "INFO - run.py:12 | hello"


def test_log_time_puts_the_time_back(monkeypatch):
    monkeypatch.setenv("LOG_TIME", "true")
    line = CustomFormatter(with_color=False).format(_record())
    assert line.startswith("20")
    assert " - INFO - run.py:12 | hello" in line


def test_prefix_starts_the_line(monkeypatch):
    monkeypatch.delenv("LOG_TIME", raising=False)
    formatter = CustomFormatter(with_color=False)
    set_log_prefix("tf1")
    try:
        assert formatter.format(_record(logging.WARNING)) == "tf1 | WARNING - run.py:12 | hello"
    finally:
        set_log_prefix(None)
    assert formatter.format(_record()) == "INFO - run.py:12 | hello"


def test_colors(monkeypatch):
    monkeypatch.delenv("LOG_TIME", raising=False)
    formatter = CustomFormatter(with_color=True)
    assert formatter.format(_record()) == "\x1b[36mINFO - run.py:12 | hello\x1b[0m"
    assert formatter.format(_record(logging.WARNING)) == "\x1b[33;20mWARNING - run.py:12 | hello\x1b[0m"


def test_no_colors_when_stdout_is_not_a_terminal(monkeypatch):
    # Cockpit (Grafana) reads the first level word of the line, which a color code in front of it hides
    monkeypatch.delenv("LOG_COLOR", raising=False)
    monkeypatch.setattr("sys.stdout.isatty", lambda: False)
    line = CustomFormatter().format(_record(msg="Exporting ads: 50/100 (error=2)"))
    assert line == "INFO - run.py:12 | Exporting ads: 50/100 (error=2)"


def test_log_color_forces_colors(monkeypatch):
    monkeypatch.setenv("LOG_COLOR", "true")
    monkeypatch.setattr("sys.stdout.isatty", lambda: False)
    assert CustomFormatter().format(_record()).startswith("\x1b[36mINFO")
