"""Unit test for the Pulsar themes XLSX parser (no DB / network)."""

import openpyxl

from rrs.pulsar.parse_themes import parse_themes_xlsx


def _write_workbook(path):
    wb = openpyxl.Workbook()
    themes = wb.active
    themes.title = "Themes"
    themes.append(["Search name", "Test Search"])
    themes.append([])
    themes.append(["Title", "Themes"])
    themes.append([])
    themes.append(["Date from", "10-06-2026 (00:00)"])
    themes.append(["Date to", "16-06-2026 (23:59)"])
    themes.append([])
    themes.append(["Themes", "Title", "Summary", "Volume of Posts", "Sentiment"])
    themes.append(["nucléaire, coût", "Nuclear Energy Debate", "A summary.", 6562, "Positive"])
    themes.append(["allemagne", "France vs Germany", "Another.", 1551, "Neutral"])
    themes.append([])

    topics = wb.create_sheet("Topics Breakdown")
    topics.append(["Search name", "Test Search"])
    topics.append([])
    topics.append(["Title", "Topics Breakdown"])
    topics.append([])
    topics.append([])
    topics.append([])
    topics.append([])
    topics.append(["Topics", "Volume of Posts", "Themes"])
    topics.append(["nucléaire", 1521, "nucléaire, coût"])
    topics.append(["coût", 204, "nucléaire, coût"])
    topics.append(["allemagne", 588, "allemagne"])
    wb.save(path)


def test_parse_themes(tmp_path):
    p = tmp_path / "themes.xlsx"
    _write_workbook(p)

    export = parse_themes_xlsx(p)

    assert export.search_name == "Test Search"
    assert export.date_from is not None and export.date_from.year == 2026 and export.date_from.month == 6
    assert export.date_to is not None and export.date_to.hour == 23
    assert len(export.themes) == 2

    nuclear = export.themes[0]
    assert nuclear.title == "Nuclear Energy Debate"
    assert nuclear.summary == "A summary."
    assert nuclear.volume == 6562
    assert nuclear.sentiment == "Positive"
    # topics joined from the Topics Breakdown sheet by the constituent-topics key
    assert sorted(t.label for t in nuclear.topics) == ["coût", "nucléaire"]
    assert {t.label: t.volume for t in nuclear.topics} == {"nucléaire": 1521, "coût": 204}

    germany = export.themes[1]
    assert [t.label for t in germany.topics] == ["allemagne"]
