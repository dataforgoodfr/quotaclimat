"""Tests for the Factiva article dataclass schema and OuestFrance XML parsing.

These tests validate:
1. The dataclass schema catches format errors at construction time
2. OuestFrance XML is correctly parsed into Factiva-format articles
"""

import json
import re
import xml.etree.ElementTree as ET
from typing import List, Optional

import pytest

from quotaclimat.data_ingestion.factiva.schemas.article_schema import (
    FactivaArticleAttributes,
    FactivaArticleEnvelope,
    FactivaS3Document,
)


# --- Helpers (copied from api_to_s3 to avoid heavy transitive imports) ---

_HTML_TAG_RE = re.compile(r"<[^>]+>")

def _strip_html(text: Optional[str]) -> str:
    if not text:
        return ""
    return _HTML_TAG_RE.sub("", text).strip()

def _get_text(element: Optional[ET.Element]) -> str:
    if element is None or element.text is None:
        return ""
    return element.text.strip()

def _parse_article_xml(article_elem: ET.Element) -> Optional[FactivaArticleEnvelope]:
    """Minimal parser matching api_to_s3.parse_article_xml logic."""
    SOURCE_CODE = "OUESTFR"
    SOURCE_NAME = "Ouest-France"

    article_id = _get_text(article_elem.find("id"))
    if not article_id:
        return None

    an = f"{SOURCE_CODE}:{article_id}"
    parution = article_elem.find("parutions/parution")
    pub_date_str = _get_text(parution.find("dateParution")) if parution is not None else ""
    publication_datetime = pub_date_str if pub_date_str else None
    publication_date = pub_date_str.split("T")[0] if pub_date_str else None

    title = _get_text(article_elem.find("titre"))
    body = _strip_html(_get_text(article_elem.find("texte")))
    snippet = _strip_html(_get_text(article_elem.find("chapeau")))
    byline = _get_text(article_elem.find("signature"))

    word_count_str = _get_text(article_elem.find("nombreMots"))
    word_count = int(word_count_str) if word_count_str else None

    photo_captions = []
    photo_credits = []
    for photo in article_elem.findall("photos/photo"):
        legende = _get_text(photo.find("legende"))
        if legende:
            photo_captions.append(_strip_html(legende))
        credit = _get_text(photo.find("credit"))
        if credit:
            photo_credits.append(_strip_html(credit))

    art = " ".join(photo_captions) if photo_captions else ""
    credit = ", ".join(photo_credits) if photo_credits else ""
    article_url = _get_text(article_elem.find("url")) or None

    tags = []
    for tag_elem in article_elem.findall("tags/tag"):
        tag_text = _get_text(tag_elem) if tag_elem is not None else ""
        if not tag_text and tag_elem is not None and tag_elem.text:
            tag_text = tag_elem.text.strip()
        if tag_text:
            tags.append(tag_text)

    attributes = FactivaArticleAttributes(
        an=an,
        source_code=SOURCE_CODE,
        source_name=SOURCE_NAME,
        action="add",
        document_type="article",
        title=title,
        body=body,
        snippet=snippet,
        art=art,
        byline=byline,
        credit=credit,
        publisher_name=SOURCE_NAME,
        section=tags[0] if tags else "",
        publication_datetime=publication_datetime,
        publication_date=publication_date,
        language_code="fr",
        region_of_origin="France",
        word_count=word_count,
        article_url=article_url,
        tags=tags if tags else None,
    )

    return FactivaArticleEnvelope(id=an, type="article", attributes=attributes)


# --- Sample XML ---

SAMPLE_OUEST_FRANCE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<contenus version="2.0">
    <descriptif>
        <dateParution>2025-01-01T00:00:00</dateParution>
        <publication>Supplément Ouest-France</publication>
    </descriptif>
    <articles>
        <article>
            <id>42ef78db-2551-4ccb-914d-b8d75de2dc0e</id>
            <parutions>
                <parution>
                    <dateParution>2025-01-01T00:00:00</dateParution>
                    <publications>
                        <publication>Quotidien Ouest-France</publication>
                        <publication>Regards</publication>
                    </publications>
                </parution>
            </parutions>
            <surtitre/>
            <titre><![CDATA[ Les choristes réapparaissent dans la soute d'un car ]]></titre>
            <chapeau><![CDATA[<p>À la Saint-Sylvestre 2009, un petit tableau d'Edgar Degas disparaît.</p>]]></chapeau>
            <texte><![CDATA[<p>Nous sommes en décembre 2009, à Marseille.</p><p>Parmi tous ces trésors.</p>]]></texte>
            <signature><![CDATA[Olivier RENAULT.]]></signature>
            <nombreMots>827</nombreMots>
            <nombreColonnes/>
            <photos>
                <photo>
                    <legende><![CDATA[Fin 2009, un petit tableau disparaît.]]></legende>
                    <credit><![CDATA[Xavier Lissilour, Ouest-France]]></credit>
                    <url>https://guichet.ouest-france.fr/ws/medias/image/test</url>
                </photo>
            </photos>
            <pages><page><id>page1</id></page></pages>
            <localisations/>
            <categorisations/>
            <url>https://www.ouest-france.fr/culture/arts/recit-les-choristes-6922085</url>
            <tags>
                <tag>Une_Edition spéciale</tag>
                <tag>Culture/Arts</tag>
            </tags>
        </article>
        <article>
            <id>second-article-id</id>
            <parutions>
                <parution>
                    <dateParution>2025-01-02T00:00:00</dateParution>
                    <publications>
                        <publication>Quotidien Ouest-France</publication>
                    </publications>
                </parution>
            </parutions>
            <surtitre/>
            <titre><![CDATA[ Deuxième article test ]]></titre>
            <chapeau/>
            <texte><![CDATA[<p>Corps du deuxième article.</p>]]></texte>
            <signature><![CDATA[Auteur Test.]]></signature>
            <nombreMots>50</nombreMots>
            <photos/>
            <localisations/>
            <categorisations/>
            <url>https://www.ouest-france.fr/test/second-article</url>
            <tags/>
        </article>
    </articles>
</contenus>
"""


# --- Dataclass schema tests ---


class TestFactivaArticleAttributes:
    def test_valid_minimal_article(self):
        attrs = FactivaArticleAttributes(
            an="TEST:123",
            source_code="OUESTFR",
            source_name="Ouest-France",
        )
        assert attrs.an == "TEST:123"
        assert attrs.source_code == "OUESTFR"
        assert attrs.action == "add"
        assert attrs.language_code == "fr"
        assert attrs.word_count is None

    def test_valid_full_article(self):
        attrs = FactivaArticleAttributes(
            an="OUESTFR:abc-123",
            source_code="OUESTFR",
            source_name="Ouest-France",
            title="Test article",
            body="Article body text",
            snippet="Lead paragraph",
            word_count=150,
            publication_datetime="2025-01-01T00:00:00",
            article_url="https://www.ouest-france.fr/article/123",
            tags=["Culture/Arts", "Environnement"],
        )
        assert attrs.title == "Test article"
        assert attrs.word_count == 150
        assert attrs.article_url == "https://www.ouest-france.fr/article/123"
        assert attrs.tags == ["Culture/Arts", "Environnement"]

    def test_empty_an_raises(self):
        with pytest.raises(ValueError, match="Article ID"):
            FactivaArticleAttributes(
                an="",
                source_code="OUESTFR",
                source_name="Ouest-France",
            )

    def test_whitespace_an_raises(self):
        with pytest.raises(ValueError, match="Article ID"):
            FactivaArticleAttributes(
                an="   ",
                source_code="OUESTFR",
                source_name="Ouest-France",
            )

    def test_empty_source_code_raises(self):
        with pytest.raises(ValueError, match="source_code"):
            FactivaArticleAttributes(
                an="TEST:123",
                source_code="",
                source_name="Ouest-France",
            )

    def test_word_count_coercion_from_string(self):
        attrs = FactivaArticleAttributes(
            an="TEST:123",
            source_code="OUESTFR",
            source_name="Ouest-France",
            word_count="827",
        )
        assert attrs.word_count == 827


class TestFactivaArticleEnvelope:
    def test_valid_envelope(self):
        envelope = FactivaArticleEnvelope(
            id="TEST:123",
            type="article",
            attributes=FactivaArticleAttributes(
                an="TEST:123",
                source_code="OUESTFR",
                source_name="Ouest-France",
            ),
        )
        assert envelope.id == "TEST:123"
        assert envelope.type == "article"
        assert envelope.attributes.source_code == "OUESTFR"

    def test_to_dict_structure(self):
        """Verify to_dict output matches the format expected by s3_factiva_to_postgre."""
        envelope = FactivaArticleEnvelope(
            id="TEST:123",
            attributes=FactivaArticleAttributes(
                an="TEST:123",
                source_code="OUESTFR",
                source_name="Ouest-France",
                title="My Title",
            ),
        )
        d = envelope.to_dict()
        assert d["id"] == "TEST:123"
        assert d["type"] == "article"
        assert d["attributes"]["an"] == "TEST:123"
        assert d["attributes"]["title"] == "My Title"
        assert d["attributes"]["source_code"] == "OUESTFR"


class TestFactivaS3Document:
    def test_valid_document(self):
        doc = FactivaS3Document(
            data=[
                FactivaArticleEnvelope(
                    id="TEST:1",
                    attributes=FactivaArticleAttributes(
                        an="TEST:1", source_code="OUESTFR", source_name="Ouest-France",
                    ),
                ),
                FactivaArticleEnvelope(
                    id="TEST:2",
                    attributes=FactivaArticleAttributes(
                        an="TEST:2", source_code="OUESTFR", source_name="Ouest-France",
                    ),
                ),
            ]
        )
        assert len(doc.data) == 2

    def test_to_dict_produces_valid_json(self):
        doc = FactivaS3Document(
            data=[
                FactivaArticleEnvelope(
                    id="TEST:1",
                    attributes=FactivaArticleAttributes(
                        an="TEST:1",
                        source_code="OUESTFR",
                        source_name="Ouest-France",
                        title="Titre test",
                    ),
                )
            ]
        )
        d = doc.to_dict()
        json_str = json.dumps(d, ensure_ascii=False)
        parsed = json.loads(json_str)
        assert len(parsed["data"]) == 1
        assert parsed["data"][0]["attributes"]["title"] == "Titre test"

    def test_empty_data_is_valid(self):
        doc = FactivaS3Document(data=[])
        assert len(doc.data) == 0


# --- OuestFrance XML parsing tests ---


class TestStripHtml:
    def test_strip_simple_tags(self):
        assert _strip_html("<p>Hello world</p>") == "Hello world"

    def test_strip_nested_tags(self):
        assert _strip_html("<p>Hello <em>world</em></p>") == "Hello world"

    def test_strip_none(self):
        assert _strip_html(None) == ""

    def test_strip_empty(self):
        assert _strip_html("") == ""

    def test_plain_text_unchanged(self):
        assert _strip_html("plain text") == "plain text"


class TestParseArticleXml:
    def test_parse_first_article(self):
        root = ET.fromstring(SAMPLE_OUEST_FRANCE_XML)
        article_elem = root.findall(".//article")[0]
        envelope = _parse_article_xml(article_elem)

        assert envelope is not None
        attrs = envelope.attributes
        assert attrs.an == "OUESTFR:42ef78db-2551-4ccb-914d-b8d75de2dc0e"
        assert attrs.source_code == "OUESTFR"
        assert attrs.source_name == "Ouest-France"
        assert "choristes" in attrs.title
        assert attrs.word_count == 827
        assert attrs.publication_datetime == "2025-01-01T00:00:00"
        assert attrs.publication_date == "2025-01-01"
        assert attrs.language_code == "fr"
        assert attrs.article_url == "https://www.ouest-france.fr/culture/arts/recit-les-choristes-6922085"
        assert attrs.tags == ["Une_Edition spéciale", "Culture/Arts"]

    def test_body_html_stripped(self):
        root = ET.fromstring(SAMPLE_OUEST_FRANCE_XML)
        article_elem = root.findall(".//article")[0]
        envelope = _parse_article_xml(article_elem)

        assert "<p>" not in envelope.attributes.body
        assert "Marseille" in envelope.attributes.body

    def test_snippet_html_stripped(self):
        root = ET.fromstring(SAMPLE_OUEST_FRANCE_XML)
        article_elem = root.findall(".//article")[0]
        envelope = _parse_article_xml(article_elem)

        assert "<p>" not in envelope.attributes.snippet
        assert "Degas" in envelope.attributes.snippet

    def test_photo_caption_in_art(self):
        root = ET.fromstring(SAMPLE_OUEST_FRANCE_XML)
        article_elem = root.findall(".//article")[0]
        envelope = _parse_article_xml(article_elem)

        assert "tableau disparaît" in envelope.attributes.art

    def test_photo_credit(self):
        root = ET.fromstring(SAMPLE_OUEST_FRANCE_XML)
        article_elem = root.findall(".//article")[0]
        envelope = _parse_article_xml(article_elem)

        assert "Xavier Lissilour" in envelope.attributes.credit

    def test_byline(self):
        root = ET.fromstring(SAMPLE_OUEST_FRANCE_XML)
        article_elem = root.findall(".//article")[0]
        envelope = _parse_article_xml(article_elem)

        assert "Olivier RENAULT" in envelope.attributes.byline

    def test_article_without_photos(self):
        root = ET.fromstring(SAMPLE_OUEST_FRANCE_XML)
        article_elem = root.findall(".//article")[1]
        envelope = _parse_article_xml(article_elem)

        assert envelope is not None
        assert envelope.attributes.art == ""
        assert envelope.attributes.word_count == 50

    def test_article_missing_id_returns_none(self):
        xml = "<article><titre>No ID</titre></article>"
        article_elem = ET.fromstring(xml)
        result = _parse_article_xml(article_elem)
        assert result is None

    def test_multiple_articles_parsed(self):
        root = ET.fromstring(SAMPLE_OUEST_FRANCE_XML)
        articles = []
        for article_elem in root.findall(".//article"):
            envelope = _parse_article_xml(article_elem)
            if envelope is not None:
                articles.append(envelope)

        assert len(articles) == 2
        assert articles[0].attributes.an == "OUESTFR:42ef78db-2551-4ccb-914d-b8d75de2dc0e"
        assert articles[1].attributes.an == "OUESTFR:second-article-id"

    def test_all_articles_have_correct_source(self):
        root = ET.fromstring(SAMPLE_OUEST_FRANCE_XML)
        for article_elem in root.findall(".//article"):
            envelope = _parse_article_xml(article_elem)
            if envelope is not None:
                assert envelope.attributes.source_code == "OUESTFR"
                assert envelope.attributes.action == "add"


class TestFactivaS3DocumentFromOuestFrance:
    """Integration: OuestFrance articles produce a valid FactivaS3Document for S3."""

    def test_round_trip_json(self):
        root = ET.fromstring(SAMPLE_OUEST_FRANCE_XML)
        articles = [
            _parse_article_xml(elem)
            for elem in root.findall(".//article")
        ]
        articles = [a for a in articles if a is not None]

        doc = FactivaS3Document(data=articles)
        json_str = json.dumps(doc.to_dict(), ensure_ascii=False)
        parsed = json.loads(json_str)

        assert len(parsed["data"]) == 2
        first = parsed["data"][0]["attributes"]
        assert first["source_code"] == "OUESTFR"
        assert first["an"].startswith("OUESTFR:")
        assert first["article_url"].startswith("https://")
        assert isinstance(first["tags"], list)

    def test_s3_document_has_data_key(self):
        """The top-level JSON must have a 'data' key (required by s3_factiva_to_postgre)."""
        root = ET.fromstring(SAMPLE_OUEST_FRANCE_XML)
        articles = [
            _parse_article_xml(elem)
            for elem in root.findall(".//article")
            if _parse_article_xml(elem) is not None
        ]

        doc = FactivaS3Document(data=articles)
        d = doc.to_dict()

        assert "data" in d
        assert isinstance(d["data"], list)
        for item in d["data"]:
            assert "id" in item
            assert "type" in item
            assert "attributes" in item
