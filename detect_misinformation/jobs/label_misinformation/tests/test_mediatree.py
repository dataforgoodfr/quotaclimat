import pytest
import sys, os

sys.path.append(os.path.abspath("/app"))
import pandas as pd
from app.mediatree_utils import (
    get_new_plaintext_from_whisper,
    get_video_urls,
    get_url_mediatree,
    get_start_and_end_of_chunk,
)

from datetime import datetime

plaintext = (
    "quatorze pour cent quinze pour cent de voitures électriques vendues en europe l' année dernière à cette année treize pour cent dont vous voyez en plus la si vous voulez parce que c' est trop cher et parce que en france une fin en europe une fois n' est pas coutume on a un problème de souveraineté et à dire que tout ce qu' elle est tout ce que sont les batteries électriques les méthodes qui permettent de sarah je viens progressivement mais de quoi rémi waquet jean-françois liée à un moment donné il faudra quand même qu' au niveau mondial on se pose la vraie question mais c' est les accords de paris un sol et la lutte mondiale contre les la production de gaz à effet de serre ça ne ça va ne pas va aller pas s' aller arrangeant si on renonce aux impératifs climatiques c' est tout c' est aussi bête que ça non mais on ne sait pas ce petit petit traité européen mine de rien quand on voit ce qu' on pèse dans le monde euh prendre des mesures qui vient pénaliser une industrie aussi consommateurs nan mais sans aller dans le mur jean-françois mais quand on voit ce que fait la chine quand on voit ce que fait la quand on voit maintenant avec donald trump ce que fait ce que ce que font les etats unis sans dire que c' est le modèle absolu on ne peut pas se dire nous absolument on va essayer de faire encore plus vertueux que les plus vertueux on a une on a une a on aujourd' a hui on aujourd' on hui est on les est leaders les dans leaders la dans transition la énergétique transition on énergétique a une énergie propre que le nucléaire et on ne peut pas se laisser si vous voulez imposer des normes notamment sur ces domaines là par les allemands qui ont arrêté le nucléaire parce que les chardons c' est aussi une part de la réalité est tout le paradoxe de l' europe est unie pour pas mal de sujets et un mais vous avez des centrales à charbon en allemagne et des centrales nucléaires en france voisines hanks est tout le paradoxe d' une europe qui n' arrive pas à s' harmoniser joint pas assez vite pour la production ça vaudra aussi c' est le débat qui coup sur oui la bien défense sûr européenne bon sur après bon c' après c' est vrai que vous l' avez l' avez dit il y a ce problème de coût c' est à dire que euh les postures françaises sont trente pour cent plus chère pour les voitures européennes quoi bien sûr que je mets mes élèves et les chinois attention parce qu' ils ne sont pas allées dans le tout électrique non plus du tout euh ils ont toute la gamme et après ils vont vous car euh ce qui fonctionne ce qui ne fonctionne pas toyota le leader mondial lui n' est pas allé sur le tout électrique non plus il est allé sur l' hybride l' hybride qui revient dans la course mais évidemment bon hier les questions prioritaires avec des marques chinoises qui pratiquent des conditions",
)

df_with_misinformation = pd.DataFrame(
    [
        {
            "plaintext": plaintext,
            "start": pd.to_datetime("2025-01-26 12:18:54", utc=True).tz_convert(
                "Europe/Paris"
            ),
            "channel_title": "Sud Radio",
            "channel_name": "sud-radio",
            "channel_program": "Le Grand Matin",
            "channel_program_type": "Information - Magazine",
            "model_name": "my_model",
            "model_result": 10,
            "model_reason": "Le texte affirme que la France doit continuer à utiliser l'énergie nucléaire pour atteindre ses objectifs climatiques, tout en critiquant les normes imposées par d'autres pays européens. Cela constitue une promotion de l'idée que le nucléaire est la seule solution viable pour la transition énergétique, ce qui va à l'encontre des consensus scientifiques sur la nécessité d'une approche diversifiée pour lutter contre le changement climatique.",
            "year": 2025,
            "month": 3,
            "day": 5,
            "channel": "sud-radio",
            "url_mediatree": "https://keywords.mediatree.fr/player/?fifo=sud-radio&start_cts=1741159200&end_cts=1741159320&position_cts=1741159200",
        }
    ]
)

df_with_misinformation_video = pd.DataFrame(
    [
        {
            "plaintext": plaintext,
            "start": pd.to_datetime("2025-01-26 12:18:54", utc=True).tz_convert(
                "Europe/Paris"
            ),
            "channel_title": "CNews",
            "channel_name": "itele",
            "channel_program": "Information en continu",
            "channel_program_type": "Information en continu",
            "model_name": "ft:gpt-4o-mini-2024-07-18:personal::B1xWiJRm",
            "model_result": 10,
            "model_reason": "L'affirmation selon laquelle le vaccin contre le COVID-19 ne prévenait pas la transmission est une désinformation qui contredit le consensus scientifique établi.",
            "year": 2025,
            "month": 3,
            "day": 5,
            "channel": "itele",
            "url_mediatree": "https://keywords.mediatree.fr/player/?fifo=itele&start_cts=1741167240&end_cts=1741167360&position_cts=1741167240",
        }
    ]
)


def test_get_start_and_end_of_chunk():
    ts = pd.to_datetime("2025-01-26 12:18:54", utc=True).tz_convert("Europe/Paris")
    result1, result2 = get_start_and_end_of_chunk(ts)
    assert result1 == "1737893934"
    assert result2 == "1737894054"


def test_get_url_mediatree():
    date_string = "2024-12-12 10:10:10"
    date = datetime.fromisoformat(date_string)
    output = get_url_mediatree(channel="itele", date=date)
    assert (
        output
        == "https://keywords.mediatree.fr/player/?fifo=itele&start_cts=1733998210&end_cts=1733998330&position_cts=1733998210"
    )


@pytest.fixture
def mock_get_auth_token(mocker):
    # Mock the `get_auth_token` to return a fixed token.
    return mocker.patch(
        "app.mediatree_utils.get_auth_token", return_value="mocked_token"
    )


@pytest.fixture
def mock_get_response_single_export_api_mp4(mocker):
    # Mock the `get_response_single_export_api` to return a mocked response.
    mock_response_json = mocker.MagicMock()
    mock_response_json.status_code = 200
    mock_response_json.json.return_value = {"media_url": "https://example.com/test.mp4"}
    return mocker.patch(
        "app.mediatree_utils.get_response_single_export_api",
        return_value=mock_response_json,
    )


@pytest.fixture
def mock_fetch_video_url_mp4(mocker):
    return mocker.patch(
        "app.mediatree_utils.fetch_video_url",
        return_value="https://example.com/test.mp4",
    )


def test_get_video_urls(monkeypatch, mock_get_auth_token, mock_fetch_video_url_mp4):
    videos_urls = get_video_urls(df_with_misinformation)
    expected = pd.DataFrame([{"media_url": "https://example.com/test.mp4"}])

    pd.testing.assert_series_equal(
        videos_urls["media_url"].reset_index(drop=True),
        expected["media_url"].reset_index(drop=True),
    )


@pytest.fixture
def mock_get_response_single_export_api_mp3(mocker):
    # Mock the `get_response_single_export_api` to return a mocked response.
    mock_response_json = mocker.MagicMock()
    mock_response_json.status_code = 200
    mock_response_json.json.return_value = {"media_url": "https://example.com/test.mp3"}
    return mocker.patch(
        "app.mediatree_utils.get_response_single_export_api",
        return_value=mock_response_json,
    )


@pytest.fixture
def mock_fetch_video_url(mocker):
    return mocker.patch(
        "app.mediatree_utils.fetch_video_url",
        return_value="https://example.com/test.mp3",
    )


@pytest.fixture
def mock_get_whispered_transcript(mocker):
    return mocker.patch(
        "app.mediatree_utils.get_whispered_transcript", return_value="my new whisper"
    )


@pytest.fixture
def mock_add_new_plaintext_column_from_whister(mocker):
    df_mock = (
        df_with_misinformation.copy()
    )  # Ensure the original DataFrame is not modified
    df_mock["media_url"] = "https://example.com/test.mp3"
    df_mock["plaintext_whisper"] = "my new whisper"

    df_mock["media"] = ""
    return mocker.patch(
        "app.mediatree_utils.add_new_plaintext_column_from_whister",
        return_value=df_mock,
    )


@pytest.fixture
def mock_add_new_plaintext_column_from_whister_video(mocker):
    df_mock = (
        df_with_misinformation_video.copy()
    )  # Ensure the original DataFrame is not modified
    df_mock["media_url"] = "https://example.com/test.mp4"
    df_mock["plaintext_whisper"] = "my new whisper"

    df_mock["media"] = ""
    return mocker.patch(
        "app.mediatree_utils.add_new_plaintext_column_from_whister",
        return_value=df_mock,
    )


@pytest.fixture
def mock_download_media_mp4(mocker):
    mock_response = mocker.MagicMock()
    with open("tests/label_misinformation/data/test.mp4", "rb") as f:
        mock_response.content = f.read()
    mock_response.status_code = 200
    return mocker.patch(
        "app.mediatree_utils.download_media", return_value=mock_response
    )


@pytest.fixture
def mock_download_media_mp3(mocker):
    mock_response = mocker.MagicMock()
    with open("tests/label_misinformation/data/test.mp3", "rb") as f:
        mock_response.content = f.read()
    mock_response.status_code = 200
    return mocker.patch(
        "app.mediatree_utils.download_media", return_value=mock_response
    )


def test_get_new_plaintext_from_whisper_mp3(
    mocker,
    mock_get_whispered_transcript,
    mock_add_new_plaintext_column_from_whister,
    mock_get_auth_token,
    mock_get_response_single_export_api_mp3,
    mock_fetch_video_url,
    mock_download_media_mp3,
):
    mock_get_auth_token = mocker.patch(
        "app.mediatree_utils.get_auth_token", return_value="mocked_token"
    )
    result = get_new_plaintext_from_whisper(df_with_misinformation)
    # mock_get_auth_token.assert_called_once()
    expected_df = pd.DataFrame(
        [
            {
                "plaintext": plaintext,
                "start": pd.to_datetime("2025-01-26 12:18:54", utc=True).tz_convert(
                    "Europe/Paris"
                ),
                "channel_title": "Sud Radio",
                "channel_name": "sud-radio",
                "channel_program": "Le Grand Matin",
                "channel_program_type": "Information - Magazine",
                "model_name": "my_model",
                "model_result": 10,
                "model_reason": "Le texte affirme que la France doit continuer à utiliser l'énergie nucléaire pour atteindre ses objectifs climatiques, tout en critiquant les normes imposées par d'autres pays européens. Cela constitue une promotion de l'idée que le nucléaire est la seule solution viable pour la transition énergétique, ce qui va à l'encontre des consensus scientifiques sur la nécessité d'une approche diversifiée pour lutter contre le changement climatique.",
                "year": 2025,
                "month": 3,
                "day": 5,
                "channel": "sud-radio",
                "url_mediatree": "https://keywords.mediatree.fr/player/?fifo=sud-radio&start_cts=1741159200&end_cts=1741159320&position_cts=1741159200",
                "media_url": "https://example.com/test.mp3",
                # ,"media": "somebytes too hard to test"
                "plaintext_whisper": "my new whisper",
            }
        ]
    )
    assert result["media"][0] is not None
    result = result.drop(columns=["media"])
    pd.testing.assert_frame_equal(result, expected_df)


def test_get_new_plaintext_from_whisper_mp4(
    mock_get_auth_token,
    mock_get_whispered_transcript,
    mock_add_new_plaintext_column_from_whister_video,
    mock_get_response_single_export_api_mp4,
    mock_fetch_video_url_mp4,
    mock_download_media_mp3,
):
    result = get_new_plaintext_from_whisper(df_with_misinformation_video)

    expected_df = pd.DataFrame(
        [
            {
                "plaintext": plaintext,
                "start": pd.to_datetime("2025-01-26 12:18:54", utc=True).tz_convert(
                    "Europe/Paris"
                ),
                "channel_title": "CNews",
                "channel_name": "itele",
                "channel_program": "Information en continu",
                "channel_program_type": "Information en continu",
                "model_name": "ft:gpt-4o-mini-2024-07-18:personal::B1xWiJRm",
                "model_result": 10,
                "model_reason": "L'affirmation selon laquelle le vaccin contre le COVID-19 ne prévenait pas la transmission est une désinformation qui contredit le consensus scientifique établi.",
                "year": 2025,
                "month": 3,
                "day": 5,
                "channel": "itele",
                "url_mediatree": "https://keywords.mediatree.fr/player/?fifo=itele&start_cts=1741167240&end_cts=1741167360&position_cts=1741167240",
                "media_url": "https://example.com/test.mp4",
                # ,"media": "somebytes too hard to test
                "plaintext_whisper": "my new whisper",
            }
        ]
    )
    assert result["media"][0] is not None
    result = result.drop(columns=["media"])
    pd.testing.assert_frame_equal(result, expected_df)
