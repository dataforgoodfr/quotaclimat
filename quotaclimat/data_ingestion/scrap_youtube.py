import datetime
import logging
import os

import isodate
import pandas as pd
from apiclient.discovery import build
from config_youtube import CHANNEL_CONFIG

API_SERVICE_NAME = "youtube"
API_VERSION = "v3"
KEY = "AIzaSyDATQZwjAZ__AARZvLZRmAWpkcG0BQgKdM"  # Google account created: email:scrapping.job.api.key@gmail.com pwd:scrappingjob12345


def create_youtube_object():

    """Fonction qui nous permet de faire appel à l'API"""

    youtube_object = build(API_SERVICE_NAME, API_VERSION, developerKey=KEY)
    return youtube_object


def retrieve_channel_infos(medias: list) -> dict:

    """
    Cette fonction est à exécuter pour récupérer l'ID des chaînes que l'on veut query.
    Elle n'est à exécuter qu'une seule fois, et il faut copier les nouvelles chaînes à ajouter
    dans le fichier 'config_youtube.py'
    Attention, le 'channel_id' et le 'uploads_id' ne sont pas tout à fait identiques.
    """

    youtube_object = create_youtube_object()

    info = {}

    for media in medias:
        request = youtube_object.search().list(
            part="snippet", q=media, type="channel", regionCode="FR", maxResults=1
        )

        response = request.execute().get("items", [])[0]

        channelId = response["id"]["channelId"]

        request_bis = youtube_object.channels().list(
            part="snippet,contentDetails", id=channelId
        )

        response_bis = request_bis.execute().get("items", [])[0]

        channel_info = {}

        channel_info["channel_id"] = channelId
        channel_info["uploads_id"] = response_bis["contentDetails"]["relatedPlaylists"][
            "uploads"
        ]

        info[media] = channel_info

    return info


def scrap_page_one(uploadsId: str) -> dict:

    """Fonction qui permet de scraper la première page de la playlist 'uploads' d'une chaîne"""

    youtube_object = create_youtube_object()

    request = youtube_object.playlistItems().list(
        part="contentDetails", playlistId=uploadsId, maxResults=50
    )

    response = request.execute()
    return response


def scrap_following_page(uploadsId: str, next_page_token: str) -> dict:

    """Fonction qui permet de scraper la page suivante de la playlist 'uploads' d'une chaîne"""

    youtube_object = create_youtube_object()

    request = youtube_object.playlistItems().list(
        part="contentDetails",
        playlistId=uploadsId,
        maxResults=50,
        pageToken=next_page_token,
    )

    response = request.execute()
    return response


def page_formatting(response: dict) -> list:

    """Fonction qui permet de récupérer les ID des vidéos d'une page de la playlist"""

    items = response.get("items", [])
    videos = []
    for i in range(len(items)):
        videoIds = {}
        videoIds["videoId"] = items[i]["contentDetails"]["videoId"]
        videos.append(videoIds)

    return videos


def videos_scraping(formatted_list: list) -> dict:

    """Fonction qui permet de scraper les informations liées aux vidéos d'une page de playlist"""

    youtube_object = create_youtube_object()

    id_list = []
    for i in range(len(formatted_list)):
        id_list.append(formatted_list[i]["videoId"])

    id_str = ",".join(id_list)

    request = youtube_object.videos().list(
        part="snippet,contentDetails,statistics", id=id_str
    )
    response = request.execute()

    return response


def video_formatting(videos_scraped: list) -> dict:

    """Fonction qui permet de formatter les résultats pour toute la playlist"""

    full = {}

    for j in range(len(videos_scraped)):
        items = videos_scraped[j].get("items", [])

        for i in range(len(items)):

            video = {}
            video["videoId"] = items[i]["id"]
            video["title"] = items[i]["snippet"]["title"]
            video["channelId"] = items[i]["snippet"]["channelId"]
            video["channel"] = items[i]["snippet"]["channelTitle"]
            video["description"] = items[i]["snippet"]["description"]
            video["publication_date"] = items[i]["snippet"]["publishedAt"]
            video["duration (s)"] = isodate.parse_duration(
                items[i]["contentDetails"]["duration"]
            ).total_seconds()
            if "viewCount" in items[i]["statistics"]:
                video["view_count"] = items[i]["statistics"]["viewCount"]
            else:
                video["view_count"] = "None"
            if "likeCount" in items[i]["statistics"]:
                video["like_count"] = items[i]["statistics"]["likeCount"]
            else:
                video["like_count"] = "None"
            if "commentCount" in items[i]["statistics"]:
                video["comment_count"] = items[i]["statistics"]["commentCount"]
            else:
                video["comment_count"] = "None"
            if "tags" in items[i]["snippet"]:
                video["tags"] = items[i]["snippet"]["tags"]
            else:
                video["tags"] = []

            full[video["title"] + " - " + video["channel"]] = video

    return full


def scrap_one_channel(uploadsId: str) -> dict:

    """Fonction qui permet de scrap toutes les vidéos d'une chaîne"""

    scraped_videos = []

    response = scrap_page_one(uploadsId)
    videos = page_formatting(response)
    scraped_videos.append(videos_scraping(videos))

    while "nextPageToken" in response:
        next_page_token = response["nextPageToken"]

        response = scrap_following_page(uploadsId, next_page_token)
        videos = page_formatting(response)
        scraped_videos.append(videos_scraping(videos))

    formatted_videos = video_formatting(scraped_videos)

    return formatted_videos


def converting_to_flate_storage(final: dict, media: str):

    """Fonction qui permet d'écrire les résultats du scrapping dans un parquet et le placer dans le github"""

    df = pd.DataFrame.from_dict(final, orient="index").reset_index()
    date = (
        datetime.date.today()
    )  # A voir pour la date d'exécution du script si la fonction today() ne pose pas problème
    landing_path = "data_public/youtube_extracts/channel=%s/year=%s/month=%s/" % (
        media,
        date.year,
        date.month,
    )
    if not os.path.exists(landing_path):
        os.makedirs(landing_path)
    df.to_parquet(landing_path + "%s.parquet" % date.strftime("%Y%m%d"))

def full_scraping():

    """Fonction qui permet de faire le scraping de toutes les chaînes présentes dans 'config_youtube.py'"""

    for media in CHANNEL_CONFIG:

        uploads_id = CHANNEL_CONFIG[media]["uploads_id"]

        date = (
            datetime.date.today()
        )  # Même chose que pour la fonction 'converting_to_flate_storage'

        if os.path.exists(  # Boucle 'if' qui permet d'éviter les requêtes à l'API pour les chaînes déjà scrapées au cas où le scraping ait été arrêté
            "data_public/youtube_extracts/channel=%s/year=%s/month=%s/%s.parquet"
            % (media, date.year, date.month, date.strftime("%Y%m%d"))
        ):
            continue
        else:
            try:
                final = scrap_one_channel(uploads_id)
                converting_to_flate_storage(final, media)
            except Exception as err:
                logging.error("Could not write data for %s: %s" % (media, err))
                continue


if __name__ == "__main__":
    full_scraping()
