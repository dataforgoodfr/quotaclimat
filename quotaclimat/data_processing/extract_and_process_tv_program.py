# %%
# from io import BytesIO
import datetime
# import plotly.express as px
from datetime import date

import pandas as pd
# from PIL import Image
import requests
import xmltodict

pd.options.plotting.backend = "plotly"


def extract_tv_program(
    headers={"User-Agent": "Mozilla"}, URL="https://xmltv.ch/xmltv/xmltv-tnt.xml"
):
    """Extract the next 2 weeks of the TV program for the TNT channels
    NB: Arte does not have 2 weeks of TV programmation in this website, only 6 days


    Parameters
    ----------
    headers : dict, optional
        Where to find the header, by default {'User-Agent': 'Mozilla'}
    URL : str, optional
        HTTP adress of the website, by default "https://xmltv.ch/xmltv/xmltv-tnt.xml"

    Returns
    -------
    dict
        Table with the TV programs for the next 2 weeks
    """
    response = requests.get(URL, headers=headers)
    with open("../../data/xmltv-tnt.xml", "wb") as outfile:
        _ = outfile.write(response.content)
    with open("../../data/xmltv-tnt.xml", "r", encoding="utf-8") as f:
        data = f.read()
    data = xmltodict.parse(data)
    return data


def create_channels_df(data):
    df_channels = pd.DataFrame(data["tv"]["channel"])
    df_channels = pd.json_normalize(df_channels.to_dict(orient="records"))
    df_channels.rename(
        columns={
            "@id": "channel_id",
            "icon.@src": "channel_icon",
            "display-name": "channel_name",
        },
        inplace=True,
    )
    return df_channels


def create_programs_df(data):
    df_programs = pd.DataFrame(data["tv"]["programme"])
    df_programs = pd.json_normalize(df_programs.to_dict(orient="records"), sep="_")
    # Clean column names
    df_programs.columns = [
        col.replace("@", "").replace("#", "").replace("-", "")
        for col in df_programs.columns
    ]
    return df_programs


def add_channels_info_to_programs(df_channels, df_programs):
    df_programs = df_programs.join(df_channels.set_index("channel_id"), on="channel")
    return df_programs


def process_programs(df_programs):
    # Drop empty columns
    df_programs.dropna(axis=1, how="all", inplace=True)
    # Convert some columns to datetime
    df_programs["start"] = pd.to_datetime(
        df_programs["start"], infer_datetime_format=True
    )
    df_programs["stop"] = pd.to_datetime(
        df_programs["stop"], infer_datetime_format=True
    )
    df_programs["day"] = df_programs["start"].dt.floor("d")
    df_programs["length_minutes"] = (df_programs["stop"] - df_programs["start"]).apply(
        lambda x: int(x.total_seconds() / 60)
    )
    df_programs["macro_category"] = df_programs.category_text.map(
        {
            "s??rie dramatique": "divertissement",
            "autre": "autre",
            "magazine jeunesse": "jeunesse",
            "m??t??o": "m??teo",
            "magazine de t??l??-achat": "divertissement",
            "t??l??r??alit??": "divertissement",
            "feuilleton sentimental": "divertissement",
            "magazine de la gastronomie": "divertissement",
            "divertissement : jeu": "divertissement",
            "journal": "journal",
            "film sentimental": "divertissement",
            "t??l??film sentimental": "divertissement",
            "divertissement-humour": "divertissement",
            "s??rie polici??re": "divertissement",
            "d??bat": "debat",
            "documentaire soci??t??": "documentaire",
            "t??l??film dramatique": "divertissement",
            "magazine religieux": "magazine",
            "documentaire d??couvertes": "documentaire",
            "feuilleton policier": "divertissement",
            "magazine de services": "magazine",
            "magazine de soci??t??": "magazine",
            "magazine du consommateur": "magazine",
            "magazine d'information": "magazine",
            "magazine litt??raire": "magazine",
            "magazine de l'art de vivre": "magazine",
            "magazine de la d??coration": "magazine",
            "magazine musical": "magazine",
            "sport : rugby": "sport",
            "magazine de reportages": "magazine",
            "magazine de la mer": "magazine",
            "magazine de d??couvertes": "magazine",
            "feuilleton r??aliste": "divertissement",
            "jeunesse : dessin anim?? dessin anim??": "divertissement",
            "s??rie d'animation": "divertissement",
            "jeunesse : dessin anim?? jeunesse": "divertissement",
            "magazine r??gional": "magazine",
            "magazine culinaire": "magazine",
            "sport : multisports": "sport",
            "magazine sportif": "sport",
            "t??l??film policier": "divertissement",
            "s??rie historique": "divertissement",
            "documentaire culture": "documentaire",
            "film : court m??trage": "divertissement",
            "sport : golf": "sport",
            "magazine du cin??ma": "magazine",
            "divertissement": "divertissement",
            "film : thriller": "divertissement",
            "film : drame historique": "divertissement",
            "magazine d'actualit??": "divertissement",
            "film fantastique": "divertissement",
            "talk-show": "magazine",  # TODO check
            "film : drame": "divertissement",
            "documentaire animalier": "documentaire",
            "documentaire musique": "documentaire",
            "documentaire histoire": "documentaire",
            "documentaire sant??": "documentaire",
            "magazine du tourisme": "magazine",
            "documentaire politique": "documentaire",
            "film d'aventures": "divertissement",
            "documentaire nature": "documentaire",
            "documentaire environnement": "documentaire",
            "magazine de g??opolitique": "magazine",
            "documentaire gastronomie": "documentaire",
            "programme ind??termin??": "autre",
            "clips": "divertissement",
            "s??rie humoristique": "divertissement",
            "magazine culturel": "magazine",
            "musique : techno": "divertissement",
            "sport : fitness": "sport",
            "interview": "magazine",  # TODO check
            "film catastrophe": "divertissement",
            "magazine du show-biz": "magazine",
            "s??rie sentimentale": "divertissement",
            "film d'animation": "divertissement",
            "s??rie fantastique": "divertissement",
            "humour": "divertissement",
            "jeunesse : emission jeunesse": "magazine",  # TODO check
            "s??rie culinaire": "divertissement",
            "s??rie jeunesse": "divertissement",
            "magazine m??dical": "magazine",
            "th????tre : pi??ce de th????tre": "divertissement",
            "d??bat parlementaire": "magazine",  # TODO check
            "magazine politique": "magazine",
            "emission politique": "magazine",
            "magazine de l'??conomie": "magazine",
            "documentaire t??l??r??alit??": "documentaire",
            "magazine de l'environnement": "magazine",
            "magazine ??ducatif": "magazine",
            "t??l??film romanesque": "divertissement",
            "s??rie r??aliste": "divertissement",
            "film de science-fiction": "divertissement",
            "film d'action": "divertissement",
            "magazine de l'automobile": "magazine",
            "sport de force": "sport",
            "sport : football": "sport",
            "sport : basket-ball": "sport",
            "documentaire justice": "documentaire",
            "sport : hippisme": "sport",
            "documentaire sciences et technique": "documentaire",
            "magazine judiciaire": "magazine",
            "magazine de la sant??": "magazine",
            "film : com??die sentimentale": "divertissement",
            "musique : pop %26 rock": "divertissement",
            "musical": "divertissement",
            "musique : vari??t??s": "divertissement",
            "film : com??die romantique": "divertissement",
            "film : court m??trage d'animation": "divertissement",
            "sport : formule 1": "sport",
            "documentaire civilisations": "documentaire",
            "magazine historique": "magazine",
            "t??l??film de suspense": "divertissement",
            "documentaire rock-pop": "documentaire",
            "documentaire cin??ma": "documentaire",
            "documentaire education": "documentaire",
            "magazine des m??dias": "magazine",
            "s??rie d'aventures": "divertissement",
            "film : com??die": "divertissement",
            "documentaire p??che": "documentaire",
            "documentaire voyage": "documentaire",
            "documentaire aventures": "documentaire",
            "documentaire beaux-arts": "documentaire",
            "loterie": "divertissement",
            "sport : jt sport": "sport",
            "film d'horreur": "divertissement",
            "s??rie d'action": "divertissement",
            "musique : reggae": "divertissement",
            "magazine de l'emploi": "magazine",
            "musique : contemporain": "divertissement",
            "musique : jazz": "divertissement",
            "op??ra comique": "divertissement",
            "t??l??film fantastique": "divertissement",
            "jeunesse : dessin anim?? manga": "divertissement",
            "s??rie de science-fiction": "divertissement",
            "sport : biathlon": "sport",
            "sport : endurance": "sport",
            "magazine du jardinage": "magazine",
            "sports m??caniques": "sport",
            "emission religieuse": "magazine",  # TODO check
            "documentaire art de vivre": "documentaire",
            "film de guerre": "divertissement",
            "sport : voile": "sport",
            "film : com??die dramatique": "divertissement",
            "documentaire sport": "documentaire",
            "magazine du court m??trage": "magazine",
            "film : court m??trage dramatique": "divertissement",
            "film policier": "divertissement",
            "documentaire musique classique": "documentaire",
            "magazine scientifique": "magazine",
            "emission du bien-??tre": "divertissement",
            "magazine animalier": "magazine",
            "sport : mma": "sport",
            "sport : ski freestyle": "sport",
            "sport : cyclo-cross": "sport",
            "sport : football am??ricain": "sport",
            "documentaire g??opolitique": "sport",
            "t??l??film humoristique": "divertissement",
            "musique : classique": "divertissement",
            "s??rie de t??l??r??alit??": "divertissement",
            "documentaire fiction": "documentaire",
            "t??l??film ??rotique": "divertissement",
        }
    )
    df_programs["%_of_channel"] = (
        100
        * df_programs["length_minutes"]
        / df_programs.groupby("channel_name")["length_minutes"].transform("sum")
    )
    return df_programs


def create_clean_programs(df_programs):
    columns = [
        "start",
        "stop",
        "day",
        "length_minutes",
        "%_of_channel",
        "channel_name",
        "title",
        "subtitle",
        "date",
        "desc_text",
        "category_text",
        "macro_category",
    ]
    df = df_programs[columns]
    return df


def create_clean_programs_with_channels(data):
    df_channels = create_channels_df(data)
    df_programs = create_programs_df(data)
    df_programs_joined = add_channels_info_to_programs(
        df_channels=df_channels, df_programs=df_programs
    )
    df_processed = process_programs(df_programs_joined)
    df_clean = create_clean_programs(df_processed)
    return df_clean


def get_tv_programs_next_days(number_of_days=4, save=True):
    data = extract_tv_program()
    df = create_clean_programs_with_channels(data)

    # Filter with only the next "number_of_days" days
    today = date.today()
    end_date = today + datetime.timedelta(days=number_of_days + 1)
    end_date = end_date.strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")
    df = df[(df.start >= today_str) & (df.stop <= end_date)]

    if save:
        date_beginning_str = today_str.replace("-", "")
        date_end_str = (
            (today + datetime.timedelta(days=number_of_days))
            .strftime("%Y-%m-%d")
            .replace("-", "")
        )
        df.to_csv(
            f"../../data/{date_beginning_str}_{date_end_str}_Programme_TV.csv",
            index=False,
        )
    return df


if __name__ == "__main__":
    df = get_tv_programs_next_days(number_of_days=5, save=True)

    # %%
    mix_channel = (
        df.groupby(["channel_name", "macro_category"])["%_of_channel"]
        .sum()
        .unstack("macro_category")
        .sort_values("journal", ascending=False)
        .fillna(0)
        .round(1)
    )
    # mix_channel.to_excel("20221110_20221114_mix_type_programs_TV_channel.xlsx")
