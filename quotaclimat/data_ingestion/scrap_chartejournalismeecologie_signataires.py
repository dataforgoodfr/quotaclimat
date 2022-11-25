import os
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

# TODO: add log in and catch expections
# TODO: enforce field type with pandera

TARGET_PATH = (
    "data_public/signataire_charte/generated_on=%s"
    % datetime.today().strftime("%Y%m%d")
)


def capitalize(name):
    """Capitalize a first name or a last name. Even the hyphenated first names.

    Examples:
     - elsa -> Elsa
     - jean-pierre or Jean-pierre -> Jean-Pierre
    """
    if name:
        if name.find("-") != -1:
            list_names = name.split("-")
            list_capitalized_names = [name.capitalize() for name in list_names]
            return "-".join(list_capitalized_names)
        else:
            list_names = name.split()
            list_capitalized_names = [name.capitalize() for name in list_names]
            return " ".join(list_capitalized_names)


def scrap_chartejournalismeecologie_dot_fr():

    URL = "https://chartejournalismeecologie.fr/les-signataires/"
    page = requests.get(URL)

    soup = BeautifulSoup(page.content, "html.parser")
    html_table = soup.table
    df_signing_journalists = pd.read_html(str(html_table), header=0)[0]

    df_signing_journalists.dropna(axis=0, how="all", inplace=True)
    df_signing_journalists.reset_index(drop=True, inplace=True)
    # Capitalize first name and last name
    df_signing_journalists["Nom"] = (
        df_signing_journalists["Nom"].astype(str).apply(capitalize)
    )
    df_signing_journalists["Prénom"] = (
        df_signing_journalists["Prénom"].astype(str).apply(capitalize)
    )
    return df_signing_journalists


def write_signataire_charte(df):

    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)

    df.to_csv(TARGET_PATH + "/signataire_chartejournalismeecologie.csv", index=False)

def run():
    df = scrap_chartejournalismeecologie_dot_fr()
    write_signataire_charte(df)


if __name__ == "__main__":
    run()
