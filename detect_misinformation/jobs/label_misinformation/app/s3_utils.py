import logging
import os
import boto3
import modin.pandas as pd
from labelstudio_utils import get_label_studio_format
from secret_utils import get_secret_docker
import shutil
import json
from country import Country, LEGACY_COUNTRIES, FRANCE_COUNTRY, convert_to_base_country_name


# Configuration for Scaleway Object Storage
ACCESS_KEY = get_secret_docker("BUCKET_ACCESS")
SECRET_KEY = get_secret_docker("BUCKET_SECRET")
REGION = "fr-par"
ENDPOINT_URL = f"https://s3.{REGION}.scw.cloud"
boto3.setup_default_session(region_name=REGION)


def get_s3_client():
    s3_client = boto3.client(
        service_name="s3",
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=ENDPOINT_URL,
    )
    return s3_client


def get_bucket_key(date, channel, filename: str = "*", suffix: str = "parquet") -> str:
    (year, month, day) = (date.year, date.month, date.day)
    return (
        f"year={year}/month={month:1}/day={day:1}/channel={channel}/{filename}.{suffix}"
    )


def get_bucket_key_folder(
    date, channel, root_folder=None, country: Country = FRANCE_COUNTRY
) -> str:
    (year, month, day) = (date.year, date.month, date.day)
    key = f"year={year}/month={month:1}/day={day:1}/channel={channel}/"
    if country != LEGACY_COUNTRIES:
        key = f"country={convert_to_base_country_name(country.name)}/" + key
    if root_folder is not None:
        return f"{root_folder}/{key}"
    return key


def check_if_object_exists_in_s3(
    day,
    channel,
    s3_client,
    bucket: str,
    root_folder=None,
    country: Country = FRANCE_COUNTRY,
) -> bool:
    folder_prefix = get_bucket_key_folder(
        day, channel, root_folder=root_folder, country=country
    )

    logging.debug(f"Checking if folder exists: {folder_prefix}")
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket, Prefix=folder_prefix, MaxKeys=1
        )
        if "Contents" in response:
            logging.info(f"Folder exists in S3: {folder_prefix}")
            return True
        else:
            logging.info(f"Folder does not exist in S3: {folder_prefix}")
            return False
    except Exception as e:
        logging.error(f"Error while checking folder in S3: {folder_prefix}\n{e}")
        return False


def upload_file_to_s3(
    local_file, bucket_name, base_s3_path, s3_client, format="json", name="empty"
) -> None:
    try:
        s3_key = f"{base_s3_path}{name}.{format}"
        logging.info(f"Reading {local_file} to upload it to {s3_key}")
        s3_client.upload_file(local_file, bucket_name, s3_key)
    except Exception as err:
        logging.fatal("upload_file_to_s3 (%s) %s" % (type(err).__name__, err))
        raise Exception


def upload_folder_to_s3(local_folder, bucket_name, base_s3_path, s3_client) -> None:
    logging.info(f"Reading local folder {local_folder} and uploading to S3")
    for root, _, files in os.walk(local_folder):
        logging.info(f"Reading files {len(files)}")
        for file in files:
            logging.info(f"Reading {file}")
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_folder)
            s3_key = os.path.join(base_s3_path, relative_path).replace(
                "\\", "/"
            )  # replace backslashes for S3 compatibility

            logging.info(f"Uploading to bucket {bucket_name} key : {s3_key}")
            s3_client.upload_file(local_file_path, bucket_name, s3_key)
            logging.info(f"Uploaded: {s3_key}")
    # Delete the local folder after successful upload
    shutil.rmtree(local_folder)
    logging.info(f"Deleted local folder: {local_folder}")


def save_csv(
    df: pd.DataFrame,
    channel: str,
    date: pd.Timestamp,
    s3_path,
    folder_inside_bucket=None,
):
    based_path = "s3"
    local_csv = "s3/misinformation.tsv"
    if folder_inside_bucket is not None:
        local_csv = f"{based_path}/{folder_inside_bucket}.tsv"

    os.makedirs(os.path.dirname(local_csv), exist_ok=True)
    df.to_csv(local_csv, sep="\t")  # tab separated

    #  local_csv_folder = f"{csv_based_path}/{s3_path}"
    logging.info(f"CSV saved locally {local_csv}")
    return local_csv


# https://labelstud.io/guide/storage#One-Task-One-JSON-File
def reformat_and_save(df, output_folder="output_json_files", shadow=False) -> str:
    # Ensure the output folder exists
    os.makedirs(os.path.dirname(output_folder), exist_ok=True)

    for idx, row in df.iterrows():
        task_data = get_label_studio_format(row)
        if shadow:
            task_data["data"]["item"].update(
                {
                    "shadow_prompt_version": row.get("shadow_prompt_version", ""),
                    "shadow_pipeline_version": row.get("shadow_pipeline_version", ""),
                    "shadow_model_result": int(row.get("shadow_model_result", 0)),
                    "shadow_model_reason": row.get("shadow_model_reason", ""),
                    "shadow_model_name": row.get("shadow_model_name", ""),
                }
            )
        # Define the file path for each row
        file_path = os.path.join(output_folder, f"{row['id']}.json")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        # Write the formatted data to a file
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(task_data, f, ensure_ascii=False, indent=4)

    return output_folder


# one json file per json row
def save_json(
    df: pd.DataFrame,
    channel: str,
    date: pd.Timestamp,
    s3_path,
    folder_inside_bucket=None,
    country: Country = FRANCE_COUNTRY,
    shadow=False,
) -> str:
    based_path = "s3"

    local_json = "s3/json"
    if folder_inside_bucket is not None:
        folder_inside_bucket = get_bucket_key_folder(
            date, channel, root_folder=folder_inside_bucket, country=country
        )
        local_json = f"{based_path}/{folder_inside_bucket}"
    os.makedirs(os.path.dirname(local_json), exist_ok=True)

    local_json = reformat_and_save(df, output_folder=local_json, shadow=shadow)
    logging.info(f"JSON saved locally {local_json}")
    return local_json


def save_parquet(
    df: pd.DataFrame,
    channel: str,
    date: pd.Timestamp,
    s3_path,
    folder_inside_bucket=None,
) -> str:
    based_path = "s3/parquet"
    os.makedirs(os.path.dirname(based_path), exist_ok=True)
    local_parquet = based_path
    if folder_inside_bucket is not None:
        local_parquet = f"{based_path}/{folder_inside_bucket}"

    os.makedirs(os.path.dirname(local_parquet), exist_ok=True)
    df.to_parquet(
        local_parquet,
        compression="gzip",
        partition_cols=["year", "month", "day", "channel"],
    )

    # saving full_path folder parquet to s3
    local_folder: str = f"{based_path}/{s3_path}"
    logging.info(f"Parquet saved locally {local_folder}")
    return local_folder


def save_to_s3(
    df: pd.DataFrame,
    channel: str,
    date: pd.Timestamp,
    s3_client,
    bucket: str,
    folder_inside_bucket=None,
    country: Country = FRANCE_COUNTRY,
    shadow: bool = False,
) -> None:
    logging.info(
        f"Saving DF with {len(df)} elements to S3 for {date}, country {country.name} and channel {channel}"
    )

    # to create partitions
    object_key = get_bucket_key(date, channel)
    logging.debug(f"Uploading partition: {object_key}")

    try:
        s3_path: str = f"{get_bucket_key_folder(date, channel, root_folder=folder_inside_bucket, country=country)}"
        logging.info(f"S3 path: {s3_path}")
        if len(df) > 0:
            # add partition columns year, month, day to dataframe
            df["year"] = date.year
            df["month"] = date.month
            df["day"] = date.day
            df["channel"] = channel  # channel_name from mediatree's api

            # df = df._to_pandas()  # collect data accross ray workers to avoid multiple subfolders

            # local_folder_parquet = save_parquet(df, channel, date, s3_path, folder_inside_bucket)
            # upload_folder_to_s3(local_folder_parquet, bucket, s3_path, s3_client=s3_client)

            # local_csv_file = save_csv(df, channel, date, s3_path, folder_inside_bucket)
            # upload_file_to_s3(local_json_file, bucket, s3_path, s3_client=s3_client, format="json")
            local_json_folder = save_json(
                df, channel, date, s3_path, folder_inside_bucket
            )
            upload_folder_to_s3(local_json_folder, bucket, s3_path, s3_client=s3_client)
        else:  # prove date and channel were done
            logging.info("Save empty file to not reprocess these data")
            empty_file = "s3/.gitkeep"
            upload_file_to_s3(
                empty_file,
                bucket,
                s3_path,
                s3_client=s3_client,
                format="txt",
                name="empty",
            )
    except Exception as err:
        logging.fatal("save_to_s3 (%s) %s" % (type(err).__name__, err))
        raise Exception
