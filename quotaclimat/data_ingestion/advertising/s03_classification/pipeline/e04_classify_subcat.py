"""Stage 4: sub-category LLM pass for ads where the dictionary couldn't decide"""

import os
from typing import Any

from dotenv import load_dotenv
from jinja2 import Template
from openai import OpenAI
from sqlalchemy import bindparam, select
from sqlalchemy.orm import sessionmaker

from postgres.database_connection import connect_to_db
from postgres.schemas.advertising.models import Ad
from quotaclimat.data_ingestion.advertising.s03_classification.pipeline.llm_chat import \
    chat_completion_to_dict
from quotaclimat.data_ingestion.advertising.s03_classification.pipeline.runner import (
    StageResult, run_stage)
from quotaclimat.data_ingestion.advertising.s03_classification.presigned_url import (
    AdvertisingS3Config, get_presigned_url_for_ad, get_s3_client)
from quotaclimat.data_ingestion.advertising.s03_classification.prompts import \
    load_prompt
from quotaclimat.data_ingestion.advertising.s03_classification.settings import \
    VLMSettings
from quotaclimat.data_ingestion.advertising.s03_classification.taxonomy import (
    dict_sectors, get_subcategory_list)
from quotaclimat.utils.logger import getLogger

METHOD_BY_INPUT_STATUS = {
    "dict_tier2_no_kw": "dict_tier2+llm_subcat",
    "dict_tier3_no_kw": "llm_subcat",
    "dict_miss": "llm_subcat",
}


def _get_ad_type_entry(prediction: list[dict] | None) -> dict:
    if not prediction:
        return {}
    for entry in prediction:
        if entry.get("stage") == "ad_type":
            return entry.get("raw_response") or {}
    return {}


def select_pending(engine, limit: int | None) -> list[dict]:
    Session = sessionmaker(bind=engine)
    with Session() as session:
        stmt = (
            select(
                Ad.id,
                Ad.prediction,
                Ad.transcript,
                Ad.predicted_sector,
                Ad.prediction_status,
            )
            .where(
                Ad.prediction_status.in_(
                    ["dict_tier2_no_kw", "dict_tier3_no_kw", "dict_miss"]
                )
            )
            .order_by(Ad.first_detection_date.desc())
        )
        if limit:
            stmt = stmt.limit(limit)
        rows: list[dict] = []
        for ad_id, prediction, transcript, sector, input_status in session.execute(
            stmt
        ).all():
            ad_type = _get_ad_type_entry(prediction)
            sector_label = (
                dict_sectors.get(sector, {}).get("sector_label_en") if sector else None
            )
            rows.append(
                {
                    "ad_id": ad_id,
                    "transcript": transcript,
                    "sector_code": sector,
                    "sector_label": sector_label,
                    "ad_subject": ad_type.get("ad_subject"),
                    "input_status": input_status,
                    "prediction": list(prediction) if prediction else [],
                }
            )
        return rows


def _build_messages(
    system_prompt: str, user_prompt_tpl: str, row: dict, video_url: str
) -> list[dict]:
    sub_cat_list = get_subcategory_list(row["sector_code"]) or ""
    user_text = Template(user_prompt_tpl).render(
        sector_code=row["sector_code"],
        sector_label=row["sector_label"],
        ad_subject=row["ad_subject"],
        sub_cat_list=sub_cat_list,
        transcript=row["transcript"],
    )
    return [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": [
                {"type": "video_url", "video_url": {"url": video_url}},
                {"type": "text", "text": user_text},
            ],
        },
    ]


def make_process_one(
    s3_client,
    s3_config,
    llm_client,
    model_name,
    system_prompt,
    user_prompt_tpl,
    rows_by_id: dict[str, dict],
    ttl,
    llm_timeout,
    retries,
):
    def process_one(ad_id: str) -> StageResult:
        try:
            url = get_presigned_url_for_ad(
                ad_id=ad_id,
                extension="mp4",
                ttl=ttl,
                config=s3_config,
                s3_client=s3_client,
            )
        except FileNotFoundError:
            return StageResult(ad_id=ad_id, status="no_data")
        except Exception as e:
            return StageResult(ad_id=ad_id, status="error", error=f"presign: {e}")

        messages = _build_messages(
            system_prompt, user_prompt_tpl, rows_by_id[ad_id], url
        )
        try:
            parsed: dict[str, Any] = chat_completion_to_dict(
                llm_client,
                model=model_name,
                messages=messages,
                schema=None,
                timeout=llm_timeout,
                retries=retries,
            )
        except ValueError as e:
            return StageResult(
                ad_id=ad_id,
                status="subcat_parse_error",
                payload={"parsed": None},
                error=f"parse: {e}",
            )
        except Exception as e:
            return StageResult(ad_id=ad_id, status="error", error=f"llm: {e}")

        return StageResult(
            ad_id=ad_id,
            status="subcat_done",
            payload={"parsed": parsed, "subcat_code": parsed.get("sub_category_code")},
        )

    return process_one


_AD = Ad.__table__
_UPDATE = (
    _AD.update()
    .where(_AD.c.id == bindparam("b_id"))
    .values(
        prediction=bindparam("b_prediction"),
        prediction_status=bindparam("b_status"),
        prediction_method=bindparam("b_method"),
        predicted_product_category=bindparam("b_subcat"),
    )
)


def make_flush(session_factory, model_name: str, rows_by_id: dict[str, dict]):
    def flush(results: list[StageResult]) -> None:
        rows: list[dict] = []
        for r in results:
            if r.status == "error":
                continue
            existing = list(rows_by_id[r.ad_id]["prediction"])
            existing = [e for e in existing if e.get("stage") != "subcat"]
            existing.append(
                {
                    "stage": "subcat",
                    "model": model_name,
                    "raw_response": (r.payload or {}).get("parsed"),
                    "error": r.error,
                }
            )
            if r.status == "subcat_done":
                input_status = rows_by_id[r.ad_id]["input_status"]
                method = METHOD_BY_INPUT_STATUS.get(input_status, "llm_subcat")
            else:
                method = None
            rows.append(
                {
                    "b_id": r.ad_id,
                    "b_prediction": existing,
                    "b_status": r.status,  # subcat_done | subcat_parse_error | no_data
                    "b_method": method,
                    "b_subcat": (r.payload or {}).get("subcat_code"),
                }
            )
        if not rows:
            return
        with session_factory() as session:
            session.execute(_UPDATE, rows)
            session.commit()

    return flush


def run(
    workers: int = 15,
    ttl: int = 600,
    batch_size: int = 50,
    llm_timeout: int = 300,
    retries: int = 1,
    limit: int | None = None,
    prompt_path: str = "src/prompts/subcat.yaml",
) -> dict[str, int]:
    load_dotenv()
    model_settings = VLMSettings()
    s3_config = AdvertisingS3Config.from_env()
    s3_client = get_s3_client(s3_config)
    llm_client = OpenAI(
        base_url=model_settings.api_url, api_key=model_settings.api_token, max_retries=0
    )

    prompt = load_prompt(prompt_path)

    engine = connect_to_db(use_custom_json_serializer=True)
    session_factory = sessionmaker(bind=engine)

    rows = select_pending(engine, limit)
    rows_by_id = {r["ad_id"]: r for r in rows}
    ad_ids = list(rows_by_id.keys())

    try:
        return run_stage(
            ad_ids=ad_ids,
            process_one=make_process_one(
                s3_client,
                s3_config,
                llm_client,
                model_settings.model_name,
                prompt.system_prompt,
                prompt.user_prompt,
                rows_by_id,
                ttl,
                llm_timeout,
                retries,
            ),
            flush=make_flush(session_factory, model_settings.model_name, rows_by_id),
            workers=workers,
            batch_size=batch_size,
            desc="classify_subcat",
        )
    finally:
        engine.dispose()


if __name__ == "__main__":
    getLogger()
    run(
        workers=int(os.environ.get("WORKERS", 15)),
        ttl=int(os.environ.get("TTL", 300)),
        batch_size=int(os.environ.get("BATCH_SIZE", 50)),
        llm_timeout=int(os.environ.get("LLM_TIMEOUT", 300)),
        retries=int(os.environ.get("RETRIES", 1)),
        limit=int(os.environ.get("LIMIT", 0)) or None,
        prompt_path=os.environ.get(
            "PROMPT_PATH",
            "quotaclimat/data_ingestion/advertising/s03_classification/prompts/subcat.yaml",
        ),
    )
