"""Stage 2: classify content type, completeness, and sector + brand + product (for ads only)"""

import os
from typing import Any

from dotenv import load_dotenv
from jinja2 import Template
from openai import OpenAI
from sqlalchemy import bindparam, select
from sqlalchemy.orm import sessionmaker

from postgres.database_connection import connect_to_db
from postgres.schemas.advertising.models import Ad
from quotaclimat.data_ingestion.advertising.s03_classification.ad_classification_schema import \
    build_ad_classification_model
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
    SECTOR_LIST, dict_sectors)
from quotaclimat.utils.logger import getLogger

CONFIDENCE_MAP = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}


def select_pending(engine, limit: int | None) -> list[tuple[str, str | None]]:
    """Return (ad_id, transcript) for ads not yet classified."""
    Session = sessionmaker(bind=engine)
    with Session() as session:
        stmt = (
            select(Ad.id, Ad.transcript)
            .where(Ad.prediction_status.is_(None))
            .where(Ad.fragment_type != "no_data")
            .order_by(Ad.first_detection_date.desc())
        )
        if limit:
            stmt = stmt.limit(limit)
        return [(r[0], r[1]) for r in session.execute(stmt).all()]


def _build_messages(
    system_prompt: str, user_prompt_tpl: str, transcript: str | None, video_url: str
) -> list[dict]:
    user_text = Template(user_prompt_tpl).render(transcript=transcript)
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


def _confidence(parsed: dict[str, Any]) -> float | None:
    a = CONFIDENCE_MAP.get(parsed.get("ad_type_confidence"))
    s = CONFIDENCE_MAP.get(parsed.get("sector_confidence"))
    if a is None:
        return None
    if s is None:
        return float(a)
    return (a + s) / 2


def make_process_one(
    s3_client,
    s3_config,
    llm_client,
    model_name,
    system_prompt,
    user_prompt_tpl,
    schema,
    transcripts,
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
            system_prompt, user_prompt_tpl, transcripts.get(ad_id), url
        )
        try:
            parsed = chat_completion_to_dict(
                llm_client,
                model=model_name,
                messages=messages,
                schema=schema,
                timeout=llm_timeout,
                retries=retries,
                schema_name="ad_classification",
            )
        except ValueError as e:
            return StageResult(
                ad_id=ad_id,
                status="ad_type_parse_error",
                payload={"parsed": None},
                error=f"parse: {e}",
            )
        except Exception as e:
            return StageResult(ad_id=ad_id, status="error", error=f"llm: {e}")

        return StageResult(
            ad_id=ad_id,
            status="ad_type_done",
            payload={
                "parsed": parsed,
                "sector_code": parsed.get("sector_code"),
                "confidence": _confidence(parsed),
            },
        )

    return process_one


_AD = Ad.__table__
_OK_UPDATE = (
    _AD.update()
    .where(_AD.c.id == bindparam("b_id"))
    .values(
        fragment_type=bindparam("b_fragment_type"),
        prediction=bindparam("b_prediction"),
        prediction_status=bindparam("b_status"),
        predicted_sector=bindparam("b_sector"),
        prediction_confidence=bindparam("b_conf"),
    )
)
_NO_DATA_UPDATE = (
    _AD.update().where(_AD.c.id == bindparam("b_id")).values(fragment_type="no_data")
)


def make_flush(session_factory, model_name: str):
    def flush(results: list[StageResult]) -> None:
        ok_rows: list[dict] = []
        no_data_rows: list[dict] = []
        for r in results:
            if r.status == "no_data":
                no_data_rows.append({"b_id": r.ad_id})
                continue
            if r.status == "error":
                continue  # leave NULL, will be retried on next run
            entry = {
                "stage": "ad_type",
                "model": model_name,
                "raw_response": r.payload.get("parsed") if r.payload else None,
                "error": r.error,
            }
            ok_rows.append(
                {
                    "b_id": r.ad_id,
                    "b_fragment_type": ((r.payload or {}).get("parsed") or {}).get(
                        "content_type", "advertising"
                    ),
                    "b_prediction": [entry],
                    "b_status": r.status,  # ad_type_done | ad_type_parse_error
                    "b_sector": (r.payload or {}).get("sector_code"),
                    "b_conf": (r.payload or {}).get("confidence"),
                }
            )
        if not ok_rows and not no_data_rows:
            return
        with session_factory() as session:
            if ok_rows:
                session.execute(_OK_UPDATE, ok_rows)
            if no_data_rows:
                session.execute(_NO_DATA_UPDATE, no_data_rows)
            session.commit()

    return flush


def run(
    workers: int = 15,
    ttl: int = 600,
    batch_size: int = 50,
    llm_timeout: int = 300,
    retries: int = 1,
    limit: int | None = None,
    prompt_path: str = "src/prompts/ad_type.yaml",
) -> dict[str, int]:
    load_dotenv()
    model_settings = VLMSettings()  # type: ignore[call-arg]
    s3_config = AdvertisingS3Config.from_env()
    s3_client = get_s3_client(s3_config)
    llm_client = OpenAI(
        base_url=model_settings.api_url, api_key=model_settings.api_token, max_retries=0
    )

    prompt = load_prompt(prompt_path)
    system_prompt = prompt.system_prompt.replace("{{ sector_list }}", SECTOR_LIST)
    schema = build_ad_classification_model(dict_sectors.keys()).model_json_schema()

    engine = connect_to_db(use_custom_json_serializer=True)
    session_factory = sessionmaker(bind=engine)

    pending = select_pending(engine, limit)
    ad_ids = [aid for aid, _ in pending]
    transcripts = {aid: t for aid, t in pending}

    try:
        return run_stage(
            ad_ids=ad_ids,
            process_one=make_process_one(
                s3_client,
                s3_config,
                llm_client,
                model_settings.model_name,
                system_prompt,
                prompt.user_prompt,
                schema,
                transcripts,
                ttl,
                llm_timeout,
                retries,
            ),
            flush=make_flush(session_factory, model_settings.model_name),
            workers=workers,
            batch_size=batch_size,
            desc="classify_ad",
        )
    finally:
        engine.dispose()


if __name__ == "__main__":
    getLogger()
    run(
        workers=int(os.environ.get("WORKERS", 15)),
        ttl=int(os.environ.get("TTL", 1200)),
        batch_size=int(os.environ.get("BATCH_SIZE", 50)),
        llm_timeout=int(os.environ.get("LLM_TIMEOUT", 300)),
        retries=int(os.environ.get("RETRIES", 1)),
        limit=int(os.environ.get("LIMIT", 0)) or None,
        prompt_path=os.environ.get(
            "PROMPT_PATH",
            "quotaclimat/data_ingestion/advertising/s03_classification/prompts/ad_type.yaml",
        ),
    )
