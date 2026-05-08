"""Stage 1: transcribe ad audio"""

import os
import time

from dotenv import load_dotenv
from openai import OpenAI
from qwen_asr import parse_asr_output
from sqlalchemy import bindparam, select
from sqlalchemy.orm import sessionmaker

from postgres.database_connection import connect_to_db
from quotaclimat.data_ingestion.advertising.s03_classification.presigned_url import (
    AdvertisingS3Config,
    get_presigned_url_for_ad,
    get_s3_client,
)
from postgres.schemas.advertising.models import Ad
from quotaclimat.data_ingestion.advertising.s03_classification.pipeline.runner import StageResult, run_stage
from quotaclimat.data_ingestion.advertising.s03_classification.settings import ASRSettings
from quotaclimat.utils.logger import getLogger


def select_pending(engine, limit: int | None) -> list[str]:
    Session = sessionmaker(bind=engine)
    with Session() as session:
        stmt = (
            select(Ad.id)
            .where(Ad.transcript.is_(None))
            .where(Ad.fragment_type != "no_data")
            .order_by(Ad.first_detection_date.desc())
        )
        if limit:
            stmt = stmt.limit(limit)
        return [r[0] for r in session.execute(stmt).all()]


def _transcribe_with_retry(
    asr_client: OpenAI, model: str, url: str, timeout: int, retries: int
) -> str:
    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = asr_client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "user",
                        "content": [{"type": "audio_url", "audio_url": {"url": url}}],
                    }
                ],
                timeout=timeout,
            )
            _, text = parse_asr_output(response.choices[0].message.content)
            return text or ""
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(2**attempt)
    assert last_err is not None
    raise last_err


_AD = Ad.__table__
_TRANSCRIPT_UPDATE = (
    _AD.update().where(_AD.c.id == bindparam("b_id")).values(transcript=bindparam("b_transcript"))
)
_NO_DATA_UPDATE = (
    _AD.update().where(_AD.c.id == bindparam("b_id")).values(fragment_type="no_data")
)


def make_flush(session_factory):
    def flush(results: list[StageResult]) -> None:
        ok = [r for r in results if r.status == "ok"]
        no_data = [r for r in results if r.status == "no_data"]
        if not ok and not no_data:
            return
        with session_factory() as session:
            if ok:
                session.execute(
                    _TRANSCRIPT_UPDATE,
                    [{"b_id": r.ad_id, "b_transcript": r.payload["transcript"]} for r in ok],
                )
            if no_data:
                session.execute(_NO_DATA_UPDATE, [{"b_id": r.ad_id} for r in no_data])
            session.commit()

    return flush


def make_process_one(s3_client, s3_config, asr_client, model_name, ttl, asr_timeout, retries):
    def process_one(ad_id: str) -> StageResult:
        try:
            url = get_presigned_url_for_ad(
                ad_id=ad_id,
                extension="mp3",
                ttl=ttl,
                config=s3_config,
                s3_client=s3_client,
            )
        except FileNotFoundError:
            return StageResult(ad_id=ad_id, status="no_data")
        except Exception as e:
            return StageResult(ad_id=ad_id, status="error", error=f"presign: {e}")

        try:
            text = _transcribe_with_retry(
                asr_client, model_name, url, asr_timeout, retries
            )
        except Exception as e:
            return StageResult(ad_id=ad_id, status="error", error=f"asr: {e}")

        return StageResult(ad_id=ad_id, status="ok", payload={"transcript": text})

    return process_one


def run(
    workers: int = 8,
    ttl: int = 300,
    batch_size: int = 100,
    asr_timeout: int = 300,
    retries: int = 0,
    limit: int | None = None,
) -> dict[str, int]:
    load_dotenv()
    model_settings = ASRSettings() # type: ignore[call-arg]
    s3_config = AdvertisingS3Config.from_env()
    s3_client = get_s3_client(s3_config)
    asr_client = OpenAI(
        base_url=model_settings.api_url, api_key=model_settings.api_token, max_retries=0
    )

    engine = connect_to_db(use_custom_json_serializer=True)
    session_factory = sessionmaker(bind=engine)

    ad_ids = select_pending(engine, limit)
    try:
        return run_stage(
            ad_ids=ad_ids,
            process_one=make_process_one(
                s3_client, s3_config, asr_client, model_settings.model_name,
                ttl, asr_timeout, retries,
            ),
            flush=make_flush(session_factory),
            workers=workers,
            batch_size=batch_size,
            desc="transcribe",
        )
    finally:
        engine.dispose()


if __name__ == "__main__":
    getLogger()
    run(
        workers=int(os.environ.get("WORKERS", 30)),
        ttl=int(os.environ.get("TTL", 300)),
        batch_size=int(os.environ.get("BATCH_SIZE", 100)),
        asr_timeout=int(os.environ.get("ASR_TIMEOUT", 300)),
        retries=int(os.environ.get("RETRIES", 0)),
        limit=int(os.environ.get("LIMIT", 0)) or None,
    )
