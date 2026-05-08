"""Shared threaded runner used by every pipeline stage. Handles concurrency, progress, batching, keyboard interruption cleanup.

Each stage provides:
  - a list of ad_ids to process,
  - a process_one(ad_id) -> StageResult (does url presign + LLM/work),
  - a flush(results) callback that writes a batch of results to the DB.
"""

from __future__ import annotations

import concurrent.futures as cf
import logging
from collections import Counter
from dataclasses import dataclass
from typing import Any, Callable

import tqdm


@dataclass
class StageResult:
    ad_id: str
    status: str  # ok | no_data | error | parse_error | dict_tier1/2/3 | dict_miss
    payload: dict[str, Any] | None = None
    error: str | None = None


def run_stage(
    ad_ids: list[str],
    process_one: Callable[[str], StageResult],
    flush: Callable[[list[StageResult]], None],
    workers: int,
    batch_size: int,
    desc: str = "stage",
) -> dict[str, int]:
    if not ad_ids:
        logging.info("[%s] nothing to process", desc)
        return {}

    counts: Counter[str] = Counter()
    buf: list[StageResult] = []
    progress = tqdm.tqdm(total=len(ad_ids), desc=desc, smoothing=0.05)
    pool = cf.ThreadPoolExecutor(max_workers=workers)

    try:
        futures = [pool.submit(process_one, ad_id) for ad_id in ad_ids]
        try:
            for fut in cf.as_completed(futures):
                result = fut.result()
                progress.update(1)
                counts[result.status] += 1
                buf.append(result)
                if result.error:
                    logging.warning("[%s][%s] %s", desc, result.ad_id, result.error)
                if len(buf) >= batch_size:
                    flush(buf)
                    buf = []
                progress.set_postfix(dict(counts))
        except KeyboardInterrupt:
            logging.warning("[%s] interrupted, cancelling pending tasks...", desc)
            for f in futures:
                f.cancel()
            raise
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        if buf:
            flush(buf)
        progress.close()

    logging.info("[%s] done. %s", desc, dict(counts))
    return dict(counts)
