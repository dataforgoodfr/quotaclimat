import asyncio
import os
from datetime import datetime, timedelta

from analyse.advertising.e00_download.mediatree import (
    CachedMediatreeAPI,
    all_intervals_between,
)
from analyse.advertising.e01_rupture_detection.rupture_detector import (
    RuptureDetector,
)
from analyse.advertising.tools.basic_elements.segments import (
    Segment,
    save_segment_groups_to_json,
)


def split_chunks(input_file: str, start_time: float):
    detector = RuptureDetector(
        sensitivity=0.1,
        context_sec=1.0,  # durée analysée de chaque côté d'un point pour évaluer la rupture.
        max_ruptures=0,  # 0 pour pas de limite
        sr=22050,  # Fréquence d'échantillonnage de travail
        hop_length=512,  # Pas entre frames (~23ms à 22050Hz)
        n_mfcc=10,  # Nombre de coefficients MFCC
        novelty_smooth_sec=0.5,  # Lissage temporel de la courbe de nouveauté.
        min_segment_sec=1.0,  # Durée minimale d'un segment pour être considéré comme tel.
        silence_percentile=8.0,  # Percentile pour définir le seuil de silence
        cosine_weight=1.0,
    )

    segments, *_rest = detector.run(input_file)

    return [
        Segment(
            start_epoch=start_time + seg.start_sec,
            end_epoch=start_time + seg.end_sec,
        )
        for seg in segments
    ]


def split_and_save_chunks(
    input_file: str, output_file: str, start_time: float, hard_stop: float | None = None
):
    segment_boundaries = split_chunks(input_file, start_time)
    if hard_stop:
        # En cas de demande de hard stop, on ne prend plus les segments qui commencent après cette limite (on aura donc un unique segment qui la dépassera).
        # On enlève également le tout premier segment puisqu'il sera à priori traité par le chunk précédent.
        segment_boundaries = [
            seg
            for (i, seg) in enumerate(segment_boundaries)
            if i != 0 and seg.start_epoch < hard_stop
        ]
    save_segment_groups_to_json(segment_boundaries, output_file)


async def split_week_in_segments(
    channel: str, week_start_date: datetime, output_folder: str
):
    api = CachedMediatreeAPI()
    semaphore = asyncio.Semaphore(5)  # Limite à 2 téléchargements simultanés

    for start_date, end_date in all_intervals_between(
        week_start_date, week_start_date + timedelta(days=7), timedelta(minutes=30)
    ):
        async with semaphore:
            export_file = await api.download_export(
                channel, start_date, end_date + timedelta(minutes=1)
            )  # on ajoute une minute pour être sûr de couvrir toute la période

        if False:
            output_file = os.path.join(
                output_folder,
                f"{start_date.strftime('%Y-%m-%d_%H-%M-%S')}.json",
            )
            split_and_save_chunks(
                export_file,
                output_file,
                start_time=start_date.timestamp(),
                hard_stop=end_date.timestamp(),
            )
