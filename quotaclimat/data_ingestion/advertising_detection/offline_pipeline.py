"""
Offline pipeline functions for advertising detection.

No network calls — works on local audio files only.
Imports only e02 (segmentation) and e03 (grouping).
"""

from typing import List

from .e02_create_segments import Segment, SegmentCreator
from .e03_group_segments import SegmentGroupingPipeline


def create_segments(audio_path: str, start_epoch: float = 0.0) -> List[Segment]:
    """Run segmentation on a single audio file. Returns a list of Segments."""
    return SegmentCreator().run(audio_path, start_epoch)


def group_segments(
    segments_list: List[List[Segment]],
    similarity_threshold: float = 0.05,
    min_matching_hashes: int = 1,
    n_peaks_by_segment: int = 5,
    neighborhood_peaks_filter: int = 15,
    min_peak_amplitude: float = 0.01,
) -> list:
    """Group segments from multiple sources by fingerprint similarity."""
    pipeline = SegmentGroupingPipeline(
        similarity_threshold=similarity_threshold,
        min_matching_hashes=min_matching_hashes,
        n_peaks_by_segment=n_peaks_by_segment,
        neighborhood_peaks_filter=neighborhood_peaks_filter,
        min_peak_amplitude=min_peak_amplitude,
    )
    return pipeline.run(segments_list)


def run_offline_pipeline(
    audio_paths: List[str],
    start_epochs: List[float] = None,
    **grouping_kwargs,
) -> dict:
    """
    Run the full offline pipeline: segmentation + grouping.

    Returns {"segments_list": [...], "groups": [...]}.
    """
    if start_epochs is None:
        start_epochs = [0.0] * len(audio_paths)

    segments_list = [
        create_segments(path, epoch)
        for path, epoch in zip(audio_paths, start_epochs)
    ]
    groups = group_segments(segments_list, **grouping_kwargs)
    return {"segments_list": segments_list, "groups": groups}
