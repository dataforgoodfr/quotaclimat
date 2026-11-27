"""Splitting an ad video into frames, for the stages that look at images."""

import base64
import io
import math
from collections.abc import Iterator
from itertools import islice

import av
from PIL import Image


def data_url(frame: Image.Image) -> str:
    buffer = io.BytesIO()
    frame.save(buffer, "JPEG", quality=90)
    return "data:image/jpeg;base64," + base64.b64encode(buffer.getvalue()).decode()


def first_frames(url: str, n: int) -> list[Image.Image]:
    with av.open(url, timeout=60) as container:
        if not container.streams.video:
            return []
        return [frame.to_image() for frame in islice(container.decode(video=0), n)]


def split_frames(video: bytes, fps: float = 1.0) -> Iterator[tuple[float, Image.Image]]:
    with av.open(io.BytesIO(video)) as container:
        if not container.streams.video:
            return
        start, n = None, 0
        for frame in container.decode(video=0):
            if start is None:
                start = frame.time
            t = round(frame.time - start, 3)
            if t >= round(n / fps, 3):
                yield t, frame.to_image()
                n = math.floor(t * fps) + 1  # the next slot, skipping any left in a gap
