import numpy as np
import soundfile as sf

from quotaclimat.data_ingestion.advertising.s01_detection.processor import (
    chunk_creator,
)

SR = chunk_creator.sr


def _tone(seconds, freq):
    t = np.arange(int(seconds * SR)) / SR
    return 0.5 * np.sin(2 * np.pi * freq * t)


def _silence(seconds):
    return np.zeros(int(seconds * SR))


def _write(tmp_path, *parts):
    path = tmp_path / "raw.wav"
    sf.write(path, np.concatenate(parts), SR)
    return str(path)


def test_ad_with_a_single_inner_silence_gets_two_chunks(tmp_path):
    # 1s margin, then the ad: 2s of sound, a short silence, 3s of sound, then 1s margin.
    # Only one silence inside the ad: before, no chunk at all was returned.
    path = _write(
        tmp_path,
        _silence(1.0),
        _tone(2.0, 440),
        _silence(0.3),
        _tone(2.7, 880),
        _silence(1.0),
    )
    fingerprints = chunk_creator.run_on_audio_file(path, offset=1.0, duration=5.0)
    assert len(fingerprints) == 2


def test_ad_without_inner_silence_gets_one_chunk(tmp_path):
    path = _write(tmp_path, _silence(1.0), _tone(4.0, 440), _silence(1.0))
    fingerprints = chunk_creator.run_on_audio_file(path, offset=1.0, duration=4.0)
    assert len(fingerprints) == 1


def test_edge_silence_is_not_an_extra_chunk(tmp_path):
    # The window starts in the silence before the ad: the silence detected right at the
    # edge must not create a tiny extra chunk.
    path = _write(tmp_path, _silence(1.0), _tone(4.0, 440), _silence(1.0))
    fingerprints = chunk_creator.run_on_audio_file(path, offset=0.8, duration=4.4)
    assert len(fingerprints) == 1
