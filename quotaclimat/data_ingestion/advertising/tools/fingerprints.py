from quotaclimat.data_ingestion.advertising.tools.fingerprint_tools.generate import (
    FingerprintComputer,
)

# This fingerprint computer class is statically instanced because its parameters should be fixed and never change.
# Those parameters directly affect the fingerprinting process and the resulting fingerprints,
# so they must remain consistent across all runs of the pipeline, or the fingerprints should all be recomputed.

fingerprint_computer = FingerprintComputer(
    sr=16000,  # Sample rate (Hz).
    hop_length=1024,  # STFT hop size (samples). Controls frame rate: fps = sr/hop_length ≈ 16.
    n_fft=2048,  # FFT size for constellation map. 2048 ≈ 128ms @ 16KHz.
    neighborhood=15,  # Local max filter size for peak detection in time×frequency plane.
    min_amplitude=0.01,  # Min normalized amplitude (0-1) for a spectral peak to be retained.
    n_peaks=20,  # Max spectral peaks retained per chunk (constellation map).
    fan_out=4,  # Pairs per peak for fingerprinting. 4 is sufficient with distance-based matching.
    max_pairs=30,  # Max fingerprint pairs retained per chunk, the more pairs, the more robust the matching, but also the more memory and compute intensive.
)
