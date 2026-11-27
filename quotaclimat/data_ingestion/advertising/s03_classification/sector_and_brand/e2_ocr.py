"""The text shown on screen, read by the OCR (``transcription/ocr/ocr.py``), run here as the
second step: it reads the ads ``e1_classify_ad`` found (``fragment_type='AD'``), and
``e5_canonicalise_brand`` uses its text to give a brand read on screen the dictionary's spelling.

Run with ``python -m quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.e2_ocr``.
"""

from quotaclimat.data_ingestion.advertising.s03_classification.transcription.ocr.ocr import (
    main,
)

if __name__ == "__main__":
    main()
