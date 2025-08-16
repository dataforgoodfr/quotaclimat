# %%
from collections import defaultdict

import numpy as np
import pandas as pd


# %%
def build_spans(df, key_col="plaintext"):
    spans_by_doc = defaultdict(list)
    for _, row in df.iterrows():
        spans_by_doc[row[key_col]].append({
            'start': row['start'],
            'end': row['end'],
            'label': row['label']
        })
    return spans_by_doc



def compute_segmentation_metrics(gt_spans, pred_spans, iou_threshold=0.5):
    """
    gt_spans / pred_spans: dict[str, list[dict]] where each span is:
      {"start": int, "end": int, "label": str}
    IoU matching is one-to-one per doc & label (greedy over ground-truth order).
    """
    total_tp_iou = 0
    total_tp_strict = 0
    total_fp = 0
    total_fn = 0
    boundary_errors = []

    # Per-class and per-doc accumulators (IoU-based)
    per_class = defaultdict(lambda: {"tp": 0, "fp": 0, "fn": 0})
    per_doc = {}

    all_docs = set(gt_spans.keys()) | set(pred_spans.keys())
    for doc in all_docs:
        true_spans = gt_spans.get(doc, [])
        pred_spans_doc = pred_spans.get(doc, [])

        matched_true = set()
        matched_pred = set()

        tp_iou_doc = 0
        tp_strict_doc = 0

        # Try to match by IoU for same label
        for i, t in enumerate(true_spans):
            for j, p in enumerate(pred_spans_doc):
                if j in matched_pred:
                    continue
                if p["label"] != t["label"]:
                    continue

                inter = max(0, min(t["end"], p["end"]) - max(t["start"], p["start"]))
                union = max(t["end"], p["end"]) - min(t["start"], p["start"])
                iou = inter / union if union > 0 else 0.0

                if iou >= iou_threshold:
                    matched_true.add(i)
                    matched_pred.add(j)
                    total_tp_iou += 1
                    tp_iou_doc += 1

                    # strict match check (exact boundaries)
                    if t["start"] == p["start"] and t["end"] == p["end"]:
                        total_tp_strict += 1
                        tp_strict_doc += 1

                    # boundary error defined on start offset (like your first fn)
                    boundary_errors.append(abs(t["start"] - p["start"]))
                    break  # move to next ground-truth span

        # Unmatched = FN / FP
        fn_doc = len(true_spans) - len(matched_true)
        fp_doc = len(pred_spans_doc) - len(matched_pred)
        total_fn += fn_doc
        total_fp += fp_doc

        # Per-class counts:
        #  - TP: labels of matched predictions
        #  - FN: labels of unmatched ground truths
        #  - FP: labels of unmatched predictions
        # (Note: we only add one outcome for each span.)
        for i, t in enumerate(true_spans):
            if i not in matched_true:
                per_class[t["label"]]["fn"] += 1
        for j, p in enumerate(pred_spans_doc):
            if j in matched_pred:
                per_class[p["label"]]["tp"] += 1
            else:
                per_class[p["label"]]["fp"] += 1

        per_doc[doc] = {
            "TP_IoU": tp_iou_doc,
            "TP_Strict": tp_strict_doc,
            "FP": fp_doc,
            "FN": fn_doc,
        }

    # ---- Global metrics (IoU-based and strict) ----
    precision_iou = total_tp_iou / (total_tp_iou + total_fp) if (total_tp_iou + total_fp) else 0.0
    recall_iou = total_tp_iou / (total_tp_iou + total_fn) if (total_tp_iou + total_fn) else 0.0
    f1_iou = (2 * precision_iou * recall_iou / (precision_iou + recall_iou)) if (precision_iou + recall_iou) else 0.0

    precision_strict = total_tp_strict / (total_tp_strict + total_fp) if (total_tp_strict + total_fp) else 0.0
    recall_strict = total_tp_strict / (total_tp_strict + total_fn) if (total_tp_strict + total_fn) else 0.0
    f1_strict = (2 * precision_strict * recall_strict / (precision_strict + recall_strict)) if (precision_strict + recall_strict) else 0.0

    mean_boundary_error = float(np.mean(boundary_errors)) if boundary_errors else None

    # ---- Per-class precision/recall/F1 ----
    by_class = {}
    for label, stats in per_class.items():
        tp, fp, fn = stats["tp"], stats["fp"], stats["fn"]
        p = tp / (tp + fp) if (tp + fp) else 0.0
        r = tp / (tp + fn) if (tp + fn) else 0.0
        f = (2 * p * r / (p + r)) if (p + r) else 0.0
        by_class[label] = {
            "TP": tp,
            "FP": fp,
            "FN": fn,
            "Precision": p,
            "Recall": r,
            "F1": f,
            "Support": tp + fn,
        }

    return {
        "Global": {
            "Strict": {
                "Precision": precision_strict,
                "Recall": recall_strict,
                "F1": f1_strict,
            },
            "IoU": {
                "Threshold": iou_threshold,
                "Precision": precision_iou,
                "Recall": recall_iou,
                "F1": f1_iou,
            },
            "Mean Boundary Error": mean_boundary_error,
            "Counts": {
                "TP_IoU": total_tp_iou,
                "TP_Strict": total_tp_strict,
                "FP": total_fp,
                "FN": total_fn,
            },
        },
        "ByClass": by_class,
        "ByDoc": per_doc,
    }

def main():
    ground_truth = pd.read_csv('data/labelstudio_ads_france3.csv', index_col=0)
    predictions = pd.read_csv('data/labellisation_41mini-prompt2.csv', index_col=0)

    gt_spans = build_spans(ground_truth)
    pred_spans = build_spans(predictions)

    segmentation_metrics = compute_segmentation_metrics(gt_spans, pred_spans)

    print('Global metrics:')
    print(segmentation_metrics['Global'])
    print('By class metrics:')
    print(segmentation_metrics['ByClass'])
    # print('By document metrics:')
    # print(segmentation_metrics['ByDoc'])

if __name__ == "__main__":
    main()
# %%
