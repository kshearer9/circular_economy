"""Score the hand-coded verification sample against the model's labels.

The coding sheet was written blind: it carries the project title, abstract and
funder, and no model output. This script joins your coding back to the key and
asks whether the tier 2 threshold delivers what it claimed.

    /opt/anaconda3/bin/python scripts/classification/score_verification.py

It reports, for each tier:

    agreement          how often your field matches the model's, with a Wilson
                       interval, because a proportion near 0.8 on fifty cases
                       has a wide one and quoting a point estimate alone would
                       overstate what the sample can show
    top-2 agreement    how often your field matches the model's first OR second
                       choice. The gap between this and plain agreement says
                       whether the model is wrong or merely ranking two
                       defensible fields in the other order, which matters for
                       an interdisciplinary corpus
    Cohen's kappa      agreement corrected for what chance would give on this
                       distribution, since Engineering alone is half the data
                       and raw agreement flatters any classifier facing that

The tier 2 verdict compares the lower bound of the interval with the declared
target, not the point estimate. If the lower bound sits below the target the
honest reading is that the sample cannot confirm the claim, which is not the
same as refuting it.

Rows you marked UNCLEAR are reported separately and excluded from the
agreement figures. A case you could not place is evidence about the coding
scheme, not about the classifier, and folding it in either direction would
misrepresent both.
"""
from pathlib import Path
import sys

import numpy as np
import pandas as pd
from sklearn.metrics import cohen_kappa_score, confusion_matrix

ROOT = Path(__file__).resolve().parents[2]
VAL = ROOT / "data" / "validation"
SHEET = VAL / "discipline_verification_sample.xlsx"
KEY = VAL / "discipline_verification_KEY.csv"
RESULTS = ROOT / "data" / "classification" / "results"

TARGET = 0.80          # the tier 2 accuracy declared before the threshold run
UNCLEAR = "UNCLEAR"


def wilson(k, n, z=1.96):
    if n == 0:
        return (float("nan"), float("nan"))
    p = k / n
    d = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * np.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, centre - half), min(1.0, centre + half))


def block(frame, label):
    n = len(frame)
    if n == 0:
        print(f"\n{label}: nothing to score")
        return None
    hits = int(frame.agree.sum())
    lo, hi = wilson(hits, n)
    top2 = int(frame.agree_top2.sum())
    t2lo, t2hi = wilson(top2, n)
    print(f"\n{label}  (n = {n})")
    print(f"  agreement        {hits / n:.3f}  [{lo:.3f}, {hi:.3f}]  {hits}/{n}")
    print(f"  top-2 agreement  {top2 / n:.3f}  [{t2lo:.3f}, {t2hi:.3f}]  {top2}/{n}")
    if frame.coder_field.nunique() > 1:
        k = cohen_kappa_score(frame.coder_mapped, frame.model_field)
        print(f"  Cohen's kappa    {k:.3f}")
    return dict(label=label, n=n, agree=hits, agreement=round(hits / n, 4),
                ci_low=round(lo, 4), ci_high=round(hi, 4),
                top2=top2, top2_agreement=round(top2 / n, 4))


def main() -> None:
    for path in (SHEET, KEY):
        if not path.exists():
            sys.exit(f"Missing {path}. Run apply_classifier.py --sample-only first.")

    coded = pd.read_excel(SHEET, sheet_name="Coding")
    key = pd.read_csv(KEY)
    df = coded.merge(key, on="sample_id", how="inner", suffixes=("", "_key"))
    if len(df) != len(key):
        print(f"WARNING: matched {len(df)} of {len(key)} sample rows on sample_id")

    df["coder_field"] = df.your_field.astype("string").str.strip()
    done = df[df.coder_field.notna() & (df.coder_field != "")].copy()
    print(f"coded {len(done)} of {len(df)}")
    if len(done) < len(df):
        print(f"  {len(df) - len(done)} rows still blank, excluded from every figure")
    if done.empty:
        sys.exit("Nothing coded yet.")

    unclear = done[done.coder_field.str.upper() == UNCLEAR]
    scored = done[done.coder_field.str.upper() != UNCLEAR].copy()
    if len(unclear):
        print(f"  {len(unclear)} marked {UNCLEAR}, reported separately and excluded")

    # The ten-class scheme merged two of the protocol's twelve fields
    # (Section 3.10 of the methodology). Coder labels in the merged classes are
    # mapped to their merged targets before scoring, so a code the scheme
    # defines as equivalent counts as agreement rather than as an automatic
    # miss. Raw twelve-class agreement is reported alongside.
    MERGE = {
        "Materials Science": "Engineering",
        "Biochemistry, Genetics and Molecular Biology":
            "Agricultural and Biological Sciences",
    }
    scored["coder_mapped"] = scored.coder_field.replace(MERGE)
    n_merged = int(scored.coder_field.isin(MERGE).sum())
    if n_merged:
        print(f"  {n_merged} codes in merged classes mapped per the ten-class scheme")

    unknown = set(scored.coder_mapped) - set(key.model_field) - set(scored.model_field)
    if unknown:
        print(f"  note: fields you used that the model never predicts: {sorted(unknown)}")

    raw = float((scored.coder_field == scored.model_field).mean())
    scored["agree"] = scored.coder_mapped == scored.model_field
    scored["agree_top2"] = scored.agree | (scored.coder_mapped == scored.model_second_field)
    print(f"  raw twelve-class agreement (no mapping): {raw:.3f}")

    print("\n" + "=" * 62)
    rows = [block(scored, "ALL model-assigned")]
    for tier in sorted(scored.tier.unique()):
        rows.append(block(scored[scored.tier == tier], f"tier {tier}"))
    rows = [r for r in rows if r]

    # --- the verdict on the declared target --------------------------------
    t2 = scored[scored.tier == 2]
    if len(t2):
        hits = int(t2.agree.sum())
        lo, hi = wilson(hits, len(t2))
        print("\n" + "=" * 62)
        print(f"TIER 2 against the declared {TARGET:.0%} target")
        print(f"  observed {hits / len(t2):.1%} on {len(t2)} cases, "
              f"interval [{lo:.1%}, {hi:.1%}]")
        if lo >= TARGET:
            print("  CONFIRMED: even the lower bound clears the target.")
        elif hits / len(t2) >= TARGET:
            print("  CONSISTENT but not confirmed: the point estimate clears the "
                  "target,\n  the lower bound does not. Report the interval, not "
                  "the point estimate.")
        elif hi >= TARGET:
            print("  NOT CONFIRMED: the sample cannot distinguish the true rate "
                  "from the\n  target. This is weak evidence, not a refutation. "
                  "Report it as such.")
        else:
            print("  BELOW TARGET: the whole interval sits under the target, so "
                  "the\n  threshold does not transfer to the unlabelled projects. "
                  "The tier 2\n  claim needs revising before these labels are used.")

    # --- where the disagreements are ---------------------------------------
    print("\n" + "=" * 62)
    print("agreement by the model's predicted field")
    by_field = (scored.groupby("model_field")
                      .agg(n=("agree", "size"), agree=("agree", "sum"),
                           top2=("agree_top2", "sum")))
    by_field["agreement"] = (by_field.agree / by_field.n).round(2)
    by_field["top2_agreement"] = (by_field.top2 / by_field.n).round(2)
    print(by_field.sort_values("n", ascending=False).to_string())

    wrong = scored[~scored.agree]
    if len(wrong):
        print("\nmost common disagreements (model said -> you said)")
        pairs = (wrong.groupby(["model_field", "coder_field"]).size()
                      .sort_values(ascending=False).head(12))
        for (m, c), n in pairs.items():
            print(f"  {n:>3}  {m}  ->  {c}")

    if "your_confidence" in scored.columns and scored.your_confidence.notna().any():
        print("\nyour own certainty against agreement")
        print(scored.groupby(scored.your_confidence.astype("string"))
                    .agree.agg(["size", "mean"]).round(2).to_string())

    labels = sorted(set(scored.coder_field) | set(scored.model_field))
    cm = pd.DataFrame(confusion_matrix(scored.coder_field, scored.model_field,
                                       labels=labels), index=labels, columns=labels)
    RESULTS.mkdir(parents=True, exist_ok=True)
    cm.to_csv(RESULTS / "verification_confusion.csv")
    pd.DataFrame(rows).to_csv(RESULTS / "verification_summary.csv", index=False)
    scored.to_csv(RESULTS / "verification_scored.csv", index=False)
    print(f"\nWrote verification_summary.csv, verification_confusion.csv and "
          f"verification_scored.csv to {RESULTS.name}/")


if __name__ == "__main__":
    main()
