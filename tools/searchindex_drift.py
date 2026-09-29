#!/usr/bin/env python3
"""Compare join parsers across the search-then-index rounds and test for drift.

Reads the cumulative ``searchindex-results-<parser>-c<N>[-noto].csv`` files written by
``SearchThenIndex`` (one row per query, tagged with its round) and answers the
two questions those files exist to answer:

1. What does each parser cost, right after an update?
2. Does that cost *drift* as rounds accumulate -- i.e. does repeated commit
   churn make any parser progressively slower?

Every query of a round is taken as is: rounds are meant to be one update
followed by cold searches on the fresh searcher, so there is no warm steady
state to separate out. What still needs care is **query difficulty**: the query
set is seeded per round (``RANDOM_SEED + round``), so it is identical across
parsers *within* a round but different *between* rounds. A round of harder
queries lifts every parser at once, which a naive trend line reads as drift.

So the trend is reported twice: raw, and normalised by the other parsers'
medians for the same round (leave-one-out geometric mean), which cancels the
per-round difficulty common to all of them. What survives that normalisation
is drift attributable to the parser itself.

Usage:
    tools/searchindex_drift.py                       # CSVs of the newest reports/ run
    tools/searchindex_drift.py reports/<run>         # CSVs of that run folder
    tools/searchindex_drift.py reports/<run>/searchindex-results-*.csv
    tools/searchindex_drift.py --per-round           # also dump the round table
    tools/searchindex_drift.py --metric wall_ms --from-round 5
"""

from __future__ import annotations

import argparse
import csv
import glob
import math
import os
import re
import sys
from collections import defaultdict
from statistics import median

FILE_RE = re.compile(r"searchindex-results-(?P<parser>[^-]+)-c(?P<concurrency>\d+)(?:-noto)?\.csv$")


# --------------------------------------------------------------------------- io


def load(path):
    """Returns (parser, concurrency, {round: [(index, qtime, wall, numFound)]}), errors."""
    m = FILE_RE.search(path)
    parser = m.group("parser") if m else path
    concurrency = int(m.group("concurrency")) if m else 1
    rounds, errors = defaultdict(list), []
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            if row.get("error"):
                errors.append((int(row["round"]), int(row["index"]), row["error"]))
                continue
            rounds[int(row["round"])].append(
                (
                    int(row["index"]),
                    int(row["qtime_ms"]),
                    int(row["wall_ms"]),
                    int(row["numFound"]),
                )
            )
    for queries in rounds.values():
        queries.sort()
    return parser, concurrency, dict(rounds), errors


# ------------------------------------------------------------------- statistics


def ols(xs, ys):
    """Least-squares slope with its standard error. Returns (slope, stderr, intercept)."""
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx
    intercept = my - slope * mx
    resid = [y - (intercept + slope * x) for x, y in zip(xs, ys)]
    stderr = math.sqrt(sum(r * r for r in resid) / (n - 2) / sxx)
    return slope, stderr, intercept


def ranks(values):
    order = sorted(range(len(values)), key=lambda i: values[i])
    out = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        shared = (i + j) / 2 + 1
        for k in range(i, j + 1):
            out[order[k]] = shared
        i = j + 1
    return out


def spearman(xs, ys):
    """Rank correlation and its z-score under the usual large-n normal approximation."""
    rx, ry = ranks(xs), ranks(ys)
    n = len(rx)
    mx, my = sum(rx) / n, sum(ry) / n
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    den = math.sqrt(sum((a - mx) ** 2 for a in rx) * sum((b - my) ** 2 for b in ry))
    rho = num / den if den else 0.0
    return rho, rho * math.sqrt(n - 1)


def geomean(values):
    return math.exp(sum(math.log(v) for v in values) / len(values))


def pct(values, p):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(p * len(ordered)))]


def resolve_paths(args):
    """CSV paths from the arguments: files as given, folders expanded; default the newest run."""
    targets = args or sorted(glob.glob(os.path.join("reports", "*", "")), key=os.path.getmtime)[-1:]
    paths = []
    for target in targets:
        if os.path.isdir(target):
            paths += sorted(glob.glob(os.path.join(target, "searchindex-results-*.csv")))
        else:
            paths.append(target)
    return paths


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("files", nargs="*", help="CSVs or run folders (default: the newest folder under reports/)")
    ap.add_argument("--metric", choices=("qtime_ms", "wall_ms"), default="qtime_ms")
    ap.add_argument(
        "--from-round",
        type=int,
        default=2,
        help="first round included in the drift fit (default 2; round 1 is the JVM/cache cold start)",
    )
    ap.add_argument("--per-round", action="store_true", help="dump the per-round median table")
    args = ap.parse_args(argv)

    paths = resolve_paths(args.files)
    if not paths:
        ap.error("no input files")

    runs, concurrencies = {}, set()
    for path in paths:
        parser, concurrency, rounds, errors = load(path)
        runs[parser] = rounds
        concurrencies.add(concurrency)
        if errors:
            print(f"WARNING: {parser}: {len(errors)} failed queries, excluded", file=sys.stderr)

    concurrency = sorted(concurrencies)[0]
    parsers = list(runs)
    metric_idx = 1 if args.metric == "qtime_ms" else 2

    common = sorted(set.intersection(*(set(r) for r in runs.values())))
    if len(concurrencies) > 1:
        print(f"WARNING: mixed concurrencies {sorted(concurrencies)}; using {concurrency}", file=sys.stderr)

    # numFound must agree query-for-query: the whole comparison rests on it.
    mismatch = 0
    if len(parsers) > 1:
        ref = parsers[0]
        for rd in common:
            found = {i: n for i, _, _, n in runs[ref][rd]}
            for other in parsers[1:]:
                for i, _, _, n in runs[other][rd]:
                    if found.get(i) != n:
                        mismatch += 1

    per_round = {}  # parser -> {round: [metric values]}
    for parser in parsers:
        per_round[parser] = {rd: [q[metric_idx] for q in runs[parser][rd]] for rd in common}

    # a round where some parser has no valid query (all failed) has nothing to median() over
    usable = [rd for rd in common if all(per_round[p][rd] for p in parsers)]
    if len(usable) < len(common):
        print(f"WARNING: {len(common) - len(usable)} round(s) without valid queries excluded", file=sys.stderr)
    common = usable
    if not common:
        ap.error("no round has a valid query for every parser")

    fit_rounds = [rd for rd in common if rd >= args.from_round]
    if len(fit_rounds) < 3:
        ap.error(f"need at least 3 rounds >= --from-round={args.from_round} to fit a trend")
    round_med = {p: {rd: median(per_round[p][rd]) for rd in common} for p in parsers}

    # ------------------------------------------------------------------ report
    width = max(len(p) for p in parsers)
    print(f"searchindex drift report -- metric={args.metric}, concurrency={concurrency}")
    print(f"rounds {common[0]}..{common[-1]} x {len(runs[parsers[0]][common[0]])} queries, all counted")
    print(f"numFound disagreements across parsers: {mismatch}" if len(parsers) > 1 else "")

    print("\n== cost after update (all queries, rounds >= %d) ==" % args.from_round)
    print(f"{'parser':{width}}  {'p50':>8} {'p90':>8} {'p95':>8} {'p99':>8} {'max':>8}   vs fastest")
    all_vals = {p: [v for rd in fit_rounds for v in per_round[p][rd]] for p in parsers}
    fastest = min(parsers, key=lambda p: median(all_vals[p]))
    for p in sorted(parsers, key=lambda p: median(all_vals[p])):
        vals = all_vals[p]
        base = median(all_vals[fastest])
        ratio = f"{median(vals) / base:6.1f}x" if base else "   n/a"
        print(
            f"{p:{width}}  {median(vals):8.0f} {pct(vals,.90):8.0f} {pct(vals,.95):8.0f} "
            f"{pct(vals,.99):8.0f} {max(vals):8.0f}   {ratio}"
        )

    print("\n== drift across rounds %d..%d (median per round) ==" % (fit_rounds[0], fit_rounds[-1]))
    print(
        f"{'parser':{width}}  {'median':>8}   raw slope %/round        difficulty-normalised   Spearman(norm)"
    )
    for p in parsers:
        xs = fit_rounds
        raw = [round_med[p][rd] for rd in xs]
        slope, stderr, intercept = ols(xs, raw)
        start = intercept + slope * xs[0]
        raw_pct = f"{100 * slope / start:+6.2f}%/rd" if start else f"{slope:+6.2f}ms/rd"
        others = [q for q in parsers if q != p]
        # a 0 ms round median (tiny index, qtime rounding) has no log; normalising is meaningless
        zero = any(round_med[q][rd] == 0 for q in parsers for rd in xs)
        if others and zero:
            norm_txt, sp_txt = "n/a (0 ms round medians)", ""
        elif others:
            norm = [math.log(round_med[p][rd] / geomean([round_med[q][rd] for q in others])) for rd in xs]
            nslope, nstderr, _ = ols(xs, norm)
            rho, z = spearman(xs, norm)
            span = 100 * (math.exp(nslope * (xs[-1] - xs[0])) - 1)
            norm_txt = f"{100*nslope:+6.2f}%/rd (z={nslope/nstderr:+5.1f}) {span:+6.1f}% over span"
            sp_txt = f"rho={rho:+.2f} z={z:+5.1f}"
        else:
            norm_txt, sp_txt = "n/a (single parser)", ""
        print(
            f"{p:{width}}  {median(raw):8.0f}   {raw_pct} (z={slope/stderr if stderr else 0:+5.1f})   {norm_txt}   {sp_txt}"
        )
    first = [round_med[p][fit_rounds[0]] for p in parsers]
    last = [round_med[p][fit_rounds[-1]] for p in parsers]
    moved = f"{geomean(first):.0f} -> {geomean(last):.0f} ms" if all(first + last) else "n/a"
    print(
        "\nraw slope includes the per-round query difficulty common to all parsers"
        f" (that pool moved {moved});"
        "\nthe normalised column divides it out and is the one to read for parser-specific drift."
        "\nz is a normal approximation and assumes independent rounds; treat |z| < 3 as weak."
    )

    if args.per_round:
        print("\n== per-round median ==")
        print("round  " + "".join(f"{p:>{max(9,width+1)}}" for p in parsers))
        for rd in common:
            print(f"{rd:5d}  " + "".join(f"{round_med[p][rd]:>{max(9,width+1)}.0f}" for p in parsers))

    return 0


if __name__ == "__main__":
    sys.exit(main())
