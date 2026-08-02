#!/usr/bin/env python3
"""Summarise the AIJOIN instrumentation lines emitted by ToLeafJoinContext.

Reads a Solr log (or several, or stdin), extracts the ``AIJOIN evt=...`` logfmt
lines, and prints a table plus the conclusions each block supports -- phrased
against the sections of the preprint, since that is what the numbers are for.

Usage:
    tools/aijoin_log_summary.py solr.log [more.log ...]
    tools/aijoin_log_summary.py --csv contexts.csv solr.log
    ... | tools/aijoin_log_summary.py -

Grouping: lines carry ``ctx=<id>``, so drains and the terminal line attach to
their context exactly, at any concurrency. Logs predating that field fall back
to "most recent ``evt=ctx`` for the same ``toSeg``", which is only exact while a
single query is in flight; the script says which mode it used and counts any
line it could not attribute.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from statistics import mean, median

LINE_RE = re.compile(r"AIJOIN\s+(evt=\S+(?:\s+\w+=\S+)*)")
KV_RE = re.compile(r"(\w+)=(\S+)")


def parse_value(raw: str):
    if raw == "true":
        return True
    if raw == "false":
        return False
    try:
        return int(raw)
    except ValueError:
        return raw


def parse_lines(streams):
    """Returns (builds, contexts, problems)."""
    builds = []
    contexts = []
    open_ctx = {}
    problems = {
        "orphan_drain": 0,
        "orphan_done": 0,
        "reopened": 0,
        "cell_mismatch": 0,
        "by_ctx_id": 0,
        "by_seg": 0,
    }

    for stream in streams:
        for line in stream:
            m = LINE_RE.search(line)
            if not m:
                continue
            kv = {k: parse_value(v) for k, v in KV_RE.findall(m.group(1))}
            evt = kv.get("evt")
            # prefer the explicit context id; fall back to to-segment for logs predating it
            ctx_id = kv.get("ctx")
            if ctx_id is not None and ctx_id != "-":
                key = ("id", ctx_id)
                problems["by_ctx_id"] += 1
            else:
                key = ("seg", kv.get("toSeg"))
                if evt != "build":
                    problems["by_seg"] += 1

            if evt == "build":
                builds.append(kv)
            elif evt == "ctx":
                if key in open_ctx:
                    # only reachable in to-segment fallback mode: the previous context never
                    # emitted evt=done, which is the normal case -- it never had to converge
                    problems["reopened"] += 1
                    contexts.append(open_ctx.pop(key))
                if kv.get("cellsCreated", 0) - kv.get("cellsDroppedApriori", 0) != kv.get(
                    "cellsLive", 0
                ):
                    problems["cell_mismatch"] += 1
                open_ctx[key] = {"ctx": kv, "drains": [], "done": None}
            elif evt == "drain":
                ctx = open_ctx.get(key)
                if ctx is None:
                    problems["orphan_drain"] += 1
                    continue
                ctx["drains"].append(kv)
            elif evt == "done":
                ctx = open_ctx.pop(key, None)
                if ctx is None:
                    problems["orphan_done"] += 1
                    continue
                ctx["done"] = kv
                contexts.append(ctx)

    contexts.extend(open_ctx.values())  # never converged; that is the interesting case
    return builds, contexts, problems


def pct(part, whole):
    return 100.0 * part / whole if whole else float("nan")


def pctile(values, p):
    if not values:
        return float("nan")
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round(p / 100.0 * len(ordered))) - 1))
    return ordered[idx]


def fmt(value, digits=1):
    if value != value:  # NaN
        return "n/a"
    if isinstance(value, int):
        return f"{value:,}"
    return f"{value:,.{digits}f}"


WIDTH = 90
LABEL_W = 54
CELL_W = 12


def row(label, *cells):
    return f"  {label:<{LABEL_W}}" + "".join(f"{str(c):>{CELL_W}}" for c in cells)


def section(title):
    print()
    print(f"  {title}")
    print("  " + "-" * (WIDTH - 2))


def report(builds, contexts, problems):
    n_ctx = len(contexts)
    if not n_ctx and not builds:
        print("No AIJOIN lines found. Is the prototype's logging at INFO?", file=sys.stderr)
        return 1

    converged = [c for c in contexts if c["done"] is not None]
    lazy_held = [c for c in contexts if c["done"] is None]

    print("=" * WIDTH)
    print("AIJOIN instrumentation summary")
    print("=" * WIDTH)
    print(row("to-segment contexts (parent segments scored)", n_ctx))
    print(row("join-index build events", len(builds)))
    if any(problems.values()):
        print(row("unattributable drain lines", problems["orphan_drain"]))
        print(row("unattributable done lines", problems["orphan_done"]))
        print(row("cellsCreated - dropped != cellsLive", problems["cell_mismatch"]))

    # ---------------------------------------------------------------- build cost
    section("Join-index build cost  (paper: 5.2, 9.1 -- lazy build and warming)")
    eager = [b for b in builds if b.get("cause") == "eager-create-weight"]
    lazy = [b for b in builds if b.get("cause") == "lazy-to-segment"]
    if builds:
        built = [b.get("builtMs", 0) for b in builds]
        waited = sum(b.get("awaitedMs", 0) for b in builds)
        print(row("build events: eager / lazy",
                  f"{len(eager)} eager", f"{len(lazy)} lazy"))
        print(row("total time building (ms)", fmt(sum(built))))
        print(row("per build: mean / p50 / max (ms)",
                  fmt(mean(built)), fmt(median(built)), fmt(max(built))))
        print(row("time waiting on another thread's build (ms)", fmt(waited)))
        print(row("pairs requested / built / awaited",
                  fmt(sum(b.get("pairsRequested", 0) for b in builds)),
                  fmt(sum(b.get("pairsBuilt", 0) for b in builds)),
                  fmt(sum(b.get("pairsAwaited", 0) for b in builds))))
        print(row("to-docs mapped by those pairs", fmt(sum(b.get("toCount", 0) for b in builds))))
    else:
        print(row("no build events -- columns were already materialised", "-"))

    # --------------------------------------------------------- a-priori pruning
    section("A-priori pruning  (paper: 7.2 -- the c_min/c_max column bypass)")
    created = sum(c["ctx"].get("cellsCreated", 0) for c in contexts)
    dropped = sum(c["ctx"].get("cellsDroppedApriori", 0) for c in contexts)
    live = sum(c["ctx"].get("cellsLive", 0) for c in contexts)
    print(row("candidate pairs with a from-side match", fmt(created)))
    print(row("dropped before any column was opened", fmt(dropped), f"{fmt(pct(dropped, created))}%"))
    print(row("surviving into confirmation", fmt(live)))

    # ------------------------------------------------- approximation tightness
    section("Approximation tightness  (paper: 5.3 -- what bounds document-level pruning)")
    covers = [
        pct(c["ctx"]["approxCard"], c["ctx"]["toMaxDoc"])
        for c in contexts
        if c["ctx"].get("toMaxDoc")
    ]
    overlap = [
        c["ctx"]["approxSpanSum"] / c["ctx"]["approxCard"]
        for c in contexts
        if c["ctx"].get("approxCard")
    ]
    if covers:
        print(row("A-hat as % of the parent segment: mean / p50 / p90",
                  f"{fmt(mean(covers))}%", f"{fmt(median(covers))}%", f"{fmt(pctile(covers, 90))}%"))
    if overlap:
        print(row("range overlap factor (spanSum / card): mean", fmt(mean(overlap), 2)))

    # ---------------------------------------------------- document-level pruning
    section("Document-level pruning  (paper: 5.3, 7.3 -- the half-read union)")
    drained = sum(len(c["drains"]) for c in contexts)
    never_opened = live - drained
    early_exits = sum(1 for c in contexts for d in c["drains"] if d.get("confirmed") is True)
    spared = sum(d.get("cellsLeft", 0) for c in contexts for d in c["drains"] if d.get("confirmed") is True)
    walked = sum(d.get("walked", 0) for c in contexts for d in c["drains"])

    print(row("parent segments where laziness held (no convergence)",
              fmt(len(lazy_held)), f"{fmt(pct(len(lazy_held), n_ctx))}%"))
    print(row("parent segments forced to full convergence",
              fmt(len(converged)), f"{fmt(pct(len(converged), n_ctx))}%"))
    print(row("columns drained", fmt(drained)))
    print(row("columns never opened", fmt(never_opened), f"{fmt(pct(never_opened, live))}%"))
    print(row("drains ending in an early confirmation", fmt(early_exits), f"{fmt(pct(early_exits, drained))}%"))
    # cellsLeft summed over confirmed drains counts columns *deferred*, not saved: if the context
    # later converges they get drained anyway. Only "columns never opened" is a real saving.
    print(row("columns deferred by them (may be drained later)", fmt(spared)))
    print(row("from-docs walked (column-read work)", fmt(walked)))

    if converged:
        calls = sum(c["done"].get("confirmCalls", 0) for c in converged)
        free = sum(c["done"].get("freeHits", 0) for c in converged)
        print(row("confirmations answered from H alone*", fmt(free), f"{fmt(pct(free, calls))}%"))
        print("  * over converged segments only -- see the caveat in the conclusions")

    # -------------------------------------------------------------- conclusions
    print()
    print("=" * WIDTH)
    print("Conclusions")
    print("=" * WIDTH)

    if builds:
        total_ms = sum(built)
        print(
            f"- Build cost: {fmt(total_ms)} ms across {len(builds)} build events"
            f"{f', plus {fmt(waited)} ms waiting on other threads' if waited else ''}.\n"
            "  Both build paths run inside a query -- the eager one from ensureJoinSegments at\n"
            "  createWeight, the lazy one from ToLeafJoinContext -- so this whole cost lands on\n"
            "  the query path and is what searcher warming would remove (9.1)."
        )
        if eager and not lazy:
            print(
                f"- All {len(eager)} builds were eager (ensureJoinSegments at createWeight); the\n"
                "  per-to-segment lazy path never fired. Section 5.2 says columns are 'built\n"
                "  lazily, on first access by the algorithm of 6' -- that describes the fallback,\n"
                "  not the path that actually does the work. The text should say the columns are\n"
                "  built on first *query* need, in bulk, before scoring starts."
            )
        elif lazy and not eager:
            print(
                f"- All {len(lazy)} builds came from the per-to-segment lazy path, matching the\n"
                "  description in 5.2."
            )
        elif lazy and eager:
            print(
                f"- Mixed: {len(eager)} eager and {len(lazy)} lazy builds. The eager pass covers\n"
                "  most pairs at createWeight; the lazy path fills gaps it missed."
            )

    if created:
        print(
            f"- A-priori pruning (7.2) dropped {fmt(pct(dropped, created))}% of candidate pairs\n"
            f"  before opening a single column. "
            + (
                "This is the level doing the visible work."
                if pct(dropped, created) >= 25
                else "Modest here -- the child ranges overlap the query's matches in most pairs."
            )
        )

    if covers:
        med = median(covers)
        print(
            f"- Approximation (5.3): A-hat covers a median {fmt(med)}% of the parent segment.\n"
            + (
                "  It is nearly uninformative, so almost every non-matching parent that the\n"
                "  sibling filter admits is a false positive -- the regime where the paper\n"
                "  predicts document-level pruning pays least, and the argument for n-range\n"
                "  approximation rather than a single interval."
                if med >= 80
                else "  Tight enough to keep false positives -- and therefore forced convergence\n"
                "  -- rare; this is the regime document-level pruning was designed for."
            )
        )

    if n_ctx:
        held = pct(len(lazy_held), n_ctx)
        print(
            f"- Document-level pruning (7.3): laziness held on {fmt(held)}% of parent segments,\n"
            f"  leaving {fmt(pct(never_opened, live))}% of surviving columns unopened.\n"
            + (
                "  The lazy variant is earning its keep."
                if held >= 50
                else "  Most segments converged, i.e. the lazy variant degenerated to the eager\n"
                "  one (Algorithm 1) on the first candidate it could not confirm -- exactly the\n"
                "  worst case described in 5.3."
            )
        )

    if converged and lazy_held:
        print(
            "- Caveat: the free-hit rate above is computed over converged segments only,\n"
            "  because a segment that never converges emits no evt=done line and its final\n"
            "  counters are never logged. Converged segments are precisely the ones where\n"
            "  laziness failed, so that rate is a pessimistic bound, not an average."
        )

    if problems["by_seg"]:
        print(
            f"- Note: {problems['by_seg']} lines carried no ctx= id, so they were grouped by\n"
            "  to-segment. That is exact only while one query is in flight; re-run at\n"
            "  concurrency 1, or rebuild Solr with the ctx= field."
        )
    if problems["orphan_drain"] or problems["orphan_done"]:
        print(
            f"- Warning: {problems['orphan_drain']} drain and {problems['orphan_done']} done lines\n"
            "  could not be attributed to a context; the numbers above undercount by that much."
        )
    return 0


def write_csv(path, contexts):
    fields = [
        "toSeg", "toMaxDoc", "cellsCreated", "cellsDroppedApriori", "cellsLive",
        "approxCard", "approxSpanSum", "colToCountSum", "buildMs",
        "cellsDrained", "earlyExits", "fromDocsWalked", "converged", "reason",
        "confirmCalls", "freeHits",
    ]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for c in contexts:
            ctx, done = c["ctx"], c["done"] or {}
            writer.writerow(
                {
                    "toSeg": ctx.get("toSeg"),
                    "toMaxDoc": ctx.get("toMaxDoc"),
                    "cellsCreated": ctx.get("cellsCreated"),
                    "cellsDroppedApriori": ctx.get("cellsDroppedApriori"),
                    "cellsLive": ctx.get("cellsLive"),
                    "approxCard": ctx.get("approxCard"),
                    "approxSpanSum": ctx.get("approxSpanSum"),
                    "colToCountSum": ctx.get("colToCountSum"),
                    "buildMs": ctx.get("buildMs"),
                    "cellsDrained": len(c["drains"]),
                    "earlyExits": sum(1 for d in c["drains"] if d.get("confirmed") is True),
                    "fromDocsWalked": sum(d.get("walked", 0) for d in c["drains"]),
                    "converged": c["done"] is not None,
                    "reason": done.get("reason", ""),
                    "confirmCalls": done.get("confirmCalls", ""),
                    "freeHits": done.get("freeHits", ""),
                }
            )


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("logs", nargs="*", default=["-"], help="Solr log files, or - for stdin")
    ap.add_argument("--csv", help="also write one row per to-segment context here")
    args = ap.parse_args(argv)

    streams, opened = [], []
    for path in args.logs or ["-"]:
        if path == "-":
            streams.append(sys.stdin)
        else:
            fh = open(path, encoding="utf-8", errors="replace")
            opened.append(fh)
            streams.append(fh)
    try:
        builds, contexts, problems = parse_lines(streams)
    finally:
        for fh in opened:
            fh.close()

    if args.csv:
        write_csv(args.csv, contexts)
        print(f"per-context rows written to {args.csv}", file=sys.stderr)
    return report(builds, contexts, problems)


if __name__ == "__main__":
    sys.exit(main())
