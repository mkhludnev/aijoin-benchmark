#!/usr/bin/env python3
"""Summarise the aux-index join instrumentation in a Solr log.

Reads a Solr log (or several, or stdin), extracts the ``AUXIJOIN evt=...`` logfmt lines (and the
``AIJOIN evt=...`` lines of older builds), the sidecar merge policy's prose lines, and the request
log's QTime, then prints a table plus the conclusions each block supports -- phrased against the
sections of the preprint where the numbers map onto one.

Events understood:
    weight          one per query: pairs needed / already built / missing
    fromLeaf        one per from-segment per query: matches, whether its FK column was loaded
    fkload          one per from-side FK column load: size and cost
    ctx             one per (query, to-segment) scored: cells, a-priori pruning, approximation
    drain           one per pair column read while confirming
    done            a context that had to converge
    build           one per build-and-persist round: compute vs persist time, model layout
    readFieldInfos  one per sidecar segment inspected: pair columns it carries
    sweepSample, strandedColumn, purgeDeclined   the dead-pair reaper
    "sidecar: ..." / "sidecar compaction: ..."    AuxIndexJoinMergePolicy rounds

Usage:
    tools/aijoin_log_summary.py solr.log [more.log ...]
    tools/aijoin_log_summary.py --csv contexts.csv solr.log
    ... | tools/aijoin_log_summary.py -

Grouping: ctx/drain/done/build lines carry ``ctx=<id>``, unique per (query, to-segment), so drains
and the terminal line attach to their context exactly, at any concurrency. Logs predating that
field fall back to "most recent ``evt=ctx`` for the same ``toSeg``", which is only exact while a
single query is in flight; the script says which mode it used and counts any line it could not
attribute.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter
from statistics import mean, median

# AUXIJOIN is the current tag; AIJOIN is what older builds emitted
LINE_RE = re.compile(r"\bA(?:UX)?IJOIN\s+evt=(\w+)(.*)$")
# a value is either a bracketed list (which may contain spaces) or a single token
KV_RE = re.compile(r"(\w+)=(\[[^\]]*\]|\S+)")
SIDECAR_RE = re.compile(r"\bA(?:UX)?IJOIN\s+sidecar( compaction)?:\s*(.*)$")
REQUEST_RE = re.compile(r"path=/select params=\{.*?\{!(\w+).*\bQTime=(\d+)")

PURGE_DECLINED_RE = re.compile(
    r"dead=(\d+) of (\d+) columns.*?reclaims=(\d+) of (\d+) bytes, threshold=(\d+)"
)
NOTHING_RE = re.compile(
    r"nothing to do on (\w+), (\d+) segments \((\d+) compactable, (\d+) merging, "
    r"(\d+) pairs pending removal\)"
)
COMPACTION_RE = re.compile(
    r"(\d+) doc-aligned merge\(s\) of (\d+) segments each, (\d+) column purge\(s\) and "
    r"(\d+) dead segment\(s\) dropped, out of (\d+) segments \((\d+) compactable, (\d+) merging, "
    r"(\d+) pairs pending removal, (\d+) segment\(s\) reapable now, (\d+) of those too little "
    r"dead to be worth rewriting\)"
)
TRIGGER_RE = re.compile(r"\bon ([A-Z_]+)\b")
REAPED_RE = re.compile(r"reaped (\d+) dead pair column\(s\), (\d+) still pending")

# events attributed to a to-segment context; everything else is per query or per sidecar
CONTEXT_EVENTS = {"ctx", "drain", "done"}


def parse_value(raw: str):
    if raw.startswith("["):
        inner = raw[1:-1].strip()
        return [item.strip() for item in inner.split(",")] if inner else []
    raw = raw.rstrip(",")
    if raw == "true":
        return True
    if raw == "false":
        return False
    try:
        return int(raw)
    except ValueError:
        return raw


class Log:
    """Everything the report needs, gathered in one pass."""

    def __init__(self):
        self.events = Counter()
        self.builds = []
        self.weights = []
        self.from_leaves = []
        self.fk_loads = []
        self.field_infos = []
        self.sweeps = []
        self.stranded = 0
        self.purge_declined = []
        self.contexts = []
        self.qtimes = {}  # parser -> [ms]
        self.nothing = Counter()  # trigger -> rounds
        self.compactions = []  # dicts
        self.reaped = []  # (reaped, pending)
        self.max_segments = 0
        self.max_pending = 0
        self.problems = Counter()


def parse_lines(streams) -> Log:
    log = Log()
    open_ctx = {}

    for stream in streams:
        for line in stream:
            if "QTime=" in line:
                rm = REQUEST_RE.search(line)
                if rm:
                    log.qtimes.setdefault(rm.group(1), []).append(int(rm.group(2)))
                    continue

            sm = SIDECAR_RE.search(line)
            if sm:
                parse_sidecar(log, bool(sm.group(1)), sm.group(2))
                continue

            m = LINE_RE.search(line)
            if not m:
                continue
            evt = m.group(1)
            rest = m.group(2)
            kv = {k: parse_value(v) for k, v in KV_RE.findall(rest)}
            log.events[evt] += 1

            if evt == "weight":
                log.weights.append(kv)
            elif evt == "fromLeaf":
                log.from_leaves.append(kv)
            elif evt == "fkload":
                log.fk_loads.append(kv)
            elif evt == "build":
                log.builds.append(kv)
            elif evt == "readFieldInfos":
                log.field_infos.append(kv)
            elif evt == "sweepSample":
                log.sweeps.append(kv)
            elif evt == "strandedColumn":
                log.stranded += 1
            elif evt == "purgeDeclined":
                pm = PURGE_DECLINED_RE.search(rest)
                if pm:
                    dead, width, reclaims, size, threshold = map(int, pm.groups())
                    log.purge_declined.append(
                        dict(dead=dead, width=width, reclaims=reclaims, size=size, threshold=threshold)
                    )
            elif evt in CONTEXT_EVENTS:
                attach_context_event(log, open_ctx, evt, kv)

    log.contexts.extend(open_ctx.values())  # never converged; that is the interesting case
    return log


def parse_sidecar(log: Log, compaction: bool, text: str):
    trigger = TRIGGER_RE.search(text)
    trigger = trigger.group(1) if trigger else "?"
    if compaction:
        cm = COMPACTION_RE.search(text)
        if not cm:
            log.problems["unparsed_sidecar"] += 1
            return
        g = list(map(int, cm.groups()))
        log.compactions.append(
            dict(merges=g[0], per_merge=g[1], purges=g[2], dropped=g[3], segments=g[4],
                 compactable=g[5], merging=g[6], pending=g[7], reapable=g[8], declined=g[9],
                 trigger=trigger)
        )
        log.max_segments = max(log.max_segments, g[4])
        log.max_pending = max(log.max_pending, g[7])
        return
    nm = NOTHING_RE.search(text)
    if nm:
        log.nothing[nm.group(1)] += 1
        log.max_segments = max(log.max_segments, int(nm.group(2)))
        log.max_pending = max(log.max_pending, int(nm.group(5)))
        return
    rm = REAPED_RE.search(text)
    if rm:
        log.reaped.append((int(rm.group(1)), int(rm.group(2))))
        return
    log.problems["unparsed_sidecar"] += 1


def attach_context_event(log: Log, open_ctx, evt, kv):
    # prefer the explicit context id; fall back to to-segment for logs predating it
    ctx_id = kv.get("ctx")
    if ctx_id is not None and ctx_id != "-":
        key = ("id", ctx_id)
        log.problems["by_ctx_id"] += 1
    else:
        key = ("seg", kv.get("toSeg"))
        log.problems["by_seg"] += 1

    if evt == "ctx":
        if key in open_ctx:
            # the previous context never emitted evt=done, which is the normal case -- it never
            # had to converge. With ctx ids this means an id was reused, which it should not be.
            log.problems["reopened"] += 1
            log.contexts.append(open_ctx.pop(key))
        if kv.get("cellsCreated", 0) - kv.get("cellsDroppedApriori", 0) != kv.get("cellsLive", 0):
            log.problems["cell_mismatch"] += 1
        open_ctx[key] = {"ctx": kv, "drains": [], "done": None}
    elif evt == "drain":
        ctx = open_ctx.get(key)
        if ctx is None:
            log.problems["orphan_drain"] += 1
            return
        ctx["drains"].append(kv)
    elif evt == "done":
        ctx = open_ctx.pop(key, None)
        if ctx is None:
            log.problems["orphan_done"] += 1
            return
        ctx["done"] = kv
        log.contexts.append(ctx)


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


def fmt_bytes(n):
    for unit, size in (("GB", 1 << 30), ("MB", 1 << 20), ("KB", 1 << 10)):
        if n >= size:
            return f"{n / size:,.1f} {unit}"
    return f"{n:,} B"


WIDTH = 104
LABEL_W = 58
CELL_W = 11


def row(label, *cells):
    return f"  {label:<{LABEL_W}}" + "".join(f" {str(c):>{CELL_W}}" for c in cells)


def dist_row(label, values, unit=""):
    """label + mean / p50 / p90 / max."""
    if not values:
        return row(label, "-")
    return row(
        f"{label}: mean/p50/p90/max{unit}",
        fmt(mean(values)), fmt(median(values)), fmt(pctile(values, 90)), fmt(max(values)),
    )


def section(title):
    print()
    print(f"  {title}")
    print("  " + "-" * (WIDTH - 2))


def report(log: Log):
    contexts = log.contexts
    n_ctx = len(contexts)
    if not (n_ctx or log.builds or log.weights or log.qtimes or log.compactions or log.nothing
            or log.field_infos or log.fk_loads):
        print("No AUXIJOIN/AIJOIN lines found. Are the diagnostics enabled at TRACE?", file=sys.stderr)
        return 1

    no_live = [c for c in contexts if c["ctx"].get("cellsLive", 0) == 0]
    with_live = [c for c in contexts if c["ctx"].get("cellsLive", 0) > 0]
    converged = [c for c in with_live if c["done"] is not None]
    lazy_held = [c for c in with_live if c["done"] is None]

    print("=" * WIDTH)
    print("Aux-index join instrumentation summary")
    print("=" * WIDTH)
    print(row("queries (evt=weight)", fmt(len(log.weights))))
    print(row("to-segment contexts scored (evt=ctx)", fmt(n_ctx)))
    print(row("build-and-persist rounds (evt=build)", fmt(len(log.builds))))
    print(row("from-side FK column loads (evt=fkload)", fmt(len(log.fk_loads))))
    other = ", ".join(f"{k}={v:,}" for k, v in sorted(log.events.items())
                      if k not in {"weight", "ctx", "build", "fkload"})
    if other:
        print(f"  other events: {other}")
    if any(log.problems[k] for k in ("orphan_drain", "orphan_done", "cell_mismatch", "reopened",
                                     "unparsed_sidecar")):
        print(row("unattributable drain / done lines",
                  fmt(log.problems["orphan_drain"]), fmt(log.problems["orphan_done"])))
        print(row("cellsCreated - dropped != cellsLive", fmt(log.problems["cell_mismatch"])))
        print(row("context ids reused", fmt(log.problems["reopened"])))
        print(row("sidecar lines not understood", fmt(log.problems["unparsed_sidecar"])))

    # ------------------------------------------------------------------ latency
    if log.qtimes:
        section("Query latency  (request log QTime, ms)")
        print(row("parser", "n", "p50", "p90", "p99", "max"))
        for parser, times in sorted(log.qtimes.items(), key=lambda kv: -len(kv[1])):
            print(row(f"{{!{parser}}}", fmt(len(times)), fmt(pctile(times, 50)),
                      fmt(pctile(times, 90)), fmt(pctile(times, 99)), fmt(max(times))))

    # ------------------------------------------------------------ query demand
    if log.weights:
        section("Per-query pair demand  (evt=weight)")
        needed = [w.get("pairsNeeded", 0) for w in log.weights]
        missing = [w.get("pairsMissing", 0) for w in log.weights]
        warm = sum(1 for x in missing if x == 0)
        claimed = sum(1 for w in log.weights if w.get("pairsClaimed", 0) > 0)
        print(dist_row("pairs needed per query", needed))
        print(row("queries needing no build (pairsMissing=0)", fmt(warm),
                  f"{fmt(pct(warm, len(log.weights)))}%"))
        print(dist_row("pairs missing, when any", [x for x in missing if x]))
        print(row("queries racing another build (claimed>0)", fmt(claimed),
                  f"{fmt(pct(claimed, len(log.weights)))}%"))
        print(row("from-segment FK loads requested (fkOrdsToLoad)",
                  fmt(sum(w.get("fkOrdsToLoad", 0) for w in log.weights))))

    # ---------------------------------------------------------------- from side
    if log.from_leaves or log.fk_loads:
        section("From side  (evt=fromLeaf, evt=fkload)")
        if log.from_leaves:
            hit = sum(1 for f in log.from_leaves if f.get("fromMatches", 0) > 0)
            loaded = sum(1 for f in log.from_leaves if f.get("fkLoaded") is True)
            print(row("from-segment visits / with matches / FK loaded",
                      fmt(len(log.from_leaves)), fmt(hit), fmt(loaded)))
        if log.fk_loads:
            took_ms = [f.get("tookUs", 0) / 1000.0 for f in log.fk_loads]
            print(row("total FK load time (ms)", fmt(sum(took_ms))))
            print(dist_row("per FK load (ms)", took_ms))
            print(dist_row("from-segment maxDoc per load", [f.get("maxDoc", 0) for f in log.fk_loads]))
            paged = [f.get("pagedBytes", 0) for f in log.fk_loads if "pagedBytes" in f]
            if paged:
                print(row("paged bytes per load: p50 / max",
                          fmt_bytes(int(median(paged))), fmt_bytes(max(paged))))
            per_seg = Counter(f.get("fromSeg") for f in log.fk_loads)
            reloaded = {seg: n for seg, n in per_seg.items() if n > 1}
            print(row("distinct from-segments loaded / loaded more than once",
                      fmt(len(per_seg)), fmt(len(reloaded))))
            if reloaded:
                repeat_ms = 0.0
                seen = set()
                for f in log.fk_loads:
                    seg = f.get("fromSeg")
                    if seg in seen:
                        repeat_ms += f.get("tookUs", 0) / 1000.0
                    seen.add(seg)
                print(row("time re-loading an already-loaded segment (ms)",
                          fmt(repeat_ms), f"{fmt(pct(repeat_ms, sum(took_ms)))}%"))
                worst = sorted(reloaded.items(), key=lambda kv: -kv[1])[:3]
                print("  most re-loaded: " + ", ".join(f"{s} x{n}" for s, n in worst))

    # --------------------------------------------------------------- build cost
    section("Join-index build cost  (paper: 5.2, 9.1 -- lazy build and warming)")
    builds = log.builds
    if builds:
        built = [b.get("builtMs", 0) for b in builds]
        compute = [b.get("computeMs", 0) for b in builds if "computeMs" in b]
        persist = [b.get("persistMs", 0) for b in builds if "persistMs" in b]
        waited = sum(b.get("awaitedMs", 0) for b in builds)
        print(row("total time building (ms)", fmt(sum(built))))
        print(dist_row("per round (ms)", built))
        if compute and persist:
            total = sum(compute) + sum(persist)
            print(row("  computing models (ms)", fmt(sum(compute)), f"{fmt(pct(sum(compute), total))}%"))
            print(row("  persisting to the sidecar (ms)", fmt(sum(persist)), f"{fmt(pct(sum(persist), total))}%"))
            print(dist_row("  persist per round (ms)", persist))
        print(row("time waiting on another thread's build (ms)", fmt(waited)))
        print(row("pairs requested / built / awaited",
                  fmt(sum(b.get("pairsRequested", 0) for b in builds)),
                  fmt(sum(b.get("pairsBuilt", 0) for b in builds)),
                  fmt(sum(b.get("pairsAwaited", 0) for b in builds))))
        written = [p for b in builds for p in (b.get("writtenPairs") or [])]
        twice = sum(1 for n in Counter(written).values() if n > 1)
        print(row("pair columns written / written more than once", fmt(len(written)), fmt(twice)))
        if any("sparseModels" in b for b in builds):
            sparse = sum(b.get("sparseModels", 0) for b in builds)
            models = sum(b.get("pairsBuilt", 0) for b in builds)
            print(row("models laid out sparse", fmt(sparse), f"{fmt(pct(sparse, models))}%"))
            mb = [b.get("modelBytes", 0) for b in builds]
            print(row("model bytes per round: p50 / max", fmt_bytes(int(median(mb))), fmt_bytes(max(mb))))
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
    print(row("contexts pruned entirely (cellsLive=0)", fmt(len(no_live)), f"{fmt(pct(len(no_live), n_ctx))}%"))

    # ------------------------------------------------- approximation tightness
    section("Approximation tightness  (paper: 5.3 -- what bounds document-level pruning)")
    covers = [
        pct(c["ctx"]["approxCard"], c["ctx"]["toMaxDoc"])
        for c in with_live
        if c["ctx"].get("toMaxDoc")
    ]
    overlap = [
        c["ctx"]["approxSpanSum"] / c["ctx"]["approxCard"]
        for c in with_live
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
    n_live = len(with_live)

    print(row("contexts with live columns", fmt(n_live)))
    print(row("  where laziness held (no convergence)", fmt(len(lazy_held)), f"{fmt(pct(len(lazy_held), n_live))}%"))
    print(row("  forced to full convergence", fmt(len(converged)), f"{fmt(pct(len(converged), n_live))}%"))
    if converged:
        reasons = Counter(c["done"].get("reason", "?") for c in converged)
        for reason, n in reasons.most_common():
            print(row(f"    reason={reason}", fmt(n)))
    print(row("columns drained", fmt(drained)))
    print(row("columns never opened", fmt(never_opened), f"{fmt(pct(never_opened, live))}%"))
    print(row("drains ending in an early confirmation", fmt(early_exits), f"{fmt(pct(early_exits, drained))}%"))
    # cellsLeft summed over confirmed drains counts columns *deferred*, not saved: if the context
    # later converges they get drained anyway. Only "columns never opened" is a real saving.
    print(row("columns deferred by them (may be drained later)", fmt(spared)))
    print(row("from-docs walked (column-read work)", fmt(walked)))
    print(dist_row("from-docs walked per drain", [d.get("walked", 0) for c in contexts for d in c["drains"]]))

    if converged:
        calls = sum(c["done"].get("confirmCalls", 0) for c in converged)
        free = sum(c["done"].get("freeHits", 0) for c in converged)
        print(row("confirmations answered from H alone*", fmt(free), f"{fmt(pct(free, calls))}%"))
        print("  * over converged segments only -- see the caveat in the conclusions")
        if any("modelsReleased" in c["done"] for c in converged):
            print(row("models released to disk / rebinds after reap",
                      fmt(sum(c["done"].get("modelsReleased", 0) for c in converged)),
                      fmt(sum(c["done"].get("rebindsAfterReap", 0) for c in converged))))

    # ------------------------------------------------------- sidecar upkeep
    if log.field_infos or log.compactions or log.nothing or log.sweeps or log.purge_declined:
        section("Sidecar upkeep  (AuxIndexJoinMergePolicy)")
        if log.compactions or log.nothing:
            # max, not last: every core's sidecar logs through the same policy class, and merge
            # threads carry no core tag, so "the last line" belongs to whichever wrote most recently
            print(row("most sidecar segments / pairs pending seen at once",
                      fmt(log.max_segments), fmt(log.max_pending)))
        if log.field_infos:
            segs = {f.get("segment") for f in log.field_infos}
            pairs = [f.get("pairs", 0) for f in log.field_infos]
            print(row("distinct sidecar segments inspected", fmt(len(segs))))
            print(dist_row("pair columns per sidecar segment", pairs))
        if log.compactions:
            by_trigger = Counter(c["trigger"] for c in log.compactions)
            print(row("rounds that did work", fmt(len(log.compactions))))
            print("    by trigger: " + ", ".join(f"{t}={n}" for t, n in by_trigger.most_common()))
            print(row("  doc-aligned merges / purges / dead segs dropped",
                      fmt(sum(c["merges"] for c in log.compactions)),
                      fmt(sum(c["purges"] for c in log.compactions)),
                      fmt(sum(c["dropped"] for c in log.compactions))))
        if log.nothing:
            print(row("rounds with nothing to do", fmt(sum(log.nothing.values()))))
            print("    by trigger: " + ", ".join(f"{t}={n}" for t, n in log.nothing.most_common()))
        if log.reaped:
            print(row("dead pair columns reaped", fmt(sum(r[0] for r in log.reaped))))
        if log.purge_declined:
            reclaim = sum(p["reclaims"] for p in log.purge_declined)
            print(row("purges declined as too small / bytes left on disk",
                      fmt(len(log.purge_declined)), fmt_bytes(reclaim)))
        if log.sweeps:
            print(row("reaper samples / pairs queued / stranded found",
                      fmt(len(log.sweeps)),
                      fmt(sum(s.get("queuedForRemoval", 0) for s in log.sweeps)),
                      fmt(sum(s.get("strandedFound", 0) for s in log.sweeps))))
        if log.stranded:
            print(row("stranded columns (side keys gone)", fmt(log.stranded)))

    # -------------------------------------------------------------- conclusions
    print()
    print("=" * WIDTH)
    print("Conclusions")
    print("=" * WIDTH)

    if log.weights:
        warm_pct = pct(sum(1 for w in log.weights if w.get("pairsMissing", 0) == 0), len(log.weights))
        print(
            f"- Demand: {fmt(warm_pct)}% of queries found every pair column already built. The rest\n"
            "  pay for FK loads and a build on the query thread, so the cold share is what the\n"
            "  build and from-side sections below are really measuring."
        )

    if builds:
        total_ms = sum(b.get("builtMs", 0) for b in builds)
        compute = sum(b.get("computeMs", 0) for b in builds)
        persist = sum(b.get("persistMs", 0) for b in builds)
        line = f"- Build cost: {fmt(total_ms)} ms across {len(builds)} rounds"
        if compute or persist:
            share = pct(persist, compute + persist)
            line += (
                f", {fmt(share)}% of it persisting.\n"
                + (
                    "  Persisting dominates: the sidecar batch walks the from-segment's full width as\n"
                    "  documents whatever the pair matched, so this is the cost that paging the\n"
                    "  columns on disk would cut, not the model computation."
                    if share >= 60
                    else "  Computing the models dominates: the FK column walk and term resolution, not\n"
                    "  the sidecar write, is where the build time goes."
                    if share <= 40
                    else "  Roughly even between computing models and persisting them, so neither alone\n"
                    "  is the build's bottleneck."
                )
            )
        else:
            line += "."
        print(line)
        print(
            "  Every build runs inside a query, so the whole cost lands on the query path and is\n"
            "  what searcher warming would remove (9.1)."
        )

    if log.fk_loads:
        per_seg = Counter(f.get("fromSeg") for f in log.fk_loads)
        repeats = sum(n - 1 for n in per_seg.values())
        if repeats:
            print(
                f"- From side: {fmt(repeats)} of {fmt(len(log.fk_loads))} FK loads re-loaded a segment already\n"
                "  loaded earlier in the log. Nothing caches the FK column across queries, so every\n"
                "  query that still misses a pair pays the full hash build again."
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

    if n_live:
        held = pct(len(lazy_held), n_live)
        unopened = pct(never_opened, live)
        if unopened < 10:
            verdict = (
                "  Avoiding convergence did not translate into avoided reads: nearly every live\n"
                "  column was drained anyway, by early confirmations that later drains caught up\n"
                "  with. Columns never opened is the only real saving, and it is negligible here."
            )
        elif held >= 50:
            verdict = "  The lazy variant is earning its keep."
        else:
            verdict = (
                "  Most contexts converged, i.e. the lazy variant degenerated to the eager\n"
                "  one (Algorithm 1) on the first candidate it could not confirm -- exactly the\n"
                "  worst case described in 5.3."
            )
        print(
            f"- Document-level pruning (7.3): laziness held on {fmt(held)}% of contexts with live\n"
            f"  columns, leaving {fmt(unopened)}% of surviving columns unopened.\n" + verdict
        )
        if no_live:
            print(
                f"  ({fmt(len(no_live))} further contexts had every column pruned a priori and are left out\n"
                "  of that rate: they never had anything to be lazy about.)"
            )

    if converged and lazy_held:
        print(
            "- Caveat: the free-hit rate above is computed over converged contexts only,\n"
            "  because a context that never converges emits no evt=done line and its final\n"
            "  counters are never logged. Converged contexts are precisely the ones where\n"
            "  laziness failed, so that rate is a pessimistic bound, not an average."
        )

    if log.purge_declined:
        print(
            f"- Sidecar: {fmt(len(log.purge_declined))} purges were declined as below the reclaim threshold,\n"
            f"  leaving {fmt_bytes(sum(p['reclaims'] for p in log.purge_declined))} of dead columns on disk until\n"
            "  compaction rewrites those segments anyway."
        )

    if log.problems["by_seg"]:
        print(
            f"- Note: {log.problems['by_seg']} context lines carried no ctx= id, so they were grouped by\n"
            "  to-segment. That is exact only while one query is in flight; re-run at\n"
            "  concurrency 1, or rebuild Solr with the ctx= field."
        )
    if log.problems["orphan_drain"] or log.problems["orphan_done"]:
        print(
            f"- Warning: {log.problems['orphan_drain']} drain and {log.problems['orphan_done']} done lines\n"
            "  could not be attributed to a context; the numbers above undercount by that much."
        )
    return 0


def write_csv(path, contexts):
    fields = [
        "ctx", "toSeg", "toMaxDoc", "cellsCreated", "cellsDroppedApriori", "cellsLive",
        "approxCard", "approxSpanSum", "colToCountSum", "buildMs",
        "cellsDrained", "earlyExits", "fromDocsWalked", "converged", "reason",
        "confirmCalls", "freeHits", "modelsReleased", "rebindsAfterReap",
    ]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for c in contexts:
            ctx, done = c["ctx"], c["done"] or {}
            writer.writerow(
                {
                    "ctx": ctx.get("ctx"),
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
                    "modelsReleased": done.get("modelsReleased", ""),
                    "rebindsAfterReap": done.get("rebindsAfterReap", ""),
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
        log = parse_lines(streams)
    finally:
        for fh in opened:
            fh.close()

    if args.csv:
        write_csv(args.csv, log.contexts)
        print(f"per-context rows written to {args.csv}", file=sys.stderr)
    return report(log)


if __name__ == "__main__":
    sys.exit(main())
