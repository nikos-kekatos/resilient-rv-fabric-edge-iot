#!/usr/bin/env python3
"""Collapse MonPoly satisfaction timepoints into distinct episodes.

MonPoly reports a satisfaction at every timepoint at which the formula holds, so a
single coordinated campaign yields a run of consecutive firings for as long as its
events stay inside the property's window. Counting raw firings therefore counts
window-ticks, not events. An *episode* is a maximal run of satisfactions separated
by no more than the property's own window: a later satisfaction is part of the same
episode if it falls within `window` seconds of the previous one, and starts a new
one otherwise.

Usage:
    python3 episodes.py --out <monpoly-output-file> [--window 30]
"""
import argparse
import re
import sys

TS = re.compile(r"^@(\d+)")


def firing_timestamps(text):
    """Timestamps of MonPoly satisfactions, in order (one per reported timepoint)."""
    out = []
    for line in text.splitlines():
        m = TS.match(line.strip())
        if m and "(" in line:          # a satisfaction carries a tuple
            out.append(int(m.group(1)))
    return out


def episodes(timestamps, window=30):
    """Number of maximal runs of satisfactions separated by <= `window` seconds."""
    if not timestamps:
        return 0
    n, prev = 1, timestamps[0]
    for t in timestamps[1:]:
        if t - prev > window:
            n += 1
        prev = t
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="MonPoly output file")
    ap.add_argument("--window", type=int, default=30,
                    help="the property's own window in seconds (default 30)")
    a = ap.parse_args()
    ts = firing_timestamps(open(a.out).read())
    print(f"firings={len(ts)} episodes={episodes(ts, a.window)} window={a.window}s")


if __name__ == "__main__":
    main()
