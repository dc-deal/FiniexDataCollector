"""
FiniexDataCollector - Tick Packages
Weekly sealed zips of finished tick files, one per collector, with a manifest and a
receipt from the consumer.

A program of its own beside the collector, started as `python -m python.packages`.
It reads the collectors' finished files and never touches a collecting process: it
was built inside a restart freeze, and the one thing it must not do is what an
unfiltered `/v1/archive` did on 2026-11-05 - parse the whole archive inside the
process that stamps the ticks.

Location: python/packages/__init__.py
"""
