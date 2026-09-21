"""
FiniexDataCollector - The viewer

A second program that draws the collector's screen from `/v1/status` and holds no
state of its own. It exists because the process that draws must not be the process
that stamps ticks: a console in QuickEdit mode suspends the next write, and one
Rich frame on the production console was measured at 3.9 s on the collector's only
event loop.

Read-only by construction - there is no route here that writes.

Location: python/viewer/__init__.py
"""
