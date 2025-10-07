from __future__ import annotations

from legendmeta import LegendSlowControlDB

scdb = LegendSlowControlDB()
scdb.connect()

scdb.status()
