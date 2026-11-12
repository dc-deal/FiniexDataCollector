"""
FiniexDataCollector - Package Schedule
When packages are built: the weekly rule, special runs, and a weekly run set aside.

Nothing is packed on request. A package comes from the schedule, and the schedule
says in advance what will come when - so a consumer reads it and plans its pickups,
and only the operator's token changes it.

**Every cutoff is a UTC day boundary.** The collector seals each day at 00:00 UTC -
its Kraken files close at the day cut, the MT5 terminal's at the same moment - so a
cutoff in the middle of a day would leave every quiet symbol's file open and the
package nearly empty. A special run packs no earlier than `special_after_seal_minutes`
after its cutoff, when the sealed files are certainly written, and never in the
quiet minutes 23:55-00:15 UTC.

**Limits keep packages from piling up**, checked on every change and refused with the
reason: one special run per collector per cutoff day, `special_max_per_week` per
collector per ISO week, none within `special_blackout_hours` before a weekly run
(which would come out almost empty), and none for a cutoff a planned run already
covers.

Stored as `<output_dir>/schedule.json`, written atomically. Every change is also
booked in the ledger, with the token that made it.

Location: python/packages/schedule.py
"""

import json
import threading
import uuid
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from python.packages.package_store import write_json_atomically
from python.utils.config_loader import PackagesConfig

SCHEDULE_NAME = "schedule.json"
WEEKDAYS = ("monday", "tuesday", "wednesday",
            "thursday", "friday", "saturday", "sunday")
ALL = "all"


class ScheduleRefused(Exception):
    """A change the rules do not allow; the message says which rule."""


def next_run(now: datetime, weekday: str, hour: int, minute: int) -> datetime:
    """
    The next weekly run strictly after now, UTC.

    Args:
        now: The current time, UTC
        weekday: Day name
        hour: Hour, UTC
        minute: Minute

    Returns:
        The moment of the next run
    """
    now = now.astimezone(timezone.utc)
    target_day = WEEKDAYS.index(weekday)
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    candidate += timedelta(days=(target_day - now.weekday()) % 7)
    if candidate <= now:
        candidate += timedelta(days=7)
    return candidate


def in_quiet_minutes(moment: datetime) -> bool:
    """23:55 to 00:15 UTC inclusive, when nothing is packed - the configuration's rule."""
    minute_of_day = moment.hour * 60 + moment.minute
    return minute_of_day >= 23 * 60 + 55 or minute_of_day <= 15


def _midnight(day: date) -> datetime:
    return datetime.combine(day, time(0, 0), tzinfo=timezone.utc)


def _parse(value: Optional[str]) -> Optional[datetime]:
    return datetime.fromisoformat(value) if value else None


class Schedule:
    """
    The weekly rule's state and the special runs, kept in one file.

    One process writes it - the service - but its API threads and its schedule
    loop both reach in, so every read-modify-write holds a lock.
    """

    def __init__(self, root: Path, config: PackagesConfig):
        """
        Args:
            root: The packages' root folder
            config: The packages block - the weekly rule and the limits
        """
        self._path = Path(root) / SCHEDULE_NAME
        self._config = config
        self._lock = threading.Lock()

    # ---------------------------------------------------------------- storage

    def _load(self) -> Dict[str, Any]:
        if not self._path.exists():
            return {"weekly": {"suspended_until": None, "skip_run_at": None}, "specials": []}
        return json.loads(self._path.read_text(encoding="utf-8"))

    def _save(self, state: Dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        write_json_atomically(self._path, state)

    # ---------------------------------------------------------------- reading

    def weekly_skipped(self, run_at: datetime, state: Optional[Dict[str, Any]] = None) -> bool:
        """Whether the weekly run at this moment is set aside."""
        weekly = (state or self._load())["weekly"]
        until = _parse(weekly.get("suspended_until"))
        if until is not None and run_at < until:
            return True
        return weekly.get("skip_run_at") == run_at.isoformat()

    def next_weekly(self, now: datetime) -> datetime:
        """The next weekly run by the rule, whether set aside or not."""
        return next_run(now, self._config.weekday, self._config.hour, self._config.minute)

    def view(self, now: Optional[datetime] = None) -> Dict[str, Any]:
        """
        What a consumer plans its pickups from: the weekly rule, whether it is set
        aside, and the special runs to come. No token names - those are in the ledger.
        """
        now = now or datetime.now(timezone.utc)
        state = self._load()
        weekly_at = self.next_weekly(now)
        upcoming = []
        for _ in range(4):
            upcoming.append({"run_at": weekly_at.isoformat(),
                             "cutoff": _midnight(weekly_at.date()).isoformat(),
                             "set_aside": self.weekly_skipped(weekly_at, state)})
            weekly_at += timedelta(days=7)
        specials = [{**{k: s[k] for k in ("id", "collector", "cutoff", "run_at", "state")},
                     "source": s["collector"]}
                    for s in state["specials"]
                    if s["state"] == "planned" or _parse(s["run_at"]) > now - timedelta(days=14)]
        return {
            "weekly": {
                "weekday": self._config.weekday,
                "time_utc": f"{self._config.hour:02d}:{self._config.minute:02d}",
                "cutoff": "00:00 UTC of the run's own day",
                "suspended_until": state["weekly"].get("suspended_until"),
                "next_runs": upcoming,
            },
            "special": sorted(specials, key=lambda s: s["run_at"]),
            "limits": {
                "special_per_collector_per_day": 1,
                "special_per_collector_per_week": self._config.special_max_per_week,
                "no_special_within_hours_before_weekly": self._config.special_blackout_hours,
                "earliest_after_cutoff_minutes": self._config.special_after_seal_minutes,
                "quiet_minutes_utc": "23:55-00:15",
            },
        }

    def due(self, now: datetime) -> List[Dict[str, Any]]:
        """Planned special runs whose time has come, oldest first."""
        return sorted((s for s in self._load()["specials"]
                       if s["state"] == "planned" and _parse(s["run_at"]) <= now),
                      key=lambda s: s["run_at"])

    def next_special_at(self) -> Optional[datetime]:
        planned = [_parse(s["run_at"]) for s in self._load()[
            "specials"] if s["state"] == "planned"]
        return min(planned) if planned else None

    # ---------------------------------------------------------------- changing

    def add(self, collector: str, cutoff_day: date, now: datetime, by: str,
            run_at: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Plan a special run. Refused, with the rule, when it breaks one.

        Args:
            collector: A configured collector, or `all`
            cutoff_day: The UTC day whose 00:00 is the cutoff
            now: The current time
            by: The token name that asks
            run_at: When to pack; default the cutoff day at `special_hour:special_minute`,
                or, when that has passed, a few minutes from now

        Returns:
            The special run as stored
        """
        config = self._config
        if collector != ALL and collector not in config.sources:
            raise ScheduleRefused(f"no collector {collector!r}; one of "
                                  f"{sorted(config.sources)} or {ALL!r}")
        cutoff = _midnight(cutoff_day)
        earliest = cutoff + \
            timedelta(minutes=config.special_after_seal_minutes)
        if run_at is None:
            run_at = cutoff.replace(
                hour=config.special_hour, minute=config.special_minute)
            if run_at < now:
                run_at = (now + timedelta(minutes=5)
                          ).replace(second=0, microsecond=0)
            run_at = max(run_at, earliest)
        run_at = run_at.astimezone(timezone.utc)
        if run_at < earliest:
            raise ScheduleRefused(
                f"a special run packs no earlier than {config.special_after_seal_minutes} "
                f"minutes after its cutoff, so not before {earliest.isoformat()}")
        if run_at <= now:
            raise ScheduleRefused("the run time is in the past")
        if in_quiet_minutes(run_at):
            raise ScheduleRefused("23:55-00:15 UTC stays quiet")

        # A weekly run that is set aside packs nothing, so it neither blocks a
        # special run before it nor covers its cutoff.
        weekly_at = self.next_weekly(run_at - timedelta(seconds=1))
        if not self.weekly_skipped(weekly_at) and \
                weekly_at - run_at < timedelta(hours=config.special_blackout_hours):
            raise ScheduleRefused(
                f"the weekly run at {weekly_at.isoformat()} is less than "
                f"{config.special_blackout_hours} h after it and would come out almost empty")
        # The weekly run on the cutoff's own day packs to that same 00:00 - before
        # the special run or after it, one of the two would find nothing.
        weekly_on_cutoff = cutoff.replace(
            hour=config.hour, minute=config.minute)
        if WEEKDAYS[cutoff.weekday()] == config.weekday and \
                not self.weekly_skipped(weekly_on_cutoff):
            raise ScheduleRefused(
                f"the weekly run at {weekly_on_cutoff.isoformat()} already packs to this cutoff")

        collectors = list(config.sources) if collector == ALL else [collector]
        with self._lock:
            state = self._load()
            planned = [s for s in state["specials"]
                       if s["state"] != "cancelled"]
            for name in collectors:
                mine = [s for s in planned if s["collector"] in (name, ALL)]
                if any(s["cutoff"] == cutoff.isoformat() for s in mine):
                    raise ScheduleRefused(
                        f"{name} already has a special run to this cutoff")
                week = run_at.isocalendar()[:2]
                in_week = [s for s in mine if _parse(
                    s["run_at"]).isocalendar()[:2] == week]
                if len(in_week) >= config.special_max_per_week:
                    raise ScheduleRefused(
                        f"{name} already has {len(in_week)} special run(s) in that week; "
                        f"at most {config.special_max_per_week}")
            special = {"id": uuid.uuid4().hex[:8], "collector": collector,
                       "cutoff": cutoff.isoformat(), "run_at": run_at.isoformat(),
                       "state": "planned", "created_at": now.isoformat(timespec="seconds"),
                       "created_by": by}
            state["specials"].append(special)
            self._save(state)
        return special

    def cancel(self, special_id: str) -> Optional[Dict[str, Any]]:
        """Cancel a planned special run; None when there is no such planned run."""
        with self._lock:
            state = self._load()
            for special in state["specials"]:
                if special["id"] == special_id and special["state"] == "planned":
                    special["state"] = "cancelled"
                    self._save(state)
                    return special
        return None

    def finish(self, special_id: str, results: List[Dict[str, Any]]) -> None:
        """Record what a special run came to."""
        with self._lock:
            state = self._load()
            for special in state["specials"]:
                if special["id"] == special_id:
                    special["state"] = "done"
                    special["results"] = {
                        r.get("source"): r.get("result") for r in results}
            self._save(state)

    def set_aside(self, now: datetime, until: Optional[date] = None) -> Dict[str, Any]:
        """
        Set the weekly run aside: the next one only, or every one before `until`.

        Returns:
            The weekly state as stored
        """
        with self._lock:
            state = self._load()
            if until is None:
                state["weekly"]["skip_run_at"] = self.next_weekly(
                    now).isoformat()
            else:
                if _midnight(until) <= now:
                    raise ScheduleRefused(
                        "the date to resume from is in the past")
                state["weekly"]["suspended_until"] = _midnight(
                    until).isoformat()
            self._save(state)
            return state["weekly"]

    def resume(self) -> Dict[str, Any]:
        """The weekly rule applies again from its next run."""
        with self._lock:
            state = self._load()
            state["weekly"] = {"suspended_until": None, "skip_run_at": None}
            self._save(state)
            return state["weekly"]
