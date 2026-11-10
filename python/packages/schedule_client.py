"""
FiniexDataCollector - Schedule Client
Read and change the package schedule over the API, from anywhere.

The schedule lives in the service on the production box; this is how the operator's
token reaches it without the token ever appearing on a command line or in a log.
Reading needs no token. Every change goes through the same routes and the same
limits as any other caller's - the client has no way around them.

Location: python/packages/schedule_client.py
"""

from datetime import date
from typing import Any, Dict, Optional, Tuple

from python.utils.logging_setup import describe_exception

TIMEOUT_SECONDS = 30


class ScheduleCallError(Exception):
    """The call did not reach the service, or it answered with something not JSON."""


def schedule_call(base_url: str, token: str, action: str, collector: str = "",
                  cutoff: Optional[date] = None, run_at: str = "", special_id: str = "",
                  until: Optional[date] = None,
                  session: Any = None) -> Tuple[int, Dict[str, Any]]:
    """
    One call on the schedule routes.

    Args:
        base_url: The collector's address, e.g. https://collector.example
        token: The bearer token; unused for `show`
        action: `show`, `add`, `cancel`, `set_aside` or `resume`
        collector: add: a configured collector, or `all`
        cutoff: add: the UTC day whose 00:00 is the cutoff
        run_at: add: optional ISO moment with offset; default the service's
        special_id: cancel: the special run's id
        until: set_aside: every weekly run before this day; default the next only
        session: A `requests.Session` or an `httpx` client; default a new session

    Returns:
        (HTTP status, the answer's JSON)
    """
    if session is None:
        import requests
        session = requests.Session()
    base = base_url.rstrip("/") + "/v1/packages/schedule"
    headers = {"Authorization": f"Bearer {token}"}
    if action == "show":
        method, url, body = "GET", base, None
        headers = {}
    elif action == "add":
        payload: Dict[str, Any] = {"collector": collector,
                                   "cutoff": cutoff.isoformat() if cutoff else None}
        if run_at:
            payload["run_at"] = run_at
        method, url, body = "POST", base, payload
    elif action == "cancel":
        method, url, body = "DELETE", f"{base}/{special_id}", None
    elif action == "set_aside":
        method, url, body = "POST", f"{base}/weekly", {
            "action": "set_aside", "until": until.isoformat() if until else None}
    elif action == "resume":
        method, url, body = "POST", f"{base}/weekly", {"action": "resume"}
    else:
        raise ValueError(f"unknown action {action!r}")
    try:
        response = session.request(method, url, headers=headers, json=body,
                                   timeout=TIMEOUT_SECONDS)
        return response.status_code, response.json()
    except ValueError as e:
        raise ScheduleCallError(
            f"the answer was not JSON: {describe_exception(e)}") from e
    except Exception as e:
        raise ScheduleCallError(describe_exception(e)) from e
