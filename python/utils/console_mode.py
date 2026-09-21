"""
FiniexDataCollector - Console Mode
Stop a mouse click from stopping the collection.

A Windows console in QuickEdit mode blocks the next write for as long as text is
selected. One stray click in the window, or a drag that catches a character, and
the writing process is suspended - not slowed, suspended, indefinitely, until
somebody presses a key in that window. It is on by default.

For this collector that is not a display glitch. The live display writes from a
task on the collector's only event loop, so a blocked write stops the WebSocket
reader, the writers and the rotation with it. No tick arrives at all, which is
why the write-ahead log cannot help here: the gap opens BEFORE the safety net,
and a gap in a tick series is the same bytes as a quiet market. Nothing
downstream can tell them apart.

FiniexRAGEngine measured 13.5 hours of exactly this on this same host on
2026-09-10, in a process whose entire job was to record what happened that night.

Unchecking QuickEdit in a shortcut's properties works and does not survive the
next person opening a fresh window. So the process turns it off for itself, every
start, on the console it actually got.

Location: python/utils/console_mode.py
"""

import sys
from typing import Optional

# Windows console input flags.
STD_INPUT_HANDLE = -10
ENABLE_QUICK_EDIT_MODE = 0x0040
# Must be set in the same call. Without it the console ignores the other flags
# entirely and QuickEdit stays on - which looks exactly like a successful call.
ENABLE_EXTENDED_FLAGS = 0x0080
INVALID_HANDLE_VALUE = -1

# Windows console output flags.
STD_OUTPUT_HANDLE = -11
ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004


def disable_quick_edit() -> Optional[bool]:
    """
    Turn QuickEdit off on this process's console input.

    Never raises. A console that cannot be reconfigured is a reason to log and
    carry on, not a reason to stop collecting - the failure this guards against
    is rare, and refusing to start over it would be the larger outage.

    Returns:
        True when QuickEdit was turned off, False when a console was found but
        would not take the change, and None when there is nothing to do - not
        Windows, or stdin is a pipe rather than a console
    """
    if sys.platform != "win32":
        return None

    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(STD_INPUT_HANDLE)
        if handle in (0, INVALID_HANDLE_VALUE, None):
            return None

        mode = ctypes.c_uint32()
        # Fails when stdin is redirected: there is no console to click in, so
        # there is nothing to protect against either.
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return None

        if not mode.value & ENABLE_QUICK_EDIT_MODE:
            return True

        wanted = (mode.value & ~ENABLE_QUICK_EDIT_MODE) | ENABLE_EXTENDED_FLAGS
        if not kernel32.SetConsoleMode(handle, wanted):
            return False

        # Read it back rather than trusting the return code: the call reports
        # success for a flag combination the console then ignores.
        check = ctypes.c_uint32()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(check)):
            return False

        return not check.value & ENABLE_QUICK_EDIT_MODE
    except Exception:
        return False


def enable_ansi_colours() -> Optional[bool]:
    """
    Teach this process's console to interpret the log's colour codes.

    The logger writes ANSI escapes. A Windows console only acts on them when
    virtual terminal processing is on, and it is off by default - the live
    display used to switch it on as a side effect of rich taking the console
    over, so running with `--no-display` left the escapes visible as text:
    `<-[37mINFO <-[0m` instead of a coloured word, measured on the production
    box 2026-09-21. The log file is unaffected either way; it never carried
    colour.

    Never raises, for the same reason as the call above: a console that will not
    take the change is worth a log line, not a refusal to collect.

    Returns:
        True when the console will now interpret colour, False when a console
        was found but would not take the change, and None when there is nothing
        to do - not Windows, or stdout is redirected rather than a console
    """
    if sys.platform != "win32":
        return None

    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(STD_OUTPUT_HANDLE)
        if handle in (0, INVALID_HANDLE_VALUE, None):
            return None

        mode = ctypes.c_uint32()
        # Fails when stdout is a pipe or a file: nothing there reads escapes,
        # and nothing there is confused by them either.
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return None

        if mode.value & ENABLE_VIRTUAL_TERMINAL_PROCESSING:
            return True

        wanted = mode.value | ENABLE_VIRTUAL_TERMINAL_PROCESSING
        if not kernel32.SetConsoleMode(handle, wanted):
            return False

        # Read it back rather than trusting the return code, exactly as above:
        # older consoles report success and keep the old mode.
        check = ctypes.c_uint32()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(check)):
            return False

        return bool(check.value & ENABLE_VIRTUAL_TERMINAL_PROCESSING)
    except Exception:
        return False


def enable_ctrl_c_handling() -> Optional[bool]:
    """
    Make sure this process can still be asked to stop.

    A console process can have Ctrl+C processing switched OFF entirely, and that
    setting is INHERITED by everything it launches. A parent that turned it off
    leaves the child unable to receive the one signal a graceful shutdown depends
    on - and the failure is completely silent: the event is accepted by the
    console, the handler never runs, and the process simply keeps going.

    Measured on 2026-09-21 while testing how NSSM stops the service. Launched from
    a shell that had it off, the collector ignored a console Ctrl+C for a full
    minute and never wrote its archive. The same build, with this call added,
    shut down in under a second. Nothing about the collector had changed.

    NSSM's `AppStopMethodConsole` sends exactly that event, and a service started
    by the Service Control Manager should inherit nothing - but "should" is what
    this call removes from the sentence, for one line and no cost.

    Never raises: a console that will not take the change is a reason to log, not
    a reason to refuse to collect.

    Returns:
        True when Ctrl+C processing is on, False when the call was refused, and
        None when there is nothing to do - not Windows
    """
    if sys.platform != "win32":
        return None

    try:
        import ctypes

        # A NULL handler with Add=False removes the "ignore Ctrl+C" entry that
        # was inherited. With Add=True it would install it, which is the exact
        # opposite and the mistake this comment exists to prevent.
        return bool(ctypes.windll.kernel32.SetConsoleCtrlHandler(None, False))
    except Exception:
        return False
