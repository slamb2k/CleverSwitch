"""Unit tests for subscriber/info_task_orchestrator.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cleverswitch.event.info_task_progress_event import InfoTaskProgressEvent
from src.cleverswitch.hidpp.constants import BOLT_PID
from src.cleverswitch.model.logi_device import LogiDevice
from src.cleverswitch.subscriber.info_task_orchestrator import InfoTaskOrchestrator
from src.cleverswitch.subscriber.task.constants import Task
from src.cleverswitch.topic.topic import Topic
from src.cleverswitch.topic.topics import Topics

PID = BOLT_PID
SLOT = 1


def _make_device(pending=None, connected=True):
    d = LogiDevice(wpid=0x407B, pid=PID, slot=SLOT, role=None, available_features={})
    d.connected = connected
    if pending is not None:
        d.pending_steps = set(pending)
    return d


def _make_topics():
    return Topics(
        hid_event=MagicMock(spec=Topic),
        write=MagicMock(spec=Topic),
        device_info=MagicMock(spec=Topic),
        flags=MagicMock(spec=Topic),
        info_progress=MagicMock(spec=Topic),
    )


def _make_orchestrator(topics=None, registry=None):
    if topics is None:
        topics = _make_topics()
    if registry is None:
        registry = MagicMock()
    return InfoTaskOrchestrator(registry, topics), topics, registry


def _progress(device, step_name=Task.Feature.Name.CID_REPORTING, success=True):
    return InfoTaskProgressEvent(slot=device.slot, pid=device.pid, step_name=step_name, success=success, device=device)


def _drain_timers():
    import threading
    import time

    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        timers = [t for t in threading.enumerate() if isinstance(t, threading.Timer)]
        if not timers:
            return
        for t in timers:
            t.join(timeout=0.1)


def test_logs_fully_discovered_when_no_pending_on_success(caplog):
    device = _make_device(pending=set())
    orch, topics, _ = _make_orchestrator()

    import logging

    with caplog.at_level(logging.INFO):
        orch.notify(_progress(device, success=True))

    assert "fully discovered" in caplog.text.lower()


def test_no_log_when_pending_steps_remain_on_success(caplog):
    device = _make_device(pending={"other_step"})
    orch, topics, _ = _make_orchestrator()

    import logging

    with caplog.at_level(logging.INFO):
        orch.notify(_progress(device, success=True))

    assert "fully discovered" not in caplog.text.lower()


def test_retries_when_device_connected():
    device = _make_device(pending={Task.Feature.Name.CID_REPORTING}, connected=True)
    orch, topics, _ = _make_orchestrator()

    with (
        patch("src.cleverswitch.subscriber.info_task_orchestrator._TASK_FACTORIES") as mock_factories,
        patch("src.cleverswitch.subscriber.info_task_orchestrator.RETRY_BASE_DELAY", 0.0),
    ):
        mock_task = MagicMock()
        mock_factories.__getitem__ = MagicMock(return_value=MagicMock(return_value=mock_task))
        orch.notify(_progress(device, step_name=Task.Feature.Name.CID_REPORTING, success=False))
        _drain_timers()
        mock_task.start.assert_called_once()


def test_no_retry_when_device_disconnected():
    device = _make_device(pending={Task.Feature.Name.CID_REPORTING}, connected=False)
    orch, topics, _ = _make_orchestrator()

    with (
        patch("src.cleverswitch.subscriber.info_task_orchestrator._TASK_FACTORIES") as mock_factories,
        patch("src.cleverswitch.subscriber.info_task_orchestrator.RETRY_BASE_DELAY", 0.0),
    ):
        mock_task = MagicMock()
        mock_factories.__getitem__ = MagicMock(return_value=MagicMock(return_value=mock_task))
        orch.notify(_progress(device, step_name=Task.Feature.Name.CID_REPORTING, success=False))
        _drain_timers()
        mock_task.start.assert_not_called()


def test_retry_delay_backs_off_exponentially():
    device = _make_device(pending={Task.Feature.Name.CID_REPORTING}, connected=True)
    orch, topics, _ = _make_orchestrator()

    delays = []
    with patch("src.cleverswitch.subscriber.info_task_orchestrator.threading.Timer") as mock_timer:
        for _ in range(3):
            orch.notify(_progress(device, step_name=Task.Feature.Name.CID_REPORTING, success=False))
        delays = [call.args[0] for call in mock_timer.call_args_list]

    assert delays == [0.5, 1.0, 2.0]


def test_retry_delay_capped_at_max():
    device = _make_device(pending={Task.Feature.Name.CID_REPORTING}, connected=True)
    orch, topics, _ = _make_orchestrator()

    with (
        patch("src.cleverswitch.subscriber.info_task_orchestrator.threading.Timer") as mock_timer,
        patch("src.cleverswitch.subscriber.info_task_orchestrator.RETRY_MAX_ATTEMPTS", 10),
    ):
        for _ in range(10):
            orch.notify(_progress(device, step_name=Task.Feature.Name.CID_REPORTING, success=False))
        delays = [call.args[0] for call in mock_timer.call_args_list]

    assert max(delays) == 10.0
    assert delays[-1] == 10.0


def test_retry_stops_after_max_attempts(caplog):
    import logging

    device = _make_device(pending={Task.Feature.Name.CID_REPORTING}, connected=True)
    orch, topics, _ = _make_orchestrator()

    with patch("src.cleverswitch.subscriber.info_task_orchestrator.threading.Timer") as mock_timer:
        for _ in range(5):
            orch.notify(_progress(device, step_name=Task.Feature.Name.CID_REPORTING, success=False))
        assert mock_timer.call_count == 5

        with caplog.at_level(logging.WARNING):
            orch.notify(_progress(device, step_name=Task.Feature.Name.CID_REPORTING, success=False))

        assert mock_timer.call_count == 5  # no further timer scheduled
        assert "giving up" in caplog.text.lower()


def test_retry_attempts_reset_on_success():
    device = _make_device(pending={Task.Feature.Name.CID_REPORTING}, connected=True)
    orch, topics, _ = _make_orchestrator()

    with patch("src.cleverswitch.subscriber.info_task_orchestrator.threading.Timer") as mock_timer:
        orch.notify(_progress(device, step_name=Task.Feature.Name.CID_REPORTING, success=False))
        orch.notify(_progress(device, step_name=Task.Feature.Name.CID_REPORTING, success=True))
        orch.notify(_progress(device, step_name=Task.Feature.Name.CID_REPORTING, success=False))
        delays = [call.args[0] for call in mock_timer.call_args_list]

    assert delays == [0.5, 0.5]  # second failure restarts from base delay


def test_logs_fully_discovered_only_once_per_device(caplog):
    device = _make_device(pending=set())
    orch, topics, _ = _make_orchestrator()

    import logging

    with caplog.at_level(logging.INFO):
        orch.notify(_progress(device, step_name=Task.Name.GET_DEVICE_NAME, success=True))
        orch.notify(_progress(device, step_name=Task.Name.GET_DEVICE_TYPE, success=True))

    assert caplog.text.lower().count("fully discovered") == 1


def test_ignores_non_progress_events():
    orch, topics, _ = _make_orchestrator()
    orch.notify("not an event")  # must not raise
