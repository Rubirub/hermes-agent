"""Tests for cron/scheduler.py — origin resolution, delivery routing, and error logging."""

import json
import logging
import multiprocessing
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

import cron.scheduler as scheduler
from cron.jobs import claim_due_jobs, create_job, get_job, load_jobs, save_jobs
from cron.scheduler import (
    _build_job_prompt,
    _deliver_result,
    _resolve_delivery_target,
    _resolve_origin,
    _send_media_via_adapter,
    run_job,
    SILENT_MARKER,
    tick,
)


@pytest.fixture()
def cron_runtime(tmp_path, monkeypatch):
    cron_dir = tmp_path / "cron"
    monkeypatch.setattr("cron.jobs.CRON_DIR", cron_dir)
    monkeypatch.setattr("cron.jobs.JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr("cron.jobs.OUTPUT_DIR", cron_dir / "output")
    monkeypatch.setattr("cron.scheduler._LOCK_DIR", cron_dir)
    monkeypatch.setattr("cron.scheduler._LOCK_FILE", cron_dir / ".tick.lock")
    monkeypatch.setattr("cron.scheduler._JOB_LOCK_DIR", cron_dir / "locks")

    scheduler.shutdown_worker_pool(wait=True, cancel_futures=True)

    yield cron_dir

    scheduler.shutdown_worker_pool(wait=True, cancel_futures=True)


def _wait_for_cron_workers(timeout_s: float = 2.5) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if scheduler._active_worker_count() == 0:
            return
        time.sleep(0.01)
    raise AssertionError("Timed out waiting for cron workers to finish")


@contextmanager
def _noop_scheduler_lock(*, non_blocking: bool):
    """Test helper: bypass scheduler file locking."""
    yield object()


class TestResolveOrigin:
    def test_full_origin(self):
        job = {
            "origin": {
                "platform": "telegram",
                "chat_id": "123456",
                "chat_name": "Test Chat",
                "thread_id": "42",
            }
        }
        result = _resolve_origin(job)
        assert isinstance(result, dict)
        assert result == job["origin"]
        assert result["platform"] == "telegram"
        assert result["chat_id"] == "123456"
        assert result["chat_name"] == "Test Chat"
        assert result["thread_id"] == "42"

    def test_no_origin(self):
        assert _resolve_origin({}) is None
        assert _resolve_origin({"origin": None}) is None

    def test_missing_platform(self):
        job = {"origin": {"chat_id": "123"}}
        assert _resolve_origin(job) is None

    def test_missing_chat_id(self):
        job = {"origin": {"platform": "telegram"}}
        assert _resolve_origin(job) is None

    def test_empty_origin(self):
        job = {"origin": {}}
        assert _resolve_origin(job) is None


class TestResolveDeliveryTarget:
    def test_origin_delivery_preserves_thread_id(self):
        job = {
            "deliver": "origin",
            "origin": {
                "platform": "telegram",
                "chat_id": "-1001",
                "thread_id": "17585",
            },
        }

        assert _resolve_delivery_target(job) == {
            "platform": "telegram",
            "chat_id": "-1001",
            "thread_id": "17585",
        }

    def test_explicit_telegram_topic_target_with_thread_id(self):
        """deliver: 'telegram:chat_id:thread_id' parses correctly."""
        job = {
            "deliver": "telegram:-1003724596514:17",
        }
        assert _resolve_delivery_target(job) == {
            "platform": "telegram",
            "chat_id": "-1003724596514",
            "thread_id": "17",
        }

    def test_explicit_telegram_chat_id_without_thread_id(self):
        """deliver: 'telegram:chat_id' sets thread_id to None."""
        job = {
            "deliver": "telegram:-1003724596514",
        }
        assert _resolve_delivery_target(job) == {
            "platform": "telegram",
            "chat_id": "-1003724596514",
            "thread_id": None,
        }

    def test_human_friendly_label_resolved_via_channel_directory(self):
        """deliver: 'whatsapp:Alice (dm)' resolves to the real JID."""
        job = {"deliver": "whatsapp:Alice (dm)"}
        with patch(
            "gateway.channel_directory.resolve_channel_name",
            return_value="12345678901234@lid",
        ) as resolve_mock:
            result = _resolve_delivery_target(job)
        resolve_mock.assert_called_once_with("whatsapp", "Alice (dm)")
        assert result == {
            "platform": "whatsapp",
            "chat_id": "12345678901234@lid",
            "thread_id": None,
        }

    def test_human_friendly_label_without_suffix_resolved(self):
        """deliver: 'telegram:My Group' resolves without display suffix."""
        job = {"deliver": "telegram:My Group"}
        with patch(
            "gateway.channel_directory.resolve_channel_name",
            return_value="-1009999",
        ):
            result = _resolve_delivery_target(job)
        assert result == {
            "platform": "telegram",
            "chat_id": "-1009999",
            "thread_id": None,
        }

    def test_human_friendly_topic_label_preserves_thread_id(self):
        """Resolved Telegram topic labels should split chat_id and thread_id."""
        job = {"deliver": "telegram:Coaching Chat / topic 17585 (group)"}
        with patch(
            "gateway.channel_directory.resolve_channel_name",
            return_value="-1009999:17585",
        ):
            result = _resolve_delivery_target(job)
        assert result == {
            "platform": "telegram",
            "chat_id": "-1009999",
            "thread_id": "17585",
        }

    def test_raw_id_not_mangled_when_directory_returns_none(self):
        """deliver: 'whatsapp:12345@lid' passes through when directory has no match."""
        job = {"deliver": "whatsapp:12345@lid"}
        with patch(
            "gateway.channel_directory.resolve_channel_name",
            return_value=None,
        ):
            result = _resolve_delivery_target(job)
        assert result == {
            "platform": "whatsapp",
            "chat_id": "12345@lid",
            "thread_id": None,
        }

    def test_bare_platform_uses_matching_origin_chat(self):
        job = {
            "deliver": "telegram",
            "origin": {
                "platform": "telegram",
                "chat_id": "-1001",
                "thread_id": "17585",
            },
        }

        assert _resolve_delivery_target(job) == {
            "platform": "telegram",
            "chat_id": "-1001",
            "thread_id": "17585",
        }

    def test_bare_platform_falls_back_to_home_channel(self, monkeypatch):
        monkeypatch.setenv("TELEGRAM_HOME_CHANNEL", "-2002")
        job = {
            "deliver": "telegram",
            "origin": {
                "platform": "discord",
                "chat_id": "abc",
            },
        }

        assert _resolve_delivery_target(job) == {
            "platform": "telegram",
            "chat_id": "-2002",
            "thread_id": None,
        }


class TestDeliverResultWrapping:
    """Verify that cron deliveries are wrapped with header/footer and no longer mirrored."""

    def test_delivery_wraps_content_with_header_and_footer(self):
        """Delivered content should include task name header and agent-invisible note."""
        from gateway.config import Platform

        pconfig = MagicMock()
        pconfig.enabled = True
        mock_cfg = MagicMock()
        mock_cfg.platforms = {Platform.TELEGRAM: pconfig}

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("tools.send_message_tool._send_to_platform", new=AsyncMock(return_value={"success": True})) as send_mock:
            job = {
                "id": "test-job",
                "name": "daily-report",
                "deliver": "origin",
                "origin": {"platform": "telegram", "chat_id": "123"},
            }
            _deliver_result(job, "Here is today's summary.")

        send_mock.assert_called_once()
        sent_content = send_mock.call_args.kwargs.get("content") or send_mock.call_args[0][-1]
        assert "Cronjob Response: daily-report" in sent_content
        assert "-------------" in sent_content
        assert "Here is today's summary." in sent_content
        assert "The agent cannot see this message" in sent_content

    def test_delivery_uses_job_id_when_no_name(self):
        """When a job has no name, the wrapper should fall back to job id."""
        from gateway.config import Platform

        pconfig = MagicMock()
        pconfig.enabled = True
        mock_cfg = MagicMock()
        mock_cfg.platforms = {Platform.TELEGRAM: pconfig}

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("tools.send_message_tool._send_to_platform", new=AsyncMock(return_value={"success": True})) as send_mock:
            job = {
                "id": "abc-123",
                "deliver": "origin",
                "origin": {"platform": "telegram", "chat_id": "123"},
            }
            _deliver_result(job, "Output.")

        sent_content = send_mock.call_args.kwargs.get("content") or send_mock.call_args[0][-1]
        assert "Cronjob Response: abc-123" in sent_content

    def test_delivery_skips_wrapping_when_config_disabled(self):
        """When cron.wrap_response is false, deliver raw content without header/footer."""
        from gateway.config import Platform

        pconfig = MagicMock()
        pconfig.enabled = True
        mock_cfg = MagicMock()
        mock_cfg.platforms = {Platform.TELEGRAM: pconfig}

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("tools.send_message_tool._send_to_platform", new=AsyncMock(return_value={"success": True})) as send_mock, \
             patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}):
            job = {
                "id": "test-job",
                "name": "daily-report",
                "deliver": "origin",
                "origin": {"platform": "telegram", "chat_id": "123"},
            }
            _deliver_result(job, "Clean output only.")

        send_mock.assert_called_once()
        sent_content = send_mock.call_args.kwargs.get("content") or send_mock.call_args[0][-1]
        assert sent_content == "Clean output only."
        assert "Cronjob Response" not in sent_content
        assert "The agent cannot see" not in sent_content

    def test_delivery_extracts_media_tags_before_send(self):
        """Cron delivery should pass MEDIA attachments separately to the send helper."""
        from gateway.config import Platform

        pconfig = MagicMock()
        pconfig.enabled = True
        mock_cfg = MagicMock()
        mock_cfg.platforms = {Platform.TELEGRAM: pconfig}

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("tools.send_message_tool._send_to_platform", new=AsyncMock(return_value={"success": True})) as send_mock, \
             patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}):
            job = {
                "id": "voice-job",
                "deliver": "origin",
                "origin": {"platform": "telegram", "chat_id": "123"},
            }
            _deliver_result(job, "Title\nMEDIA:/tmp/test-voice.ogg")

        send_mock.assert_called_once()
        args, kwargs = send_mock.call_args
        # Text content should have MEDIA: tag stripped
        assert "MEDIA:" not in args[3]
        assert "Title" in args[3]
        # Media files should be forwarded separately
        assert kwargs["media_files"] == [("/tmp/test-voice.ogg", False)]

    def test_live_adapter_sends_media_as_attachments(self):
        """When a live adapter is available, MEDIA files should be sent as native
        platform attachments (e.g., Discord voice, Telegram audio) rather than
        as literal 'MEDIA:/path' text."""
        from gateway.config import Platform
        from concurrent.futures import Future

        adapter = AsyncMock()
        adapter.send.return_value = MagicMock(success=True)
        adapter.send_voice.return_value = MagicMock(success=True)

        pconfig = MagicMock()
        pconfig.enabled = True
        mock_cfg = MagicMock()
        mock_cfg.platforms = {Platform.DISCORD: pconfig}

        loop = MagicMock()
        loop.is_running.return_value = True

        # run_coroutine_threadsafe returns concurrent.futures.Future (has timeout kwarg)
        def fake_run_coro(coro, _loop):
            future = Future()
            future.set_result(MagicMock(success=True))
            coro.close()
            return future

        job = {
            "id": "tts-job",
            "deliver": "origin",
            "origin": {"platform": "discord", "chat_id": "9876"},
        }

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}), \
             patch("asyncio.run_coroutine_threadsafe", side_effect=fake_run_coro):
            _deliver_result(
                job,
                "Here is TTS\nMEDIA:/tmp/cron-voice.mp3",
                adapters={Platform.DISCORD: adapter},
                loop=loop,
            )

        # Text should be sent without the MEDIA tag
        adapter.send.assert_called_once()
        text_sent = adapter.send.call_args[0][1]
        assert "MEDIA:" not in text_sent
        assert "Here is TTS" in text_sent

        # Audio file should be sent as a voice attachment
        adapter.send_voice.assert_called_once()
        voice_call = adapter.send_voice.call_args
        assert voice_call[1]["audio_path"] == "/tmp/cron-voice.mp3"

    def test_live_adapter_routes_image_to_send_image_file(self):
        """Image MEDIA files should be routed to send_image_file, not send_voice."""
        from gateway.config import Platform
        from concurrent.futures import Future

        adapter = AsyncMock()
        adapter.send.return_value = MagicMock(success=True)
        adapter.send_image_file.return_value = MagicMock(success=True)

        pconfig = MagicMock()
        pconfig.enabled = True
        mock_cfg = MagicMock()
        mock_cfg.platforms = {Platform.DISCORD: pconfig}

        loop = MagicMock()
        loop.is_running.return_value = True

        def fake_run_coro(coro, _loop):
            future = Future()
            future.set_result(MagicMock(success=True))
            coro.close()
            return future

        job = {
            "id": "img-job",
            "deliver": "origin",
            "origin": {"platform": "discord", "chat_id": "1234"},
        }

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}), \
             patch("asyncio.run_coroutine_threadsafe", side_effect=fake_run_coro):
            _deliver_result(
                job,
                "Chart attached\nMEDIA:/tmp/chart.png",
                adapters={Platform.DISCORD: adapter},
                loop=loop,
            )

        adapter.send_image_file.assert_called_once()
        assert adapter.send_image_file.call_args[1]["image_path"] == "/tmp/chart.png"
        adapter.send_voice.assert_not_called()

    def test_live_adapter_media_only_no_text(self):
        """When content is ONLY a MEDIA tag with no text, media should still be sent."""
        from gateway.config import Platform
        from concurrent.futures import Future

        adapter = AsyncMock()
        adapter.send_voice.return_value = MagicMock(success=True)

        pconfig = MagicMock()
        pconfig.enabled = True
        mock_cfg = MagicMock()
        mock_cfg.platforms = {Platform.TELEGRAM: pconfig}

        loop = MagicMock()
        loop.is_running.return_value = True

        def fake_run_coro(coro, _loop):
            future = Future()
            future.set_result(MagicMock(success=True))
            coro.close()
            return future

        job = {
            "id": "voice-only",
            "deliver": "origin",
            "origin": {"platform": "telegram", "chat_id": "999"},
        }

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}), \
             patch("asyncio.run_coroutine_threadsafe", side_effect=fake_run_coro):
            _deliver_result(
                job,
                "MEDIA:/tmp/voice.ogg",
                adapters={Platform.TELEGRAM: adapter},
                loop=loop,
            )

        # Text send should NOT be called (no text after stripping MEDIA tag)
        adapter.send.assert_not_called()
        # Audio should still be delivered
        adapter.send_voice.assert_called_once()

    def test_live_adapter_sends_cleaned_text_not_raw(self):
        """The live adapter path must send cleaned text (MEDIA tags stripped),
        not the raw delivery_content with embedded MEDIA: tags."""
        from gateway.config import Platform
        from concurrent.futures import Future

        adapter = AsyncMock()
        adapter.send.return_value = MagicMock(success=True)

        pconfig = MagicMock()
        pconfig.enabled = True
        mock_cfg = MagicMock()
        mock_cfg.platforms = {Platform.TELEGRAM: pconfig}

        loop = MagicMock()
        loop.is_running.return_value = True

        def fake_run_coro(coro, _loop):
            future = Future()
            future.set_result(MagicMock(success=True))
            coro.close()
            return future

        job = {
            "id": "img-job",
            "deliver": "origin",
            "origin": {"platform": "telegram", "chat_id": "555"},
        }

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}), \
             patch("asyncio.run_coroutine_threadsafe", side_effect=fake_run_coro):
            _deliver_result(
                job,
                "Report\nMEDIA:/tmp/chart.png",
                adapters={Platform.TELEGRAM: adapter},
                loop=loop,
            )

        text_sent = adapter.send.call_args[0][1]
        assert "MEDIA:" not in text_sent
        assert "Report" in text_sent

    def test_no_mirror_to_session_call(self):
        """Cron deliveries should NOT mirror into the gateway session."""
        from gateway.config import Platform

        pconfig = MagicMock()
        pconfig.enabled = True
        mock_cfg = MagicMock()
        mock_cfg.platforms = {Platform.TELEGRAM: pconfig}

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("tools.send_message_tool._send_to_platform", new=AsyncMock(return_value={"success": True})), \
             patch("gateway.mirror.mirror_to_session") as mirror_mock:
            job = {
                "id": "test-job",
                "deliver": "origin",
                "origin": {"platform": "telegram", "chat_id": "123"},
            }
            _deliver_result(job, "Hello!")

        mirror_mock.assert_not_called()

    def test_origin_delivery_preserves_thread_id(self):
        """Origin delivery should forward thread_id to the send helper."""
        from gateway.config import Platform

        pconfig = MagicMock()
        pconfig.enabled = True
        mock_cfg = MagicMock()
        mock_cfg.platforms = {Platform.TELEGRAM: pconfig}

        job = {
            "id": "test-job",
            "name": "topic-job",
            "deliver": "origin",
            "origin": {
                "platform": "telegram",
                "chat_id": "-1001",
                "thread_id": "17585",
            },
        }

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("tools.send_message_tool._send_to_platform", new=AsyncMock(return_value={"success": True})) as send_mock:
            _deliver_result(job, "hello")

        send_mock.assert_called_once()
        assert send_mock.call_args.kwargs["thread_id"] == "17585"


class TestDeliverResultErrorReturns:
    """Verify _deliver_result returns error strings on failure, None on success."""

    def test_returns_none_on_successful_delivery(self):
        from gateway.config import Platform

        pconfig = MagicMock()
        pconfig.enabled = True
        mock_cfg = MagicMock()
        mock_cfg.platforms = {Platform.TELEGRAM: pconfig}

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("tools.send_message_tool._send_to_platform", new=AsyncMock(return_value={"success": True})):
            job = {
                "id": "ok-job",
                "deliver": "origin",
                "origin": {"platform": "telegram", "chat_id": "123"},
            }
            result = _deliver_result(job, "Output.")
        assert result is None

    def test_returns_none_for_local_delivery(self):
        """local-only jobs don't deliver — not a failure."""
        job = {"id": "local-job", "deliver": "local"}
        result = _deliver_result(job, "Output.")
        assert result is None

    def test_returns_error_for_unknown_platform(self):
        job = {
            "id": "bad-platform",
            "deliver": "origin",
            "origin": {"platform": "fax", "chat_id": "123"},
        }
        with patch("gateway.config.load_gateway_config"):
            result = _deliver_result(job, "Output.")
        assert result is not None
        assert "unknown platform" in result

    def test_returns_error_when_platform_disabled(self):
        from gateway.config import Platform

        pconfig = MagicMock()
        pconfig.enabled = False
        mock_cfg = MagicMock()
        mock_cfg.platforms = {Platform.TELEGRAM: pconfig}

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg):
            job = {
                "id": "disabled",
                "deliver": "origin",
                "origin": {"platform": "telegram", "chat_id": "123"},
            }
            result = _deliver_result(job, "Output.")
        assert result is not None
        assert "not configured" in result

    def test_returns_error_on_send_failure(self):
        from gateway.config import Platform

        pconfig = MagicMock()
        pconfig.enabled = True
        mock_cfg = MagicMock()
        mock_cfg.platforms = {Platform.TELEGRAM: pconfig}

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("tools.send_message_tool._send_to_platform", new=AsyncMock(return_value={"error": "rate limited"})):
            job = {
                "id": "rate-limited",
                "deliver": "origin",
                "origin": {"platform": "telegram", "chat_id": "123"},
            }
            result = _deliver_result(job, "Output.")
        assert result is not None
        assert "rate limited" in result

    def test_returns_error_for_unresolved_target(self, monkeypatch):
        """Non-local delivery with no resolvable target should return an error."""
        monkeypatch.delenv("TELEGRAM_HOME_CHANNEL", raising=False)
        job = {"id": "no-target", "deliver": "telegram"}
        result = _deliver_result(job, "Output.")
        assert result is not None
        assert "no delivery target" in result


class TestRunJobSessionPersistence:
    def test_run_job_passes_session_db_and_cron_platform(self, tmp_path):
        job = {
            "id": "test-job",
            "name": "test",
            "prompt": "hello",
        }
        fake_db = MagicMock()

        with patch("cron.scheduler._hermes_home", tmp_path), \
             patch("cron.scheduler._resolve_origin", return_value=None), \
             patch("dotenv.load_dotenv"), \
             patch("hermes_state.SessionDB", return_value=fake_db), \
             patch(
                 "hermes_cli.runtime_provider.resolve_runtime_provider",
                 return_value={
                     "api_key": "test-key",
                     "base_url": "https://example.invalid/v1",
                     "provider": "openrouter",
                     "api_mode": "chat_completions",
                 },
             ), \
             patch("run_agent.AIAgent") as mock_agent_cls:
            mock_agent = MagicMock()
            mock_agent.run_conversation.return_value = {"final_response": "ok"}
            mock_agent_cls.return_value = mock_agent

            success, output, final_response, error = run_job(job)

        assert success is True
        assert error is None
        assert final_response == "ok"
        assert "ok" in output

        kwargs = mock_agent_cls.call_args.kwargs
        assert kwargs["session_db"] is fake_db
        assert kwargs["platform"] == "cron"
        assert kwargs["session_id"].startswith("cron_test-job_")
        fake_db.end_session.assert_called_once()
        call_args = fake_db.end_session.call_args
        assert call_args[0][0].startswith("cron_test-job_")
        assert call_args[0][1] == "cron_complete"
        fake_db.close.assert_called_once()

    def test_run_job_empty_response_returns_empty_not_placeholder(self, tmp_path):
        """Empty final_response should stay empty for delivery logic (issue #2234).
        
        The placeholder '(No response generated)' should only appear in the
        output log, not in the returned final_response that's used for delivery.
        """
        job = {
            "id": "silent-job",
            "name": "silent test",
            "prompt": "do work via tools only",
        }
        fake_db = MagicMock()

        with patch("cron.scheduler._hermes_home", tmp_path), \
             patch("cron.scheduler._resolve_origin", return_value=None), \
             patch("dotenv.load_dotenv"), \
             patch("hermes_state.SessionDB", return_value=fake_db), \
             patch(
                 "hermes_cli.runtime_provider.resolve_runtime_provider",
                 return_value={
                     "api_key": "test-key",
                     "base_url": "https://example.invalid/v1",
                     "provider": "openrouter",
                     "api_mode": "chat_completions",
                 },
             ), \
             patch("run_agent.AIAgent") as mock_agent_cls:
            mock_agent = MagicMock()
            # Agent did work via tools but returned no text
            mock_agent.run_conversation.return_value = {"final_response": ""}
            mock_agent_cls.return_value = mock_agent

            success, output, final_response, error = run_job(job)

        assert success is True
        assert error is None
        # final_response should be empty for delivery logic to skip
        assert final_response == ""
        # But the output log should show the placeholder
        assert "(No response generated)" in output

    def test_run_job_sets_auto_delivery_env_from_dotenv_home_channel(self, tmp_path, monkeypatch):
        job = {
            "id": "test-job",
            "name": "test",
            "prompt": "hello",
            "deliver": "telegram",
        }
        fake_db = MagicMock()
        seen = {}

        (tmp_path / ".env").write_text("TELEGRAM_HOME_CHANNEL=-2002\n")
        monkeypatch.delenv("TELEGRAM_HOME_CHANNEL", raising=False)
        monkeypatch.delenv("HERMES_CRON_AUTO_DELIVER_PLATFORM", raising=False)
        monkeypatch.delenv("HERMES_CRON_AUTO_DELIVER_CHAT_ID", raising=False)
        monkeypatch.delenv("HERMES_CRON_AUTO_DELIVER_THREAD_ID", raising=False)

        class FakeAgent:
            def __init__(self, *args, **kwargs):
                pass

            def run_conversation(self, *args, **kwargs):
                seen["platform"] = os.getenv("HERMES_CRON_AUTO_DELIVER_PLATFORM")
                seen["chat_id"] = os.getenv("HERMES_CRON_AUTO_DELIVER_CHAT_ID")
                seen["thread_id"] = os.getenv("HERMES_CRON_AUTO_DELIVER_THREAD_ID")
                return {"final_response": "ok"}

        with patch("cron.scheduler._hermes_home", tmp_path), \
             patch("hermes_state.SessionDB", return_value=fake_db), \
             patch(
                 "hermes_cli.runtime_provider.resolve_runtime_provider",
                 return_value={
                     "api_key": "***",
                     "base_url": "https://example.invalid/v1",
                     "provider": "openrouter",
                     "api_mode": "chat_completions",
                 },
             ), \
             patch("run_agent.AIAgent", FakeAgent):
            success, output, final_response, error = run_job(job)

        assert success is True
        assert error is None
        assert final_response == "ok"
        assert "ok" in output
        assert seen == {
            "platform": "telegram",
            "chat_id": "-2002",
            "thread_id": None,
        }
        assert os.getenv("HERMES_CRON_AUTO_DELIVER_PLATFORM") is None
        assert os.getenv("HERMES_CRON_AUTO_DELIVER_CHAT_ID") is None
        assert os.getenv("HERMES_CRON_AUTO_DELIVER_THREAD_ID") is None
        fake_db.close.assert_called_once()


class TestRunJobConfigLogging:
    """Verify that config.yaml parse failures are logged, not silently swallowed."""

    def test_bad_config_yaml_is_logged(self, caplog, tmp_path):
        """When config.yaml is malformed, a warning should be logged."""
        bad_yaml = tmp_path / "config.yaml"
        bad_yaml.write_text("invalid: yaml: [[[bad")

        job = {
            "id": "test-job",
            "name": "test",
            "prompt": "hello",
        }

        with patch("cron.scheduler._hermes_home", tmp_path), \
             patch("cron.scheduler._resolve_origin", return_value=None), \
             patch("dotenv.load_dotenv"), \
             patch("run_agent.AIAgent") as mock_agent_cls:
            mock_agent = MagicMock()
            mock_agent.run_conversation.return_value = {"final_response": "ok"}
            mock_agent_cls.return_value = mock_agent

            with caplog.at_level(logging.WARNING, logger="cron.scheduler"):
                run_job(job)

        assert any("failed to load config.yaml" in r.message for r in caplog.records), \
            f"Expected 'failed to load config.yaml' warning in logs, got: {[r.message for r in caplog.records]}"

    def test_bad_prefill_messages_is_logged(self, caplog, tmp_path):
        """When the prefill messages file contains invalid JSON, a warning should be logged."""
        # Valid config.yaml that points to a bad prefill file
        config_yaml = tmp_path / "config.yaml"
        config_yaml.write_text("prefill_messages_file: prefill.json\n")

        bad_prefill = tmp_path / "prefill.json"
        bad_prefill.write_text("{not valid json!!!")

        job = {
            "id": "test-job",
            "name": "test",
            "prompt": "hello",
        }

        with patch("cron.scheduler._hermes_home", tmp_path), \
             patch("cron.scheduler._resolve_origin", return_value=None), \
             patch("dotenv.load_dotenv"), \
             patch("run_agent.AIAgent") as mock_agent_cls:
            mock_agent = MagicMock()
            mock_agent.run_conversation.return_value = {"final_response": "ok"}
            mock_agent_cls.return_value = mock_agent

            with caplog.at_level(logging.WARNING, logger="cron.scheduler"):
                run_job(job)

        assert any("failed to parse prefill messages" in r.message for r in caplog.records), \
            f"Expected 'failed to parse prefill messages' warning in logs, got: {[r.message for r in caplog.records]}"


class TestRunJobPerJobOverrides:
    def test_job_level_model_provider_and_base_url_overrides_are_used(self, tmp_path):
        config_yaml = tmp_path / "config.yaml"
        config_yaml.write_text(
            "model:\n"
            "  default: gpt-5.4\n"
            "  provider: openai-codex\n"
            "  base_url: https://chatgpt.com/backend-api/codex\n"
        )

        job = {
            "id": "briefing-job",
            "name": "briefing",
            "prompt": "hello",
            "model": "perplexity/sonar-pro",
            "provider": "custom",
            "base_url": "http://127.0.0.1:4000/v1",
        }

        fake_db = MagicMock()
        fake_runtime = {
            "provider": "openrouter",
            "api_mode": "chat_completions",
            "base_url": "http://127.0.0.1:4000/v1",
            "api_key": "***",
        }

        with patch("cron.scheduler._hermes_home", tmp_path), \
             patch("cron.scheduler._resolve_origin", return_value=None), \
             patch("dotenv.load_dotenv"), \
             patch("hermes_state.SessionDB", return_value=fake_db), \
             patch("hermes_cli.runtime_provider.resolve_runtime_provider", return_value=fake_runtime) as runtime_mock, \
             patch("run_agent.AIAgent") as mock_agent_cls:
            mock_agent = MagicMock()
            mock_agent.run_conversation.return_value = {"final_response": "ok"}
            mock_agent_cls.return_value = mock_agent

            success, output, final_response, error = run_job(job)

        assert success is True
        assert error is None
        assert final_response == "ok"
        assert "ok" in output
        runtime_mock.assert_called_once_with(
            requested="custom",
            explicit_base_url="http://127.0.0.1:4000/v1",
        )
        assert mock_agent_cls.call_args.kwargs["model"] == "perplexity/sonar-pro"
        fake_db.close.assert_called_once()


class TestRunJobSkillBacked:
    def test_run_job_loads_skill_and_disables_recursive_cron_tools(self, tmp_path):
        job = {
            "id": "skill-job",
            "name": "skill test",
            "prompt": "Check the feeds and summarize anything new.",
            "skill": "blogwatcher",
        }

        fake_db = MagicMock()

        with patch("cron.scheduler._hermes_home", tmp_path), \
             patch("cron.scheduler._resolve_origin", return_value=None), \
             patch("dotenv.load_dotenv"), \
             patch("hermes_state.SessionDB", return_value=fake_db), \
             patch(
                 "hermes_cli.runtime_provider.resolve_runtime_provider",
                 return_value={
                     "api_key": "***",
                     "base_url": "https://example.invalid/v1",
                     "provider": "openrouter",
                     "api_mode": "chat_completions",
                 },
             ), \
             patch("tools.skills_tool.skill_view", return_value=json.dumps({"success": True, "content": "# Blogwatcher\nFollow this skill."})), \
             patch("run_agent.AIAgent") as mock_agent_cls:
            mock_agent = MagicMock()
            mock_agent.run_conversation.return_value = {"final_response": "ok"}
            mock_agent_cls.return_value = mock_agent

            success, output, final_response, error = run_job(job)

        assert success is True
        assert error is None
        assert final_response == "ok"

        kwargs = mock_agent_cls.call_args.kwargs
        assert "cronjob" in (kwargs["disabled_toolsets"] or [])

        prompt_arg = mock_agent.run_conversation.call_args.args[0]
        assert "blogwatcher" in prompt_arg
        assert "Follow this skill" in prompt_arg
        assert "Check the feeds and summarize anything new." in prompt_arg

    def test_run_job_loads_multiple_skills_in_order(self, tmp_path):
        job = {
            "id": "multi-skill-job",
            "name": "multi skill test",
            "prompt": "Combine the results.",
            "skills": ["blogwatcher", "find-nearby"],
        }

        fake_db = MagicMock()

        def _skill_view(name):
            return json.dumps({"success": True, "content": f"# {name}\nInstructions for {name}."})

        with patch("cron.scheduler._hermes_home", tmp_path), \
             patch("cron.scheduler._resolve_origin", return_value=None), \
             patch("dotenv.load_dotenv"), \
             patch("hermes_state.SessionDB", return_value=fake_db), \
             patch(
                 "hermes_cli.runtime_provider.resolve_runtime_provider",
                 return_value={
                     "api_key": "***",
                     "base_url": "https://example.invalid/v1",
                     "provider": "openrouter",
                     "api_mode": "chat_completions",
                 },
             ), \
             patch("tools.skills_tool.skill_view", side_effect=_skill_view) as skill_view_mock, \
             patch("run_agent.AIAgent") as mock_agent_cls:
            mock_agent = MagicMock()
            mock_agent.run_conversation.return_value = {"final_response": "ok"}
            mock_agent_cls.return_value = mock_agent

            success, output, final_response, error = run_job(job)

        assert success is True
        assert error is None
        assert final_response == "ok"
        assert skill_view_mock.call_count == 2
        assert [call.args[0] for call in skill_view_mock.call_args_list] == ["blogwatcher", "find-nearby"]

        prompt_arg = mock_agent.run_conversation.call_args.args[0]
        assert prompt_arg.index("blogwatcher") < prompt_arg.index("find-nearby")
        assert "Instructions for blogwatcher." in prompt_arg
        assert "Instructions for find-nearby." in prompt_arg
        assert "Combine the results." in prompt_arg


class TestSilentDelivery:
    """Verify that [SILENT] responses suppress delivery while still saving output."""

    def _make_job(self):
        return {
            "id": "monitor-job",
            "name": "monitor",
            "deliver": "origin",
            "origin": {"platform": "telegram", "chat_id": "123"},
        }

    def test_normal_response_delivers(self):
        with patch("cron.scheduler._deliver_result") as deliver_mock:
            scheduler._deliver_job_result(self._make_job(), True, "Results here", None)
        deliver_mock.assert_called_once()

    def test_silent_response_suppresses_delivery(self, caplog):
        with patch("cron.scheduler._deliver_result") as deliver_mock:
            with caplog.at_level(logging.INFO, logger="cron.scheduler"):
                scheduler._deliver_job_result(self._make_job(), True, "[SILENT]", None)
        deliver_mock.assert_not_called()
        assert any(SILENT_MARKER in r.message for r in caplog.records)

    def test_silent_with_note_suppresses_delivery(self):
        with patch("cron.scheduler._deliver_result") as deliver_mock:
            scheduler._deliver_job_result(
                self._make_job(),
                True,
                "[SILENT] No changes detected",
                None,
            )
        deliver_mock.assert_not_called()

    def test_silent_trailing_suppresses_delivery(self):
        """Agent appended [SILENT] after explanation text — must still suppress."""
        response = "2 deals filtered out (like<10, reply<15).\n\n[SILENT]"
        with patch("cron.scheduler.get_due_jobs", return_value=[self._make_job()]), \
             patch("cron.scheduler.run_job", return_value=(True, "# output", response, None)), \
             patch("cron.scheduler.save_job_output", return_value="/tmp/out.md"), \
             patch("cron.scheduler._deliver_result") as deliver_mock, \
             patch("cron.scheduler.mark_job_run"):
            from cron.scheduler import tick
            tick(verbose=False)
        deliver_mock.assert_not_called()

    def test_silent_is_case_insensitive(self):
        with patch("cron.scheduler._deliver_result") as deliver_mock:
            scheduler._deliver_job_result(
                self._make_job(),
                True,
                "[silent] nothing new",
                None,
            )
        deliver_mock.assert_not_called()

    def test_failed_job_always_delivers(self):
        """Failed jobs deliver regardless of [SILENT] in output."""
        with patch("cron.scheduler._deliver_result") as deliver_mock:
            scheduler._deliver_job_result(self._make_job(), False, "", "some error")
        deliver_mock.assert_called_once()

    def test_output_saved_even_when_delivery_suppressed(self):
        with patch("cron.scheduler.save_job_output") as save_mock:
            save_mock.return_value = "/tmp/out.md"
            result = scheduler._save_job_output(
                self._make_job(),
                "# full output",
                verbose=False,
            )
        save_mock.assert_called_once_with("monitor-job", "# full output")
        assert result == "/tmp/out.md"


class TestBuildJobPromptSilentHint:
    """Verify _build_job_prompt always injects [SILENT] guidance."""

    def test_hint_always_present(self):
        job = {"prompt": "Check for updates"}
        result = _build_job_prompt(job)
        assert "[SILENT]" in result
        assert "Check for updates" in result

    def test_hint_present_even_without_prompt(self):
        job = {"prompt": ""}
        result = _build_job_prompt(job)
        assert "[SILENT]" in result

    def test_delivery_guidance_present(self):
        """Cron hint tells agents their final response is auto-delivered."""
        job = {"prompt": "Generate a report"}
        result = _build_job_prompt(job)
        assert "do NOT use send_message" in result
        assert "automatically delivered" in result

    def test_delivery_guidance_precedes_user_prompt(self):
        """System guidance appears before the user's prompt text."""
        job = {"prompt": "My custom prompt"}
        result = _build_job_prompt(job)
        system_pos = result.index("do NOT use send_message")
        prompt_pos = result.index("My custom prompt")
        assert system_pos < prompt_pos


class TestBuildJobPromptMissingSkill:
    """Verify that a missing skill logs a warning and does not crash the job."""

    def _missing_skill_view(self, name: str) -> str:
        return json.dumps({"success": False, "error": f"Skill '{name}' not found."})

    def test_missing_skill_does_not_raise(self):
        """Job should run even when a referenced skill is not installed."""
        with patch("tools.skills_tool.skill_view", side_effect=self._missing_skill_view):
            result = _build_job_prompt({"skills": ["ghost-skill"], "prompt": "do something"})
        # prompt is preserved even though skill was skipped
        assert "do something" in result

    def test_missing_skill_injects_user_notice_into_prompt(self):
        """A system notice about the missing skill is injected into the prompt."""
        with patch("tools.skills_tool.skill_view", side_effect=self._missing_skill_view):
            result = _build_job_prompt({"skills": ["ghost-skill"], "prompt": "do something"})
        assert "ghost-skill" in result
        assert "not found" in result.lower() or "skipped" in result.lower()

    def test_missing_skill_logs_warning(self, caplog):
        """A warning is logged when a skill cannot be found."""
        with caplog.at_level(logging.WARNING, logger="cron.scheduler"):
            with patch("tools.skills_tool.skill_view", side_effect=self._missing_skill_view):
                _build_job_prompt({"name": "My Job", "skills": ["ghost-skill"], "prompt": "do something"})
        assert any("ghost-skill" in record.message for record in caplog.records)

    def test_valid_skill_loaded_alongside_missing(self):
        """A valid skill is still loaded when another skill in the list is missing."""

        def _mixed_skill_view(name: str) -> str:
            if name == "real-skill":
                return json.dumps({"success": True, "content": "Real skill content."})
            return json.dumps({"success": False, "error": f"Skill '{name}' not found."})

        with patch("tools.skills_tool.skill_view", side_effect=_mixed_skill_view):
            result = _build_job_prompt({"skills": ["ghost-skill", "real-skill"], "prompt": "go"})
        assert "Real skill content." in result
        assert "go" in result


class TestParallelCronExecution:
    def _set_due_now(self):
        jobs = load_jobs()
        due_at = (scheduler._hermes_now() - timedelta(minutes=1)).isoformat()
        for job in jobs:
            job["next_run_at"] = due_at
        save_jobs(jobs)

    def test_cross_job_concurrency(self, cron_runtime, monkeypatch):
        create_job(prompt="A", schedule="every 1h", name="job-a")
        create_job(prompt="B", schedule="every 1h", name="job-b")
        self._set_due_now()

        monkeypatch.setattr("cron.scheduler.load_config", lambda: {"cron": {"max_parallel_jobs": 2, "wrap_response": False}})

        running = 0
        max_running = 0
        lock = threading.Lock()
        started = set()
        both_started = threading.Event()

        def fake_run_job(job):
            nonlocal running, max_running
            with lock:
                running += 1
                max_running = max(max_running, running)
                started.add(job["id"])
                if len(started) == 2:
                    both_started.set()
            both_started.wait(timeout=1.0)
            time.sleep(0.05)
            with lock:
                running -= 1
            return True, f"# output {job['id']}", f"done-{job['id']}", None

        with patch("cron.scheduler.run_job", side_effect=fake_run_job), \
             patch("cron.scheduler.save_job_output", return_value=cron_runtime / "out.md"), \
             patch("cron.scheduler._deliver_result"):
            dispatched = tick(verbose=False)
            assert dispatched == 2
            _wait_for_cron_workers()

        assert max_running >= 2, "expected different jobs to run concurrently"

    def test_same_job_non_overlap(self, cron_runtime, monkeypatch):
        job = create_job(prompt="single", schedule="every 1h", name="job-single")
        self._set_due_now()
        monkeypatch.setattr("cron.scheduler.load_config", lambda: {"cron": {"max_parallel_jobs": 2, "wrap_response": False}})

        started = threading.Event()
        release = threading.Event()
        run_count = 0

        def fake_run_job(_job):
            nonlocal run_count
            run_count += 1
            started.set()
            release.wait(timeout=1.0)
            return True, "# output", "done", None

        with patch("cron.scheduler.run_job", side_effect=fake_run_job), \
             patch("cron.scheduler.save_job_output", return_value=cron_runtime / "out.md"), \
             patch("cron.scheduler._deliver_result"):
            first = tick(verbose=False)
            assert first == 1
            assert started.wait(timeout=1.0)

            # Force due again while still running; in_flight ownership should prevent overlap.
            jobs = load_jobs()
            for rec in jobs:
                if rec["id"] == job["id"]:
                    rec["next_run_at"] = (scheduler._hermes_now() - timedelta(minutes=1)).isoformat()
            save_jobs(jobs)

            second = tick(verbose=False)
            assert second == 0

            release.set()
            _wait_for_cron_workers()

        assert run_count == 1

    def test_tick_recovers_orphan_and_dispatches_job(self, cron_runtime, monkeypatch):
        job = create_job(prompt="orphan", schedule="every 1h", name="job-orphan")
        self._set_due_now()
        monkeypatch.setattr("cron.scheduler.load_config", lambda: {"cron": {"max_parallel_jobs": 1, "wrap_response": False}})

        claimed = claim_due_jobs(now=scheduler._hermes_now(), owner_instance_id="instance-a", max_parallel=1)
        assert len(claimed) == 1

        monkeypatch.setattr(
            "cron.jobs._get_inflight_owner_state",
            lambda inflight, now_dt=None: ("dead", "owner pid not alive"),
        )
        monkeypatch.setattr("cron.jobs._orphan_recovery_grace_seconds", lambda: 0.0)

        with patch("cron.scheduler.run_job", return_value=(True, "# output", "done", None)), \
             patch("cron.scheduler.save_job_output", return_value=cron_runtime / "out.md"), \
             patch("cron.scheduler._deliver_result"):
            dispatched = tick(verbose=False)
            assert dispatched == 1
            _wait_for_cron_workers()

        updated = get_job(job["id"])
        assert updated is not None
        assert updated.get("in_flight") is None
        assert updated.get("last_status") == "ok"

    def test_tick_does_not_reclaim_when_owner_is_alive(self, cron_runtime, monkeypatch):
        job = create_job(prompt="alive", schedule="every 1h", name="job-alive")
        self._set_due_now()
        monkeypatch.setattr("cron.scheduler.load_config", lambda: {"cron": {"max_parallel_jobs": 1, "wrap_response": False}})

        claimed = claim_due_jobs(now=scheduler._hermes_now(), owner_instance_id="instance-a", max_parallel=1)
        assert len(claimed) == 1
        original_run_id = get_job(job["id"])["in_flight"]["run_id"]

        monkeypatch.setattr(
            "cron.jobs._get_inflight_owner_state",
            lambda inflight, now_dt=None: ("alive", "owner still alive"),
        )
        monkeypatch.setattr("cron.jobs._orphan_recovery_grace_seconds", lambda: 0.0)

        with patch("cron.scheduler.run_job") as run_job_mock, \
             patch("cron.scheduler.save_job_output") as save_mock, \
             patch("cron.scheduler._deliver_result") as deliver_mock:
            assert tick(verbose=False) == 0

        run_job_mock.assert_not_called()
        save_mock.assert_not_called()
        deliver_mock.assert_not_called()

        updated = get_job(job["id"])
        assert updated is not None
        assert updated.get("in_flight", {}).get("run_id") == original_run_id
        assert updated.get("last_status") is None

    def test_tick_waits_for_orphan_grace_before_reclaim(self, cron_runtime, monkeypatch):
        job = create_job(prompt="grace", schedule="every 1h", name="job-grace")
        self._set_due_now()
        monkeypatch.setattr("cron.scheduler.load_config", lambda: {"cron": {"max_parallel_jobs": 1, "wrap_response": False}})

        claimed = claim_due_jobs(now=scheduler._hermes_now(), owner_instance_id="instance-a", max_parallel=1)
        assert len(claimed) == 1

        monkeypatch.setattr(
            "cron.jobs._get_inflight_owner_state",
            lambda inflight, now_dt=None: ("dead", "owner pid not alive"),
        )
        monkeypatch.setattr("cron.jobs._orphan_recovery_grace_seconds", lambda: 60.0)

        with patch("cron.scheduler.run_job") as run_job_mock, \
             patch("cron.scheduler.save_job_output") as save_mock, \
             patch("cron.scheduler._deliver_result") as deliver_mock:
            assert tick(verbose=False) == 0

        run_job_mock.assert_not_called()
        save_mock.assert_not_called()
        deliver_mock.assert_not_called()
        assert get_job(job["id"])["in_flight"] is not None

        jobs = load_jobs()
        jobs[0]["in_flight"]["claimed_at"] = (
            datetime.fromisoformat(jobs[0]["in_flight"]["claimed_at"]) - timedelta(seconds=61)
        ).isoformat()
        save_jobs(jobs)

        with patch("cron.scheduler.run_job", return_value=(True, "# output", "done", None)), \
             patch("cron.scheduler.save_job_output", return_value=cron_runtime / "out.md"), \
             patch("cron.scheduler._deliver_result"):
            assert tick(verbose=False) == 1
            _wait_for_cron_workers()

        updated = get_job(job["id"])
        assert updated is not None
        assert updated.get("in_flight") is None
        assert updated.get("last_status") == "ok"

    def test_legacy_orphaned_inflight_can_be_reclaimed_on_tick(self, cron_runtime, monkeypatch):
        job = create_job(prompt="legacy orphan", schedule="every 1h", name="job-legacy")
        self._set_due_now()
        monkeypatch.setattr("cron.scheduler.load_config", lambda: {"cron": {"max_parallel_jobs": 1, "wrap_response": False}})

        claimed = claim_due_jobs(now=scheduler._hermes_now(), owner_instance_id="43210-legacyaa", max_parallel=1)
        assert len(claimed) == 1

        jobs = load_jobs()
        jobs[0]["in_flight"] = {
            "run_id": jobs[0]["in_flight"]["run_id"],
            "owner_instance_id": "43210-legacyaa",
            "claimed_at": jobs[0]["in_flight"]["claimed_at"],
            "timeout_at": jobs[0]["in_flight"]["timeout_at"],
            "started_at": jobs[0]["in_flight"]["started_at"],
            "status": jobs[0]["in_flight"]["status"],
        }
        save_jobs(jobs)

        monkeypatch.setattr("cron.jobs._legacy_owner_pid_is_dead", lambda pid: True)
        monkeypatch.setattr("cron.jobs._orphan_recovery_grace_seconds", lambda: 0.0)

        with patch("cron.scheduler.run_job", return_value=(True, "# output", "done", None)), \
             patch("cron.scheduler.save_job_output", return_value=cron_runtime / "out.md"), \
             patch("cron.scheduler._deliver_result"):
            assert tick(verbose=False) == 1
            _wait_for_cron_workers()

        updated = get_job(job["id"])
        assert updated is not None
        assert updated.get("in_flight") is None
        assert updated.get("last_status") == "ok"

    def test_tick_recovers_mismatched_owner_and_dispatches_job(self, cron_runtime, monkeypatch):
        job = create_job(prompt="mismatch", schedule="every 1h", name="job-mismatch")
        self._set_due_now()
        monkeypatch.setattr("cron.scheduler.load_config", lambda: {"cron": {"max_parallel_jobs": 1, "wrap_response": False}})

        claimed = claim_due_jobs(now=scheduler._hermes_now(), owner_instance_id="instance-a", max_parallel=1)
        assert len(claimed) == 1

        monkeypatch.setattr(
            "cron.jobs._get_inflight_owner_state",
            lambda inflight, now_dt=None: ("mismatch", "owner pid fingerprint mismatch"),
        )
        monkeypatch.setattr("cron.jobs._orphan_recovery_grace_seconds", lambda: 0.0)

        with patch("cron.scheduler.run_job", return_value=(True, "# output", "done", None)), \
             patch("cron.scheduler.save_job_output", return_value=cron_runtime / "out.md"), \
             patch("cron.scheduler._deliver_result"):
            assert tick(verbose=False) == 1
            _wait_for_cron_workers()

        updated = get_job(job["id"])
        assert updated is not None
        assert updated.get("in_flight") is None
        assert updated.get("last_status") == "ok"

    def test_per_job_lock_busy_clears_claim_without_counting_attempt(self, cron_runtime, monkeypatch):
        job = create_job(prompt="single", schedule="every 1h", name="job-single", repeat=1)
        self._set_due_now()
        monkeypatch.setattr("cron.scheduler.load_config", lambda: {"cron": {"max_parallel_jobs": 1, "wrap_response": False}})

        claimed = claim_due_jobs(now=scheduler._hermes_now(), owner_instance_id="instance-a", max_parallel=1)
        assert len(claimed) == 1

        with patch("cron.scheduler._try_acquire_job_lock", return_value=None), \
             patch("cron.scheduler._scheduler_lock", side_effect=_noop_scheduler_lock), \
             patch("cron.scheduler.run_job") as run_job_mock, \
             patch("cron.scheduler.save_job_output") as save_mock, \
             patch("cron.scheduler.finalize_job_run") as finalize_mock, \
             patch("cron.scheduler._deliver_result") as deliver_mock:
            result = scheduler._run_claimed_job(claimed[0], verbose=False)

        assert result is False
        run_job_mock.assert_not_called()
        save_mock.assert_not_called()
        finalize_mock.assert_not_called()
        deliver_mock.assert_not_called()

        updated = get_job(job["id"])
        assert updated is not None
        assert updated.get("in_flight") is None
        assert updated.get("last_status") == "error"
        assert "job execution lock busy" in (updated.get("last_error") or "")
        assert updated.get("repeat", {}).get("completed") == 0
        assert updated.get("enabled") is True
        assert updated.get("state") == "scheduled"

    def test_capacity_limited_ticks_dispatch_remaining_jobs_in_next_wave(self, cron_runtime, monkeypatch):
        create_job(prompt="A", schedule="every 1h", name="job-a")
        create_job(prompt="B", schedule="every 1h", name="job-b")
        create_job(prompt="C", schedule="every 1h", name="job-c")
        self._set_due_now()
        monkeypatch.setattr("cron.scheduler.load_config", lambda: {"cron": {"max_parallel_jobs": 2, "wrap_response": False}})

        invocation_count = 0
        started_ids = []
        first_wave_started = threading.Event()
        release_first_wave = threading.Event()
        lock = threading.Lock()

        def fake_run_job(job):
            nonlocal invocation_count
            with lock:
                invocation_count += 1
                current = invocation_count
                started_ids.append(job["id"])
                if current >= 2:
                    first_wave_started.set()
            if current <= 2:
                release_first_wave.wait(timeout=1.0)
            return True, f"# output {job['id']}", f"done-{job['id']}", None

        with patch("cron.scheduler.run_job", side_effect=fake_run_job), \
             patch("cron.scheduler.save_job_output", return_value=cron_runtime / "out.md"), \
             patch("cron.scheduler._deliver_result"):
            first = tick(verbose=False)
            assert first == 2
            assert first_wave_started.wait(timeout=1.0)

            second = tick(verbose=False)
            assert second == 0

            release_first_wave.set()
            _wait_for_cron_workers()

            third = tick(verbose=False)
            assert third == 1
            _wait_for_cron_workers()

        assert len(started_ids) == 3
        assert len(set(started_ids)) == 3

    def test_concurrent_ticks_do_not_double_dispatch_same_due_job(self, cron_runtime, monkeypatch):
        create_job(prompt="solo", schedule="every 1h", name="job-solo")
        self._set_due_now()
        monkeypatch.setattr("cron.scheduler.load_config", lambda: {"cron": {"max_parallel_jobs": 2, "wrap_response": False}})

        scheduler_gate = threading.Lock()
        start_barrier = threading.Barrier(2)
        started = threading.Event()
        release = threading.Event()
        run_count = 0
        run_count_lock = threading.Lock()
        tick_results = []

        @contextmanager
        def thread_scheduler_lock(*, non_blocking: bool):
            acquired = scheduler_gate.acquire(blocking=not non_blocking)
            if not acquired:
                yield None
                return
            try:
                yield object()
            finally:
                scheduler_gate.release()

        def fake_run_job(_job):
            nonlocal run_count
            with run_count_lock:
                run_count += 1
            started.set()
            release.wait(timeout=1.0)
            return True, "# output", "done", None

        def invoke_tick():
            start_barrier.wait(timeout=1.0)
            tick_results.append(tick(verbose=False))

        with patch("cron.scheduler._scheduler_lock", side_effect=thread_scheduler_lock), \
             patch("cron.scheduler.run_job", side_effect=fake_run_job), \
             patch("cron.scheduler.save_job_output", return_value=cron_runtime / "out.md"), \
             patch("cron.scheduler._deliver_result"):
            t1 = threading.Thread(target=invoke_tick)
            t2 = threading.Thread(target=invoke_tick)
            t1.start()
            t2.start()
            assert started.wait(timeout=1.0)
            release.set()
            t1.join(timeout=1.0)
            t2.join(timeout=1.0)
            _wait_for_cron_workers()

        assert sorted(tick_results) == [0, 1]
        assert run_count == 1

    @pytest.mark.skipif(os.name == "nt", reason="requires fork-style multiprocessing for deterministic lock inheritance")
    def test_separate_process_ticks_do_not_double_dispatch_same_due_job(self, cron_runtime, monkeypatch):
        create_job(prompt="solo", schedule="every 1h", name="job-solo")
        self._set_due_now()
        monkeypatch.setattr("cron.scheduler.load_config", lambda: {"cron": {"max_parallel_jobs": 2, "wrap_response": False}})

        try:
            ctx = multiprocessing.get_context("fork")
        except ValueError:
            pytest.skip("fork start method unavailable")

        started_queue = ctx.Queue()
        result_queue = ctx.Queue()
        release = ctx.Event()

        def fake_run_job(job):
            started_queue.put(job["id"])
            release.wait(timeout=1.0)
            return True, f"# output {job['id']}", f"done-{job['id']}", None

        def invoke_tick():
            try:
                result_queue.put(("ok", tick(verbose=False)))
            except Exception as exc:  # pragma: no cover - defensive test plumbing
                result_queue.put(("err", repr(exc)))

        monkeypatch.setattr(scheduler, "run_job", fake_run_job)
        monkeypatch.setattr(scheduler, "save_job_output", lambda job_id, output: cron_runtime / f"{job_id}.md")
        monkeypatch.setattr(scheduler, "_deliver_result", lambda job, content: None)

        p1 = ctx.Process(target=invoke_tick)
        p2 = ctx.Process(target=invoke_tick)
        p1.start()
        p2.start()

        started_job_id = started_queue.get(timeout=2.0)
        assert started_job_id
        release.set()

        results = [result_queue.get(timeout=2.0), result_queue.get(timeout=2.0)]
        p1.join(timeout=2.0)
        p2.join(timeout=2.0)
        assert p1.exitcode == 0
        assert p2.exitcode == 0

        statuses = [status for status, _ in results]
        assert statuses == ["ok", "ok"]
        dispatched_counts = sorted(value for status, value in results if status == "ok")
        assert dispatched_counts == [0, 1]

    def test_stale_finalize_result_discarded(self):
        claimed = {
            "id": "job-1",
            "name": "Job 1",
            "deliver": "local",
            "in_flight": {"run_id": "run-1"},
        }

        with patch("cron.scheduler._try_acquire_job_lock", return_value=MagicMock()), \
             patch("cron.scheduler._scheduler_lock", side_effect=_noop_scheduler_lock), \
             patch("cron.scheduler._release_lock_file"), \
             patch("cron.scheduler.mark_job_started", return_value=True), \
             patch("cron.scheduler.run_job", return_value=(True, "# output", "ok", None)), \
             patch("cron.scheduler.finalize_job_run", return_value=False) as finalize_mock, \
             patch("cron.scheduler.save_job_output") as save_mock, \
             patch("cron.scheduler._deliver_result") as deliver_mock:
            result = scheduler._run_claimed_job(claimed, verbose=False)

        assert result is False
        finalize_mock.assert_called_once()
        save_mock.assert_called_once_with("job-1", "# output")
        deliver_mock.assert_not_called()

    def test_old_owner_completion_is_discarded_after_ownership_change(self):
        claimed = {
            "id": "job-1",
            "name": "Job 1",
            "deliver": "local",
            "in_flight": {"run_id": "run-old"},
        }

        with patch("cron.scheduler._try_acquire_job_lock", return_value=MagicMock()), \
             patch("cron.scheduler._scheduler_lock", side_effect=_noop_scheduler_lock), \
             patch("cron.scheduler._release_lock_file"), \
             patch("cron.scheduler.mark_job_started", return_value=True), \
             patch("cron.scheduler.run_job", return_value=(True, "# output", "ok", None)), \
             patch("cron.scheduler.save_job_output"), \
             patch("cron.scheduler.finalize_job_run", return_value=False) as finalize_mock, \
             patch("cron.scheduler._deliver_result") as deliver_mock:
            result = scheduler._run_claimed_job(claimed, verbose=False)

        assert result is False
        finalize_mock.assert_called_once_with("job-1", "run-old", True, None)
        deliver_mock.assert_not_called()

    def test_output_is_saved_before_finalize(self):
        claimed = {
            "id": "job-1",
            "name": "Job 1",
            "deliver": "local",
            "in_flight": {"run_id": "run-1"},
        }
        call_order = []

        def fake_save(job_id, output):
            call_order.append(("save", job_id, output))
            return "/tmp/out.md"

        def fake_finalize(job_id, run_id, success, error, **kwargs):
            call_order.append(("finalize", job_id, run_id, success, error))
            return True

        with patch("cron.scheduler._try_acquire_job_lock", return_value=MagicMock()), \
             patch("cron.scheduler._scheduler_lock", side_effect=_noop_scheduler_lock), \
             patch("cron.scheduler._release_lock_file"), \
             patch("cron.scheduler.mark_job_started", return_value=True), \
             patch("cron.scheduler.run_job", return_value=(True, "# output", "ok", None)), \
             patch("cron.scheduler.save_job_output", side_effect=fake_save), \
             patch("cron.scheduler.finalize_job_run", side_effect=fake_finalize), \
             patch("cron.scheduler._deliver_result"):
            result = scheduler._run_claimed_job(claimed, verbose=False)

        assert result is True
        assert call_order == [
            ("save", "job-1", "# output"),
            ("finalize", "job-1", "run-1", True, None),
        ]

    def test_save_failure_records_error_before_clearing_ownership(self):
        claimed = {
            "id": "job-1",
            "name": "Job 1",
            "deliver": "local",
            "in_flight": {"run_id": "run-1"},
        }

        with patch("cron.scheduler._try_acquire_job_lock", return_value=MagicMock()), \
             patch("cron.scheduler._scheduler_lock", side_effect=_noop_scheduler_lock), \
             patch("cron.scheduler._release_lock_file"), \
             patch("cron.scheduler.mark_job_started", return_value=True), \
             patch("cron.scheduler.run_job", return_value=(True, "# output", "ok", None)), \
             patch("cron.scheduler.save_job_output", side_effect=OSError("disk full")), \
             patch("cron.scheduler.finalize_job_run", return_value=True) as finalize_mock, \
             patch("cron.scheduler._deliver_result") as deliver_mock:
            result = scheduler._run_claimed_job(claimed, verbose=False)

        assert result is False
        finalize_mock.assert_called_once()
        args = finalize_mock.call_args.args
        assert args[:2] == ("job-1", "run-1")
        assert args[2] is False
        assert "disk full" in args[3]
        deliver_mock.assert_not_called()


class TestSendMediaViaAdapter:
    """Unit tests for _send_media_via_adapter — routes files to typed adapter methods."""

    @staticmethod
    def _run_with_loop(adapter, chat_id, media_files, metadata, job):
        """Helper: run _send_media_via_adapter with a real running event loop."""
        import asyncio
        import threading

        loop = asyncio.new_event_loop()
        t = threading.Thread(target=loop.run_forever, daemon=True)
        t.start()
        try:
            _send_media_via_adapter(adapter, chat_id, media_files, metadata, loop, job)
        finally:
            loop.call_soon_threadsafe(loop.stop)
            t.join(timeout=5)
            loop.close()

    def test_video_dispatched_to_send_video(self):
        adapter = MagicMock()
        adapter.send_video = AsyncMock()
        media_files = [("/tmp/clip.mp4", False)]
        self._run_with_loop(adapter, "123", media_files, None, {"id": "j1"})
        adapter.send_video.assert_called_once()
        assert adapter.send_video.call_args[1]["video_path"] == "/tmp/clip.mp4"

    def test_unknown_ext_dispatched_to_send_document(self):
        adapter = MagicMock()
        adapter.send_document = AsyncMock()
        media_files = [("/tmp/report.pdf", False)]
        self._run_with_loop(adapter, "123", media_files, None, {"id": "j2"})
        adapter.send_document.assert_called_once()
        assert adapter.send_document.call_args[1]["file_path"] == "/tmp/report.pdf"

    def test_multiple_media_files_all_delivered(self):
        adapter = MagicMock()
        adapter.send_voice = AsyncMock()
        adapter.send_image_file = AsyncMock()
        media_files = [("/tmp/voice.mp3", False), ("/tmp/photo.jpg", False)]
        self._run_with_loop(adapter, "123", media_files, None, {"id": "j3"})
        adapter.send_voice.assert_called_once()
        adapter.send_image_file.assert_called_once()

    def test_single_failure_does_not_block_others(self):
        adapter = MagicMock()
        adapter.send_voice = AsyncMock(side_effect=RuntimeError("network error"))
        adapter.send_image_file = AsyncMock()
        media_files = [("/tmp/voice.ogg", False), ("/tmp/photo.png", False)]
        self._run_with_loop(adapter, "123", media_files, None, {"id": "j4"})
        adapter.send_voice.assert_called_once()
        adapter.send_image_file.assert_called_once()
