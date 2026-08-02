"""notify.client PushDeer HTTP 客户端单测。"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestPushDeerClient:
    @patch("rquant.notify.client.requests.post")
    def test_push_calls_endpoint_with_payload(self, mock_post) -> None:
        from rquant.notify.client import PushDeerClient

        mock_post.return_value = MagicMock(json=lambda: {"code": 0, "content": {"result": ["ok"]}})

        client = PushDeerClient(
            keys=["PDU_test_key"],
            endpoint="https://api2.pushdeer.com/message/push",
        )
        results = client.push("test title", "test body")

        assert mock_post.call_count == 1
        args, kwargs = mock_post.call_args
        assert args[0] == "https://api2.pushdeer.com/message/push"
        assert kwargs["data"]["pushkey"] == "PDU_test_key"
        assert kwargs["data"]["text"] == "test title"
        assert kwargs["data"]["desp"] == "test body"
        assert kwargs["data"]["type"] == "markdown"
        assert kwargs["timeout"] == 10

        assert results == [(True, None)]

    @patch("rquant.notify.client.requests.post")
    def test_push_multiple_keys_concurrent(self, mock_post) -> None:
        from rquant.notify.client import PushDeerClient

        mock_post.return_value = MagicMock(json=lambda: {"code": 0})

        client = PushDeerClient(
            keys=["k1", "k2", "k3"],
            endpoint="https://api2.pushdeer.com/message/push",
        )
        results = client.push("t", "b")

        assert mock_post.call_count == 3
        assert all(success for success, _ in results)

    @patch("rquant.notify.client.requests.post")
    def test_push_failure_returns_error(self, mock_post) -> None:
        from rquant.notify.client import PushDeerClient

        mock_post.return_value = MagicMock(json=lambda: {"code": 1, "error": "invalid pushkey"})
        client = PushDeerClient(
            keys=["bad_key"],
            endpoint="https://api2.pushdeer.com/message/push",
        )
        results = client.push("t", "b")

        assert results == [(False, "provider_rejected")]

    @patch("rquant.notify.client.requests.post")
    def test_push_exception_caught(self, mock_post) -> None:
        from rquant.notify.client import PushDeerClient

        mock_post.side_effect = Exception("connection timeout")
        client = PushDeerClient(
            keys=["k"],
            endpoint="https://api2.pushdeer.com/message/push",
        )
        results = client.push("t", "b")

        assert len(results) == 1
        success, err = results[0]
        assert success is False
        assert err == "transport_error"

    def test_push_no_keys_returns_empty(self) -> None:
        from rquant.notify.client import PushDeerClient

        client = PushDeerClient(keys=[], endpoint="https://pushdeer.invalid/send")
        results = client.push("t", "b")
        assert results == []

    def test_default_transport_rejects_non_https_endpoint(self) -> None:
        from rquant.notify.client import PushDeerClient

        with pytest.raises(ValueError, match="HTTPS"):
            PushDeerClient(keys=["secret"], endpoint="http://pushdeer.invalid/send")

    def test_explicit_test_transport_can_use_http_endpoint(self) -> None:
        from rquant.notify.client import PushDeerClient

        calls: list[str] = []

        def transport(endpoint: str, **_kwargs: object) -> MagicMock:
            calls.append(endpoint)
            return MagicMock(json=lambda: {"code": 0})

        result = PushDeerClient(
            keys=["test-key"],
            endpoint="http://127.0.0.1:9999/push",
            transport=transport,
        ).push("title", "body")

        assert result == [(True, None)]
        assert calls == ["http://127.0.0.1:9999/push"]

    @patch("rquant.notify.client.logger.error")
    @patch("rquant.notify.client.requests.post")
    def test_failure_log_never_contains_any_push_key_fragment(
        self,
        mock_post,
        mock_log_error,
    ) -> None:
        from rquant.notify.client import PushDeerClient

        secret = "PDU_super-secret-value"
        provider_response = "denied-for-private-provider-reason"
        mock_post.return_value = MagicMock(json=lambda: {"code": 1, "error": provider_response})

        PushDeerClient(
            keys=[secret],
            endpoint="https://api2.pushdeer.com/message/push",
        ).push("title", "body")

        rendered = " ".join(str(call) for call in mock_log_error.call_args_list)
        assert secret not in rendered
        assert secret[:8] not in rendered
        assert provider_response not in rendered
        assert "provider_rejected" in rendered


class TestPushPlusClient:
    @patch("rquant.notify.client.requests.post")
    def test_push_calls_endpoint_with_payload(self, mock_post) -> None:
        from rquant.notify.client import PushPlusClient

        mock_post.return_value = MagicMock(json=lambda: {"code": 200, "msg": "ok"})

        client = PushPlusClient(
            tokens=["pp_token_xyz"],
            endpoint="https://www.pushplus.plus/send",
        )
        results = client.push("test title", "test body")

        assert mock_post.call_count == 1
        args, kwargs = mock_post.call_args
        assert args[0] == "https://www.pushplus.plus/send"
        assert kwargs["json"]["token"] == "pp_token_xyz"
        assert kwargs["json"]["title"] == "test title"
        assert kwargs["json"]["content"] == "test body"
        assert kwargs["json"]["template"] == "markdown"
        assert kwargs["timeout"] == 10

        assert results == [(True, None)]

    @patch("rquant.notify.client.requests.post")
    def test_failure_returns_msg(self, mock_post) -> None:
        from rquant.notify.client import PushPlusClient

        mock_post.return_value = MagicMock(json=lambda: {"code": 903, "msg": "token 无效"})
        client = PushPlusClient(tokens=["bad"], endpoint="https://pushplus.invalid/send")
        results = client.push("t", "b")
        assert results == [(False, "provider_rejected")]

    @patch("rquant.notify.client.requests.post")
    def test_exception_caught(self, mock_post) -> None:
        from rquant.notify.client import PushPlusClient

        mock_post.side_effect = Exception("connection refused")
        client = PushPlusClient(tokens=["t1"], endpoint="https://pushplus.invalid/send")
        results = client.push("t", "b")
        success, err = results[0]
        assert success is False
        assert err == "transport_error"

    def test_no_tokens_returns_empty(self) -> None:
        from rquant.notify.client import PushPlusClient

        client = PushPlusClient(tokens=[], endpoint="https://pushplus.invalid/send")
        results = client.push("t", "b")
        assert results == []

    def test_default_transport_rejects_non_https_endpoint(self) -> None:
        from rquant.notify.client import PushPlusClient

        with pytest.raises(ValueError, match="HTTPS"):
            PushPlusClient(tokens=["secret"], endpoint="http://pushplus.invalid/send")

    @patch("rquant.notify.client.logger.error")
    @patch("rquant.notify.client.requests.post")
    def test_failure_log_never_contains_any_pushplus_token_fragment(
        self,
        mock_post,
        mock_log_error,
    ) -> None:
        from rquant.notify.client import PushPlusClient

        secret = "pushplus-super-secret"
        provider_exception = "network-down-with-provider-detail"
        mock_post.side_effect = RuntimeError(provider_exception)

        PushPlusClient(tokens=[secret], endpoint="https://www.pushplus.plus/send").push(
            "title", "body"
        )

        rendered = " ".join(str(call) for call in mock_log_error.call_args_list)
        assert secret not in rendered
        assert secret[:8] not in rendered
        assert provider_exception not in rendered
        assert "transport_error" in rendered
