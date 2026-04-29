"""notify.client PushDeer HTTP 客户端单测。"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestPushDeerClient:
    @patch("rquant.notify.client.requests.post")
    def test_push_calls_endpoint_with_payload(self, mock_post) -> None:
        from rquant.notify.client import PushDeerClient

        mock_post.return_value = MagicMock(
            json=lambda: {"code": 0, "content": {"result": ["ok"]}}
        )

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

        mock_post.return_value = MagicMock(
            json=lambda: {"code": 0}
        )

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

        mock_post.return_value = MagicMock(
            json=lambda: {"code": 1, "error": "invalid pushkey"}
        )
        client = PushDeerClient(
            keys=["bad_key"],
            endpoint="https://api2.pushdeer.com/message/push",
        )
        results = client.push("t", "b")

        assert results == [(False, "invalid pushkey")]

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
        assert "connection timeout" in err

    def test_push_no_keys_returns_empty(self) -> None:
        from rquant.notify.client import PushDeerClient

        client = PushDeerClient(keys=[], endpoint="x")
        results = client.push("t", "b")
        assert results == []


class TestPushPlusClient:
    @patch("rquant.notify.client.requests.post")
    def test_push_calls_endpoint_with_payload(self, mock_post) -> None:
        from rquant.notify.client import PushPlusClient

        mock_post.return_value = MagicMock(json=lambda: {"code": 200, "msg": "ok"})

        client = PushPlusClient(
            tokens=["pp_token_xyz"],
            endpoint="http://www.pushplus.plus/send",
        )
        results = client.push("test title", "test body")

        assert mock_post.call_count == 1
        args, kwargs = mock_post.call_args
        assert args[0] == "http://www.pushplus.plus/send"
        assert kwargs["json"]["token"] == "pp_token_xyz"
        assert kwargs["json"]["title"] == "test title"
        assert kwargs["json"]["content"] == "test body"
        assert kwargs["json"]["template"] == "markdown"
        assert kwargs["timeout"] == 10

        assert results == [(True, None)]

    @patch("rquant.notify.client.requests.post")
    def test_failure_returns_msg(self, mock_post) -> None:
        from rquant.notify.client import PushPlusClient

        mock_post.return_value = MagicMock(
            json=lambda: {"code": 903, "msg": "token 无效"}
        )
        client = PushPlusClient(tokens=["bad"], endpoint="http://x")
        results = client.push("t", "b")
        assert results == [(False, "token 无效")]

    @patch("rquant.notify.client.requests.post")
    def test_exception_caught(self, mock_post) -> None:
        from rquant.notify.client import PushPlusClient

        mock_post.side_effect = Exception("connection refused")
        client = PushPlusClient(tokens=["t1"], endpoint="http://x")
        results = client.push("t", "b")
        success, err = results[0]
        assert success is False
        assert "connection refused" in err

    def test_no_tokens_returns_empty(self) -> None:
        from rquant.notify.client import PushPlusClient

        client = PushPlusClient(tokens=[], endpoint="x")
        results = client.push("t", "b")
        assert results == []
