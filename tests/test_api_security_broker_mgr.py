"""Phase 0/3/4 regression tests for Broker Manager R1.4/R1.5, dynamic port security, backups & SPA guards."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient
import httpx

from gammad.api.broker_selection import write_env_backup
from gammad.broker import gamma_r14, gamma_r15
from gammad.broker.base import TokenHealth
from gammad.broker.zerodha.feed import _scrub_ws_err


class _FakeAdapter:
    def __init__(self, valid: bool = True, prices: bool | None = True) -> None:
        self._health = TokenHealth(valid=valid, validity_ist="2099-01-01T00:00:00+05:30", error=None, prices=prices)

    def token_health(self) -> TokenHealth:
        return self._health

    def ltp(self, sids, seg):
        return {13: 24500.0}


class TestApiSecurityBrokerMgr(unittest.TestCase):
    def setUp(self) -> None:
        with gamma_r15._LOCK:
            gamma_r15._PROOFS.clear()
            gamma_r15._LAST_TRY.clear()

    def test_write_env_backup_prunes_to_3_and_skips_symlinks(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gamma_bak_") as tmp:
            root = Path(tmp)
            env_path = root / ".env"
            env_path.write_text("BROKER=fyers\n", encoding="utf-8")
            with patch("gammad.store.security.check_or_enforce_acls") as acl_mock:
                paths = []
                for idx in range(5):
                    p = write_env_backup(env_path, f"BROKER=fyers\n# {idx}\n".encode(), prefix=".env.test-")
                    paths.append(p)
                kept = sorted((root / "data" / "backups").glob(".env.test-*"))
                self.assertEqual(len(kept), 3)
                self.assertTrue(acl_mock.called)

    def test_ws_error_redacts_api_key_and_access_token(self) -> None:
        err = RuntimeError("failed connecting wss://ws.kite.trade?api_key=abc123&access_token=secret999")
        msg = _scrub_ws_err(err)
        self.assertNotIn("abc123", msg)
        self.assertNotIn("secret999", msg)
        self.assertIn("<redacted>", msg)

    def test_r14_r15_dynamic_port_and_verified_switch_workflow(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gamma_bm_") as tmp:
            root = Path(tmp)
            (root / ".env").write_text("BROKER=fyers\nFYERS_APP_ID=A\nFYERS_SECRET_KEY=S1\nFYERS_ACCESS_TOKEN=T1\n", encoding="utf-8")
            vault = {
                "zerodha": {
                    "keys": {"KITE_API_KEY": "k1", "KITE_API_SECRET": "s1", "KITE_ACCESS_TOKEN": "tok1"},
                    "exchange_completed": True,
                }
            }

            class _Runtime:
                broker_entry = type("E", (), {"name": "fyers"})()

                def token_status(self):
                    return {"valid": True}

            app = FastAPI()
            with (
                patch("gammad.REPO_ROOT", root),
                patch.object(gamma_r14, "_load_vault", side_effect=lambda: vault),
                patch.object(gamma_r14, "_save_vault", side_effect=lambda d: vault.update(d)),
                patch("gammad.store.security.check_or_enforce_acls"),
            ):
                gamma_r14.install_routes(app, _Runtime())
                # Dynamic non-8760 port MUST pass host & origin checks; wrong host/port MUST be 403
                c8765 = TestClient(app, base_url="http://127.0.0.1:8765")
                r_ok = c8765.get("/api/broker-manager/status")
                self.assertEqual(r_ok.status_code, 200)
                st0 = r_ok.json()
                z0 = next(b for b in st0["brokers"] if b["id"] == "zerodha")
                self.assertFalse(z0["can_stage"])

                # Switch blocked before R1.5 verification
                r_unverified = c8765.post(
                    "/api/broker-manager/switch/zerodha",
                    headers={"X-Gamma-Control": "1", "Content-Type": "application/json",
                             "Origin": "http://127.0.0.1:8765"},
                    json={"expected_active": "fyers", "confirmation": "SWITCH TO ZERODHA", "acknowledge": True},
                )
                self.assertEqual(r_unverified.status_code, 409)

                # DNS rebinding / mismatched port / forwarded proxy rejected
                self.assertEqual(c8765.get("/api/broker-manager/status", headers={"Host": "evil.com:8765"}).status_code, 403)
                self.assertEqual(c8765.get("/api/broker-manager/status", headers={"Host": "127.0.0.1:9999"}).status_code, 403)
                self.assertEqual(c8765.get("/api/broker-manager/status",
                                           headers={"X-Forwarded-For": "192.168.1.50"}).status_code, 403)

                # Mock Zerodha profile + lightweight /quote/ltp probe (and FYERS read-only probe)
                def kite_transport(req: httpx.Request) -> httpx.Response:
                    if "/user/profile" in str(req.url):
                        return httpx.Response(200, json={"status": "success",
                                                         "data": {"user_id": "Z1", "broker": "ZERODHA"}})
                    if "/quote/ltp" in str(req.url):
                        return httpx.Response(200, json={"status": "success", "data": {"NSE:RELIANCE": {"last_price": 1200}}})
                    return httpx.Response(404)

                with patch("gammad.broker.gamma_r15._probe_adapter",
                           return_value=(True, True, "authenticated_and_feed_verified")):
                    # FYERS read-only verify no longer blocked with 409
                    rf = c8765.post(
                        "/api/broker-manager/r15/verify-auth/fyers",
                        headers={"X-Gamma-Control": "1", "Content-Type": "application/json",
                                 "Origin": "http://127.0.0.1:8765"},
                        json={},
                    )
                    self.assertEqual(rf.status_code, 200)
                    self.assertTrue(rf.json()["feed_verified"])

                    rz = c8765.post(
                        "/api/broker-manager/r15/verify-auth/zerodha",
                        headers={"X-Gamma-Control": "1", "Content-Type": "application/json",
                                 "Origin": "http://127.0.0.1:8765"},
                        json={},
                    )
                    self.assertEqual(rz.status_code, 200)
                    self.assertTrue(rz.json()["switch_eligible"])

                # Status now unlocks can_stage for Zerodha, and switch creates backup in data/backups/ & updates .env
                z1 = next(b for b in c8765.get("/api/broker-manager/status").json()["brokers"] if b["id"] == "zerodha")
                self.assertTrue(z1["feed_verified"])
                self.assertTrue(z1["can_stage"])

                r_sw = c8765.post(
                    "/api/broker-manager/switch/zerodha",
                    headers={"X-Gamma-Control": "1", "Content-Type": "application/json",
                             "Origin": "http://127.0.0.1:8765"},
                    json={"expected_active": "fyers", "confirmation": "SWITCH TO ZERODHA", "acknowledge": True},
                )
                self.assertEqual(r_sw.status_code, 200)
                body = r_sw.json()
                self.assertEqual(body["staged"], "zerodha")
                self.assertIn('BROKER="zerodha"', (root / ".env").read_text(encoding="utf-8"))
                self.assertTrue((root / "data" / "backups" / body["backup_name"]).is_file())

    def test_spa_mutating_posts_require_json_and_same_origin_without_ctrl_header(self) -> None:
        from unittest.mock import AsyncMock, MagicMock

        from gammad.api.server import create_app
        from gammad.app import Runtime
        from gammad.config import settings

        with tempfile.TemporaryDirectory(prefix="gamma_srv_") as tmp:
            root = Path(tmp)
            base = settings()
            cfg = base.__class__(
                **{
                    **base.__dict__,
                    "paths": base.paths.__class__(
                        **{**base.paths.__dict__, "data_dir": root, "logs_dir": root, "db_path": root / "g.db"}
                    ),
                }
            )
            rt = Runtime(cfg)
            try:
                rt.set_paused = MagicMock()
                rt.run_job = AsyncMock(return_value=7)
                rt.update_token = MagicMock(return_value=True)
                app = create_app(rt, dist_dir=root)
                c = TestClient(app, base_url="http://127.0.0.1:8760")

                # Missing/non-JSON Content-Type -> 415
                self.assertEqual(
                    c.post("/api/alerts/pause", content='{"paused":true}',
                           headers={"Content-Type": "text/plain", "Origin": "http://127.0.0.1:8760"}).status_code,
                    415,
                )
                # Cross-origin / cross-site -> 403
                self.assertEqual(
                    c.post("/api/jobs/mark_levels/run", json={},
                           headers={"Origin": "https://evil.example"}).status_code,
                    403,
                )
                self.assertEqual(
                    c.post("/api/token", json={"access_token": "abc"},
                           headers={"Sec-Fetch-Site": "cross-site", "Origin": "http://127.0.0.1:8760"}).status_code,
                    403,
                )
                # Compiled SPA POST (Content-Type: application/json + same-origin, WITHOUT X-Gamma-Control) -> 200
                r_pause = c.post("/api/alerts/pause", json={"paused": True},
                                 headers={"Origin": "http://127.0.0.1:8760"})
                self.assertEqual(r_pause.status_code, 200)
                rt.set_paused.assert_called_once_with(True)
            finally:
                rt.conn.close()

    def test_r14_empty_active_without_dotenv_and_creates_dotenv_on_switch(self) -> None:
        from gammad.config import load_settings

        with tempfile.TemporaryDirectory(prefix="gamma_bm_empty_") as tmp:
            root = Path(tmp)
            # First run: neither .env nor active broker exists; .env.example seeds blank BROKER=
            (root / ".env.example").write_text("BROKER=\n", encoding="utf-8")
            (root / "config").mkdir(parents=True, exist_ok=True)
            (root / "config" / "strategy.yaml").write_text("spec_version: '1.0'\n", encoding="utf-8")

            vault = {
                "zerodha": {
                    "keys": {"KITE_API_KEY": "k1", "KITE_API_SECRET": "s1", "KITE_ACCESS_TOKEN": "tok1"},
                    "exchange_completed": True,
                }
            }

            class _RuntimeEmpty:
                broker_entry = None

                def token_status(self):
                    return {"valid": False}

            app = FastAPI()
            with (
                patch("gammad.REPO_ROOT", root),
                patch.object(gamma_r14, "_load_vault", side_effect=lambda: vault),
                patch.object(gamma_r14, "_save_vault", side_effect=lambda d: vault.update(d)),
                patch("gammad.store.security.check_or_enforce_acls"),
            ):
                gamma_r14.install_routes(app, _RuntimeEmpty())
                c = TestClient(app, base_url="http://127.0.0.1:8765")

                st0 = c.get("/api/broker-manager/status").json()
                self.assertEqual(st0["active"], "")
                self.assertEqual(st0["staged"], "")
                self.assertFalse(st0["restart_pending"])
                self.assertTrue(all(not b["active"] for b in st0["brokers"]))

                with patch("gammad.broker.gamma_r15._probe_adapter",
                           return_value=(True, True, "authenticated_and_feed_verified")):
                    rz = c.post(
                        "/api/broker-manager/r15/verify-auth/zerodha",
                        headers={"X-Gamma-Control": "1", "Content-Type": "application/json",
                                 "Origin": "http://127.0.0.1:8765"},
                        json={},
                    )
                    self.assertEqual(rz.status_code, 200)
                    self.assertTrue(rz.json()["switch_eligible"])

                r_sw = c.post(
                    "/api/broker-manager/switch/zerodha",
                    headers={"X-Gamma-Control": "1", "Content-Type": "application/json",
                             "Origin": "http://127.0.0.1:8765"},
                    json={"expected_active": "", "confirmation": "SWITCH TO ZERODHA", "acknowledge": True},
                )
                self.assertEqual(r_sw.status_code, 200)
                self.assertTrue((root / ".env").is_file())
                self.assertIn('BROKER="zerodha"', (root / ".env").read_text(encoding="utf-8"))

                st1 = c.get("/api/broker-manager/status").json()
                self.assertEqual(st1["active"], "")
                self.assertEqual(st1["staged"], "zerodha")
                self.assertTrue(st1["restart_pending"])

            # Simulated restart reads persisted BROKER from created .env
            restarted_cfg = load_settings(root)
            self.assertEqual(restarted_cfg.env.broker, "zerodha")

            class _RuntimeZerodha:
                broker_entry = type("E", (), {"name": "zerodha"})()

                def token_status(self):
                    return {"valid": True}

            app2 = FastAPI()
            with (
                patch("gammad.REPO_ROOT", root),
                patch.object(gamma_r14, "_load_vault", side_effect=lambda: vault),
                patch.object(gamma_r14, "_save_vault", side_effect=lambda d: vault.update(d)),
                patch("gammad.store.security.check_or_enforce_acls"),
            ):
                gamma_r14.install_routes(app2, _RuntimeZerodha())
                c2 = TestClient(app2, base_url="http://127.0.0.1:8765")
                st2 = c2.get("/api/broker-manager/status").json()
                self.assertEqual(st2["active"], "zerodha")
                self.assertEqual(st2["staged"], "zerodha")
                self.assertFalse(st2["restart_pending"])


if __name__ == "__main__":
    unittest.main()
