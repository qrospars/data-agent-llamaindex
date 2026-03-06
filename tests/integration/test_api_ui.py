from __future__ import annotations

from fastapi.testclient import TestClient

from data_agent_core.api.app import app


def test_ui_root_serves_index() -> None:
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert "Data Agent Console" in response.text


def test_ui_assets_served() -> None:
    client = TestClient(app)
    css_response = client.get("/ui/styles.css")
    js_response = client.get("/ui/app.js")
    assert css_response.status_code == 200
    assert js_response.status_code == 200
