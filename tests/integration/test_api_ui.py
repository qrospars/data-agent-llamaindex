from __future__ import annotations

import json

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


def test_chat_stream_emits_progress_and_final() -> None:
    client = TestClient(app)
    payload = {
        "message": "show one row",
        "session_id": "stream-test",
        "db_url": "sqlite:///./examples/sqlite_demo/demo.db",
        "semantic_config_path": "./examples/sqlite_demo/semantic.yaml",
        "llm_provider": "mock",
        "llm_model": "gemini-2.5-flash",
    }

    response = client.post("/chat/stream", json=payload)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")

    raw = response.text.replace("\r\n", "\n")
    chunks = [chunk for chunk in raw.split("\n\n") if chunk.strip()]
    assert any("event: progress" in chunk for chunk in chunks)
    final_chunk = next((chunk for chunk in chunks if "event: final" in chunk), "")
    assert final_chunk

    data_line = next((line for line in final_chunk.split("\n") if line.startswith("data: ")), "")
    assert data_line
    final_data = json.loads(data_line[len("data: ") :])
    assert final_data["mode"] in {"query", "chat", "meta", "error"}
    assert "message" in final_data
