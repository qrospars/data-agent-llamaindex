from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from typing import Literal

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from data_agent_core.agents.conversation_agent import ConversationAgent, InMemoryConversationStore
from data_agent_core.agents.query_agent import QueryAgent
from data_agent_core.config.models import AppConfig, ProviderConfig
from data_agent_core.output.conversation_notes import MarkdownConversationLogger

router = APIRouter()
CHAT_STORE = InMemoryConversationStore()
NOTES_LOGGER = MarkdownConversationLogger(Path("docs") / "conversations")


class AskRequest(BaseModel):
    question: str
    db_url: str
    semantic_config_path: str | None = None
    llm_provider: Literal["gemini", "mock"] = "mock"
    llm_model: str = "gemini-2.5-flash"
    llm_api_key_env: str = "GEMINI_API_KEY"
    llm_temperature: float = 0.0


class SQLRequest(BaseModel):
    sql: str
    db_url: str
    semantic_config_path: str | None = None
    llm_provider: Literal["gemini", "mock"] = "mock"
    llm_model: str = "gemini-2.5-flash"
    llm_api_key_env: str = "GEMINI_API_KEY"
    llm_temperature: float = 0.0


class ChatRequest(BaseModel):
    message: str
    session_id: str = "default"
    db_url: str
    semantic_config_path: str | None = None
    llm_provider: Literal["gemini", "mock"] = "mock"
    llm_model: str = "gemini-2.5-flash"
    llm_api_key_env: str = "GEMINI_API_KEY"
    llm_temperature: float = 0.0


def _agent(
    db_url: str,
    semantic_path: str | None,
    llm_provider: Literal["gemini", "mock"],
    llm_model: str,
    llm_api_key_env: str,
    llm_temperature: float,
) -> QueryAgent:
    config = AppConfig(
        db_url=db_url,
        semantic_config_path=Path(semantic_path) if semantic_path else None,
        llm_provider=ProviderConfig(
            provider=llm_provider,
            model=llm_model,
            api_key_env=llm_api_key_env,
            temperature=llm_temperature,
        ),
    )
    return QueryAgent(config)


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.post("/ask")
def ask(payload: AskRequest) -> dict[str, object]:
    return _agent(
        payload.db_url,
        payload.semantic_config_path,
        payload.llm_provider,
        payload.llm_model,
        payload.llm_api_key_env,
        payload.llm_temperature,
    ).ask(payload.question).model_dump()


@router.post("/generate-sql")
def generate_sql(payload: AskRequest) -> dict[str, object]:
    return _agent(
        payload.db_url,
        payload.semantic_config_path,
        payload.llm_provider,
        payload.llm_model,
        payload.llm_api_key_env,
        payload.llm_temperature,
    ).generate_sql(payload.question).model_dump()


@router.post("/run-sql")
def run_sql(payload: SQLRequest) -> dict[str, object]:
    return _agent(
        payload.db_url,
        payload.semantic_config_path,
        payload.llm_provider,
        payload.llm_model,
        payload.llm_api_key_env,
        payload.llm_temperature,
    ).run_sql(payload.sql).model_dump()


@router.post("/chat")
def chat(payload: ChatRequest) -> dict[str, object]:
    conversation_agent = ConversationAgent(
        _agent(
            payload.db_url,
            payload.semantic_config_path,
            payload.llm_provider,
            payload.llm_model,
            payload.llm_api_key_env,
            payload.llm_temperature,
        ),
        CHAT_STORE,
        notes_logger=NOTES_LOGGER,
    )
    return conversation_agent.chat(payload.message, payload.session_id).model_dump()


def _encode_sse(event: str, data: object) -> bytes:
    payload = json.dumps(data, ensure_ascii=True)
    return f"event: {event}\ndata: {payload}\n\n".encode("utf-8")


@router.post("/chat/stream")
async def chat_stream(payload: ChatRequest) -> StreamingResponse:
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[tuple[str, object]] = asyncio.Queue()

    def push(event: str, data: object) -> None:
        loop.call_soon_threadsafe(queue.put_nowait, (event, data))

    conversation_agent = ConversationAgent(
        _agent(
            payload.db_url,
            payload.semantic_config_path,
            payload.llm_provider,
            payload.llm_model,
            payload.llm_api_key_env,
            payload.llm_temperature,
        ),
        CHAT_STORE,
        notes_logger=NOTES_LOGGER,
    )

    def run_chat() -> None:
        try:
            response = conversation_agent.chat(
                payload.message,
                payload.session_id,
                progress_callback=lambda stage, message: push(
                    "progress",
                    {"stage": stage, "message": message},
                ),
            )
            push("final", response.model_dump())
        except Exception as exc:
            push("error", {"message": str(exc)})
        finally:
            push("done", {"ok": True})

    threading.Thread(target=run_chat, daemon=True).start()

    async def event_stream() -> object:
        while True:
            event, data = await queue.get()
            yield _encode_sse(event, data)
            if event == "done":
                break

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
