from __future__ import annotations

from dataclasses import dataclass, field

from data_agent_core.agents.query_agent import QueryAgent
from data_agent_core.llm.prompts import build_follow_up_rewrite_prompt, build_intent_prompt
from data_agent_core.output.conversation_notes import MarkdownConversationLogger
from data_agent_core.output.models import AgentResponse, ConversationResponse, ConversationTurn


@dataclass
class ConversationState:
    turns: list[ConversationTurn] = field(default_factory=list)
    last_query_response: AgentResponse | None = None


class InMemoryConversationStore:
    def __init__(self) -> None:
        self._sessions: dict[str, ConversationState] = {}

    def get(self, session_id: str) -> ConversationState:
        state = self._sessions.get(session_id)
        if state is None:
            state = ConversationState()
            self._sessions[session_id] = state
        return state


class ConversationAgent:
    def __init__(
        self,
        query_agent: QueryAgent,
        store: InMemoryConversationStore,
        notes_logger: MarkdownConversationLogger | None = None,
    ) -> None:
        self.query_agent = query_agent
        self.store = store
        self.notes_logger = notes_logger

    def chat(self, message: str, session_id: str = "default") -> ConversationResponse:
        clean_message = message.strip()
        if not clean_message:
            raise ValueError("message cannot be empty")

        state = self.store.get(session_id)

        if self._is_last_sql_request(clean_message):
            if state.last_query_response is None:
                return self._record_and_return(
                    state,
                    session_id,
                    clean_message,
                    ConversationResponse(
                        session_id=session_id,
                        mode="meta",
                        message="No previous SQL in this session yet.",
                    ),
                )
            return self._record_and_return(
                state,
                session_id,
                clean_message,
                ConversationResponse(
                    session_id=session_id,
                    mode="meta",
                    message="Here is the SQL from your previous query.",
                    sql=state.last_query_response.sql,
                ),
            )

        intent = self._infer_intent(clean_message, state)
        if intent == "QUERY":
            resolved_question = self._resolve_follow_up(clean_message, state)
            try:
                result = self.query_agent.ask(resolved_question)
            except Exception as exc:
                return self._record_and_return(
                    state,
                    session_id,
                    clean_message,
                    ConversationResponse(
                        session_id=session_id,
                        mode="error",
                        message=f"I could not execute that request: {exc}",
                    ),
                )

            state.last_query_response = result
            payload = ConversationResponse(
                session_id=session_id,
                mode="query",
                message=self._format_query_message(result),
                sql=result.sql,
                columns=result.columns,
                rows=result.rows,
                row_count=result.row_count,
                chart_suggestion=result.chart_suggestion,
            )
            return self._record_and_return(state, session_id, clean_message, payload)

        if intent == "META":
            meta_message = self._meta_reply(clean_message, state)
            return self._record_and_return(
                state,
                session_id,
                clean_message,
                ConversationResponse(session_id=session_id, mode="meta", message=meta_message),
            )

        chat_message = self._chat_reply(clean_message, state)
        return self._record_and_return(
            state,
            session_id,
            clean_message,
            ConversationResponse(session_id=session_id, mode="chat", message=chat_message),
        )

    def _record_and_return(
        self,
        state: ConversationState,
        session_id: str,
        user_message: str,
        response: ConversationResponse,
    ) -> ConversationResponse:
        turn = ConversationTurn(
            user_message=user_message,
            assistant_message=response.message,
            mode=response.mode,
            sql=response.sql,
        )
        state.turns.append(turn)

        if self.notes_logger is not None:
            notes_path = self.notes_logger.log_turn(session_id=session_id, turn=turn)
            response.notes_path = str(notes_path)
        return response

    def _format_query_message(self, result: AgentResponse) -> str:
        if result.row_count == 0:
            return "I ran the query successfully, but it returned no rows."
        output = result.summary
        if result.columns and result.rows:
            first_row = {col: result.rows[0][i] for i, col in enumerate(result.columns)}
            output = f"{output} First row preview: {first_row}"
        follow_ups = self._suggest_follow_ups(result)
        if follow_ups:
            output = f"{output} Suggested follow-ups: {follow_ups}"
        return output

    def _suggest_follow_ups(self, result: AgentResponse) -> str:
        semantic = self.query_agent.semantic
        metric_names = [m.get("name", "") for m in semantic.metrics if m.get("name")]
        dimension_names = [d.get("name", "") for d in semantic.dimensions if d.get("name")]
        metrics = [name for name in metric_names if name in result.columns] or metric_names[:1]
        dimensions = [name for name in dimension_names if name in result.columns] or dimension_names[:1]
        suggestions: list[str] = []
        if metrics and dimensions:
            suggestions.append(f"break down {metrics[0]} by {dimensions[0]}")
        if metrics:
            suggestions.append(f"compare {metrics[0]} vs previous period")
        if not suggestions:
            suggestions.append("ask for a trend over time")
        return "; ".join(suggestions[:2])

    def _is_last_sql_request(self, message: str) -> bool:
        text = message.lower()
        if "sql" not in text and "query" not in text:
            return False
        markers = {"last", "previous", "that", "generated", "used", "use", "show"}
        return any(marker in text for marker in markers)

    def _is_data_intent(self, message: str, state: ConversationState) -> bool:
        text = message.lower()
        data_keywords = {
            "top",
            "count",
            "sum",
            "avg",
            "average",
            "revenue",
            "sales",
            "customer",
            "orders",
            "list",
            "show",
            "compare",
            "trend",
            "how many",
            "what",
            "which",
        }
        follow_up_tokens = {"that", "those", "them", "same", "again", "previous", "above"}

        if any(keyword in text for keyword in data_keywords):
            return True
        if state.last_query_response and any(token in text for token in follow_up_tokens):
            return True
        return False

    def _infer_intent(self, message: str, state: ConversationState) -> str:
        lower = message.lower()
        summary_markers = {"summary", "summarize", "takeaway", "recommend", "recommendation", "insight"}
        if any(marker in lower for marker in summary_markers) and not self._is_last_sql_request(message):
            return "CHAT"

        if self.query_agent.config.llm_provider.provider == "mock":
            if self._is_last_sql_request(message):
                return "META"
            return "QUERY" if self._is_data_intent(message, state) else "CHAT"

        recent_turns = [(t.user_message, t.assistant_message) for t in state.turns[-4:]]
        semantic_objects = self._semantic_objects()
        prompt = build_intent_prompt(message, recent_turns, semantic_objects)
        try:
            completion = self.query_agent.llm.complete(prompt)
            text = completion.text if hasattr(completion, "text") else str(completion)
            label = text.strip().upper().split()[0] if text.strip() else ""
            if label in {"QUERY", "META", "CHAT"}:
                if self._is_last_sql_request(message):
                    return "META"
                return label
        except Exception:
            pass
        if self._is_last_sql_request(message):
            return "META"
        return "QUERY" if self._is_data_intent(message, state) else "CHAT"

    def _resolve_follow_up(self, message: str, state: ConversationState) -> str:
        previous = state.last_query_response
        if previous is None:
            return message

        follow_up_tokens = {"that", "those", "them", "same", "again", "previous", "above"}
        text = message.lower()
        if not any(token in text for token in follow_up_tokens):
            return message

        if self.query_agent.config.llm_provider.provider != "mock":
            prompt = build_follow_up_rewrite_prompt(
                message=message,
                previous_question=previous.question,
                previous_sql=previous.sql,
                semantic_objects=self._semantic_objects(),
            )
            try:
                completion = self.query_agent.llm.complete(prompt)
                rewritten = completion.text if hasattr(completion, "text") else str(completion)
                clean = rewritten.strip()
                if clean:
                    return clean
            except Exception:
                pass

        return (
            "Use this previous context to resolve references.\n"
            f"Previous question: {previous.question}\n"
            f"Previous SQL: {previous.sql}\n"
            f"Follow-up question: {message}"
        )

    def _meta_reply(self, message: str, state: ConversationState) -> str:
        if self._is_last_sql_request(message):
            if state.last_query_response is None:
                return "No previous SQL in this session yet."
            return f"Previous SQL:\n{state.last_query_response.sql}"
        if state.last_query_response is None:
            return "No previous query result in this session yet."
        return (
            f"Last query returned {state.last_query_response.row_count} rows. "
            "Ask for a refinement, comparison, or a time filter."
        )

    def _chat_reply(self, message: str, state: ConversationState) -> str:
        provider = self.query_agent.config.llm_provider.provider
        semantic_context = self._semantic_context_hint()
        if provider == "mock":
            if state.last_query_response is None:
                return (
                    "I can help you analyze your data. Ask a question like "
                    f"'Top customers by revenue in the last 30 days'. {semantic_context}"
                )
            return (
                "I can continue from the last result. Ask for filters, breakdowns, "
                "comparisons, or ask for the previous SQL."
            )

        history_text = self._build_history_text(state.turns[-4:])
        previous_sql = state.last_query_response.sql if state.last_query_response else "None"
        prompt = (
            "You are a conversational data analyst assistant. "
            "Reply as plain text, concise and business-oriented. "
            "Prefer recommendations aligned with known metrics and dimensions. "
            "If the user asks for analysis, suggest a concrete next analysis question.\n"
            f"Recent conversation:\n{history_text}\n"
            f"Semantic context: {semantic_context}\n"
            f"Previous SQL: {previous_sql}\n"
            f"User: {message}\n"
            "Assistant:"
        )
        try:
            completion = self.query_agent.llm.complete(prompt)
            text = completion.text if hasattr(completion, "text") else str(completion)
            clean = text.strip()
            if clean:
                return clean
        except Exception:
            pass
        return "How would you like to analyze your data next?"

    def _build_history_text(self, turns: list[ConversationTurn]) -> str:
        if not turns:
            return "No previous turns."
        lines: list[str] = []
        for turn in turns:
            lines.append(f"User: {turn.user_message}")
            lines.append(f"Assistant: {turn.assistant_message}")
        return "\n".join(lines)

    def _semantic_objects(self) -> list[str]:
        semantic = self.query_agent.semantic
        tables = [table.name for table in semantic.tables]
        views = list(semantic.preferred_views)
        metrics = [metric.get("name", "") for metric in semantic.metrics if metric.get("name")]
        dimensions = [dim.get("name", "") for dim in semantic.dimensions if dim.get("name")]
        return [*tables, *views, *metrics, *dimensions]

    def _semantic_context_hint(self) -> str:
        semantic = self.query_agent.semantic
        metric_names = [m.get("name", "") for m in semantic.metrics if m.get("name")]
        dimension_names = [d.get("name", "") for d in semantic.dimensions if d.get("name")]
        preferred = semantic.preferred_views
        parts: list[str] = []
        if preferred:
            parts.append(f"Preferred views: {', '.join(preferred)}.")
        if metric_names:
            parts.append(f"Metrics: {', '.join(metric_names[:4])}.")
        if dimension_names:
            parts.append(f"Dimensions: {', '.join(dimension_names[:4])}.")
        return " ".join(parts) if parts else "No semantic metadata loaded."
