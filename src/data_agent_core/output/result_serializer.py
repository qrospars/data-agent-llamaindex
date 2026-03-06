from data_agent_core.output.models import AgentResponse


def to_json(response: AgentResponse) -> str:
    return response.model_dump_json(indent=2)
