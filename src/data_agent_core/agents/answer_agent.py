from data_agent_core.output.models import QueryExecutionResult
from data_agent_core.output.summarizer import AnswerSummarizer


class AnswerAgent:
    def __init__(self) -> None:
        self.summarizer = AnswerSummarizer()

    def explain(self, question: str, result: QueryExecutionResult) -> str:
        return self.summarizer.summarize(question, result)
