from data_agent_core.output.models import QueryExecutionResult


class ChartSuggester:
    def suggest(self, result: QueryExecutionResult) -> str | None:
        if result.row_count == 0:
            return None
        column_count = len(result.columns)
        if column_count >= 2 and any("date" in column.lower() or "time" in column.lower() for column in result.columns):
            return "line"
        if column_count == 2:
            return "bar"
        if column_count > 2:
            return "table"
        return None
