# Architecture

Flow:

1. Question enters `QueryAgent`.
2. `SchemaIntrospector` builds prompt context from database + semantic metadata.
3. `SQLGenerator` creates SQL via LLM.
4. `SQLValidator` and SQL normalization enforce safety.
5. `SafeSQLExecutor` runs SQL through connector.
6. `AnswerSummarizer` and `ChartSuggester` produce user-facing output.

LlamaIndex is used as the LLM integration layer, while orchestration remains modular and inspectable in the package.
