# wow_demo

Demonstrates domain-specific semantics layered on top of the generic `data-agent-core` runtime.

## Purpose
- Keep SQL safety/runtime generic.
- Encode business/domain language in semantic config and prompts.
- Reuse deterministic analytics modules with domain-specific metrics/dimensions.

## Suggested flow
1. Ingest WoW-related dataset(s) with `analytics_ingest_csv`.
2. Profile and build semantic model (`analytics_profile_dataset`, `analytics_build_semantic_layer`).
3. Run `analytics_run_analysis_plan` for `diagnostic` and `deep_dive` questions.

## After setup
1. Curate metric definitions and caveats for game-specific KPIs.
2. Add fixture-based tests for key balance/economy questions.
3. Track evidence bundle quality (finding coverage, caveat quality, follow-up usefulness).
