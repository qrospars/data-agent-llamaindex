from data_agent_core.sql.formatter import ensure_limit


def test_appends_limit_when_missing() -> None:
    sql, rewritten, warnings = ensure_limit("SELECT * FROM items", row_limit=50, max_row_limit=100)
    assert rewritten
    assert not warnings
    assert sql.endswith("LIMIT 50")


def test_caps_large_existing_limit() -> None:
    sql, rewritten, warnings = ensure_limit("SELECT * FROM items LIMIT 5000", row_limit=50, max_row_limit=100)
    assert rewritten
    assert warnings
    assert sql.endswith("LIMIT 100")
