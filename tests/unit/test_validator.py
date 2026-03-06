from data_agent_core.sql.validator import SQLValidator


def test_blocks_mutation() -> None:
    validator = SQLValidator()
    result = validator.validate("DELETE FROM x")
    assert not result.passed


def test_allows_select() -> None:
    validator = SQLValidator()
    result = validator.validate("SELECT 1")
    assert result.passed


def test_blocks_multiple_statements() -> None:
    validator = SQLValidator()
    result = validator.validate("SELECT 1; SELECT 2")
    assert not result.passed


def test_blocks_forbidden_table() -> None:
    validator = SQLValidator(forbidden_tables=["secret_table"])
    result = validator.validate("SELECT * FROM secret_table")
    assert not result.passed


def test_blocks_disallowed_table_when_allow_list_set() -> None:
    validator = SQLValidator(allowed_tables=["public_view"])
    result = validator.validate("SELECT * FROM other_table")
    assert not result.passed
