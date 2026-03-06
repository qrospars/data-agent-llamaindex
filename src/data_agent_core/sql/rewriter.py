import re


def normalize_sql(sql_text: str) -> str:
    sql = sql_text.strip()
    sql = re.sub(r"^```sql", "", sql, flags=re.IGNORECASE).strip()
    sql = sql.removeprefix("```").removesuffix("```").strip()
    if sql.lower().startswith("sql "):
        sql = sql[4:]
    return sql.strip()
