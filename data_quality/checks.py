from __future__ import annotations

REQUIRED_SILVER_COLUMNS = {
    "date",
    "hour",
    "project",
    "language",
    "project_family",
    "page_title",
    "normalized_title",
    "view_count",
    "response_size",
    "source_file",
}


def validate_silver_schema(df) -> dict[str, object]:
    columns = set(df.columns)
    missing = sorted(REQUIRED_SILVER_COLUMNS - columns)
    unexpected = sorted(columns - REQUIRED_SILVER_COLUMNS)
    return {
        "passed": not missing,
        "missing_columns": missing,
        "unexpected_columns": unexpected,
    }


def validate_silver_values(df) -> dict[str, object]:
    negative_counts = df.filter((df.view_count < 0) | (df.response_size < 0)).count()
    null_projects = df.filter(df.project.isNull() | (df.project == "")).count()
    null_titles = df.filter(df.page_title.isNull() | (df.page_title == "")).count()
    return {
        "passed": negative_counts == 0 and null_projects == 0 and null_titles == 0,
        "negative_counts": negative_counts,
        "null_projects": null_projects,
        "null_titles": null_titles,
    }

