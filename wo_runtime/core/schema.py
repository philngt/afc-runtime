"""Dependency-free JSON Schema validation for runtime contracts.

The runtime deliberately implements the bounded schema subset used by its
instances.  It is a local trust boundary for structured task results and fails
closed when it encounters an unsupported local ``$ref``.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class SchemaIssue:
    path: str
    message: str


class SchemaValidationError(ValueError):
    def __init__(self, issues: Sequence[SchemaIssue]):
        self.issues = tuple(issues)
        detail = "; ".join(f"{issue.path}: {issue.message}" for issue in self.issues)
        super().__init__(detail or "value did not match its JSON Schema")


def load_schema(path: Path | str) -> Mapping[str, Any]:
    schema_path = Path(path)
    try:
        payload = json.loads(schema_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid JSON Schema at {schema_path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"JSON Schema root must be an object: {schema_path}")
    return payload


def validate_json_schema(instance: Any, schema: Mapping[str, Any]) -> None:
    issues: list[SchemaIssue] = []
    _validate(instance, schema, "$", issues)
    if issues:
        raise SchemaValidationError(issues)


def _validate(
    instance: Any,
    schema: Mapping[str, Any],
    path: str,
    issues: list[SchemaIssue],
) -> None:
    if "$ref" in schema:
        issues.append(SchemaIssue(path, f"unsupported $ref: {schema['$ref']}"))
        return

    if "allOf" in schema:
        for branch in schema["allOf"]:
            _validate(instance, branch, path, issues)
    if "anyOf" in schema:
        branch_issues = [_branch_issues(instance, branch, path) for branch in schema["anyOf"]]
        if not any(not result for result in branch_issues):
            issues.append(SchemaIssue(path, "does not satisfy anyOf"))
    if "oneOf" in schema:
        matches = sum(not _branch_issues(instance, branch, path) for branch in schema["oneOf"])
        if matches != 1:
            issues.append(
                SchemaIssue(path, f"must satisfy exactly one oneOf branch (matched {matches})")
            )
    condition = schema.get("if")
    if isinstance(condition, Mapping):
        matched = not _branch_issues(instance, condition, path)
        branch = schema.get("then" if matched else "else")
        if isinstance(branch, Mapping):
            _validate(instance, branch, path, issues)

    if "const" in schema and instance != schema["const"]:
        issues.append(SchemaIssue(path, f"must equal {schema['const']!r}"))
    if "enum" in schema and instance not in schema["enum"]:
        issues.append(SchemaIssue(path, f"must be one of {schema['enum']!r}"))

    expected = schema.get("type")
    if expected is not None:
        accepted = [expected] if isinstance(expected, str) else list(expected)
        if not any(_is_type(instance, kind) for kind in accepted):
            issues.append(SchemaIssue(path, f"expected type {'|'.join(accepted)}"))
            return

    if isinstance(instance, dict):
        _validate_object(instance, schema, path, issues)
    elif isinstance(instance, list):
        _validate_array(instance, schema, path, issues)
    elif isinstance(instance, str):
        _validate_string(instance, schema, path, issues)
    elif _is_number(instance):
        _validate_number(instance, schema, path, issues)


def _validate_object(
    instance: Mapping[str, Any],
    schema: Mapping[str, Any],
    path: str,
    issues: list[SchemaIssue],
) -> None:
    required = schema.get("required", [])
    for key in required:
        if key not in instance:
            issues.append(SchemaIssue(path, f"missing required property {key!r}"))

    properties = schema.get("properties", {})
    additional = schema.get("additionalProperties", True)
    for key, value in instance.items():
        child_path = f"{path}.{key}"
        if key in properties:
            _validate(value, properties[key], child_path, issues)
        elif additional is False:
            issues.append(SchemaIssue(child_path, "additional property is not allowed"))
        elif isinstance(additional, dict):
            _validate(value, additional, child_path, issues)

    if len(instance) < schema.get("minProperties", 0):
        issues.append(SchemaIssue(path, "has too few properties"))
    if "maxProperties" in schema and len(instance) > schema["maxProperties"]:
        issues.append(SchemaIssue(path, "has too many properties"))


def _validate_array(
    instance: Sequence[Any],
    schema: Mapping[str, Any],
    path: str,
    issues: list[SchemaIssue],
) -> None:
    if len(instance) < schema.get("minItems", 0):
        issues.append(SchemaIssue(path, "has too few items"))
    if "maxItems" in schema and len(instance) > schema["maxItems"]:
        issues.append(SchemaIssue(path, "has too many items"))
    if schema.get("uniqueItems"):
        encoded = [json.dumps(item, sort_keys=True, ensure_ascii=False) for item in instance]
        if len(encoded) != len(set(encoded)):
            issues.append(SchemaIssue(path, "items must be unique"))
    item_schema = schema.get("items")
    if isinstance(item_schema, dict):
        for index, value in enumerate(instance):
            _validate(value, item_schema, f"{path}[{index}]", issues)
    contains_schema = schema.get("contains")
    if isinstance(contains_schema, dict):
        matches = sum(not _branch_issues(value, contains_schema, path) for value in instance)
        minimum = int(schema.get("minContains", 1))
        maximum = schema.get("maxContains")
        if matches < minimum:
            issues.append(
                SchemaIssue(path, f"contains matched {matches}; expected at least {minimum}")
            )
        if maximum is not None and matches > int(maximum):
            issues.append(
                SchemaIssue(path, f"contains matched {matches}; expected at most {maximum}")
            )


def _validate_string(
    value: str,
    schema: Mapping[str, Any],
    path: str,
    issues: list[SchemaIssue],
) -> None:
    if len(value) < schema.get("minLength", 0):
        issues.append(SchemaIssue(path, "is shorter than minLength"))
    if "maxLength" in schema and len(value) > schema["maxLength"]:
        issues.append(SchemaIssue(path, "is longer than maxLength"))
    pattern = schema.get("pattern")
    if pattern is not None and re.search(pattern, value) is None:
        issues.append(SchemaIssue(path, f"does not match pattern {pattern!r}"))


def _validate_number(
    value: int | float,
    schema: Mapping[str, Any],
    path: str,
    issues: list[SchemaIssue],
) -> None:
    if "minimum" in schema and value < schema["minimum"]:
        issues.append(SchemaIssue(path, f"must be >= {schema['minimum']}"))
    if "maximum" in schema and value > schema["maximum"]:
        issues.append(SchemaIssue(path, f"must be <= {schema['maximum']}"))
    if "exclusiveMinimum" in schema and value <= schema["exclusiveMinimum"]:
        issues.append(SchemaIssue(path, f"must be > {schema['exclusiveMinimum']}"))
    if "exclusiveMaximum" in schema and value >= schema["exclusiveMaximum"]:
        issues.append(SchemaIssue(path, f"must be < {schema['exclusiveMaximum']}"))


def _branch_issues(
    instance: Any,
    schema: Mapping[str, Any],
    path: str,
) -> list[SchemaIssue]:
    result: list[SchemaIssue] = []
    _validate(instance, schema, path, result)
    return result


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_type(value: Any, expected: str) -> bool:
    return {
        "null": value is None,
        "boolean": isinstance(value, bool),
        "object": isinstance(value, dict),
        "array": isinstance(value, list),
        "number": _is_number(value),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "string": isinstance(value, str),
    }.get(expected, False)
