# Service Code Style

This document captures the coding conventions shared by the services in this monorepo. The Python rules reflect the canonical implementation in `mail-processor/` and `google-sheets-accessor/`. Use it as the primary reference for contributions across services.

## Golang

- Prefer table-driven tests (`[]struct{ name string; ... }`) over many similar test functions; keep `t.Parallel()` where it does not break expectations.
- Target full coverage when adding unit tests: cover both successful logic and error branches, exercising private helpers through public methods.
- Tests live next to the source file (`y_test.go` beside `y.go`) and use the external package form (`package x_test`); keep one test file per source file.
- Use `github.com/stretchr/testify/mock` for doubles and place reusable mocks under the shared `mocks/` packages.
- When touching private func behaviour, drive it through public entry points instead of calling unexported helpers directly.
- For mocking rely on `github.com/stretchr/testify/mock` only; avoid custom stubs/fakes unless the library cannot cover the case.

## Python

### Coding Guidelines

- **Python version**: Assume Python 3.13 features. Use `match`/`|` typing syntax and `list[str]` generics.
- **Type hints**: Every function (public or private) requires explicit return and parameter annotations.
- **Indentation & spacing**: Use 4-space indents, separate logical blocks with blank lines, and insert a blank line before each `return`.
- **Returns**: Assign expressions to a local variable before returning to improve readability and instrumentation.
- **Control flow**: Guard clauses should return well-named dataclasses or dictionaries; avoid inline ternary expressions in returns.
- **Exceptions**: Always capture exceptions as `e` (`except SomeError as e:`) and propagate using `raise ... from e` when rethrowing.
- **Logging**: Retrieve module-level loggers via `logging.getLogger(__name__)`. Use descriptive messages with context data.
- **Naming**: `snake_case` for modules/functions/variables, `PascalCase` for classes & dataclasses, `CONSTANT_CASE` for constants. Keep names descriptive.
- **Data containers**: Favour `dataclass` DTOs for outbound API payloads and Pydantic models for inbound requests.
- **Enum handling**: Expose enum helpers (`is_valid`, `get_all`) on enum classes for validation.
- **Comments & language**: Comments are in English only and used sparingly when logic is not human-readable. Prefer Python 3.13 style constructs when refactoring.
- **Style**: Keep function bodies human readable. Split logical parts with blank lines, add spaces before `return`, return named variables instead of inline expressions, and keep functions small. Example:
```
def main(bar: int) -> None:
    foo_result = foo(bar)

    processed_value = foo_result["rar"]
    result = far(processed_value)

    print(result)


def foo(bar: int) -> dict[str, int]:
    data: dict[str, int] = {}

    updated_bar = bar + 1

    data["rar"] = updated_bar
    data["rut"] = 0

    return data


def far(bar: int) -> str:
    bar_str = str(bar)

    return bar_str
```

### Testing & Tooling Expectations

Prefer focussed `pytest` modules under `service/tests/` mirroring the `app/` structure.

Adhering to this style keeps services interchangeable, makes handlers predictable, and streamlines cross-service maintenance.
