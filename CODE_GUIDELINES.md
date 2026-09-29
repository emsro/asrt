# Code Guidelines

This document defines how code should be written and changed in this repository.
It applies to all contributors, including human developers and AI coding agents.

## 1. Core Principles

- Keep `asrtl`, `asrtr`, and `asrtc` as pure C libraries.
- Treat C++ layers (`asrtlpp`, `asrtrpp`, `asrtcpp`) as wrappers and integration tooling.
- Keep the code concise and minimal. Avoid unnecessary abstractions or over-engineering.

## 2. Build and Test Workflow

Use CMake workflow presets to build and test the project.

- Required command pattern:
  - `cmake --workflow --preset <name>`
- Do not run bare `cmake --build` or `ctest` on their own.
- Common preset for regular validation:
  - `cmake --workflow --preset debug`
- Other supported presets:
  - `asan`, `ubsan`, `coverage`, `release`
  - `windows-debug` (Windows only)

## 3. Required Fix Workflow (Test-First)

When fixing a bug:

1. Add or update a test that reproduces the issue.
2. Run workflow and confirm it fails for the expected reason.
3. Implement the fix.
4. Run workflow and confirm tests pass.

A fix without a reproduction test is incomplete unless there is a strong reason (document that reason in the PR/commit message).

## 4. Style and Formatting

- Follow existing naming and API style in the touched module.
- Expect pre-commit hooks to enforce:
  - trailing whitespace cleanup
  - end-of-file newline
  - clang-format for `*.c` and `*.h`
  - cmake-format for CMake files

## 5. C/C++ code guidelines

- Public functions should return `enum asrt_status` unless there is a strong existing pattern in that module.
- Validate inputs early and return explicit status codes.
- Keep ownership rules explicit:
  - Caller-owned buffers stay caller-owned.
  - Allocator usage must be consistent with module init/deinit conventions.
- Keep callback contracts precise:
  - Document callback invocation order and one-shot vs multi-shot behavior.
- Avoid hidden state or side effects. Make all state changes explicit in the API.
  - No global mutable state.
  - No static variables.
- Any error path should be tested and have error log statement.
- For any asynchronous behavior, there should be a callback called on completion or failure.
- Avoid introducing transitive include dependencies as a hidden requirement.
- Preserve license header conventions on C/C header files.
- C++ files shall contain type alises to C types, and use only those aliases.
- In C, any pointer to callback is immediately followed by a pointer to user data for that callback, and the callback is always called with that user data as the first argument.

## 6. Tests

- Add tests nearest to the changed layer:
  - C core changes -> `test/asrtl_test.cpp`, `test/asrtr_test.cpp`, `test/asrtc_test.cpp`
  - C++ wrappers -> corresponding `*_test.cpp` in `test/`
  - asrtio flow changes -> `test/asrtio_test.cpp`
- Prefer deterministic tests with clear setup/teardown.
- Avoid hidden timing dependencies. Use explicit tick/event loop progression.
- Each public API must have a documentation comment (`///`) and at least one unit test.
- Each public C++ API must have a sender-based alternative, unless the function itself already returns a sender.
- In tests, never discard return values with `(void)` or `std::ignore`. Always check every status code, even when the test is about a different API — this ensures coverage.
- Use log statements (`ASRT_LOG` / `ASRT_ERR_LOG`) generously in tests so failures are diagnosable from the test output alone.
