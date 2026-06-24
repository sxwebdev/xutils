# xutils

Collection of Go utility packages (Go 1.26+). Each top-level directory is an
independent package.

## Testing

**The goal of testing is to find real bugs, not to make tests pass.** A green
suite proves nothing on its own — it may just be fitted to the current code.

- **Trust nothing; verify against the code.** Existing tests, comments, and docs
  can all be wrong or stale. Read the implementation and confirm what it actually
  does before relying on any claim about it.
- **Treat every doubt as a red flag.** A surprising result, an odd assertion, a
  test you had to "adjust" to make pass — stop and re-check the code. The
  surprise is usually a real bug, not a test that needs tweaking.
- **Prove a test catches its bug.** When a test guards a specific bug, confirm it
  fails against the broken code (e.g. temporarily revert the fix), then restore.
  A test that passes both ways guards nothing.

Write tests that verify **behavior**, not the implementation. Concretely:

- **Test the contract, not the code.** Assertions must follow from the public
  promise. If a refactor that keeps behavior identical breaks a test, the test
  was fitted to the code — fix the test.
- **Count real effects, not self-reported ones.** Assert on observable behavior
  (number of times a callback ran, attempts made, side effects) rather than on
  a field the code sets unconditionally. A counter that proves "ran exactly N
  times" catches off-by-one bugs; re-checking a value the code copied from input
  does not.
- **Assert both directions on timing/ranges.** Use a lower _and_ an upper bound
  so the test catches both "too little" (e.g. delay not applied) and "too much"
  (e.g. delay grew when it shouldn't). A single upper bound hides whole classes
  of bugs.
- **Cover the full surface:** success, success-after-failures, exhaustion,
  early-exit/short-circuit, invalid input (nil/zero/negative), every policy or
  mode, and all error branches. Table-drive repetitive cases instead of copying
  near-identical tests.
- **Use `t.Context()`** as the base context in every test. Derive cancellable
  or deadline contexts from it (`context.WithCancel(t.Context())`,
  `context.WithTimeout(t.Context(), …)`) — never `context.Background()`. It is
  auto-cancelled at test end, preventing leaks.
- **Verify errors by identity, not text.** Use `errors.Is` / `errors.As` /
  `errors.AsType` to match wrapped causes and custom error types. Check the
  exact message string at most once, only where the format is part of the
  contract.
- **Run with `-race`** for anything that spawns goroutines or touches shared
  state.

### Test layout

- **Default to the external `package <pkg>_test`** (black-box): it forces tests
  through the public API, the way real callers use it. Drop to the internal
  `package <pkg>` only when a test genuinely needs unexported symbols, and keep
  those internal tests to a minimum.
- **Mirror the source files.** Ideally each source file has its own test file
  (`number.go` → `number_test.go`), so tests sit next to the code they cover.
  This is a preference, not a rule — a couple of small files can share one — but
  never let tests for several source files pile into one sprawling catch-all
  test file. Split by the file under test.

### Coverage

- **Always aim for 100% statement coverage per package.** ≥ 95% is the hard
  floor, not the target — every uncovered line is a line no test exercises, so
  treat the gap as work left to do, not as "good enough".
- Close the gap by covering real behavior (error branches, edge cases, every
  policy/mode), never by adding tautological tests just to move the number.
- Check it before finishing, and inspect what is still uncovered:
  ```bash
  go test ./<pkg>/ -count=1 -race -coverprofile=cover.out
  go tool cover -func=cover.out | tail -1        # total
  go tool cover -func=cover.out | grep -v 100.0% # what is left
  ```
- The only acceptable gaps are defensive branches that cannot be triggered
  without unreasonable cost (e.g. an integer-overflow guard that would require a
  multi-day sleep, or a `default` made unreachable by prior validation). Each
  such line must be a deliberate, named exclusion — explain why it is
  unreachable rather than leaving it silently uncovered.

## Documentation

Documentation must always match the code. Treat it like tests — it can be stale,
so never trust it over the implementation.

- When doc comments, `doc.go`, or `README.md` contradict the code, the code is
  the source of truth — but the docs are then a bug that must be fixed.
- Do **not** rewrite docs to match the code silently. First confirm whether the
  code or the doc reflects the intended behavior, then **ask the developer**
  before changing it — the contradiction may mean the code is wrong, not the doc.

## Before committing

```bash
go vet ./...
go test ./... -count=1 -race
```
