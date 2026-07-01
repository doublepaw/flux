# Coding Guidelines

## Conciseness
- Write less code; every line is a liability
- Delete dead code immediately; don't comment it out
- Avoid premature abstraction; duplicate 2-3 times before extracting
- No speculative generality; solve today's problem
- Prefer standard library over custom implementations
- Use macros to eliminate repetitive patterns, not to be clever

## Code Organization
- One concept per file; split when a file exceeds ~300 lines
- Group related functionality into modules
- Public API at top of file, private helpers below
- Keep function bodies short (<30 lines); extract when logic is reusable
- Colocate tests with implementation (`#[cfg(test)]` in same file)
- Flat is better than nested; avoid deep module hierarchies

## Documentation
- Each module folder should have a README.md describing purpose and usage
- Update README.md when adding/changing endpoints, public APIs, or architecture
- Keep READMEs concise: architecture overview, usage example, API reference
- Code should be self-documenting; READMEs explain the "why" and "how to use"

## Git Commits
- Commit when you have a coherent, working unit of change
- One logical change per commit; avoid mixing unrelated changes
- Commit before risky refactors or experiments (easy to revert)
- Commit after completing a feature, fixing a bug, or finishing a refactor
- Don't commit after every line; don't wait until everything is done
- Message format: imperative verb + what changed + why (if not obvious)
- If a commit message needs "and", consider splitting the commit

## Naming
- Names should be self-documenting; avoid comments that restate code
- Functions: verb phrases (`parse_schema`, `write_batch`)
- Booleans: `is_`, `has_`, `should_` prefixes
- Avoid abbreviations except well-known ones (`config`, `ctx`, `err`)

## Rust Style
- Use `rustfmt` defaults
- Prefer `snake_case` for functions/variables, `PascalCase` for types
- Keep lines under 100 chars when practical

## Error Handling
- Use `thiserror` for library errors, `anyhow` for applications
- Prefer `?` over `.unwrap()` in production code
- Return `Result<T, E>` rather than panicking

## Ownership & Borrowing
- Prefer borrowing (`&T`) over ownership when possible
- Use `Clone` sparingly; prefer references
- Use `Cow<'a, T>` when ownership is conditionally needed

## Patterns
- Use builders for complex struct construction
- Prefer iterators over loops: `.iter().map().filter().collect()`
- Use `match` exhaustively; avoid `_ =>` catch-alls when possible
- Prefer `if let` for single-variant matching
- Consolidate similar match arms with `|`

## Performance
- Use `&str` over `String` for function parameters
- Prefer `Vec::with_capacity()` when size is known
- Use `#[inline]` sparingly and only with benchmarks
- Profile before optimizing

## Testing
- Test behavior, not implementation
- One logical assertion per test
- Avoid low-signal tests (trivial getters, obvious behavior)
- Name tests descriptively: `test_parse_invalid_schema_returns_error`

## Dependencies
- Prefer well-maintained crates from the ecosystem
- Pin major versions in `Cargo.toml`
- Use feature flags to minimize compile times

## Async
- Use `tokio` as the async runtime
- Prefer `async fn` over manual `Future` implementations
- Avoid blocking operations in async contexts
