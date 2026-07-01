# Flux Whitepaper (Typst)

This crate compiles Typst-based whitepapers to PDF using Rust dependencies (`typst-as-lib` + `typst-pdf`).

## Build the eventbus whitepaper PDF

```bash
cargo run -p flux-whitepaper
```

Default paths:

- Input: `crates/flux-whitepaper/typst/eventbus_whitepaper.typ`
- Output: `crates/flux-whitepaper/out/eventbus_whitepaper.pdf`

## Custom paths

```bash
cargo run -p flux-whitepaper -- \
  --input crates/flux-whitepaper/typst/eventbus_whitepaper.typ \
  --output crates/flux-whitepaper/out/eventbus_whitepaper.pdf
```
