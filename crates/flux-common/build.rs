fn main() {
    // Best-effort: point git at the repo's tracked hooks (pre-push guard).
    // Cargo analog of tabletop's pnpm `prepare` script.
    let _ = std::process::Command::new("git")
        .args(["config", "core.hooksPath", ".githooks"])
        .status();
}
