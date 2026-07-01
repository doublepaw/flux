use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use anyhow::{Context, Result, anyhow, bail};

const DEFAULT_DATABASE_URL: &str = "postgres://postgres:postgres@localhost:5433";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("xtask error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let mut args = env::args();
    let _bin = args.next();
    let command = args.next().unwrap_or_else(|| "help".to_string());

    match command.as_str() {
        "help" | "--help" | "-h" => {
            print_help();
            Ok(())
        }
        "gen-proto" => gen_proto(),
        "build" => build_workspace(),
        "test-rust" => test_rust(),
        "test-db" => test_db(),
        "test-sdk" => test_sdk(),
        "test-all" => test_all(),
        "ci" => ci(),
        other => {
            print_help();
            bail!("unknown subcommand: {other}");
        }
    }
}

fn print_help() {
    println!("cargo xtask <command>");
    println!();
    println!("Commands:");
    println!("  gen-proto   Regenerate protobuf code for Java/Python and rebuild Rust proto");
    println!("  build       Build the Rust workspace");
    println!("  test-rust   Run Rust tests (workspace)");
    println!("  test-db     Run flux-broker DB suites with --include-ignored");
    println!("  test-sdk    Run Java and Python SDK tests");
    println!("  test-all    Run all tests across Rust + Java + Python + DB suites");
    println!("  ci          Run gen-proto + build + test-rust + test-sdk + test-db");
}

fn ci() -> Result<()> {
    gen_proto()?;
    build_workspace()?;
    test_rust()?;
    test_sdk()?;
    test_db()?;
    Ok(())
}

fn test_all() -> Result<()> {
    ci()
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask lives at <repo>/xtask")
        .to_path_buf()
}

fn sh_path(rel: &str) -> PathBuf {
    repo_root().join(rel)
}

fn run_cmd(cmd: &mut Command) -> Result<()> {
    eprintln!("+ {:?}", cmd);
    let status = cmd.status().context("failed to spawn command")?;
    if status.success() {
        Ok(())
    } else {
        Err(anyhow!("command exited with status {status}: {:?}", cmd))
    }
}

fn build_workspace() -> Result<()> {
    let mut cmd = Command::new("cargo");
    cmd.current_dir(repo_root()).arg("build").arg("--workspace");
    run_cmd(&mut cmd)
}

fn test_rust() -> Result<()> {
    let mut cmd = Command::new("cargo");
    cmd.current_dir(repo_root()).arg("test").arg("--workspace");
    run_cmd(&mut cmd)
}

fn gen_proto() -> Result<()> {
    let root = repo_root();
    ensure_exists(&root.join("proto/flux_wire.proto"))?;

    let mut java = Command::new("protoc");
    java.current_dir(&root).args([
        "-I",
        "proto",
        "--java_out=sdks/java/flux-sdk/src/main/java",
        "proto/flux_wire.proto",
    ]);
    run_cmd(&mut java).context("failed to generate Java protobuf files")?;

    let mut py = Command::new("protoc");
    py.current_dir(&root).args([
        "-I",
        "proto",
        "--python_out=sdks/python/flux/proto",
        "proto/flux_wire.proto",
    ]);
    run_cmd(&mut py).context("failed to generate Python protobuf file")?;

    let mut rust = Command::new("cargo");
    rust.current_dir(&root)
        .args(["build", "-p", "flux-wire"]);
    run_cmd(&mut rust).context("failed to rebuild Rust protobuf output")?;

    Ok(())
}

fn test_db() -> Result<()> {
    let db_url = env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_string());
    let root = repo_root();
    let suites = discover_broker_test_suites()?;
    if suites.is_empty() {
        bail!("no flux-broker integration test suites found");
    }
    eprintln!("Discovered {} flux-broker test suite(s)", suites.len());

    let mut failed = Vec::new();

    for suite in &suites {
        eprintln!("=== {suite} ===");
        let mut cmd = Command::new("cargo");
        cmd.current_dir(&root).env("DATABASE_URL", &db_url).args([
            "test",
            "-q",
            "-p",
            "flux-broker",
            "--test",
            suite.as_str(),
            "--",
            "--include-ignored",
        ]);

        if let Err(err) = run_cmd(&mut cmd) {
            eprintln!("FAIL {suite}: {err:#}");
            failed.push(suite.clone());
        }
    }

    if failed.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("DB test suites failed: {}", failed.join(", ")))
    }
}

fn discover_broker_test_suites() -> Result<Vec<String>> {
    let tests_dir = sh_path("crates/flux-broker/tests");
    ensure_exists(&tests_dir)?;

    let mut suites = Vec::new();
    for entry in fs::read_dir(&tests_dir).context("failed to read flux-broker tests dir")? {
        let entry = entry.context("failed to read test dir entry")?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        let stem = match path.file_stem().and_then(|s| s.to_str()) {
            Some(s) => s,
            None => continue,
        };
        if stem == "mod" {
            continue;
        }
        suites.push(stem.to_string());
    }

    suites.sort();
    Ok(suites)
}

fn test_sdk() -> Result<()> {
    test_java_sdk()?;
    test_python_sdk()?;
    Ok(())
}

fn test_java_sdk() -> Result<()> {
    let mut cmd = Command::new("mvn");
    cmd.current_dir(sh_path("sdks/java/flux-sdk"))
        .args(["test"]);
    run_cmd(&mut cmd).context("java SDK tests failed")
}

fn test_python_sdk() -> Result<()> {
    let sdk_dir = sh_path("sdks/python");

    let mut venv = Command::new("python3");
    venv.current_dir(&sdk_dir).args(["-m", "venv", ".venv"]);
    run_cmd(&mut venv).context("python venv creation failed")?;

    let pip = venv_bin(&sdk_dir, "pip");
    let mut pip_cmd = Command::new(&pip);
    pip_cmd
        .current_dir(&sdk_dir)
        .args(["install", "-e", ".[dev]"]);
    run_cmd(&mut pip_cmd).context("python SDK dependency install failed")?;

    let pytest = venv_bin(&sdk_dir, "pytest");
    let mut test_cmd = Command::new(&pytest);
    test_cmd.current_dir(&sdk_dir).arg("tests/");
    run_cmd(&mut test_cmd).context("python SDK tests failed")?;

    Ok(())
}

fn venv_bin(sdk_dir: &Path, name: &str) -> PathBuf {
    if cfg!(windows) {
        sdk_dir
            .join(".venv")
            .join("Scripts")
            .join(format!("{name}.exe"))
    } else {
        sdk_dir.join(".venv").join("bin").join(name)
    }
}

fn ensure_exists(path: &Path) -> Result<()> {
    if path.exists() {
        Ok(())
    } else {
        Err(anyhow!("required path missing: {}", path.display()))
    }
}
