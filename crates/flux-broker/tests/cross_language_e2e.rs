// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Cross-language E2E tests.
//!
//! These tests verify that Java and Python SDKs can communicate with the Rust broker.
//!
//! Prerequisites:
//! - Python 3.10+ with websockets installed: pip install websockets
//! - Java 17+ with the SDK built: cd sdks/java/flux-sdk && mvn package
//! - DATABASE_URL pointing to a Postgres instance
//!
//! ```bash
//! DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test --test cross_language_e2e
//! ```

mod common;

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::OnceLock;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::process::Command as TokioCommand;
use tokio::sync::Mutex;

use common::TestDb;
use flux_broker::buffer::BufferConfig;
use flux_broker::{BrokerConfig, BrokerState, LocalFsStore};

static CROSS_LANGUAGE_TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

async fn cross_language_test_lock() -> tokio::sync::MutexGuard<'static, ()> {
    CROSS_LANGUAGE_TEST_MUTEX
        .get_or_init(|| Mutex::new(()))
        .lock()
        .await
}

/// Find the project root directory.
fn project_root() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(manifest_dir)
        .parent() // crates/
        .unwrap()
        .parent() // flux/
        .unwrap()
        .to_path_buf()
}

/// Find an available port.
async fn find_available_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

/// Start the broker server in background.
async fn start_server(
    pool: sqlx::PgPool,
    temp_dir: &TempDir,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let addr = find_available_port().await;
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());

    let config = BrokerConfig {
        bind_addr: addr,
        bucket: "test".to_string(),
        key_prefix: "cross-lang-test".to_string(),
        buffer: BufferConfig::default(),
        flush_interval: Duration::from_millis(50),
        require_auth: false,
        auth_timeout: Duration::from_secs(10),
        #[cfg(feature = "iceberg")]
        iceberg: None,
        readahead_max_bytes: 0,
            flush_pipeline_depth: 4,
    };

    let state = BrokerState::new(pool, store, config).await;

    let handle = tokio::spawn(async move {
        if let Err(e) = flux_broker::run(state).await {
            eprintln!("Server error: {}", e);
        }
    });

    // Wait for server to become reachable before launching external clients.
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return (addr, handle);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    panic!("Server did not become reachable at {}", addr);
}

#[derive(Debug, Serialize, Deserialize)]
struct WriteResult {
    writer: String,
    writer_id: String,
    topic_id: i32,
    start_offset: i64,
    end_offset: i64,
    record_count: i32,
}

#[derive(Debug, Serialize, Deserialize)]
struct SchemaWriteResult {
    writer: String,
    topic_id: i32,
    start_offset: i64,
    end_offset: i64,
    record_count: i32,
    schema_json: String,
    fields: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct SchemaReadResult {
    reader: String,
    topic_id: i32,
    record_count: i32,
    records: Vec<SchemaRecordData>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SchemaRecordData {
    key: Option<String>,
    name: String,
    amount: i64,
    active: bool,
    tags: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReadResult {
    reader: String,
    topic_id: i32,
    record_count: i32,
    records: Vec<RecordData>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RecordData {
    key: Option<String>,
    value: serde_json::Value,
}

/// Run Python writer.
async fn run_python_writer(
    url: &str,
    topic_id: i32,
    num_records: i32,
) -> Result<WriteResult, String> {
    let root = project_root();
    let script = root.join("tests/cross_language/python_writer.py");

    let output = TokioCommand::new("python3")
        .arg(&script)
        .arg(url)
        .arg(topic_id.to_string())
        .arg(num_records.to_string())
        .env("PYTHONPATH", root.join("sdks/python"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("Failed to run Python writer: {}", e))?;

    parse_output::<WriteResult>(output, "Python writer")
}

/// Run Python reader.
async fn run_python_reader(
    url: &str,
    topic_id: i32,
    expected_count: i32,
) -> Result<ReadResult, String> {
    let root = project_root();
    let script = root.join("tests/cross_language/python_reader.py");

    let output = TokioCommand::new("python3")
        .arg(&script)
        .arg(url)
        .arg(topic_id.to_string())
        .arg(expected_count.to_string())
        .env("PYTHONPATH", root.join("sdks/python"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("Failed to run Python reader: {}", e))?;

    parse_output::<ReadResult>(output, "Python reader")
}

/// Run Python raw (zero-copy) reader.
async fn run_python_raw_reader(
    url: &str,
    topic_id: i32,
    expected_count: i32,
) -> Result<ReadResult, String> {
    let root = project_root();
    let script = root.join("tests/cross_language/python_raw_reader.py");

    let output = TokioCommand::new("python3")
        .arg(&script)
        .arg(url)
        .arg(topic_id.to_string())
        .arg(expected_count.to_string())
        .env("PYTHONPATH", root.join("sdks/python"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("Failed to run Python raw reader: {}", e))?;

    parse_output::<ReadResult>(output, "Python raw reader")
}

/// Run Java writer.
async fn run_java_writer(
    url: &str,
    topic_id: i32,
    num_records: i32,
) -> Result<WriteResult, String> {
    let root = project_root();
    let jar_path = root.join("sdks/java/flux-sdk/target/flux-sdk-0.1.0.jar");
    let dependency_glob = root.join("sdks/java/flux-sdk/target/dependency/*");
    let test_classes = root.join("tests/cross_language");
    let compile_classpath = format!("{}:{}", jar_path.display(), dependency_glob.display());

    // First compile the Java test class if not already compiled
    let compile_output = TokioCommand::new("javac")
        .arg("-cp")
        .arg(&compile_classpath)
        .arg("-d")
        .arg(&test_classes)
        .arg(test_classes.join("JavaWriter.java"))
        .output()
        .await;

    let compile_output =
        compile_output.map_err(|e| format!("Failed to run javac for writer: {}", e))?;
    if !compile_output.status.success() {
        return Err(format!(
            "Failed to compile Java writer (status {:?})\nstderr: {}\nstdout: {}",
            compile_output.status.code(),
            String::from_utf8_lossy(&compile_output.stderr),
            String::from_utf8_lossy(&compile_output.stdout)
        ));
    }

    let classpath = format!(
        "{}:{}:{}",
        jar_path.display(),
        dependency_glob.display(),
        test_classes.display()
    );

    let output = TokioCommand::new("java")
        .arg("-cp")
        .arg(&classpath)
        .arg("io.flux.test.JavaWriter")
        .arg(url)
        .arg(topic_id.to_string())
        .arg(num_records.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("Failed to run Java writer: {}", e))?;

    parse_output::<WriteResult>(output, "Java writer")
}

/// Run Java reader.
async fn run_java_reader(
    url: &str,
    topic_id: i32,
    expected_count: i32,
) -> Result<ReadResult, String> {
    let root = project_root();
    let jar_path = root.join("sdks/java/flux-sdk/target/flux-sdk-0.1.0.jar");
    let dependency_glob = root.join("sdks/java/flux-sdk/target/dependency/*");
    let test_classes = root.join("tests/cross_language");
    let compile_classpath = format!("{}:{}", jar_path.display(), dependency_glob.display());

    // First compile the Java test class if not already compiled
    let compile_output = TokioCommand::new("javac")
        .arg("-cp")
        .arg(&compile_classpath)
        .arg("-d")
        .arg(&test_classes)
        .arg(test_classes.join("JavaReader.java"))
        .output()
        .await;

    let compile_output =
        compile_output.map_err(|e| format!("Failed to run javac for reader: {}", e))?;
    if !compile_output.status.success() {
        return Err(format!(
            "Failed to compile Java reader (status {:?})\nstderr: {}\nstdout: {}",
            compile_output.status.code(),
            String::from_utf8_lossy(&compile_output.stderr),
            String::from_utf8_lossy(&compile_output.stdout)
        ));
    }

    let classpath = format!(
        "{}:{}:{}",
        jar_path.display(),
        dependency_glob.display(),
        test_classes.display()
    );

    let output = TokioCommand::new("java")
        .arg("-cp")
        .arg(&classpath)
        .arg("io.flux.test.JavaReader")
        .arg(url)
        .arg(topic_id.to_string())
        .arg(expected_count.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("Failed to run Java reader: {}", e))?;

    parse_output::<ReadResult>(output, "Java reader")
}

/// Parse command output as JSON.
fn parse_output<T: for<'de> Deserialize<'de>>(output: Output, name: &str) -> Result<T, String> {
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(format!(
            "{} failed with exit code {:?}\nstderr: {}\nstdout: {}",
            name,
            output.status.code(),
            stderr,
            stdout
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(&stdout)
        .map_err(|e| format!("Failed to parse {} output: {}\nOutput: {}", name, e, stdout))
}

/// Check if Python is available with required packages.
fn python_available() -> bool {
    Command::new("python3")
        .arg("-c")
        .arg("import websockets")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Check if Java SDK is built.
fn java_sdk_available() -> bool {
    let root = project_root();
    let jar_path = root.join("sdks/java/flux-sdk/target/flux-sdk-0.1.0.jar");
    let dep_dir = root.join("sdks/java/flux-sdk/target/dependency");
    let class_dir = root.join("sdks/java/flux-sdk/target/classes/io/flux/sdk");
    let required_jars = ["protobuf-java-4.33.4.jar", "gson-2.11.0.jar"];
    let required_classes = [
        "Writer.class",
        "WriterConfig.class",
        "GroupReader.class",
        "ReaderConfig.class",
    ];
    let has_required_jars = required_jars.iter().all(|name| dep_dir.join(name).exists());
    let has_required_classes = required_classes
        .iter()
        .all(|name| class_dir.join(name).exists());
    let has_stale_protobuf = dep_dir.join("protobuf-java-4.33.0.jar").exists();
    jar_path.exists() && has_required_jars && has_required_classes && !has_stale_protobuf
}

fn ensure_java_sdk_built() -> Result<(), String> {
    if java_sdk_available() {
        return Ok(());
    }

    let root = project_root();
    let sdk_dir = root.join("sdks/java/flux-sdk");
    let status = Command::new("mvn")
        .arg("-DskipTests")
        .arg("clean")
        .arg("package")
        .current_dir(&sdk_dir)
        .status()
        .map_err(|e| format!("Failed to run mvn package in {}: {}", sdk_dir.display(), e))?;

    if !status.success() {
        return Err(format!("mvn package failed with status {}", status));
    }

    let copy_status = Command::new("mvn")
        .arg("-DincludeScope=runtime")
        .arg("dependency:copy-dependencies")
        .current_dir(&sdk_dir)
        .status()
        .map_err(|e| {
            format!(
                "Failed to copy Java dependencies in {}: {}",
                sdk_dir.display(),
                e
            )
        })?;

    if !copy_status.success() {
        return Err(format!(
            "mvn dependency:copy-dependencies failed with status {}",
            copy_status
        ));
    }

    // Remove stale protobuf runtime versions that can conflict on classpath ordering.
    let stale_protobuf = sdk_dir.join("target/dependency/protobuf-java-4.33.0.jar");
    if stale_protobuf.exists() {
        std::fs::remove_file(&stale_protobuf).map_err(|e| {
            format!(
                "Failed to remove stale protobuf jar {}: {}",
                stale_protobuf.display(),
                e
            )
        })?;
    }

    if java_sdk_available() {
        Ok(())
    } else {
        Err("Java SDK jar not found after mvn package".to_string())
    }
}

/// Check if Postgres is available for creating isolated test databases.
async fn database_available() -> bool {
    let base_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5433".to_string());
    let admin_url = format!("{}/postgres", base_url);

    let connect = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url);

    match tokio::time::timeout(Duration::from_secs(2), connect).await {
        Ok(Ok(pool)) => {
            pool.close().await;
            true
        }
        _ => false,
    }
}

async fn assert_cross_language_prerequisites(require_python: bool, require_java: bool) {
    assert!(
        database_available().await,
        "Postgres is not available via DATABASE_URL"
    );

    if require_python {
        assert!(python_available(), "Python3 with websockets is required");
    }

    if require_java {
        ensure_java_sdk_built().unwrap_or_else(|e| panic!("Java SDK setup failed: {}", e));
    }
}

/// Test: Python writer -> Rust broker -> Python reader
#[tokio::test]
async fn test_python_to_python() {
    let _lock = cross_language_test_lock().await;
    assert_cross_language_prerequisites(true, false).await;

    let db = TestDb::new().await;
    let topic_id = db.create_topic("py-to-py-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // Python produces 5 records
    let produce_result = run_python_writer(&url, topic_id, 5)
        .await
        .expect("Python writer should succeed");

    assert_eq!(produce_result.writer, "python");
    assert_eq!(produce_result.record_count, 5);
    assert_eq!(produce_result.start_offset, 0);
    assert_eq!(produce_result.end_offset, 5);

    // Python consumes the records
    let consume_result = run_python_reader(&url, topic_id, 5)
        .await
        .expect("Python reader should succeed");

    assert_eq!(consume_result.reader, "python");
    assert_eq!(consume_result.record_count, 5);

    // Verify record content
    for (i, record) in consume_result.records.iter().enumerate() {
        assert_eq!(record.key, Some(format!("py-key-{}", i)));
        assert_eq!(record.value["source"], "python");
        assert_eq!(record.value["index"], i as i64);
    }
}

/// Test: Python writer -> Rust broker -> Python zero-copy raw reader
#[tokio::test]
async fn test_python_raw_read() {
    let _lock = cross_language_test_lock().await;
    assert_cross_language_prerequisites(true, false).await;

    let db = TestDb::new().await;
    let topic_id = db.create_topic("py-raw-read-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    let produce_result = run_python_writer(&url, topic_id, 7)
        .await
        .expect("Python writer should succeed");
    assert_eq!(produce_result.record_count, 7);

    let consume_result = run_python_raw_reader(&url, topic_id, 7)
        .await
        .expect("Python raw reader should succeed");

    assert_eq!(consume_result.reader, "python-raw");
    assert_eq!(consume_result.record_count, 7);
    for (i, record) in consume_result.records.iter().enumerate() {
        assert_eq!(record.key, Some(format!("py-key-{}", i)));
        assert_eq!(record.value["source"], "python");
        assert_eq!(record.value["index"], i as i64);
    }
}


/// Run Java raw (zero-copy) reader.
async fn run_java_raw_reader(
    url: &str,
    topic_id: i32,
    expected_count: i32,
) -> Result<ReadResult, String> {
    let root = project_root();
    let jar_path = root.join("sdks/java/flux-sdk/target/flux-sdk-0.1.0.jar");
    let dependency_glob = root.join("sdks/java/flux-sdk/target/dependency/*");
    let test_classes = root.join("tests/cross_language");
    let compile_classpath = format!("{}:{}", jar_path.display(), dependency_glob.display());

    let compile_output = TokioCommand::new("javac")
        .arg("-cp")
        .arg(&compile_classpath)
        .arg("-d")
        .arg(&test_classes)
        .arg(test_classes.join("JavaRawReader.java"))
        .output()
        .await
        .map_err(|e| format!("Failed to run javac for raw reader: {}", e))?;
    if !compile_output.status.success() {
        return Err(format!(
            "Failed to compile Java raw reader\nstderr: {}",
            String::from_utf8_lossy(&compile_output.stderr)
        ));
    }

    let classpath = format!(
        "{}:{}:{}",
        jar_path.display(),
        dependency_glob.display(),
        test_classes.display()
    );
    let output = TokioCommand::new("java")
        .arg("-cp")
        .arg(&classpath)
        .arg("io.flux.test.JavaRawReader")
        .arg(url)
        .arg(topic_id.to_string())
        .arg(expected_count.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("Failed to run Java raw reader: {}", e))?;

    parse_output::<ReadResult>(output, "Java raw reader")
}

/// Run Java raw group consumer (zero-copy poll path).
async fn run_java_raw_group_reader(
    url: &str,
    topic_id: i32,
    expected_count: i32,
) -> Result<ReadResult, String> {
    let root = project_root();
    let jar_path = root.join("sdks/java/flux-sdk/target/flux-sdk-0.1.0.jar");
    let dependency_glob = root.join("sdks/java/flux-sdk/target/dependency/*");
    let test_classes = root.join("tests/cross_language");
    let compile_classpath = format!("{}:{}", jar_path.display(), dependency_glob.display());

    let compile_output = TokioCommand::new("javac")
        .arg("-cp")
        .arg(&compile_classpath)
        .arg("-d")
        .arg(&test_classes)
        .arg(test_classes.join("JavaRawGroupReader.java"))
        .output()
        .await
        .map_err(|e| format!("Failed to run javac for raw group reader: {}", e))?;
    if !compile_output.status.success() {
        return Err(format!(
            "Failed to compile Java raw group reader\nstderr: {}",
            String::from_utf8_lossy(&compile_output.stderr)
        ));
    }

    let classpath = format!(
        "{}:{}:{}",
        jar_path.display(),
        dependency_glob.display(),
        test_classes.display()
    );
    let output = TokioCommand::new("java")
        .arg("-cp")
        .arg(&classpath)
        .arg("io.flux.test.JavaRawGroupReader")
        .arg(url)
        .arg(topic_id.to_string())
        .arg(expected_count.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("Failed to run Java raw group reader: {}", e))?;

    parse_output::<ReadResult>(output, "Java raw group reader")
}

/// Test: Java writer -> Rust broker -> Java raw group consumer (zero-copy poll)
#[tokio::test]
async fn test_java_raw_group_consume() {
    let _lock = cross_language_test_lock().await;
    assert_cross_language_prerequisites(false, true).await;

    let db = TestDb::new().await;
    let topic_id = db.create_topic("java-raw-group-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    let produce_result = run_java_writer(&url, topic_id, 9)
        .await
        .expect("Java writer should succeed");
    assert_eq!(produce_result.record_count, 9);

    let consume_result = run_java_raw_group_reader(&url, topic_id, 9)
        .await
        .expect("Java raw group reader should succeed");

    assert_eq!(consume_result.reader, "java-raw-group");
    assert_eq!(consume_result.record_count, 9);
    for (i, record) in consume_result.records.iter().enumerate() {
        assert_eq!(record.key, Some(format!("java-key-{}", i)));
        assert_eq!(record.value["source"], "java");
        assert_eq!(record.value["index"], i as f64); // JSON numbers come as f64
    }
}

/// Test: Java writer -> Rust broker -> Java zero-copy raw reader
#[tokio::test]
async fn test_java_raw_read() {
    let _lock = cross_language_test_lock().await;
    assert_cross_language_prerequisites(false, true).await;

    let db = TestDb::new().await;
    let topic_id = db.create_topic("java-raw-read-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    let produce_result = run_java_writer(&url, topic_id, 7)
        .await
        .expect("Java writer should succeed");
    assert_eq!(produce_result.record_count, 7);

    let consume_result = run_java_raw_reader(&url, topic_id, 7)
        .await
        .expect("Java raw reader should succeed");

    assert_eq!(consume_result.reader, "java-raw");
    assert_eq!(consume_result.record_count, 7);
    for (i, record) in consume_result.records.iter().enumerate() {
        assert_eq!(record.key, Some(format!("java-key-{}", i)));
        assert_eq!(record.value["source"], "java");
        assert_eq!(record.value["index"], i as f64); // JSON numbers come as f64
    }
}

/// Test: Java writer -> Rust broker -> Java reader
#[tokio::test]
async fn test_java_to_java() {
    let _lock = cross_language_test_lock().await;
    assert_cross_language_prerequisites(false, true).await;

    let db = TestDb::new().await;
    let topic_id = db.create_topic("java-to-java-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // Java produces 5 records
    let produce_result = run_java_writer(&url, topic_id, 5)
        .await
        .expect("Java writer should succeed");

    assert_eq!(produce_result.writer, "java");
    assert_eq!(produce_result.record_count, 5);
    assert_eq!(produce_result.start_offset, 0);
    assert_eq!(produce_result.end_offset, 5);

    // Java consumes the records
    let consume_result = run_java_reader(&url, topic_id, 5)
        .await
        .expect("Java reader should succeed");

    assert_eq!(consume_result.reader, "java");
    assert_eq!(consume_result.record_count, 5);

    // Verify record content
    for (i, record) in consume_result.records.iter().enumerate() {
        assert_eq!(record.key, Some(format!("java-key-{}", i)));
        assert_eq!(record.value["source"], "java");
    }
}

/// Test: Java writer -> Rust broker -> Python reader (CROSS-LANGUAGE)
#[tokio::test]
async fn test_java_to_python() {
    let _lock = cross_language_test_lock().await;
    assert_cross_language_prerequisites(true, true).await;

    let db = TestDb::new().await;
    let topic_id = db.create_topic("java-to-python-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // Java produces 10 records
    let produce_result = run_java_writer(&url, topic_id, 10)
        .await
        .expect("Java writer should succeed");

    assert_eq!(produce_result.writer, "java");
    assert_eq!(produce_result.record_count, 10);

    // Python consumes the Java-produced records
    let consume_result = run_python_reader(&url, topic_id, 10)
        .await
        .expect("Python reader should succeed");

    assert_eq!(consume_result.reader, "python");
    assert_eq!(consume_result.record_count, 10);

    // Verify Python can read Java's records
    for (i, record) in consume_result.records.iter().enumerate() {
        assert_eq!(record.key, Some(format!("java-key-{}", i)));
        assert_eq!(record.value["source"], "java");
        assert_eq!(record.value["index"], i as f64); // JSON numbers come as f64
    }

    println!(
        "Java writer -> Python reader: {} records transferred",
        consume_result.record_count
    );
}

/// Test: Python writer -> Rust broker -> Java reader (CROSS-LANGUAGE)
#[tokio::test]
async fn test_python_to_java() {
    let _lock = cross_language_test_lock().await;
    assert_cross_language_prerequisites(true, true).await;

    let db = TestDb::new().await;
    let topic_id = db.create_topic("python-to-java-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // Python produces 10 records
    let produce_result = run_python_writer(&url, topic_id, 10)
        .await
        .expect("Python writer should succeed");

    assert_eq!(produce_result.writer, "python");
    assert_eq!(produce_result.record_count, 10);

    // Java consumes the Python-produced records
    let consume_result = run_java_reader(&url, topic_id, 10)
        .await
        .expect("Java reader should succeed");

    assert_eq!(consume_result.reader, "java");
    assert_eq!(consume_result.record_count, 10);

    // Verify Java can read Python's records
    for (i, record) in consume_result.records.iter().enumerate() {
        assert_eq!(record.key, Some(format!("py-key-{}", i)));
        assert_eq!(record.value["source"], "python");
    }

    println!(
        "Python writer -> Java reader: {} records transferred",
        consume_result.record_count
    );
}

// ---- Schema E2E helpers ----

/// Run Python schema writer.
async fn run_python_schema_writer(url: &str, topic_id: i32) -> Result<SchemaWriteResult, String> {
    let root = project_root();
    let script = root.join("tests/cross_language/python_schema_writer.py");

    let output = TokioCommand::new("python3")
        .arg(&script)
        .arg(url)
        .arg(topic_id.to_string())
        .env("PYTHONPATH", root.join("sdks/python"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("Failed to run Python schema writer: {}", e))?;

    parse_output::<SchemaWriteResult>(output, "Python schema writer")
}

/// Run Python schema reader.
async fn run_python_schema_reader(
    url: &str,
    topic_id: i32,
    expected_count: i32,
) -> Result<SchemaReadResult, String> {
    let root = project_root();
    let script = root.join("tests/cross_language/python_schema_reader.py");

    let output = TokioCommand::new("python3")
        .arg(&script)
        .arg(url)
        .arg(topic_id.to_string())
        .arg(expected_count.to_string())
        .env("PYTHONPATH", root.join("sdks/python"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("Failed to run Python schema reader: {}", e))?;

    parse_output::<SchemaReadResult>(output, "Python schema reader")
}

/// Run Java schema writer.
async fn run_java_schema_writer(url: &str, topic_id: i32) -> Result<SchemaWriteResult, String> {
    let root = project_root();
    let jar_path = root.join("sdks/java/flux-sdk/target/flux-sdk-0.1.0.jar");
    let dependency_glob = root.join("sdks/java/flux-sdk/target/dependency/*");
    let test_classes = root.join("tests/cross_language");
    let compile_classpath = format!("{}:{}", jar_path.display(), dependency_glob.display());

    let compile_output = TokioCommand::new("javac")
        .arg("-cp")
        .arg(&compile_classpath)
        .arg("-d")
        .arg(&test_classes)
        .arg(test_classes.join("JavaSchemaWriter.java"))
        .output()
        .await
        .map_err(|e| format!("Failed to run javac for schema writer: {}", e))?;

    if !compile_output.status.success() {
        return Err(format!(
            "Failed to compile Java schema writer\nstderr: {}\nstdout: {}",
            String::from_utf8_lossy(&compile_output.stderr),
            String::from_utf8_lossy(&compile_output.stdout)
        ));
    }

    let classpath = format!(
        "{}:{}:{}",
        jar_path.display(),
        dependency_glob.display(),
        test_classes.display()
    );

    let output = TokioCommand::new("java")
        .arg("-cp")
        .arg(&classpath)
        .arg("io.flux.test.JavaSchemaWriter")
        .arg(url)
        .arg(topic_id.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("Failed to run Java schema writer: {}", e))?;

    parse_output::<SchemaWriteResult>(output, "Java schema writer")
}

/// Run Java schema reader.
async fn run_java_schema_reader(
    url: &str,
    topic_id: i32,
    expected_count: i32,
) -> Result<SchemaReadResult, String> {
    let root = project_root();
    let jar_path = root.join("sdks/java/flux-sdk/target/flux-sdk-0.1.0.jar");
    let dependency_glob = root.join("sdks/java/flux-sdk/target/dependency/*");
    let test_classes = root.join("tests/cross_language");
    let compile_classpath = format!("{}:{}", jar_path.display(), dependency_glob.display());

    let compile_output = TokioCommand::new("javac")
        .arg("-cp")
        .arg(&compile_classpath)
        .arg("-d")
        .arg(&test_classes)
        .arg(test_classes.join("JavaSchemaReader.java"))
        .output()
        .await
        .map_err(|e| format!("Failed to run javac for schema reader: {}", e))?;

    if !compile_output.status.success() {
        return Err(format!(
            "Failed to compile Java schema reader\nstderr: {}\nstdout: {}",
            String::from_utf8_lossy(&compile_output.stderr),
            String::from_utf8_lossy(&compile_output.stdout)
        ));
    }

    let classpath = format!(
        "{}:{}:{}",
        jar_path.display(),
        dependency_glob.display(),
        test_classes.display()
    );

    let output = TokioCommand::new("java")
        .arg("-cp")
        .arg(&classpath)
        .arg("io.flux.test.JavaSchemaReader")
        .arg(url)
        .arg(topic_id.to_string())
        .arg(expected_count.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("Failed to run Java schema reader: {}", e))?;

    parse_output::<SchemaReadResult>(output, "Java schema reader")
}

fn assert_test_order(record: &SchemaRecordData) {
    assert_eq!(record.key.as_deref(), Some("order-1"));
    assert_eq!(record.name, "widget");
    assert_eq!(record.amount, 42);
    assert!(record.active);
    assert_eq!(record.tags, vec!["rush", "fragile"]);
}

/// Test: Python writes Avro-encoded schema values -> Java reads and decodes
#[tokio::test]
async fn test_python_schema_to_java() {
    let _lock = cross_language_test_lock().await;
    assert_cross_language_prerequisites(true, true).await;

    let db = TestDb::new().await;
    let topic_id = db.create_topic("py-schema-to-java-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    let write_result = run_python_schema_writer(&url, topic_id)
        .await
        .expect("Python schema writer should succeed");

    assert_eq!(write_result.writer, "python");
    assert_eq!(write_result.record_count, 1);

    let read_result = run_java_schema_reader(&url, topic_id, 1)
        .await
        .expect("Java schema reader should succeed");

    assert_eq!(read_result.reader, "java");
    assert_eq!(read_result.record_count, 1);
    assert_test_order(&read_result.records[0]);
}

/// Test: Java writes Avro-encoded schema values -> Python reads and decodes
#[tokio::test]
async fn test_java_schema_to_python() {
    let _lock = cross_language_test_lock().await;
    assert_cross_language_prerequisites(true, true).await;

    let db = TestDb::new().await;
    let topic_id = db.create_topic("java-schema-to-py-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    let write_result = run_java_schema_writer(&url, topic_id)
        .await
        .expect("Java schema writer should succeed");

    assert_eq!(write_result.writer, "java");
    assert_eq!(write_result.record_count, 1);

    let read_result = run_python_schema_reader(&url, topic_id, 1)
        .await
        .expect("Python schema reader should succeed");

    assert_eq!(read_result.reader, "python");
    assert_eq!(read_result.record_count, 1);
    assert_test_order(&read_result.records[0]);
}

/// Test: Mixed writers (Java + Python) -> single topic -> both readers read all
#[tokio::test]
async fn test_mixed_writers() {
    let _lock = cross_language_test_lock().await;
    assert_cross_language_prerequisites(true, true).await;

    let db = TestDb::new().await;
    let topic_id = db.create_topic("mixed-writer-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // Python produces 5 records
    let py_result = run_python_writer(&url, topic_id, 5)
        .await
        .expect("Python writer should succeed");
    assert_eq!(py_result.record_count, 5);

    // Java produces 5 more records
    let java_result = run_java_writer(&url, topic_id, 5)
        .await
        .expect("Java writer should succeed");
    assert_eq!(java_result.record_count, 5);

    // Python reader reads all 10
    let py_consume = run_python_reader(&url, topic_id, 10)
        .await
        .expect("Python reader should succeed");
    assert_eq!(py_consume.record_count, 10);

    // Verify we got records from both writers
    let python_records: Vec<_> = py_consume
        .records
        .iter()
        .filter(|r| r.value.get("source").and_then(|s| s.as_str()) == Some("python"))
        .collect();
    let java_records: Vec<_> = py_consume
        .records
        .iter()
        .filter(|r| r.value.get("source").and_then(|s| s.as_str()) == Some("java"))
        .collect();

    assert_eq!(python_records.len(), 5, "Should have 5 Python records");
    assert_eq!(java_records.len(), 5, "Should have 5 Java records");

    println!(
        "Mixed writers: {} Python + {} Java records in single topic",
        python_records.len(),
        java_records.len()
    );
}
