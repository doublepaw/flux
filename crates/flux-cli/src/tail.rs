// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Tail command — live-tail a topic via ratatui TUI or JSON output.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use apache_avro::Schema as AvroSchema;
use apache_avro::from_avro_datum;
use crossterm::ExecutableCommand;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use flux_common::ids::{SchemaId, TopicId};
use flux_sdk::reader::{Reader, ReaderConfig};
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use serde::Serialize;
use tokio::sync::{Mutex, mpsc};

use crate::client::FluxClient;

const MAX_RECORDS: usize = 10_000;

/// Output format for the tail command.
#[derive(Debug, Clone, clap::ValueEnum)]
pub enum OutputFormat {
    Tui,
    Json,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Tui => write!(f, "tui"),
            OutputFormat::Json => write!(f, "json"),
        }
    }
}

/// A record ready for display or serialization.
#[derive(Serialize)]
pub struct TailRecord {
    pub offset: u64,
    pub key: Option<String>,
    pub value: String,
}

/// Cached schema decoder — fetches schemas from the admin API on first use.
struct SchemaCache {
    client: FluxClient,
    schemas: HashMap<u32, AvroSchema>,
}

impl SchemaCache {
    fn new(client: FluxClient) -> Self {
        Self {
            client,
            schemas: HashMap::new(),
        }
    }

    async fn decode(&mut self, schema_id: SchemaId, bytes: &[u8]) -> String {
        let sid = schema_id.0;
        // Try Avro decode if we have or can fetch the schema
        if let Some(schema) = self.get_or_fetch(sid).await
            && let Ok(value) = from_avro_datum(schema, &mut &*bytes, None)
            && let Ok(json) = serde_json::Value::try_from(value)
        {
            return serde_json::to_string(&json).unwrap_or_else(|_| format!("{json:?}"));
        }
        // Fallback: show as UTF-8 string (for pre-Avro records or decode failures)
        String::from_utf8_lossy(bytes).into_owned()
    }

    async fn get_or_fetch(&mut self, schema_id: u32) -> Option<&AvroSchema> {
        if !self.schemas.contains_key(&schema_id)
            && let Ok(resp) = self.client.get_schema(schema_id).await
        {
            let json = serde_json::to_string(&resp.schema).ok()?;
            if let Ok(schema) = AvroSchema::parse_str(&json) {
                self.schemas.insert(schema_id, schema);
            }
        }
        self.schemas.get(&schema_id)
    }
}

pub async fn run(
    ws_url: &str,
    api_key: Option<&str>,
    client: &FluxClient,
    topic_id: u32,
    output: OutputFormat,
) -> Result<()> {
    let (tx, rx) = mpsc::channel::<TailRecord>(1024);

    let group_id = format!("flux-cli-tail-{}", uuid::Uuid::new_v4());
    let config = ReaderConfig {
        url: ws_url.to_string(),
        api_key: api_key.map(str::to_string),
        group_id,
        topic_id: TopicId(topic_id),
        ..Default::default()
    };

    let reader = Reader::join(config).await?;
    let _heartbeat = reader.start_heartbeat();

    let cache = Arc::new(Mutex::new(SchemaCache::new(client.clone())));

    // Spawn reader task
    let reader_clone = reader.clone();
    let cache_clone = cache.clone();
    tokio::spawn(async move {
        poll_loop_with_cache(reader_clone, tx, Some(cache_clone)).await;
    });

    let result = match output {
        OutputFormat::Tui => run_tui(rx, topic_id).await,
        OutputFormat::Json => run_json(rx).await,
    };

    let _ = reader.stop().await;
    result
}

#[allow(dead_code)] // used via lib crate in integration tests
pub async fn poll_loop(reader: Arc<Reader>, tx: mpsc::Sender<TailRecord>) {
    poll_loop_with_cache(reader, tx, None).await;
}

async fn poll_loop_with_cache(
    reader: Arc<Reader>,
    tx: mpsc::Sender<TailRecord>,
    schema_cache: Option<Arc<Mutex<SchemaCache>>>,
) {
    loop {
        match reader.poll().await {
            Ok(batch) => {
                let mut record_offset = batch.start_offset.0;
                for result in &batch.results {
                    let schema_id = result.schema_id;

                    for record in result.records.iter() {
                        let offset = record_offset;
                        record_offset += 1;

                        let key = record
                            .key
                            .as_ref()
                            .map(|b| String::from_utf8_lossy(b).into_owned());
                        let value = match &schema_cache {
                            Some(cache) => {
                                cache.lock().await.decode(schema_id, &record.value).await
                            }
                            None => String::from_utf8_lossy(&record.value).into_owned(),
                        };

                        if tx.send(TailRecord { offset, key, value }).await.is_err() {
                            return;
                        }
                    }
                }
                // Commit the batch after processing (tail is best-effort)
                let _ = reader.commit(&batch).await;
            }
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(500)).await;
                if tx.is_closed() {
                    return;
                }
            }
        }
    }
}

// ============ JSON output ============

async fn run_json(mut rx: mpsc::Receiver<TailRecord>) -> Result<()> {
    while let Some(record) = rx.recv().await {
        println!("{}", serde_json::to_string(&record)?);
    }
    Ok(())
}

// ============ TUI output ============

struct TuiApp {
    records: Vec<TailRecord>,
    topic_id: u32,
}

impl TuiApp {
    fn push(&mut self, record: TailRecord) {
        self.records.push(record);
        if self.records.len() > MAX_RECORDS {
            self.records.drain(..self.records.len() - MAX_RECORDS);
        }
    }
}

async fn run_tui(mut rx: mpsc::Receiver<TailRecord>, topic_id: u32) -> Result<()> {
    terminal::enable_raw_mode()?;
    io::stdout().execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;

    let mut app = TuiApp {
        records: Vec::new(),
        topic_id,
    };

    loop {
        while let Ok(record) = rx.try_recv() {
            app.push(record);
        }

        terminal.draw(|f| render(f, &app))?;

        if event::poll(Duration::from_millis(50))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
            && matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
        {
            break;
        }
    }

    terminal::disable_raw_mode()?;
    io::stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}

fn render(f: &mut Frame, app: &TuiApp) {
    let area = f.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(1)])
        .split(area);

    let rows: Vec<Row> = app
        .records
        .iter()
        .map(|r| {
            Row::new(vec![
                Cell::from(r.offset.to_string()),
                Cell::from(r.key.as_deref().unwrap_or("")),
                Cell::from(r.value.as_str()),
            ])
        })
        .collect();

    let total = rows.len();
    let table_height = chunks[0].height.saturating_sub(3) as usize;
    let skip = rows.len().saturating_sub(table_height);
    let visible_rows: Vec<Row> = rows.into_iter().skip(skip).collect();

    let header = Row::new(vec![
        Cell::from("Offset").style(Style::default().bold()),
        Cell::from("Key").style(Style::default().bold()),
        Cell::from("Value").style(Style::default().bold()),
    ]);

    let widths = [
        Constraint::Length(10),
        Constraint::Length(20),
        Constraint::Fill(1),
    ];

    let title = format!(" Flux Tail: topic {} ", app.topic_id);
    let table = Table::new(visible_rows, widths)
        .header(header)
        .block(Block::default().title(title).borders(Borders::ALL));

    f.render_widget(table, chunks[0]);

    let status = format!(" Records: {} | Press q to quit ", total,);
    let status_bar = Paragraph::new(status).style(Style::default().fg(Color::DarkGray));
    f.render_widget(status_bar, chunks[1]);
}
