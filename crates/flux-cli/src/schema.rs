// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Schema management commands.

use std::io::Read;

use anyhow::{Context, Result};
use clap::Subcommand;

use crate::client::FluxClient;

#[derive(Subcommand)]
pub enum SchemaAction {
    /// Register an Avro schema (reads JSON from --file or stdin)
    Register {
        /// Topic name or numeric ID
        #[arg(long)]
        topic: String,
        /// Path to schema JSON file (reads stdin if omitted)
        #[arg(long)]
        file: Option<String>,
    },
    /// Get schema by ID
    Get { id: u32 },
    /// List schemas for a topic
    List {
        /// Topic name or numeric ID
        #[arg(long)]
        topic: String,
    },
    /// Check schema compatibility
    Check {
        /// Topic name or numeric ID
        #[arg(long)]
        topic: String,
        /// Path to schema JSON file (reads stdin if omitted)
        #[arg(long)]
        file: Option<String>,
    },
}

fn read_schema(file: Option<&str>) -> Result<serde_json::Value> {
    let json_str = match file {
        Some(path) => std::fs::read_to_string(path).context("failed to read schema file")?,
        None => {
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .context("failed to read schema from stdin")?;
            buf
        }
    };
    serde_json::from_str(&json_str).context("invalid JSON schema")
}

pub async fn run(action: SchemaAction, client: &FluxClient) -> Result<()> {
    match action {
        SchemaAction::Register { topic, file } => {
            let topic_id = client.resolve_topic_id(&topic).await?;
            let schema = read_schema(file.as_deref())?;
            let resp = client.register_schema(topic_id, schema).await?;
            println!("Registered schema (id={})", resp.schema_id);
        }
        SchemaAction::Get { id } => {
            let s = client.get_schema(id).await?;
            println!("Schema ID:  {}", s.schema_id);
            println!("Created:    {}", s.created_at);
            println!("{}", serde_json::to_string_pretty(&s.schema)?);
        }
        SchemaAction::List { topic } => {
            let topic_id = client.resolve_topic_id(&topic).await?;
            let schemas = client.list_topic_schemas(topic_id).await?;
            if schemas.is_empty() {
                println!("No schemas found for topic {topic_id}.");
                return Ok(());
            }
            println!("{:<12} {:<12} Created", "Schema ID", "Topic ID");
            println!("{}", "-".repeat(50));
            for s in schemas {
                println!("{:<12} {:<12} {}", s.schema_id, s.topic_id, s.created_at);
            }
        }
        SchemaAction::Check { topic, file } => {
            let topic_id = client.resolve_topic_id(&topic).await?;
            let schema = read_schema(file.as_deref())?;
            let resp = client.check_compatibility(topic_id, schema).await?;
            if resp.is_compatible {
                println!("Schema is compatible.");
            } else {
                println!("Schema is NOT compatible.");
                std::process::exit(1);
            }
        }
    }
    Ok(())
}
