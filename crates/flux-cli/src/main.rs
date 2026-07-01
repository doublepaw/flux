// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

mod bootstrap;
mod client;
mod schema;
mod tail;
mod topic;
mod write;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "flux", about = "Flux CLI")]
struct Cli {
    /// Admin API URL
    #[arg(long, default_value = "http://localhost:9001", global = true)]
    admin_url: String,

    /// WebSocket URL
    #[arg(long, default_value = "ws://localhost:9000", global = true)]
    ws_url: String,

    /// API key for authentication
    #[arg(long, global = true, env = "FLUX_API_KEY")]
    api_key: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create an admin API key (requires DATABASE_URL)
    Bootstrap,
    /// Manage topics
    Topic {
        #[command(subcommand)]
        action: topic::TopicAction,
    },
    /// Manage schemas
    Schema {
        #[command(subcommand)]
        action: schema::SchemaAction,
    },
    /// Append records to a topic
    Write {
        /// Topic name or numeric ID
        #[arg(long)]
        topic: String,
        /// Schema ID (uses latest for the topic if omitted)
        #[arg(long)]
        schema_id: Option<u32>,
        /// Key for the record
        #[arg(long)]
        key: Option<String>,
        /// Value (reads from stdin if omitted)
        value: Option<String>,
    },
    /// Tail a topic (TUI or JSON output)
    Tail {
        /// Topic name or numeric ID
        #[arg(long)]
        topic: String,
        /// Output format
        #[arg(long, default_value = "tui")]
        output: tail::OutputFormat,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = client::FluxClient::new(&cli.admin_url, cli.api_key.as_deref());

    match cli.command {
        Command::Bootstrap => {
            let database_url =
                std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for bootstrap");
            bootstrap::run(&database_url).await
        }
        Command::Topic { action } => topic::run(action, &client).await,
        Command::Schema { action } => schema::run(action, &client).await,
        Command::Write {
            topic,
            schema_id,
            key,
            value,
        } => {
            let topic_id = client.resolve_topic_id(&topic).await?;
            let schema_id = match schema_id {
                Some(id) => id,
                None => client.get_latest_schema(topic_id).await?.schema_id,
            };
            write::run(&cli.ws_url, None, &client, topic_id, schema_id, key, value).await
        }
        Command::Tail { topic, output } => {
            let topic_id = client.resolve_topic_id(&topic).await?;
            tail::run(&cli.ws_url, None, &client, topic_id, output).await
        }
    }
}
