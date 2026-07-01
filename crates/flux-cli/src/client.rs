// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! HTTP client for admin and schema registry APIs.

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct FluxClient {
    http: reqwest::Client,
    admin_url: String,
}

// --- Topic types (mirror server types) ---

#[derive(Debug, Deserialize)]
pub struct TopicInfo {
    pub topic_id: i32,
    pub name: String,
    pub retention_hours: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
pub struct CreateTopicRequest {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_hours: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct CreateTopicResponse {
    pub topic_id: i32,
}

#[derive(Debug, Serialize)]
pub struct UpdateTopicRequest {
    pub retention_hours: Option<i32>,
}

// --- Schema types (mirror server types) ---

#[derive(Debug, Serialize)]
pub struct RegisterSchemaRequest {
    pub topic_id: u32,
    pub schema: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct RegisterSchemaResponse {
    pub schema_id: u32,
}

#[derive(Debug, Deserialize)]
pub struct SchemaResponse {
    pub schema_id: u32,
    pub schema: serde_json::Value,
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct TopicSchemaResponse {
    pub topic_id: u32,
    pub schema_id: u32,
    pub created_at: String,
}

#[derive(Debug, Serialize)]
pub struct CompatibilityCheckRequest {
    pub schema: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct CompatibilityResponse {
    pub is_compatible: bool,
}

impl FluxClient {
    pub fn new(admin_url: &str, api_key: Option<&str>) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(key) = api_key {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {key}").parse().unwrap(),
            );
        }
        Self {
            http: reqwest::Client::builder()
                .default_headers(headers)
                .build()
                .unwrap(),
            admin_url: admin_url.trim_end_matches('/').to_string(),
        }
    }

    /// Resolve a topic argument that is either a numeric ID or a name.
    pub async fn resolve_topic_id(&self, topic: &str) -> Result<u32> {
        if let Ok(id) = topic.parse::<u32>() {
            return Ok(id);
        }
        let topics = self.list_topics().await?;
        let matched = topics
            .iter()
            .find(|t| t.name == topic)
            .ok_or_else(|| anyhow::anyhow!("topic '{}' not found", topic))?;
        Ok(matched.topic_id as u32)
    }

    // --- Topics ---

    pub async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        let resp = self
            .http
            .get(format!("{}/topics", self.admin_url))
            .send()
            .await
            .context("failed to reach admin API")?;
        check_status(&resp)?;
        Ok(resp.json().await?)
    }

    pub async fn create_topic(
        &self,
        name: &str,
        retention_hours: Option<i32>,
    ) -> Result<CreateTopicResponse> {
        let resp = self
            .http
            .post(format!("{}/topics", self.admin_url))
            .json(&CreateTopicRequest {
                name: name.to_string(),
                retention_hours,
            })
            .send()
            .await
            .context("failed to reach admin API")?;
        check_status(&resp)?;
        Ok(resp.json().await?)
    }

    pub async fn get_topic(&self, id: i32) -> Result<TopicInfo> {
        let resp = self
            .http
            .get(format!("{}/topics/{id}", self.admin_url))
            .send()
            .await
            .context("failed to reach admin API")?;
        check_status(&resp)?;
        Ok(resp.json().await?)
    }

    pub async fn update_topic(&self, id: i32, retention_hours: i32) -> Result<()> {
        let resp = self
            .http
            .put(format!("{}/topics/{id}", self.admin_url))
            .json(&UpdateTopicRequest {
                retention_hours: Some(retention_hours),
            })
            .send()
            .await
            .context("failed to reach admin API")?;
        check_status(&resp)?;
        Ok(())
    }

    pub async fn delete_topic(&self, id: i32) -> Result<()> {
        let resp = self
            .http
            .delete(format!("{}/topics/{id}", self.admin_url))
            .send()
            .await
            .context("failed to reach admin API")?;
        check_status(&resp)?;
        Ok(())
    }

    // --- Schemas ---

    pub async fn register_schema(
        &self,
        topic_id: u32,
        schema: serde_json::Value,
    ) -> Result<RegisterSchemaResponse> {
        let resp = self
            .http
            .post(format!("{}/schemas", self.admin_url))
            .json(&RegisterSchemaRequest { topic_id, schema })
            .send()
            .await
            .context("failed to reach admin API")?;
        check_status(&resp)?;
        Ok(resp.json().await?)
    }

    pub async fn get_schema(&self, id: u32) -> Result<SchemaResponse> {
        let resp = self
            .http
            .get(format!("{}/schemas/{id}", self.admin_url))
            .send()
            .await
            .context("failed to reach admin API")?;
        check_status(&resp)?;
        Ok(resp.json().await?)
    }

    pub async fn get_latest_schema(&self, topic_id: u32) -> Result<SchemaResponse> {
        let resp = self
            .http
            .get(format!(
                "{}/topics/{topic_id}/schemas/latest",
                self.admin_url
            ))
            .send()
            .await
            .context("failed to reach admin API")?;
        check_status(&resp)?;
        Ok(resp.json().await?)
    }

    pub async fn list_topic_schemas(&self, topic_id: u32) -> Result<Vec<TopicSchemaResponse>> {
        let resp = self
            .http
            .get(format!("{}/topics/{topic_id}/schemas", self.admin_url))
            .send()
            .await
            .context("failed to reach admin API")?;
        check_status(&resp)?;
        Ok(resp.json().await?)
    }

    pub async fn check_compatibility(
        &self,
        topic_id: u32,
        schema: serde_json::Value,
    ) -> Result<CompatibilityResponse> {
        let resp = self
            .http
            .post(format!(
                "{}/topics/{topic_id}/schemas/check",
                self.admin_url
            ))
            .json(&CompatibilityCheckRequest { schema })
            .send()
            .await
            .context("failed to reach admin API")?;
        check_status(&resp)?;
        Ok(resp.json().await?)
    }
}

fn check_status(resp: &reqwest::Response) -> Result<()> {
    if resp.status().is_success() {
        return Ok(());
    }
    bail!(
        "API returned {} {}",
        resp.status().as_u16(),
        resp.status().canonical_reason().unwrap_or(""),
    );
}
