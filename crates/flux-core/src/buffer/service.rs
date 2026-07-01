// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

use std::sync::Arc;

use apache_avro::{Reader, Schema as AvroSchema, from_avro_datum};
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

use crate::buffer::avro_converter::RecordBatchBuilder;
use crate::buffer::error::{FluxError, Result};
use crate::buffer::parquet_writer::{ParquetFileWriter, ParquetWriterConfig};

pub struct AppState {
    pub parquet_writer: ParquetFileWriter,
    pub schemas: RwLock<std::collections::HashMap<String, AvroSchema>>,
}

impl AppState {
    pub fn new(config: ParquetWriterConfig) -> Result<Self> {
        Ok(Self {
            parquet_writer: ParquetFileWriter::new(config)?,
            schemas: RwLock::new(std::collections::HashMap::new()),
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct WriteResponse {
    pub file_path: String,
    pub rows_written: usize,
    pub file_id: String,
}

#[derive(Deserialize)]
pub struct RegisterSchemaRequest {
    pub name: String,
    pub schema: String,
}

#[derive(Serialize, Deserialize)]
pub struct RegisterSchemaResponse {
    pub name: String,
    pub registered: bool,
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/health",
            get(|| async { Json(serde_json::json!({"status": "healthy"})) }),
        )
        .route("/schema", post(register_schema))
        .route("/schema/:name", get(get_schema))
        .route("/write/:schema_name", post(write_avro_data))
        .route("/write-raw", post(write_avro_container))
        .with_state(state)
}

async fn register_schema(
    State(state): State<Arc<AppState>>,
    Json(request): Json<RegisterSchemaRequest>,
) -> Result<Json<RegisterSchemaResponse>> {
    let schema = AvroSchema::parse_str(&request.schema)
        .map_err(|e| FluxError::InvalidSchema(e.to_string()))?;
    state.schemas.write().insert(request.name.clone(), schema);
    info!(schema_name = %request.name, "Schema registered");
    Ok(Json(RegisterSchemaResponse {
        name: request.name,
        registered: true,
    }))
}

async fn get_schema(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Response> {
    let schemas = state.schemas.read();
    match schemas.get(&name) {
        Some(schema) => Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            schema.canonical_form(),
        )
            .into_response()),
        None => Ok((StatusCode::NOT_FOUND, "Schema not found").into_response()),
    }
}

async fn write_avro_data(
    State(state): State<Arc<AppState>>,
    Path(schema_name): Path<String>,
    body: Bytes,
) -> Result<Json<WriteResponse>> {
    let schema = state
        .schemas
        .read()
        .get(&schema_name)
        .cloned()
        .ok_or_else(|| {
            FluxError::InvalidSchema(format!("Schema '{}' not found", schema_name))
        })?;

    let mut builder = RecordBatchBuilder::new(schema.clone())?;
    let mut cursor = body.as_ref();
    while !cursor.is_empty() {
        match from_avro_datum(&schema, &mut cursor, None) {
            Ok(value) => builder.append_value(&value)?,
            Err(_) if cursor.is_empty() => break,
            Err(e) => return Err(FluxError::AvroParse(Box::new(e))),
        }
    }

    if builder.row_count() == 0 {
        return Err(FluxError::Conversion("No records found".to_string()));
    }

    write_batch(&state, builder).await
}

async fn write_avro_container(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> Result<Json<WriteResponse>> {
    let reader = Reader::new(body.as_ref())?;
    let schema = reader.writer_schema().clone();
    let mut builder = RecordBatchBuilder::new(schema)?;

    for value_result in reader {
        builder.append_value(&value_result?)?;
    }

    if builder.row_count() == 0 {
        return Err(FluxError::Conversion("No records found".to_string()));
    }

    write_batch(&state, builder).await
}

async fn write_batch(state: &AppState, builder: RecordBatchBuilder) -> Result<Json<WriteResponse>> {
    let rows = builder.row_count();
    let batch = builder.finish()?;
    let file_id = Uuid::new_v4().to_string();
    let path = state
        .parquet_writer
        .write_batch(&batch, &format!("{}.parquet", file_id))?;
    info!(rows = rows, file = %path.display(), "Wrote Parquet file");
    Ok(Json(WriteResponse {
        file_path: path.to_string_lossy().to_string(),
        rows_written: rows,
        file_id,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{Writer, to_avro_datum, types::Value as AvroValue};
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use serde_json::json;
    use tempfile::tempdir;
    use tower::ServiceExt;

    fn sample_schema_json() -> &'static str {
        r#"{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#
    }

    async fn create_test_app() -> (Router, Arc<AppState>, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();
        let config = ParquetWriterConfig::default().with_output_dir(temp_dir.path());
        let state = Arc::new(AppState::new(config).unwrap());
        (build_router(state.clone()), state, temp_dir)
    }

    #[tokio::test]
    async fn test_schema_registration() {
        let (app, state, _temp) = create_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/schema")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"name": "user", "schema": sample_schema_json()}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(state.schemas.read().contains_key("user"));
    }

    #[tokio::test]
    async fn test_invalid_schema_registration() {
        let (app, _, _temp) = create_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/schema")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"name": "bad", "schema": "invalid"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_write_avro_data() {
        let (app, state, temp_dir) = create_test_app().await;

        let schema = AvroSchema::parse_str(sample_schema_json()).unwrap();
        state
            .schemas
            .write()
            .insert("user".to_string(), schema.clone());

        let mut avro_data = to_avro_datum(
            &schema,
            AvroValue::Record(vec![
                ("id".to_string(), AvroValue::Long(1)),
                ("name".to_string(), AvroValue::String("Alice".to_string())),
            ]),
        )
        .unwrap();
        avro_data.extend(
            to_avro_datum(
                &schema,
                AvroValue::Record(vec![
                    ("id".to_string(), AvroValue::Long(2)),
                    ("name".to_string(), AvroValue::String("Bob".to_string())),
                ]),
            )
            .unwrap(),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/write/user")
                    .body(Body::from(avro_data))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let resp: WriteResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(resp.rows_written, 2);
        assert!(
            temp_dir
                .path()
                .join(format!("{}.parquet", resp.file_id))
                .exists()
        );
    }

    #[tokio::test]
    async fn test_write_avro_container() {
        let (app, _, temp_dir) = create_test_app().await;

        let schema = AvroSchema::parse_str(sample_schema_json()).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());
        for i in 0..5 {
            writer
                .append(AvroValue::Record(vec![
                    ("id".to_string(), AvroValue::Long(i)),
                    ("name".to_string(), AvroValue::String(format!("User{}", i))),
                ]))
                .unwrap();
        }

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/write-raw")
                    .body(Body::from(writer.into_inner().unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let resp: WriteResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(resp.rows_written, 5);
        assert!(
            temp_dir
                .path()
                .join(format!("{}.parquet", resp.file_id))
                .exists()
        );
    }

    #[tokio::test]
    async fn test_write_errors() {
        let (app, _, _temp) = create_test_app().await;

        // Schema not found
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/write/missing")
                    .body(Body::from(vec![0u8; 10]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // Invalid container
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/write-raw")
                    .body(Body::from("invalid"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
