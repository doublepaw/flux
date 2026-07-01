// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Admin API integration tests.
//!
//! Tests the HTTP admin endpoints.

mod common;

use common::TestDb;
use flux_broker::{
    ApiKeyValidator, Coordinator, CoordinatorConfig,
    admin::{self, AdminState},
};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use tower::util::ServiceExt;

async fn create_admin_api(db: &TestDb) -> axum::Router {
    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());
    let state = AdminState::new(db.pool.clone(), coordinator);
    admin::create_router(state)
}

async fn create_admin_key(db: &TestDb) -> String {
    let validator = ApiKeyValidator::new(db.pool.clone());
    let (api_key, _) = validator
        .create_key("admin-key", "admin", None)
        .await
        .expect("Failed to create admin key");
    api_key
}

fn auth_header(api_key: &str) -> String {
    format!("Bearer {}", api_key)
}

#[tokio::test]
async fn test_admin_api_requires_auth() {
    let db = TestDb::new().await;
    let app = create_admin_api(&db).await;

    // No auth header
    let response = app
        .oneshot(
            Request::builder()
                .uri("/topics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_admin_api_requires_admin_permission() {
    let db = TestDb::new().await;
    let app = create_admin_api(&db).await;

    // Create a non-admin key
    let validator = ApiKeyValidator::new(db.pool.clone());
    let (api_key, _) = validator
        .create_key("user-key", "user:alice", None)
        .await
        .expect("Failed to create key");

    // Should be forbidden (403) not unauthorized (401)
    let response = app
        .oneshot(
            Request::builder()
                .uri("/topics")
                .header("Authorization", auth_header(&api_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_topic_crud() {
    let db = TestDb::new().await;
    let admin_key = create_admin_key(&db).await;

    // Create topic
    let app = create_admin_api(&db).await;
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/topics")
                .header("Authorization", auth_header(&admin_key))
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "name": "test-topic"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let topic_id = json["topic_id"].as_i64().unwrap();
    assert!(topic_id > 0);

    // List topics
    let app = create_admin_api(&db).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/topics")
                .header("Authorization", auth_header(&admin_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let topics: Vec<Value> = serde_json::from_slice(&body).unwrap();
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0]["name"], "test-topic");

    // Get single topic
    let app = create_admin_api(&db).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/topics/{}", topic_id))
                .header("Authorization", auth_header(&admin_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Update topic
    let app = create_admin_api(&db).await;
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/topics/{}", topic_id))
                .header("Authorization", auth_header(&admin_key))
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "retention_hours": 24
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Delete topic
    let app = create_admin_api(&db).await;
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/topics/{}", topic_id))
                .header("Authorization", auth_header(&admin_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Verify deleted
    let app = create_admin_api(&db).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/topics/{}", topic_id))
                .header("Authorization", auth_header(&admin_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_api_key_management() {
    let db = TestDb::new().await;
    let admin_key = create_admin_key(&db).await;

    // Create a new API key
    let app = create_admin_api(&db).await;
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api-keys")
                .header("Authorization", auth_header(&admin_key))
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "name": "new-key",
                        "principal": "service:orders"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let api_key = json["api_key"].as_str().unwrap();
    let key_id = json["key_id"].as_str().unwrap();
    assert!(api_key.starts_with("tb_"));

    // List API keys
    let app = create_admin_api(&db).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api-keys")
                .header("Authorization", auth_header(&admin_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let keys: Vec<Value> = serde_json::from_slice(&body).unwrap();
    assert_eq!(keys.len(), 2); // admin key + new key

    // Revoke API key
    let app = create_admin_api(&db).await;
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api-keys/{}", key_id))
                .header("Authorization", auth_header(&admin_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn test_acl_management() {
    let db = TestDb::new().await;
    let admin_key = create_admin_key(&db).await;

    // Create ACL
    let app = create_admin_api(&db).await;
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/acls")
                .header("Authorization", auth_header(&admin_key))
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "principal": "service:orders",
                        "resource_type": "topic",
                        "resource_name": "orders",
                        "operation": "append",
                        "allow": true
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let acl_id = json["acl_id"].as_i64().unwrap();

    // List ACLs
    let app = create_admin_api(&db).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/acls?principal=service:orders")
                .header("Authorization", auth_header(&admin_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let acls: Vec<Value> = serde_json::from_slice(&body).unwrap();
    assert_eq!(acls.len(), 1);
    assert_eq!(acls[0]["principal"], "service:orders");

    // Delete ACL
    let app = create_admin_api(&db).await;
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/acls/{}", acl_id))
                .header("Authorization", auth_header(&admin_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn test_consumer_group_management() {
    let db = TestDb::new().await;
    let admin_key = create_admin_key(&db).await;

    // Create a topic first
    db.create_topic("test-topic").await;

    // Create a reader group via the coordinator
    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());
    coordinator
        .join_group("test-group", flux_common::ids::TopicId(1), "reader-1")
        .await
        .expect("Failed to join group");

    // List groups
    let app = create_admin_api(&db).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/groups")
                .header("Authorization", auth_header(&admin_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let groups: Vec<Value> = serde_json::from_slice(&body).unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0]["group_id"], "test-group");

    // Get group details
    let app = create_admin_api(&db).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/groups/test-group/topics/1")
                .header("Authorization", auth_header(&admin_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let group: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(group["group_id"], "test-group");
    assert!(!group["members"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_force_reset() {
    let db = TestDb::new().await;
    let admin_key = create_admin_key(&db).await;

    // Create a topic and reader group
    db.create_topic("reset-topic").await;
    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());
    coordinator
        .join_group("reset-group", flux_common::ids::TopicId(1), "reader-1")
        .await
        .expect("Failed to join group");

    // Force reset
    let app = create_admin_api(&db).await;
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/groups/reset-group/topics/1/reset")
                .header("Authorization", auth_header(&admin_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(result["success"], true, "force reset should return success");
}
