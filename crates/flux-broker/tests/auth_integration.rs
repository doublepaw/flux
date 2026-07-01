// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Auth integration tests.
//!
//! Tests API key validation and ACL checking.

mod common;

use common::TestDb;
use flux_broker::{AclChecker, ApiKeyValidator, Operation, Principal, ResourceType};
use uuid::Uuid;

#[tokio::test]
async fn test_api_key_create_and_validate() {
    let db = TestDb::new().await;
    let validator = ApiKeyValidator::new(db.pool.clone());

    // Create an API key
    let (api_key, key_id) = validator
        .create_key("test-key", "service:orders", None)
        .await
        .expect("Failed to create API key");

    assert!(api_key.starts_with("tb_"));
    assert_ne!(key_id, Uuid::nil());

    // Validate the key
    let principal = validator
        .validate(&api_key)
        .await
        .expect("Failed to validate API key");

    assert_eq!(principal.id, "service:orders");
    assert_eq!(principal.key_id, key_id);
    assert!(!principal.is_admin());
    assert_eq!(principal.service_type(), Some("orders"));
}

#[tokio::test]
async fn test_api_key_invalid() {
    let db = TestDb::new().await;
    let validator = ApiKeyValidator::new(db.pool.clone());

    // Invalid format
    let result = validator.validate("invalid_key").await;
    assert!(result.is_err());

    // Valid format but non-existent
    let fake_key = "tb_00000000000000000000000000000000_fakesecret";
    let result = validator.validate(fake_key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_api_key_revoke() {
    let db = TestDb::new().await;
    let validator = ApiKeyValidator::new(db.pool.clone());

    // Create and then revoke a key
    let (api_key, key_id) = validator
        .create_key("revoke-test", "service:test", None)
        .await
        .expect("Failed to create API key");

    // Should work before revocation
    validator
        .validate(&api_key)
        .await
        .expect("Should validate before revoke");

    // Revoke the key
    let revoked = validator
        .revoke_key(key_id)
        .await
        .expect("Failed to revoke");
    assert!(revoked);

    // Should fail after revocation
    let result = validator.validate(&api_key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_api_key_expiration() {
    let db = TestDb::new().await;
    let validator = ApiKeyValidator::new(db.pool.clone());

    // Create an already-expired key
    let expired = chrono::Utc::now() - chrono::Duration::hours(1);
    let (api_key, _) = validator
        .create_key("expired-test", "service:test", Some(expired))
        .await
        .expect("Failed to create API key");

    // Should fail due to expiration
    let result = validator.validate(&api_key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_api_key_list() {
    let db = TestDb::new().await;
    let validator = ApiKeyValidator::new(db.pool.clone());

    // Create some keys
    validator
        .create_key("key1", "principal1", None)
        .await
        .unwrap();
    validator
        .create_key("key2", "principal2", None)
        .await
        .unwrap();

    let keys = validator.list_keys().await.expect("Failed to list keys");
    assert_eq!(keys.len(), 2);
}

#[tokio::test]
async fn test_acl_check_allow() {
    let db = TestDb::new().await;
    let checker = AclChecker::new(db.pool.clone());

    let principal = Principal::new("service:writer".to_string(), Uuid::new_v4());

    // Create an allow ACL
    checker
        .create_acl(
            "service:writer",
            ResourceType::Topic,
            "orders",
            Operation::Append,
            true,
        )
        .await
        .expect("Failed to create ACL");

    // Should be allowed
    let allowed = checker
        .check(&principal, ResourceType::Topic, "orders", Operation::Append)
        .await;
    assert!(allowed);

    // Should be denied for different topic
    let allowed = checker
        .check(&principal, ResourceType::Topic, "other", Operation::Append)
        .await;
    assert!(!allowed);

    // Should be denied for different operation
    let allowed = checker
        .check(
            &principal,
            ResourceType::Topic,
            "orders",
            Operation::Consume,
        )
        .await;
    assert!(!allowed);
}

#[tokio::test]
async fn test_acl_check_wildcard() {
    let db = TestDb::new().await;
    let checker = AclChecker::new(db.pool.clone());

    let principal = Principal::new("service:admin".to_string(), Uuid::new_v4());

    // Create a wildcard allow ACL
    checker
        .create_acl(
            "service:admin",
            ResourceType::Topic,
            "*",
            Operation::Append,
            true,
        )
        .await
        .expect("Failed to create ACL");

    // Should be allowed for any topic
    let allowed = checker
        .check(&principal, ResourceType::Topic, "orders", Operation::Append)
        .await;
    assert!(allowed);

    let allowed = checker
        .check(&principal, ResourceType::Topic, "users", Operation::Append)
        .await;
    assert!(allowed);
}

#[tokio::test]
async fn test_acl_check_deny_precedence() {
    let db = TestDb::new().await;
    let checker = AclChecker::new(db.pool.clone());

    let principal = Principal::new("service:restricted".to_string(), Uuid::new_v4());

    // Create a wildcard allow ACL
    checker
        .create_acl(
            "service:restricted",
            ResourceType::Topic,
            "*",
            Operation::Append,
            true,
        )
        .await
        .expect("Failed to create allow ACL");

    // Create a specific deny ACL
    checker
        .create_acl(
            "service:restricted",
            ResourceType::Topic,
            "secret",
            Operation::Append,
            false,
        )
        .await
        .expect("Failed to create deny ACL");

    // Should be allowed for normal topics
    let allowed = checker
        .check(&principal, ResourceType::Topic, "orders", Operation::Append)
        .await;
    assert!(allowed);

    // Should be denied for the specific topic (deny takes precedence)
    let allowed = checker
        .check(&principal, ResourceType::Topic, "secret", Operation::Append)
        .await;
    assert!(!allowed);
}

#[tokio::test]
async fn test_acl_admin_principal_always_allowed() {
    let db = TestDb::new().await;
    let checker = AclChecker::new(db.pool.clone());

    let admin = Principal::new("admin".to_string(), Uuid::new_v4());

    // Admin should be allowed even without explicit ACL
    let allowed = checker
        .check(&admin, ResourceType::Topic, "anything", Operation::Append)
        .await;
    assert!(allowed);

    let allowed = checker
        .check(&admin, ResourceType::Cluster, "*", Operation::Admin)
        .await;
    assert!(allowed);
}

#[tokio::test]
async fn test_acl_cache_invalidation() {
    let db = TestDb::new().await;
    let checker = AclChecker::new(db.pool.clone());

    let principal = Principal::new("service:cached".to_string(), Uuid::new_v4());

    // Create an allow ACL
    checker
        .create_acl(
            "service:cached",
            ResourceType::Topic,
            "test",
            Operation::Append,
            true,
        )
        .await
        .expect("Failed to create ACL");

    // Should be allowed (populates cache)
    let allowed = checker
        .check(&principal, ResourceType::Topic, "test", Operation::Append)
        .await;
    assert!(allowed);

    // Delete the ACL directly in DB (bypassing cache)
    sqlx::query("DELETE FROM acls WHERE principal = 'service:cached'")
        .execute(&db.pool)
        .await
        .expect("Failed to delete ACL");

    // Invalidate cache
    checker.invalidate_principal("service:cached").await;

    // Should now be denied
    let allowed = checker
        .check(&principal, ResourceType::Topic, "test", Operation::Append)
        .await;
    assert!(!allowed);
}

#[tokio::test]
async fn test_acl_list_and_delete() {
    let db = TestDb::new().await;
    let checker = AclChecker::new(db.pool.clone());

    // Create some ACLs
    let acl_id = checker
        .create_acl(
            "service:test",
            ResourceType::Topic,
            "topic1",
            Operation::Append,
            true,
        )
        .await
        .expect("Failed to create ACL");

    // List ACLs
    let acls = checker
        .list_acls(Some("service:test"), None)
        .await
        .expect("Failed to list ACLs");
    assert_eq!(acls.len(), 1);
    assert_eq!(acls[0].acl_id, acl_id);

    // Delete ACL
    let deleted = checker.delete_acl(acl_id).await.expect("Failed to delete");
    assert!(deleted.is_some());

    // Should be empty now
    let acls = checker
        .list_acls(Some("service:test"), None)
        .await
        .expect("Failed to list ACLs");
    assert!(acls.is_empty());
}
