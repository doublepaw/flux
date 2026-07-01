// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! ACL-based authorization with TTL cache.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio::sync::RwLock;

use super::error::AuthError;
use super::principal::Principal;

/// Resource type for ACL checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceType {
    Topic,
    Group,
    Cluster,
}

impl ResourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ResourceType::Topic => "topic",
            ResourceType::Group => "group",
            ResourceType::Cluster => "cluster",
        }
    }
}

/// Operation for ACL checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
    Append,
    Consume,
    Admin,
    Describe,
}

impl Operation {
    pub fn as_str(&self) -> &'static str {
        match self {
            Operation::Append => "append",
            Operation::Consume => "consume",
            Operation::Admin => "admin",
            Operation::Describe => "describe",
        }
    }
}

/// Cached ACL entry.
#[derive(Debug, Clone)]
struct AclEntry {
    resource_name: String,
    operation: String,
    allow: bool,
}

/// Cache entry with TTL.
struct CacheEntry {
    entries: Vec<AclEntry>,
    fetched_at: Instant,
}

/// ACL checker with TTL cache.
pub struct AclChecker {
    pool: PgPool,
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
    cache_ttl: Duration,
}

impl Clone for AclChecker {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            cache: self.cache.clone(),
            cache_ttl: self.cache_ttl,
        }
    }
}

impl AclChecker {
    /// Create a new ACL checker with default TTL (60 seconds).
    pub fn new(pool: PgPool) -> Self {
        Self::with_ttl(pool, Duration::from_secs(60))
    }

    /// Create a new ACL checker with custom TTL.
    pub fn with_ttl(pool: PgPool, cache_ttl: Duration) -> Self {
        Self {
            pool,
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl,
        }
    }

    /// Check if a principal has permission for an operation on a resource.
    ///
    /// Priority order:
    /// 1. Explicit deny on specific resource
    /// 2. Explicit allow on specific resource
    /// 3. Wildcard deny
    /// 4. Wildcard allow
    /// 5. Default deny
    pub async fn check(
        &self,
        principal: &Principal,
        resource_type: ResourceType,
        resource_name: &str,
        operation: Operation,
    ) -> bool {
        // Admin principals always have access
        if principal.is_admin() {
            return true;
        }

        // Get ACLs for this principal
        let entries = match self.get_acls(&principal.id, resource_type).await {
            Ok(e) => e,
            Err(_) => return false, // Fail closed on error
        };

        let op_str = operation.as_str();

        // Check for explicit rules on the specific resource
        let mut specific_allow = false;
        let mut specific_deny = false;
        let mut wildcard_allow = false;
        let mut wildcard_deny = false;

        for entry in &entries {
            let matches_op = entry.operation == op_str || entry.operation == "*";
            if !matches_op {
                continue;
            }

            if entry.resource_name == resource_name {
                if entry.allow {
                    specific_allow = true;
                } else {
                    specific_deny = true;
                }
            } else if entry.resource_name == "*" {
                if entry.allow {
                    wildcard_allow = true;
                } else {
                    wildcard_deny = true;
                }
            }
        }

        // Priority: specific deny > specific allow > wildcard deny > wildcard allow
        if specific_deny {
            return false;
        }
        if specific_allow {
            return true;
        }
        if wildcard_deny {
            return false;
        }
        if wildcard_allow {
            return true;
        }

        // Default deny
        false
    }

    /// Get cached ACLs for a principal and resource type.
    async fn get_acls(
        &self,
        principal_id: &str,
        resource_type: ResourceType,
    ) -> Result<Vec<AclEntry>, AuthError> {
        let cache_key = format!("{}:{}", principal_id, resource_type.as_str());

        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(&cache_key)
                && entry.fetched_at.elapsed() < self.cache_ttl
            {
                return Ok(entry.entries.clone());
            }
        }

        // Read from database
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            r#"
            SELECT resource_name, operation, permission
            FROM acls
            WHERE principal = $1 AND resource_type = $2
            "#,
        )
        .bind(principal_id)
        .bind(resource_type.as_str())
        .fetch_all(&self.pool)
        .await?;

        let entries: Vec<AclEntry> = rows
            .into_iter()
            .map(|(resource_name, operation, permission)| AclEntry {
                resource_name,
                operation,
                allow: permission == "allow",
            })
            .collect();

        // Update cache
        {
            let mut cache = self.cache.write().await;
            cache.insert(
                cache_key,
                CacheEntry {
                    entries: entries.clone(),
                    fetched_at: Instant::now(),
                },
            );
        }

        Ok(entries)
    }

    /// Invalidate cache for a principal.
    pub async fn invalidate_principal(&self, principal_id: &str) {
        let mut cache = self.cache.write().await;
        let prefix = format!("{}:", principal_id);
        cache.retain(|k, _| !k.starts_with(&prefix));
    }

    /// Invalidate entire cache.
    pub async fn invalidate_all(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }

    /// Create an ACL rule.
    pub async fn create_acl(
        &self,
        principal_id: &str,
        resource_type: ResourceType,
        resource_name: &str,
        operation: Operation,
        allow: bool,
    ) -> Result<i32, AuthError> {
        let permission = if allow { "allow" } else { "deny" };

        let acl_id: i32 = sqlx::query_scalar(
            r#"
            INSERT INTO acls (principal, resource_type, resource_name, operation, permission)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (principal, resource_type, resource_name, operation)
            DO UPDATE SET permission = $5
            RETURNING acl_id
            "#,
        )
        .bind(principal_id)
        .bind(resource_type.as_str())
        .bind(resource_name)
        .bind(operation.as_str())
        .bind(permission)
        .fetch_one(&self.pool)
        .await?;

        // Invalidate cache for this principal
        self.invalidate_principal(principal_id).await;

        Ok(acl_id)
    }

    /// Delete an ACL rule by ID.
    pub async fn delete_acl(&self, acl_id: i32) -> Result<Option<String>, AuthError> {
        // First get the principal so we can invalidate cache
        let principal: Option<String> =
            sqlx::query_scalar("SELECT principal FROM acls WHERE acl_id = $1")
                .bind(acl_id)
                .fetch_optional(&self.pool)
                .await?;

        if let Some(ref p) = principal {
            sqlx::query("DELETE FROM acls WHERE acl_id = $1")
                .bind(acl_id)
                .execute(&self.pool)
                .await?;

            self.invalidate_principal(p).await;
        }

        Ok(principal)
    }

    /// List ACLs with optional filters.
    pub async fn list_acls(
        &self,
        principal_filter: Option<&str>,
        resource_type_filter: Option<ResourceType>,
    ) -> Result<Vec<AclInfo>, AuthError> {
        let rows: Vec<AclInfoRow> = match (principal_filter, resource_type_filter) {
            (Some(p), Some(rt)) => {
                sqlx::query_as(
                    r#"
                    SELECT acl_id, principal, resource_type, resource_name, operation, permission, created_at
                    FROM acls
                    WHERE principal = $1 AND resource_type = $2
                    ORDER BY created_at DESC
                    "#,
                )
                .bind(p)
                .bind(rt.as_str())
                .fetch_all(&self.pool)
                .await?
            }
            (Some(p), None) => {
                sqlx::query_as(
                    r#"
                    SELECT acl_id, principal, resource_type, resource_name, operation, permission, created_at
                    FROM acls
                    WHERE principal = $1
                    ORDER BY created_at DESC
                    "#,
                )
                .bind(p)
                .fetch_all(&self.pool)
                .await?
            }
            (None, Some(rt)) => {
                sqlx::query_as(
                    r#"
                    SELECT acl_id, principal, resource_type, resource_name, operation, permission, created_at
                    FROM acls
                    WHERE resource_type = $1
                    ORDER BY created_at DESC
                    "#,
                )
                .bind(rt.as_str())
                .fetch_all(&self.pool)
                .await?
            }
            (None, None) => {
                sqlx::query_as(
                    r#"
                    SELECT acl_id, principal, resource_type, resource_name, operation, permission, created_at
                    FROM acls
                    ORDER BY created_at DESC
                    "#,
                )
                .fetch_all(&self.pool)
                .await?
            }
        };

        Ok(rows
            .into_iter()
            .map(|r| AclInfo {
                acl_id: r.acl_id,
                principal: r.principal,
                resource_type: r.resource_type,
                resource_name: r.resource_name,
                operation: r.operation,
                permission: r.permission,
                created_at: r.created_at,
            })
            .collect())
    }
}

#[derive(Debug, sqlx::FromRow)]
struct AclInfoRow {
    acl_id: i32,
    principal: String,
    resource_type: String,
    resource_name: String,
    operation: String,
    permission: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

/// ACL information.
#[derive(Debug, Clone)]
pub struct AclInfo {
    pub acl_id: i32,
    pub principal: String,
    pub resource_type: String,
    pub resource_name: String,
    pub operation: String,
    pub permission: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_type_as_str() {
        assert_eq!(ResourceType::Topic.as_str(), "topic");
        assert_eq!(ResourceType::Group.as_str(), "group");
        assert_eq!(ResourceType::Cluster.as_str(), "cluster");
    }

    #[test]
    fn test_operation_as_str() {
        assert_eq!(Operation::Append.as_str(), "append");
        assert_eq!(Operation::Consume.as_str(), "consume");
        assert_eq!(Operation::Admin.as_str(), "admin");
        assert_eq!(Operation::Describe.as_str(), "describe");
    }
}
