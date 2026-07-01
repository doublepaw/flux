// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! API key validation.

use sqlx::PgPool;
use uuid::Uuid;

use super::error::AuthError;
use super::principal::Principal;

/// API key validator.
#[derive(Clone)]
pub struct ApiKeyValidator {
    pool: PgPool,
}

impl ApiKeyValidator {
    /// Create a new API key validator.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Validate an API key and return the associated principal.
    ///
    /// API key format: `tb_{key_id}_{secret}`
    /// - `tb_` is a fixed prefix
    /// - `key_id` is a UUID without dashes (32 hex chars)
    /// - `secret` is the raw secret to be verified against the bcrypt hash
    pub async fn validate(&self, api_key: &str) -> Result<Principal, AuthError> {
        // Parse the API key
        let (key_id, secret) = parse_api_key(api_key)?;

        // Look up the key in the database
        let row: Option<ApiKeyRow> = sqlx::query_as(
            r#"
            SELECT key_id, key_hash, principal, expires_at, revoked_at
            FROM api_keys
            WHERE key_id = $1
            "#,
        )
        .bind(key_id)
        .fetch_optional(&self.pool)
        .await?;

        let row = row.ok_or(AuthError::InvalidApiKey)?;

        // Check if revoked
        if row.revoked_at.is_some() {
            return Err(AuthError::RevokedApiKey);
        }

        // Check expiration
        if let Some(expires_at) = row.expires_at
            && expires_at < chrono::Utc::now()
        {
            return Err(AuthError::ExpiredApiKey);
        }

        // Verify the secret against the bcrypt hash
        let hash = String::from_utf8_lossy(&row.key_hash);
        let valid = bcrypt::verify(secret, &hash).unwrap_or(false);
        if !valid {
            return Err(AuthError::InvalidApiKey);
        }

        // Fire-and-forget update of last_used_at
        let pool = self.pool.clone();
        let key_id_clone = key_id;
        tokio::spawn(async move {
            let _ = sqlx::query("UPDATE api_keys SET last_used_at = NOW() WHERE key_id = $1")
                .bind(key_id_clone)
                .execute(&pool)
                .await;
        });

        Ok(Principal::new(row.principal, key_id))
    }

    /// Generate a new API key for a principal.
    ///
    /// Returns the raw API key (only shown once) and the key ID.
    pub async fn create_key(
        &self,
        name: &str,
        principal: &str,
        expires_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(String, Uuid), AuthError> {
        let key_id = Uuid::new_v4();
        let secret = generate_secret();
        let api_key = format_api_key(key_id, &secret);

        // Hash the secret
        let hash = bcrypt::hash(&secret, bcrypt::DEFAULT_COST).map_err(|_| {
            AuthError::Database(sqlx::Error::Protocol("bcrypt hash failed".to_string()))
        })?;

        // Store in database
        sqlx::query(
            r#"
            INSERT INTO api_keys (key_id, key_hash, name, principal, expires_at)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(key_id)
        .bind(hash.as_bytes())
        .bind(name)
        .bind(principal)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;

        Ok((api_key, key_id))
    }

    /// Revoke an API key.
    pub async fn revoke_key(&self, key_id: Uuid) -> Result<bool, AuthError> {
        let result = sqlx::query(
            "UPDATE api_keys SET revoked_at = NOW() WHERE key_id = $1 AND revoked_at IS NULL",
        )
        .bind(key_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// List all API keys (without secrets).
    pub async fn list_keys(&self) -> Result<Vec<ApiKeyInfo>, AuthError> {
        let rows: Vec<ApiKeyInfoRow> = sqlx::query_as(
            r#"
            SELECT key_id, name, principal, created_at, expires_at, revoked_at, last_used_at
            FROM api_keys
            ORDER BY created_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| ApiKeyInfo {
                key_id: r.key_id,
                name: r.name,
                principal: r.principal,
                created_at: r.created_at,
                expires_at: r.expires_at,
                revoked_at: r.revoked_at,
                last_used_at: r.last_used_at,
            })
            .collect())
    }

    /// Get a single API key by ID.
    pub async fn get_key(&self, key_id: Uuid) -> Result<Option<ApiKeyInfo>, AuthError> {
        let row: Option<ApiKeyInfoRow> = sqlx::query_as(
            r#"
            SELECT key_id, name, principal, created_at, expires_at, revoked_at, last_used_at
            FROM api_keys
            WHERE key_id = $1
            "#,
        )
        .bind(key_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| ApiKeyInfo {
            key_id: r.key_id,
            name: r.name,
            principal: r.principal,
            created_at: r.created_at,
            expires_at: r.expires_at,
            revoked_at: r.revoked_at,
            last_used_at: r.last_used_at,
        }))
    }
}

/// Parse an API key into its components.
fn parse_api_key(api_key: &str) -> Result<(Uuid, &str), AuthError> {
    // Format: tb_{key_id_no_dashes}_{secret}
    if !api_key.starts_with("tb_") {
        return Err(AuthError::InvalidKeyFormat);
    }

    let rest = &api_key[3..];
    if rest.len() < 33 {
        // 32 hex chars + underscore minimum
        return Err(AuthError::InvalidKeyFormat);
    }

    let key_id_hex = &rest[..32];
    if !rest[32..].starts_with('_') {
        return Err(AuthError::InvalidKeyFormat);
    }

    let secret = &rest[33..];
    if secret.is_empty() {
        return Err(AuthError::InvalidKeyFormat);
    }

    let key_id = Uuid::parse_str(key_id_hex).map_err(|_| AuthError::InvalidKeyFormat)?;
    Ok((key_id, secret))
}

/// Format an API key from its components.
fn format_api_key(key_id: Uuid, secret: &str) -> String {
    format!("tb_{}_{}", key_id.as_simple(), secret)
}

/// Generate a random secret for an API key.
fn generate_secret() -> String {
    use rand::RngCore;
    let mut rng = rand::thread_rng();
    let mut bytes = [0u8; 32];
    rng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// Database row for API key lookup.
#[derive(Debug, sqlx::FromRow)]
struct ApiKeyRow {
    #[allow(dead_code)]
    key_id: Uuid,
    key_hash: Vec<u8>,
    principal: String,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
    revoked_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Database row for API key info listing.
#[derive(Debug, sqlx::FromRow)]
struct ApiKeyInfoRow {
    key_id: Uuid,
    name: String,
    principal: String,
    created_at: chrono::DateTime<chrono::Utc>,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
    revoked_at: Option<chrono::DateTime<chrono::Utc>>,
    last_used_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// API key information (without secret).
#[derive(Debug, Clone)]
pub struct ApiKeyInfo {
    pub key_id: Uuid,
    pub name: String,
    pub principal: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_api_key_valid() {
        let key_id = Uuid::new_v4();
        let secret = "abcdef123456";
        let api_key = format_api_key(key_id, secret);

        let (parsed_id, parsed_secret) = parse_api_key(&api_key).unwrap();
        assert_eq!(parsed_id, key_id);
        assert_eq!(parsed_secret, secret);
    }

    #[test]
    fn test_parse_api_key_invalid_prefix() {
        let result = parse_api_key("invalid_key");
        assert!(matches!(result, Err(AuthError::InvalidKeyFormat)));
    }

    #[test]
    fn test_parse_api_key_too_short() {
        let result = parse_api_key("tb_abc_secret");
        assert!(matches!(result, Err(AuthError::InvalidKeyFormat)));
    }

    #[test]
    fn test_parse_api_key_empty_secret() {
        // 32 hex chars but no secret
        let result = parse_api_key("tb_00000000000000000000000000000000_");
        assert!(matches!(result, Err(AuthError::InvalidKeyFormat)));
    }

    #[test]
    fn test_format_api_key() {
        let key_id = Uuid::nil();
        let secret = "testsecret";
        let formatted = format_api_key(key_id, secret);
        assert_eq!(formatted, "tb_00000000000000000000000000000000_testsecret");
    }
}
