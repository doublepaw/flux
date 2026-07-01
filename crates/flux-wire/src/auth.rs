// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Auth message encoding/decoding using protobuf.

use prost::Message;

use crate::{DecodeError, EncodeError, proto};

/// Auth request sent by client after WebSocket connect.
#[derive(Debug, Clone)]
pub struct AuthRequest {
    /// The API key for authentication.
    pub api_key: String,
}

/// Auth response sent by server after validating the API key.
#[derive(Debug, Clone)]
pub struct AuthResponse {
    /// Whether authentication was successful.
    pub success: bool,
    /// Error message if authentication failed.
    pub error_message: String,
    /// Error code (0 if success).
    pub error_code: u8,
}

/// Encode an AuthRequest into a buffer.
pub fn encode_auth_request(req: &AuthRequest, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let msg = proto::AuthRequest {
        api_key: req.api_key.clone(),
    };
    let encoded = msg.encode_to_vec();
    if encoded.len() > buf.len() {
        return Err(EncodeError::BufferTooSmall {
            needed: encoded.len(),
            available: buf.len(),
        });
    }
    buf[..encoded.len()].copy_from_slice(&encoded);
    Ok(encoded.len())
}

/// Decode an AuthRequest from a buffer.
pub fn decode_auth_request(buf: &[u8]) -> Result<(AuthRequest, usize), DecodeError> {
    let msg = proto::AuthRequest::decode(buf).map_err(|_| DecodeError::InvalidData {
        msg: "invalid protobuf auth request",
    })?;
    Ok((
        AuthRequest {
            api_key: msg.api_key,
        },
        buf.len(),
    ))
}

/// Encode an AuthResponse into a buffer.
pub fn encode_auth_response(resp: &AuthResponse, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let msg = proto::AuthResponse {
        success: resp.success,
        error_code: resp.error_code as u32,
        error_message: resp.error_message.clone(),
    };
    let encoded = msg.encode_to_vec();
    if encoded.len() > buf.len() {
        return Err(EncodeError::BufferTooSmall {
            needed: encoded.len(),
            available: buf.len(),
        });
    }
    buf[..encoded.len()].copy_from_slice(&encoded);
    Ok(encoded.len())
}

/// Decode an AuthResponse from a buffer.
pub fn decode_auth_response(buf: &[u8]) -> Result<(AuthResponse, usize), DecodeError> {
    let msg = proto::AuthResponse::decode(buf).map_err(|_| DecodeError::InvalidData {
        msg: "invalid protobuf auth response",
    })?;
    Ok((
        AuthResponse {
            success: msg.success,
            error_message: msg.error_message,
            error_code: msg.error_code as u8,
        },
        buf.len(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_request_roundtrip() {
        let req = AuthRequest {
            api_key: "tb_00000000000000000000000000000000_secret123".to_string(),
        };

        let mut buf = [0u8; 256];
        let len = encode_auth_request(&req, &mut buf).unwrap();

        let (decoded, consumed) = decode_auth_request(&buf[..len]).unwrap();
        assert_eq!(consumed, len);
        assert_eq!(decoded.api_key, req.api_key);
    }

    #[test]
    fn test_auth_response_success_roundtrip() {
        let resp = AuthResponse {
            success: true,
            error_message: String::new(),
            error_code: 0,
        };

        let mut buf = [0u8; 256];
        let len = encode_auth_response(&resp, &mut buf).unwrap();

        let (decoded, consumed) = decode_auth_response(&buf[..len]).unwrap();
        assert_eq!(consumed, len);
        assert!(decoded.success);
        assert_eq!(decoded.error_code, 0);
        assert!(decoded.error_message.is_empty());
    }

    #[test]
    fn test_auth_response_failure_roundtrip() {
        let resp = AuthResponse {
            success: false,
            error_message: "Invalid API key".to_string(),
            error_code: 9,
        };

        let mut buf = [0u8; 256];
        let len = encode_auth_response(&resp, &mut buf).unwrap();

        let (decoded, consumed) = decode_auth_response(&buf[..len]).unwrap();
        assert_eq!(consumed, len);
        assert!(!decoded.success);
        assert_eq!(decoded.error_code, 9);
        assert_eq!(decoded.error_message, "Invalid API key");
    }
}
