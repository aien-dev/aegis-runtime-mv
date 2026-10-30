//! Operator authentication and listen-address policy for the gateway.
//!
//! - The gateway listens on 127.0.0.1 unless the operator passes an explicit
//!   opt-in flag for any other address.
//! - Every route is protected by default. Only a short list of read-only
//!   `GET`/`HEAD` routes is public. Everything else (all `POST` routes and the
//!   WebSocket, which can run shell and skills) needs the operator token in
//!   `Authorization: Bearer <token>` or `X-Aegis-Operator-Token: <token>`.
//! - With no token configured, protected routes refuse every request.
//! - There is no bypass: no loopback exemption, no debug flag, no query param.

use axum::{
    extract::{Request, State},
    http::{header, HeaderMap, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;

/// Environment variable holding the operator token.
pub const OPERATOR_TOKEN_ENV: &str = "AEGIS_OPERATOR_TOKEN";
/// Alternate header carrying the operator token.
pub const OPERATOR_TOKEN_HEADER: &str = "x-aegis-operator-token";
/// Shortest token accepted.
pub const MIN_TOKEN_LEN: usize = 32;

/// Read-only routes that stay open without a token (GET/HEAD only).
pub const PUBLIC_READ_PATHS: &[&str] = &[
    "/health",
    "/api/v1/health",
    "/v1/models",
    "/api/v1/skills",
    "/api/v1/events",
    "/events",
];

#[derive(Clone, Default)]
pub struct OperatorAuth {
    token: Option<Arc<str>>,
}

impl std::fmt::Debug for OperatorAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperatorAuth")
            .field("configured", &self.token.is_some())
            .finish()
    }
}

impl OperatorAuth {
    /// No token: every protected route is refused.
    pub fn disabled() -> Self {
        Self { token: None }
    }

    pub fn with_token(token: &str) -> Result<Self, String> {
        let t = token.trim();
        if t.len() < MIN_TOKEN_LEN {
            return Err(format!(
                "operator token must be at least {} characters",
                MIN_TOKEN_LEN
            ));
        }
        if !t.chars().all(|c| c.is_ascii_graphic()) {
            return Err("operator token must be printable ASCII without spaces".to_string());
        }
        Ok(Self {
            token: Some(Arc::from(t)),
        })
    }

    /// Token from a file (if given) or from `AEGIS_OPERATOR_TOKEN`.
    pub fn load(token_file: Option<&Path>) -> Result<Self, String> {
        if let Some(p) = token_file {
            let raw = std::fs::read_to_string(p)
                .map_err(|e| format!("cannot read operator token file {}: {}", p.display(), e))?;
            return Self::with_token(&raw);
        }
        match std::env::var(OPERATOR_TOKEN_ENV) {
            Ok(v) if !v.trim().is_empty() => Self::with_token(&v),
            _ => Ok(Self::disabled()),
        }
    }

    pub fn is_configured(&self) -> bool {
        self.token.is_some()
    }

    /// True only when a token is configured and the presented one matches.
    pub fn verify(&self, presented: Option<&str>) -> bool {
        match (&self.token, presented) {
            (Some(expected), Some(got)) => constant_time_eq(expected.as_bytes(), got.as_bytes()),
            _ => false,
        }
    }
}

/// Comparison whose time does not depend on where the first mismatch is.
/// (Only the length is observable, and the length is not secret.)
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The token a request presents, if any.
pub fn presented_token(headers: &HeaderMap) -> Option<&str> {
    if let Some(v) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(rest) = v.strip_prefix("Bearer ") {
            return Some(rest.trim());
        }
    }
    headers
        .get(OPERATOR_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
}

/// Public means: read-only method AND an exact path on the public list.
pub fn is_public(method: &Method, path: &str) -> bool {
    (*method == Method::GET || *method == Method::HEAD) && PUBLIC_READ_PATHS.contains(&path)
}

/// Middleware: deny by default, allow the public list, else require the token.
pub async fn require_operator(
    State(auth): State<OperatorAuth>,
    req: Request,
    next: Next,
) -> Response {
    if is_public(req.method(), req.uri().path()) {
        return next.run(req).await;
    }
    if !auth.is_configured() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "operator token not configured; protected routes are disabled",
            })),
        )
            .into_response();
    }
    if auth.verify(presented_token(req.headers())) {
        return next.run(req).await;
    }
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        Json(serde_json::json!({ "error": "operator token required" })),
    )
        .into_response()
}

/// Listen address. Loopback unless the operator explicitly opts in.
pub fn resolve_bind_addr(
    host: &str,
    port: u16,
    allow_non_loopback: bool,
) -> Result<SocketAddr, String> {
    let ip: IpAddr = host
        .trim()
        .parse()
        .map_err(|_| format!("bind address {:?} is not an IP address", host))?;
    if !ip.is_loopback() && !allow_non_loopback {
        return Err(format!(
            "refusing to listen on non-loopback address {}; pass --allow-non-loopback-bind to opt in",
            ip
        ));
    }
    Ok(SocketAddr::new(ip, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn bind_defaults_to_loopback_only() {
        assert!(resolve_bind_addr("127.0.0.1", 1, false).is_ok());
        assert!(resolve_bind_addr("::1", 1, false).is_ok());
        assert!(resolve_bind_addr("0.0.0.0", 1, false).is_err());
        assert!(resolve_bind_addr("::", 1, false).is_err());
        assert!(resolve_bind_addr("192.0.2.10", 1, false).is_err());
        assert!(resolve_bind_addr("localhost", 1, false).is_err());
        assert!(resolve_bind_addr("0.0.0.0", 1, true).is_ok());
    }

    #[test]
    fn token_rules() {
        assert!(OperatorAuth::with_token("short").is_err());
        assert!(OperatorAuth::with_token(&format!("{} x", TOKEN)).is_err());
        let a = OperatorAuth::with_token(TOKEN).unwrap();
        assert!(a.verify(Some(TOKEN)));
        assert!(!a.verify(None));
        assert!(!a.verify(Some("")));
        assert!(!a.verify(Some(&TOKEN[..31])));
        assert!(!a.verify(Some(&format!("{}0", TOKEN))));
        assert!(!a.verify(Some(&TOKEN.to_uppercase())));
        assert!(!OperatorAuth::disabled().verify(Some(TOKEN)));
        assert!(!OperatorAuth::disabled().verify(Some("")));
    }

    #[test]
    fn only_listed_reads_are_public() {
        assert!(is_public(&Method::GET, "/health"));
        assert!(!is_public(&Method::POST, "/health"));
        assert!(!is_public(&Method::POST, "/api/v1/tasks"));
        // Task records carry payloads and results (command text and output).
        assert!(!is_public(&Method::GET, "/api/v1/tasks"));
        assert!(!is_public(&Method::GET, "/ws"));
        assert!(!is_public(&Method::GET, "/api/v1/ws"));
        assert!(!is_public(&Method::GET, "/health/"));
        assert!(!is_public(&Method::GET, "/HEALTH"));
        assert!(!is_public(&Method::OPTIONS, "/api/v1/shell"));
    }
}
