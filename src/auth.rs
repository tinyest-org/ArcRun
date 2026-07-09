//! Static bearer-token authentication middleware (Audit 2, A6).
//!
//! ArcRun's only built-in middleware used to be the Prometheus recorder, so
//! every endpoint was open: anyone reachable on the network could create tasks
//! with arbitrary outbound webhooks (an SSRF/DoS launcher even with A5's
//! delivery-time SSRF checks in place), cancel/stop any batch, read all task
//! metadata, or scrape `/metrics` and Swagger.
//!
//! This module adds an **optional** static bearer token as a minimal mitigation:
//!
//! * `AUTH_TOKEN` absent/empty ⇒ auth is disabled and this middleware is a total
//!   pass-through (zero per-request cost — it forwards immediately). This keeps
//!   the change non-breaking; `main` logs a loud warning in release builds when
//!   auth is off, because SSRF protection alone is not enough if the API is open.
//! * `AUTH_TOKEN` set ⇒ every request must carry `Authorization: Bearer <token>`
//!   **except** the `/health` and `/ready` probes (so k8s liveness/readiness keep
//!   working without a secret). When active, this also gates `/metrics`, the
//!   Swagger UI, `/view` (the DAG UI) and `/favicon.ico` — there is no separate
//!   opt-out flag.
//!
//! ## Known limitations (documented, out of scope here)
//!
//! * The token is only accepted in the `Authorization` header — never in a query
//!   string. The static DAG UI at `/view` therefore cannot authenticate itself
//!   from a browser; it is only reachable behind a reverse proxy that injects the
//!   header. This is intentional (no capability URLs / secrets in access logs).
//! * The webhook callback `?handle=<host>/task/<id>` capability URL is **not**
//!   changed here (moving it to a header or HMAC signature is a breaking change
//!   deferred to a later lot).

use actix_web::{
    Error, HttpResponse,
    body::{BoxBody, MessageBody},
    dev::{ServiceRequest, ServiceResponse},
    http::header::AUTHORIZATION,
    middleware::Next,
};

/// Paths that are always reachable without a token (k8s liveness/readiness).
const EXEMPT_PATHS: [&str; 2] = ["/health", "/ready"];

/// Constant-time byte-slice equality.
///
/// We deliberately avoid the `==` operator (which short-circuits on the first
/// differing byte and therefore leaks, via timing, how much of a guessed prefix
/// was correct). Length is not secret, so an early length-mismatch return is
/// fine; for equal-length inputs we OR together the XOR of every byte pair so the
/// total work — and thus the timing — is independent of *where* the first
/// difference is. `subtle` is only a transitive dependency (not a direct one), so
/// rather than promote it to a direct dependency for a one-liner we implement the
/// comparison by hand.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Extract the token from an `Authorization` header value.
///
/// Accepts `Bearer <token>` (scheme is matched case-insensitively per RFC 6750);
/// returns the trimmed token, or `None` if the header is not a bearer credential.
pub fn parse_bearer(header_value: &str) -> Option<&str> {
    let (scheme, rest) = header_value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    Some(rest.trim())
}

/// Build the uniform 401 response, matching the existing `ApiError` JSON shape
/// (`{"error": ..., "status": ...}`).
fn unauthorized_response() -> HttpResponse {
    HttpResponse::Unauthorized().json(serde_json::json!({
        "error": "Unauthorized: valid bearer token required",
        "status": 401
    }))
}

/// Bearer-auth gate for use with `actix_web::middleware::from_fn`.
///
/// `expected` is the configured token (`None` ⇒ auth disabled ⇒ pass-through).
/// A single instance of this future logic is created per request; when auth is
/// disabled it forwards immediately with no header inspection.
///
/// The 401 is emitted as a genuine `ServiceResponse` (not an `Err`), so it flows
/// identically through the real server and `actix_web::test::call_service` (the
/// latter panics on a returned `Err`). Response bodies are normalized to
/// `BoxBody` so the allow and deny paths share one return type.
pub async fn authorize<B>(
    expected: Option<String>,
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<BoxBody>, Error>
where
    B: MessageBody + 'static,
{
    // Auth disabled: total pass-through.
    let Some(expected) = expected else {
        return Ok(next.call(req).await?.map_into_boxed_body());
    };

    // Exempt the liveness/readiness probes (exact path match).
    if EXEMPT_PATHS.contains(&req.path()) {
        return Ok(next.call(req).await?.map_into_boxed_body());
    }

    // Compute authorization as an owned bool so the borrow of `req` (via the
    // header) ends before we move `req` into `next.call` / `into_response`.
    let authorized = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(parse_bearer)
        .map(|provided| constant_time_eq(provided.as_bytes(), expected.as_bytes()))
        .unwrap_or(false);

    if authorized {
        Ok(next.call(req).await?.map_into_boxed_body())
    } else {
        Ok(req.into_response(unauthorized_response()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_matches_only_identical_slices() {
        assert!(constant_time_eq(b"secret-token", b"secret-token"));
        assert!(!constant_time_eq(b"secret-token", b"secret-tokeX"));
        assert!(!constant_time_eq(b"secret", b"secret-token")); // length mismatch
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b"")); // empty == empty
    }

    #[test]
    fn parse_bearer_extracts_token() {
        assert_eq!(parse_bearer("Bearer abc123"), Some("abc123"));
        assert_eq!(parse_bearer("bearer abc123"), Some("abc123")); // case-insensitive scheme
        assert_eq!(parse_bearer("Bearer   spaced  "), Some("spaced")); // trimmed
    }

    #[test]
    fn parse_bearer_rejects_non_bearer() {
        assert_eq!(parse_bearer("Basic abc123"), None);
        assert_eq!(parse_bearer("abc123"), None);
        assert_eq!(parse_bearer(""), None);
        assert_eq!(parse_bearer("Bearerabc"), None); // no space after scheme
    }
}
