//! The board view (HANDOFF 20): the static page at `/ui/` and the read
//! sessions it signs in with.
//!
//! The daemon token never reaches the browser. `POST /v1/sessions` (with the
//! token) gives a one-time code; the page trades it at
//! `POST /v1/sessions/redeem` for an `HttpOnly` cookie. A cookie can only
//! read: the guard refuses any other method that comes with only a cookie.

use axum::body::Body;
use axum::extract::Path;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// How long a sign-in code works, and how long a session lasts.
pub const CODE_TTL: Duration = Duration::from_secs(60);
pub const SESSION_TTL: Duration = Duration::from_secs(12 * 60 * 60);
/// Codes and sessions kept at once. Old ones are dropped first.
const MAX_CODES: usize = 32;
const MAX_SESSIONS: usize = 256;

/// The page's files, built into the binary (20.5): name, type, bytes.
const FILES: &[(&str, &str, &[u8])] = &[
    (
        "index.html",
        "text/html; charset=utf-8",
        include_bytes!("../ui/index.html"),
    ),
    (
        "app.css",
        "text/css; charset=utf-8",
        include_bytes!("../ui/app.css"),
    ),
    (
        "main.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../ui/main.js"),
    ),
    (
        "state.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../ui/state.js"),
    ),
    (
        "columns.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../ui/columns.js"),
    ),
    (
        "render.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../ui/render.js"),
    ),
    (
        "icon.svg",
        "image/svg+xml",
        include_bytes!("../ui/icon.svg"),
    ),
];

const CSP: &str = "default-src 'self'; img-src 'self' data:; frame-ancestors 'none'; \
                   base-uri 'none'; form-action 'none'";

fn etags() -> &'static [String] {
    static E: OnceLock<Vec<String>> = OnceLock::new();
    E.get_or_init(|| {
        FILES
            .iter()
            .map(|(_, _, b)| format!("\"{}\"", &hex::encode(Sha256::digest(b))[..16]))
            .collect()
    })
}

/// The names of the page's files, for tests and size checks.
pub fn file_names() -> impl Iterator<Item = &'static str> {
    FILES.iter().map(|(n, _, _)| *n)
}

fn file(name: &str, headers: &HeaderMap) -> Response {
    let Some(i) = FILES.iter().position(|(n, _, _)| *n == name) else {
        return (StatusCode::NOT_FOUND, "no such file").into_response();
    };
    let (_, ty, bytes) = FILES[i];
    let tag = &etags()[i];
    let fresh = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(',').any(|t| t.trim() == tag));
    let mut r = if fresh {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        Response::new(Body::from(bytes))
    };
    let h = r.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static(ty));
    h.insert(header::ETAG, HeaderValue::from_str(tag).expect("hex"));
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CSP),
    );
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    r
}

pub async fn index(headers: HeaderMap) -> Response {
    file("index.html", &headers)
}

pub async fn asset(Path(name): Path<String>, headers: HeaderMap) -> Response {
    file(&name, &headers)
}

pub async fn to_slash() -> Response {
    (
        StatusCode::PERMANENT_REDIRECT,
        [(header::LOCATION, HeaderValue::from_static("/ui/"))],
    )
        .into_response()
}

/// 32 random bytes, hex. Read from the OS (macOS and Linux only, 16).
pub fn random_hex() -> std::io::Result<String> {
    let mut buf = [0u8; 32];
    std::io::Read::read_exact(&mut std::fs::File::open("/dev/urandom")?, &mut buf)?;
    Ok(hex::encode(buf))
}

fn key(secret: &str) -> String {
    hex::encode(Sha256::digest(secret.as_bytes()))
}

#[derive(Debug, Clone)]
struct Grant {
    person: String,
    epoch: String,
    until: Instant,
}

/// Codes and sessions, in memory only: a restart ends them all (20.6).
/// Only hashes of the secrets are kept.
#[derive(Default)]
pub struct Sessions {
    codes: Mutex<HashMap<String, Grant>>,
    live: Mutex<HashMap<String, Grant>>,
}

fn keep(map: &mut HashMap<String, Grant>, max: usize, now: Instant) {
    map.retain(|_, g| g.until > now);
    while map.len() >= max {
        let oldest = map
            .iter()
            .min_by_key(|(_, g)| g.until)
            .map(|(k, _)| k.clone());
        match oldest {
            Some(k) => map.remove(&k),
            None => break,
        };
    }
}

impl Sessions {
    /// A one-time code for `person`, valid for [`CODE_TTL`].
    pub fn issue(&self, person: &str, epoch: &str) -> std::io::Result<String> {
        let code = random_hex()?;
        let now = Instant::now();
        let mut codes = self.codes.lock().unwrap();
        keep(&mut codes, MAX_CODES, now);
        codes.insert(
            key(&code),
            Grant {
                person: person.into(),
                epoch: epoch.into(),
                until: now + CODE_TTL,
            },
        );
        Ok(code)
    }

    /// Trade a code for a session id. A code works once, and only while
    /// the credential it was issued under is still current.
    pub fn redeem(&self, code: &str, epoch: &str) -> Option<(String, String)> {
        let now = Instant::now();
        let g = self.codes.lock().unwrap().remove(&key(code))?;
        if g.until <= now || g.epoch != epoch {
            return None;
        }
        let id = random_hex().ok()?;
        let mut live = self.live.lock().unwrap();
        keep(&mut live, MAX_SESSIONS, now);
        let person = g.person.clone();
        live.insert(
            key(&id),
            Grant {
                until: now + SESSION_TTL,
                ..g
            },
        );
        Some((id, person))
    }

    /// The person a session acts for, if it is still live.
    pub fn check(&self, id: &str, epoch: &str) -> Option<String> {
        let now = Instant::now();
        let mut live = self.live.lock().unwrap();
        let k = key(id);
        match live.get(&k) {
            Some(g) if g.until > now && g.epoch == epoch => Some(g.person.clone()),
            Some(_) => {
                live.remove(&k);
                None
            }
            None => None,
        }
    }
}

/// The session cookie's value, if the request has one.
pub fn cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .find_map(|p| {
            let (k, v) = p.trim().split_once('=')?;
            (k == name).then_some(v)
        })
}

pub fn set_cookie(name: &str, id: &str) -> HeaderValue {
    HeaderValue::from_str(&format!(
        "{name}={id}; HttpOnly; SameSite=Strict; Path=/v1; Max-Age={}",
        SESSION_TTL.as_secs()
    ))
    .expect("cookie is ascii")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_code_works_once() {
        let s = Sessions::default();
        let c = s.issue("chris", "e1").unwrap();
        let (id, p) = s.redeem(&c, "e1").unwrap();
        assert_eq!(p, "chris");
        assert!(s.redeem(&c, "e1").is_none());
        assert_eq!(s.check(&id, "e1").as_deref(), Some("chris"));
    }

    #[test]
    fn a_new_epoch_ends_codes_and_sessions() {
        let s = Sessions::default();
        let c = s.issue("chris", "e1").unwrap();
        assert!(s.redeem(&c, "e2").is_none());
        let c = s.issue("chris", "e1").unwrap();
        let (id, _) = s.redeem(&c, "e1").unwrap();
        assert!(s.check(&id, "e2").is_none());
        assert!(s.check(&id, "e1").is_none(), "an ended session stays ended");
    }

    #[test]
    fn reads_the_cookie() {
        let mut h = HeaderMap::new();
        h.insert(
            header::COOKIE,
            HeaderValue::from_static("a=1; cork_session=xyz"),
        );
        assert_eq!(cookie(&h, "cork_session"), Some("xyz"));
        assert_eq!(cookie(&h, "b"), None);
    }

    #[test]
    fn every_file_is_served_with_the_headers() {
        for n in file_names() {
            let r = file(n, &HeaderMap::new());
            assert_eq!(r.status(), 200, "{n}");
            assert!(r.headers().contains_key(header::CONTENT_SECURITY_POLICY));
            assert_eq!(r.headers()[header::X_CONTENT_TYPE_OPTIONS], "nosniff");
        }
        assert_eq!(file("nope.js", &HeaderMap::new()).status(), 404);
    }
}
