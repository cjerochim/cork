//! The board view (HANDOFF 20): the page at `/ui/`, read sessions, and
//! `GET /v1/stats`. No agents run here: workers are dummy profiles.

mod common;

use common::*;
use cork_server::Authenticator;
use cork_server::http_types::HeaderMap;
use reqwest::header::{CONTENT_SECURITY_POLICY, COOKIE, ETAG, IF_NONE_MATCH, ORIGIN, SET_COOKIE};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

fn root(d: &Daemon) -> String {
    format!("http://127.0.0.1:{}", d.port)
}

fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

/// A fresh code from `POST /v1/sessions`, with the token.
async fn code(d: &Daemon) -> String {
    let r = d
        .client("")
        .post("/sessions", json!({"scope": "read"}))
        .await
        .ok();
    assert_eq!(r["expires_in_ms"], 60_000);
    r["code"].as_str().unwrap().to_string()
}

/// Redeem a code the way the page does. Returns the status and the cookie.
async fn redeem(d: &Daemon, code: &str) -> (u16, Option<String>) {
    let r = http()
        .post(format!("{}/v1/sessions/redeem", root(d)))
        .header("x-cork-ui", "1")
        .header(ORIGIN, root(d))
        .json(&json!({ "code": code }))
        .send()
        .await
        .unwrap();
    let cookie = r
        .headers()
        .get(SET_COOKIE)
        .map(|v| v.to_str().unwrap().to_string());
    (r.status().as_u16(), cookie)
}

/// `name=value` from a Set-Cookie header.
fn pair(set_cookie: &str) -> String {
    set_cookie.split(';').next().unwrap().to_string()
}

async fn with_cookie(d: &Daemon, m: reqwest::Method, path: &str, cookie: &str) -> (u16, Value) {
    let r = http()
        .request(m, format!("{}/v1{path}", root(d)))
        .header(COOKIE, cookie)
        .json(&json!({"channel": "work", "text": "x"}))
        .send()
        .await
        .unwrap();
    let s = r.status().as_u16();
    (s, r.json().await.unwrap_or(Value::Null))
}

#[tokio::test]
async fn the_page_is_served_with_its_headers() {
    let d = Daemon::start().await;
    let r = http()
        .get(format!("{}/ui/", root(&d)))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let csp = r.headers()[CONTENT_SECURITY_POLICY].to_str().unwrap();
    assert!(csp.contains("default-src 'self'") && csp.contains("frame-ancestors 'none'"));
    assert_eq!(r.headers()["x-content-type-options"], "nosniff");
    assert_eq!(r.headers()["cache-control"], "no-cache");
    let tag = r.headers()[ETAG].to_str().unwrap().to_string();
    assert!(
        r.text()
            .await
            .unwrap()
            .contains("<title>Cork board</title>")
    );

    // The same file again, unchanged: 304.
    let r = http()
        .get(format!("{}/ui/", root(&d)))
        .header(IF_NONE_MATCH, &tag)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 304);

    let r = http()
        .get(format!("{}/ui/main.js", root(&d)))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.headers()["content-type"],
        "text/javascript; charset=utf-8"
    );
    let r = http()
        .get(format!("{}/ui/nope.js", root(&d)))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    let r = http().get(format!("{}/ui", root(&d))).send().await.unwrap();
    assert_eq!(r.status(), 308);
    assert_eq!(r.headers()["location"], "/ui/");

    // The Host check still applies (10).
    let r = http()
        .get(format!("{}/ui/", root(&d)))
        .header("host", "evil.example:80")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
}

#[tokio::test]
async fn the_files_stay_under_the_size_budget() {
    // 20.5: under 60 KB compressed for everything; the raw size bounds it.
    let total: usize = cork_server::ui::file_names()
        .map(|n| {
            let p = format!("{}/ui/{n}", env!("CARGO_MANIFEST_DIR"));
            std::fs::metadata(p).unwrap().len() as usize
        })
        .sum();
    assert!(total < 120_000, "{total} bytes before compression");
}

#[tokio::test]
async fn a_code_signs_in_once_and_the_session_only_reads() {
    let d = Daemon::start().await;
    d.client("lead")
        .create("work", "Dummy task", json!({}))
        .await;

    let c = code(&d).await;
    let (status, cookie) = redeem(&d, &c).await;
    assert_eq!(status, 200);
    let set = cookie.unwrap();
    for part in ["HttpOnly", "SameSite=Strict", "Path=/v1", "Max-Age=43200"] {
        assert!(set.contains(part), "{set}");
    }
    let cookie = pair(&set);

    // The code works once.
    assert_eq!(redeem(&d, &c).await.0, 401);

    // Reads work with the cookie alone.
    let (s, who) = with_cookie(&d, reqwest::Method::GET, "/whoami", &cookie).await;
    assert_eq!(s, 200);
    assert_eq!(who["person"], "chris");
    let (s, page) = with_cookie(&d, reqwest::Method::GET, "/events?after=0", &cookie).await;
    assert_eq!(s, 200);
    assert!(!page["events"].as_array().unwrap().is_empty());

    // Writes do not, even ones that would otherwise succeed.
    let (s, e) = with_cookie(&d, reqwest::Method::POST, "/tasks", &cookie).await;
    assert_eq!(s, 403, "{e}");
    assert_eq!(e["error"]["code"], "forbidden");
    let (s, _) = with_cookie(&d, reqwest::Method::POST, "/sessions", &cookie).await;
    assert_eq!(s, 403, "a session cannot mint more sessions");

    // A made-up cookie gets nothing.
    let (s, _) = with_cookie(&d, reqwest::Method::GET, "/whoami", "cork_session=00ff").await;
    assert_eq!(s, 401);
}

#[tokio::test]
async fn redeem_needs_the_header_and_an_allowed_origin() {
    let d = Daemon::start().await;
    let c = code(&d).await;
    // No X-Cork-UI: refused, and the code is not spent.
    let r = http()
        .post(format!("{}/v1/sessions/redeem", root(&d)))
        .json(&json!({ "code": c }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    // Another origin: refused by the Origin check.
    let r = http()
        .post(format!("{}/v1/sessions/redeem", root(&d)))
        .header("x-cork-ui", "1")
        .header(ORIGIN, "https://evil.example")
        .json(&json!({ "code": c }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    assert_eq!(
        redeem(&d, &c).await.0,
        200,
        "the code still works after refusals"
    );
    assert_eq!(redeem(&d, "not-a-code").await.0, 401);
}

#[tokio::test]
async fn sessions_need_the_token_and_only_read_scope() {
    let d = Daemon::start().await;
    let mut c = d.client("");
    let r = c.post("/sessions", json!({"scope": "write"})).await;
    assert_eq!(r.status, 400, "{}", r.body);
    c.token = "wrong".into();
    assert_eq!(c.post("/sessions", json!({})).await.status, 401);
}

/// An authenticator whose token can change, like `cork token rotate`.
struct Rotating(Mutex<String>);

impl Authenticator for Rotating {
    fn authenticate(&self, headers: &HeaderMap) -> cork_core::Result<String> {
        cork_server::LocalToken::new(self.0.lock().unwrap().clone(), "chris").authenticate(headers)
    }
    fn epoch(&self) -> String {
        self.0.lock().unwrap().clone()
    }
}

#[tokio::test]
async fn rotating_the_token_ends_sessions() {
    let auth = Arc::new(Rotating(Mutex::new(TOKEN.into())));
    let d = Daemon::start_with(auth.clone()).await;
    let pending = code(&d).await;
    let (_, set) = redeem(&d, &code(&d).await).await;
    let cookie = pair(&set.unwrap());
    assert_eq!(
        with_cookie(&d, reqwest::Method::GET, "/whoami", &cookie)
            .await
            .0,
        200
    );

    *auth.0.lock().unwrap() = "new-token".into();
    assert_eq!(
        with_cookie(&d, reqwest::Method::GET, "/whoami", &cookie)
            .await
            .0,
        401
    );
    assert_eq!(
        redeem(&d, &pending).await.0,
        401,
        "codes from before end too"
    );
}

#[tokio::test]
async fn a_restart_ends_sessions() {
    let d = Daemon::start().await;
    let (_, set) = redeem(&d, &code(&d).await).await;
    let cookie = pair(&set.unwrap());
    let (path, port, dir) = d.shutdown().await;
    let d = Daemon::open(path, Some(port)).await;
    assert_eq!(
        with_cookie(&d, reqwest::Method::GET, "/whoami", &cookie)
            .await
            .0,
        401
    );
    drop(dir);
}

#[tokio::test]
async fn stats_match_a_replay_of_the_log() {
    let d = Daemon::start().await;
    let lead = d.client("lead");
    let w = d.client("rust-1");
    let mut done = 0;
    for (i, cost) in [0.25, 0.5].into_iter().enumerate() {
        let t = lead
            .create("work/rust", &format!("Dummy {i}"), json!({}))
            .await;
        let id = t["task"]["id"].as_str().unwrap().to_string();
        let c = w.claim(json!({"task_id": id})).await.ok();
        let claim = c["claim_id"].as_str().unwrap().to_string();
        w.post(
            &format!("/threads/{id}/replies"),
            json!({"text": "usage", "labels": {"usage": "attempt"},
                   "data": {"task": id, "attempt": 1, "claim_id": claim, "profile": "rust-1",
                            "pool": "rust", "model": "dummy", "cost_usd": cost}}),
        )
        .await
        .ok();
        w.settle(&id, "complete", json!({"claim_id": claim}))
            .await
            .ok();
        done += 1;
    }
    let s = lead.get("/stats?since=0").await.ok();
    assert_eq!(s["total"]["completed"], done);
    assert_eq!(s["total"]["usage"]["cost_usd"], 0.75);
    assert_eq!(s["groups"][0]["key"], "rust");

    let events: Vec<cork_api::EventOut> = all_events(&lead)
        .await
        .into_iter()
        .map(|e| serde_json::from_value(e).unwrap())
        .collect();
    let want = cork_stats::report(
        &events,
        &cork_stats::Options {
            by: Some(cork_stats::By::Pool),
            since: Some(0),
            ..Default::default()
        },
    );
    // Through JSON text both ways, so float parsing is the same on each side.
    let want: Value = serde_json::from_str(&serde_json::to_string(&want).unwrap()).unwrap();
    assert_eq!(s, want);

    // Tasks created after `since` only; and a bad `by` is a clear error.
    let later = lead.get("/stats?since=99999999999999").await.ok();
    assert_eq!(later["total"]["tasks"], 0);
    assert_eq!(lead.get("/stats?by=nonsense").await.status, 400);
}
