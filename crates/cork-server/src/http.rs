//! The HTTP API under `/v1` (HANDOFF 7), the Host and Origin checks and the
//! token check (10), and the event stream (14.2).

use crate::auth::Authenticator;
use crate::board::{Board, Caller, Idem, Settle, event_out};
use axum::body::{Body, Bytes};
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use axum_extra::extract::Query;
use cork_api::*;
use cork_core::labels::{MENTION_LABEL, parse_filter};
use cork_core::{Actor, BoardError, ChannelSelector, ErrorCode, TaskState};
use cork_store::{EventFilter, MessageQuery, Store, TaskQuery};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

/// Which `Host` and `Origin` values the daemon answers (10, 13.5). Both come
/// from config; Milestone 1 sets only loopback values.
#[derive(Debug, Clone, Default)]
pub struct HostPolicy {
    /// Allowed `Host` header values, such as `127.0.0.1:7420`.
    pub hosts: Vec<String>,
    /// Allowed `Origin` values. The daemon's own origins are added from `hosts`.
    pub origins: Vec<String>,
}

impl HostPolicy {
    /// Portable mode: the loopback names for `port`.
    pub fn loopback(port: u16) -> Self {
        Self {
            hosts: vec![
                format!("127.0.0.1:{port}"),
                format!("localhost:{port}"),
                format!("[::1]:{port}"),
            ],
            origins: vec![],
        }
    }

    fn host_ok(&self, host: &str) -> bool {
        self.hosts.iter().any(|h| h.eq_ignore_ascii_case(host))
    }

    fn origin_ok(&self, origin: &str) -> bool {
        self.hosts.iter().any(|h| {
            origin.eq_ignore_ascii_case(&format!("http://{h}"))
                || origin.eq_ignore_ascii_case(&format!("https://{h}"))
        }) || self.origins.iter().any(|o| o.eq_ignore_ascii_case(origin))
    }
}

pub struct App<S: Store> {
    pub board: Arc<Board<S>>,
    pub auth: Arc<dyn Authenticator>,
    pub hosts: HostPolicy,
}

type AppState<S> = Arc<App<S>>;

/// An error as an HTTP response: `{ "error": { "code", "message" } }`.
pub struct ApiErr(pub BoardError);

impl From<BoardError> for ApiErr {
    fn from(e: BoardError) -> Self {
        Self(e)
    }
}

impl IntoResponse for ApiErr {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.0.code.http_status())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let body = ErrorBody {
            error: ApiError {
                code: self.0.code.as_str().into(),
                message: self.0.message,
            },
        };
        (status, Json(body)).into_response()
    }
}

type ApiResult<T> = Result<T, ApiErr>;

fn err(code: ErrorCode, msg: impl Into<String>) -> ApiErr {
    ApiErr(BoardError::new(code, msg))
}

/// A read response with `X-Cork-Head`: a position read before the data, so
/// the data is at least that new (14.1).
fn with_head<T: serde::Serialize>(head: u64, body: T) -> Response {
    let mut r = Json(body).into_response();
    r.headers_mut().insert(HEADER_HEAD, HeaderValue::from(head));
    r
}

pub fn router<S: Store>(app: AppState<S>) -> Router {
    let v1 = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready::<S>))
        .route("/whoami", get(whoami::<S>))
        .route("/channels", get(channels::<S>))
        .route("/channels/{channel}/messages", post(post_message::<S>))
        .route("/threads/{id}/replies", post(reply::<S>))
        .route("/threads/{id}", get(thread::<S>))
        .route("/messages", get(messages::<S>))
        .route("/events", get(events::<S>))
        .route("/events/stream", get(stream::<S>))
        .route("/tasks", post(create_task::<S>).get(list_tasks::<S>))
        .route("/tasks/claim", post(claim::<S>))
        .route("/tasks/plan", post(create_plan::<S>))
        .route("/tasks/{id}", get(get_task::<S>))
        .route("/tasks/{id}/history", get(history::<S>))
        .route("/tasks/{id}/{op}", post(settle::<S>))
        .route("/export", get(export::<S>))
        .layer(middleware::from_fn_with_state(app.clone(), guard::<S>))
        .layer(axum::extract::DefaultBodyLimit::max(MAX_BODY));
    // `/metrics` sits at the root, where Prometheus looks, behind the same
    // Host check and token as the API (18.4).
    let metrics = Router::new()
        .route("/metrics", get(metrics::<S>))
        .layer(middleware::from_fn_with_state(app.clone(), guard::<S>));
    Router::new()
        .nest(PREFIX, v1)
        .merge(metrics)
        .fallback(|| async { err(ErrorCode::NotFound, "no such route") })
        .with_state(app)
}

/// Host check, Origin check, then the token (10). `/health` and `/ready`
/// need no token but still get the Host check.
async fn guard<S: Store>(State(app): State<AppState<S>>, mut req: Request, next: Next) -> Response {
    let headers = req.headers();
    let host = headers.get(header::HOST).and_then(|h| h.to_str().ok());
    match host {
        Some(h) if app.hosts.host_ok(h) => {}
        Some(h) => {
            return err(
                ErrorCode::Forbidden,
                format!("host `{h}` is not served here"),
            )
            .into_response();
        }
        None => return err(ErrorCode::Forbidden, "missing Host header").into_response(),
    }
    if let Some(o) = headers.get(header::ORIGIN) {
        let o = o.to_str().unwrap_or("");
        if !app.hosts.origin_ok(o) {
            return err(ErrorCode::Forbidden, format!("origin `{o}` is not allowed"))
                .into_response();
        }
    }
    let path = req.uri().path();
    if path.ends_with("/health") || path.ends_with("/ready") {
        return next.run(req).await;
    }
    let person = match app.auth.authenticate(headers) {
        Ok(p) => p,
        Err(e) => return ApiErr(e).into_response(),
    };
    let profile = headers
        .get(HEADER_PROFILE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let actor = match Actor::new(person, profile) {
        Ok(a) => a,
        Err(e) => return ApiErr(e).into_response(),
    };
    req.extensions_mut().insert(Caller { actor });
    next.run(req).await
}

/// The idempotency key and request hash for a `POST` (7.5).
fn idem(method: &Method, path: &str, headers: &HeaderMap, body: &[u8]) -> ApiResult<Option<Idem>> {
    let Some(k) = headers.get(HEADER_IDEMPOTENCY_KEY) else {
        return Ok(None);
    };
    let key = k
        .to_str()
        .ok()
        .filter(|k| !k.is_empty() && k.len() <= 256)
        .ok_or_else(|| {
            err(
                ErrorCode::InvalidInput,
                "Idempotency-Key must be 1-256 visible characters",
            )
        })?;
    let mut h = Sha256::new();
    h.update(method.as_str().as_bytes());
    h.update(b"\n");
    h.update(path.as_bytes());
    h.update(b"\n");
    h.update(body);
    Ok(Some(Idem {
        key: key.to_string(),
        hash: hex::encode(h.finalize()),
    }))
}

fn body<T: DeserializeOwned + Default>(b: &Bytes) -> ApiResult<T> {
    if b.iter().all(u8::is_ascii_whitespace) {
        return Ok(T::default());
    }
    serde_json::from_slice(b)
        .map_err(|e| err(ErrorCode::InvalidInput, format!("bad JSON body: {e}")))
}

/// What every write handler pulls from the request.
struct WriteReq {
    caller: Caller,
    idem: Option<Idem>,
    body: Bytes,
}

fn write_req(
    caller: Caller,
    method: &Method,
    path: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> ApiResult<WriteReq> {
    Ok(WriteReq {
        idem: idem(method, path, headers, &body)?,
        caller,
        body,
    })
}

// ---------- service ----------

async fn metrics<S: Store>(State(app): State<AppState<S>>) -> Response {
    let mut r = app.board.metrics_text().into_response();
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    r
}

async fn health() -> Json<Health> {
    Json(Health {
        ok: true,
        api_version: API_VERSION,
        server_version: env!("CARGO_PKG_VERSION").into(),
    })
}

async fn ready<S: Store>(State(app): State<AppState<S>>) -> Response {
    if app.board.is_degraded() {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(Ready {
                status: "degraded".into(),
            }),
        )
            .into_response()
    } else {
        Json(Ready {
            status: "ready".into(),
        })
        .into_response()
    }
}

async fn whoami<S: Store>(
    State(app): State<AppState<S>>,
    Extension(c): Extension<Caller>,
) -> ApiResult<Response> {
    let w = app.board.whoami(&c).await?;
    Ok(with_head(w.head, w))
}

// ---------- messages ----------

async fn channels<S: Store>(State(app): State<AppState<S>>) -> ApiResult<Response> {
    let head = app.board.head().await?;
    Ok(with_head(head, app.board.channels().await?))
}

async fn post_message<S: Store>(
    State(app): State<AppState<S>>,
    Extension(c): Extension<Caller>,
    Path(channel): Path<String>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    raw: Bytes,
) -> ApiResult<Json<serde_json::Value>> {
    let w = write_req(c, &method, uri.path(), &headers, raw)?;
    let req: PostMessage = body(&w.body)?;
    Ok(Json(app.board.post(&w.caller, w.idem, channel, req).await?))
}

async fn reply<S: Store>(
    State(app): State<AppState<S>>,
    Extension(c): Extension<Caller>,
    Path(id): Path<String>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    raw: Bytes,
) -> ApiResult<Json<serde_json::Value>> {
    let w = write_req(c, &method, uri.path(), &headers, raw)?;
    let req: PostMessage = body(&w.body)?;
    Ok(Json(app.board.reply(&w.caller, w.idem, id, req).await?))
}

#[derive(Debug, Default, Deserialize)]
struct PageQ {
    #[serde(default)]
    after: u64,
    #[serde(default)]
    limit: usize,
}

async fn thread<S: Store>(
    State(app): State<AppState<S>>,
    Path(id): Path<String>,
    Query(q): Query<PageQ>,
) -> ApiResult<Response> {
    let head = app.board.head().await?;
    Ok(with_head(
        head,
        app.board.thread(&id, q.after, q.limit).await?,
    ))
}

#[derive(Debug, Default, Deserialize)]
struct MessagesQ {
    /// Channel patterns; may repeat, and each may hold several, comma-separated (HANDOFF 19.2).
    #[serde(default)]
    channel: Vec<String>,
    thread: Option<String>,
    #[serde(default)]
    label: Vec<String>,
    #[serde(default)]
    roots: bool,
    #[serde(default)]
    after: u64,
    #[serde(default)]
    limit: usize,
    #[serde(default)]
    newest: bool,
}

fn selector(s: &[String]) -> ApiResult<Option<ChannelSelector>> {
    let parts: Vec<&String> = s.iter().filter(|s| !s.is_empty()).collect();
    if parts.is_empty() {
        return Ok(None);
    }
    Ok(Some(ChannelSelector::parse_all(parts)?))
}

async fn messages<S: Store>(
    State(app): State<AppState<S>>,
    Query(q): Query<MessagesQ>,
) -> ApiResult<Response> {
    let head = app.board.head().await?;
    let page = app
        .board
        .messages(MessageQuery {
            channel: selector(&q.channel)?,
            thread_id: q.thread,
            labels: parse_filter(&q.label)?,
            roots_only: q.roots,
            after: q.after,
            limit: q.limit,
            newest_first: q.newest,
            now: 0,
        })
        .await?;
    Ok(with_head(head, page))
}

// ---------- events ----------

#[derive(Debug, Default, Deserialize)]
struct EventsQ {
    after: Option<u64>,
    /// Channel patterns; may repeat, and each may hold several, comma-separated (HANDOFF 19.2).
    #[serde(default)]
    channel: Vec<String>,
    thread: Option<String>,
    stream: Option<String>,
    #[serde(default)]
    label: Vec<String>,
    #[serde(default, rename = "type")]
    types: Vec<String>,
    #[serde(default)]
    mentions: bool,
    #[serde(default)]
    wait_ms: u64,
    #[serde(default)]
    limit: usize,
}

fn event_filter(q: &EventsQ, caller: &Caller) -> ApiResult<EventFilter> {
    let mut labels = parse_filter(&q.label)?;
    if q.mentions {
        labels.push((MENTION_LABEL.into(), caller.actor.handle().into()));
    }
    Ok(EventFilter {
        after: 0,
        limit: q.limit,
        stream_id: q.stream.clone(),
        channel: selector(&q.channel)?,
        thread_id: q.thread.clone(),
        labels,
        types: q.types.clone(),
    })
}

async fn events<S: Store>(
    State(app): State<AppState<S>>,
    Extension(c): Extension<Caller>,
    Query(q): Query<EventsQ>,
) -> ApiResult<Response> {
    let f = event_filter(&q, &c)?;
    let page = app.board.wait_events(f, q.after, q.wait_ms).await?;
    Ok(with_head(page.next_after, page))
}

/// Server-Sent Events: `id: <position>` and `data: <envelope>`, no `event:`
/// field, `: ping` every 15 s, and `Last-Event-ID` resume (14.2). A stream
/// that lags the broadcast reads from the store, then rejoins; it is never
/// dropped for being slow.
async fn stream<S: Store>(
    State(app): State<AppState<S>>,
    Extension(c): Extension<Caller>,
    headers: HeaderMap,
    Query(q): Query<EventsQ>,
) -> ApiResult<Response> {
    let mut f = event_filter(&q, &c)?;
    let resume = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok());
    let slot = app.board.open_stream()?;
    let board = slot.board().clone();
    let mut rx = board.subscribe();
    let mut cursor = resume.or(q.after).unwrap_or_else(|| board.last_position());
    let s = async_stream::stream! {
        let _slot = slot;
        'catch_up: loop {
            // Catch up (from memory when it can, else the store) to the head.
            loop {
                f.after = cursor;
                f.limit = 500;
                let (events, head) = match board.catch_up(&f).await {
                    Ok(p) => p,
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                };
                for e in &events {
                    cursor = e.position;
                    yield Ok::<Event, Infallible>(sse_event(&event_out(e)));
                }
                if events.len() < 500 {
                    cursor = cursor.max(head);
                    break;
                }
            }
            // Then follow the broadcast.
            loop {
                match rx.recv().await {
                    Ok(e) => {
                        if e.position <= cursor {
                            continue;
                        }
                        if e.position > cursor + 1 {
                            continue 'catch_up;
                        }
                        cursor = e.position;
                        if f.matches(&e) {
                            yield Ok(sse_event(&event_out(&e)));
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue 'catch_up,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break 'catch_up,
                }
            }
        }
    };
    Ok(Sse::new(s)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("ping"),
        )
        .into_response())
}

fn sse_event(e: &EventOut) -> Event {
    Event::default()
        .id(e.position.to_string())
        .data(serde_json::to_string(e).expect("envelope serializes"))
}

// ---------- tasks ----------

async fn create_task<S: Store>(
    State(app): State<AppState<S>>,
    Extension(c): Extension<Caller>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    raw: Bytes,
) -> ApiResult<Json<serde_json::Value>> {
    let w = write_req(c, &method, uri.path(), &headers, raw)?;
    let req: CreateTaskReq = body(&w.body)?;
    Ok(Json(app.board.create_task(&w.caller, w.idem, req).await?))
}

async fn create_plan<S: Store>(
    State(app): State<AppState<S>>,
    Extension(c): Extension<Caller>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    raw: Bytes,
) -> ApiResult<Json<serde_json::Value>> {
    let w = write_req(c, &method, uri.path(), &headers, raw)?;
    let req: PlanReq = body(&w.body)?;
    Ok(Json(app.board.create_plan(&w.caller, w.idem, req).await?))
}

#[derive(Debug, Default, Deserialize)]
struct TasksQ {
    /// Channel patterns; may repeat, and each may hold several, comma-separated (HANDOFF 19.2).
    #[serde(default)]
    channel: Vec<String>,
    state: Option<String>,
    #[serde(default)]
    label: Vec<String>,
    owner: Option<String>,
    #[serde(default)]
    after: u64,
    #[serde(default)]
    limit: usize,
    waiting_on: Option<String>,
    #[serde(default)]
    stuck: bool,
}

async fn list_tasks<S: Store>(
    State(app): State<AppState<S>>,
    Query(q): Query<TasksQ>,
) -> ApiResult<Response> {
    let head = app.board.head().await?;
    let state: Option<TaskState> = q.state.as_deref().map(parse_state).transpose()?;
    let page = app
        .board
        .tasks(
            TaskQuery {
                channel: selector(&q.channel)?,
                state,
                labels: parse_filter(&q.label)?,
                owner: q.owner,
                claimable_by: None,
                after: q.after,
                limit: q.limit,
                now: 0,
            },
            q.waiting_on,
            q.stuck,
        )
        .await?;
    Ok(with_head(head, page))
}

#[derive(Debug, Default, Deserialize)]
struct TaskQ {
    at: Option<u64>,
}

async fn get_task<S: Store>(
    State(app): State<AppState<S>>,
    Path(id): Path<String>,
    Query(q): Query<TaskQ>,
) -> ApiResult<Response> {
    let head = app.board.head().await?;
    Ok(with_head(head, app.board.task(&id, q.at).await?))
}

async fn history<S: Store>(
    State(app): State<AppState<S>>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    let head = app.board.head().await?;
    Ok(with_head(head, app.board.history(&id).await?))
}

async fn claim<S: Store>(
    State(app): State<AppState<S>>,
    Extension(c): Extension<Caller>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    raw: Bytes,
) -> ApiResult<Json<serde_json::Value>> {
    let w = write_req(c, &method, uri.path(), &headers, raw)?;
    let req: ClaimReq = body(&w.body)?;
    Ok(Json(app.board.claim(&w.caller, w.idem, req).await?))
}

async fn settle<S: Store>(
    State(app): State<AppState<S>>,
    Extension(c): Extension<Caller>,
    Path((id, op)): Path<(String, String)>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    raw: Bytes,
) -> ApiResult<Json<serde_json::Value>> {
    let op = Settle::parse(&op)
        .ok_or_else(|| err(ErrorCode::NotFound, format!("no task operation `{op}`")))?;
    let w = write_req(c, &method, uri.path(), &headers, raw)?;
    let req: SettleReq = body(&w.body)?;
    Ok(Json(
        app.board.settle(&w.caller, w.idem, id, op, req).await?,
    ))
}

/// The whole log as JSONL, one envelope per line, in position order (13.4).
async fn export<S: Store>(State(app): State<AppState<S>>) -> ApiResult<Response> {
    let board = app.board.clone();
    let head = board.head().await?;
    let s = async_stream::stream! {
        let mut after = 0;
        while after < head {
            let page = match board.export_page(after).await {
                Ok(p) => p,
                Err(e) => {
                    yield Err(std::io::Error::other(e.message));
                    return;
                }
            };
            if page.is_empty() {
                break;
            }
            let mut buf = String::new();
            for e in &page {
                after = e.position;
                if e.position > head {
                    break;
                }
                buf.push_str(&serde_json::to_string(&event_out(e)).expect("envelope serializes"));
                buf.push('\n');
            }
            yield Ok::<_, std::io::Error>(Bytes::from(buf));
        }
    };
    let mut r = Response::new(Body::from_stream(s));
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    r.headers_mut().insert(HEADER_HEAD, HeaderValue::from(head));
    Ok(r)
}
