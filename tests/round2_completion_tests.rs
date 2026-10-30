//! Round-two completion contract: real loopback transports, deterministic faults, no model service.
use aegis::{create_router, AgentEngine, Database, GatewayState, HeartbeatEngine, HttpInferenceBackend,
    SkillRegistry, ToolDispatcher, ToolFault, FailureClass, EffectCertainty};
use axum::{body::Body, http::{Request, StatusCode}};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::time::{Duration, Instant};
use tower::ServiceExt;

const TOKEN: &str = "round-two-operator-0123456789abcdef";
struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server { fn drop(&mut self) { self.0.abort(); } }
async fn serve(app: axum::Router) -> (std::net::SocketAddr, Server) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
    (address, Server(handle))
}
struct Dispatch { attempts: AtomicUsize, recover: bool }
#[async_trait::async_trait]
impl ToolDispatcher for Dispatch {
    async fn dispatch(&self, _: &str, _: &Value) -> Result<String, ToolFault> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.recover && attempt > 0 { return Ok("retried read succeeded".into()); }
        Err(ToolFault::new(
            if self.recover { FailureClass::Unavailable } else { FailureClass::Failed },
            EffectCertainty::NoEffect, "injected read failure",
        ))
    }
}
async fn transport_case(websocket: bool, case: &'static str) {
    let model = axum::Router::new().route("/v1/chat/completions", axum::routing::post(
        move |axum::Json(body): axum::Json<Value>| async move {
            let tool_done = body["messages"].as_array().unwrap().iter().any(|m| m["role"] == "tool");
            let response = if case == "empty_choices" { json!({"choices":[]}) }
            else if matches!(case, "tool_failure" | "retry_success" | "length_tools") && !tool_done {
                json!({"choices":[{"message":{"tool_calls":[{"id":"read","type":"function",
                    "function":{"name":"read_file","arguments":"{\"path\":\"fixture\"}"}}]},"finish_reason":if case == "length_tools" { "length" } else { "tool_calls" }}]})
            } else {
                json!({"choices":[{"message":{"content":if case == "empty_text" { " " } else { "final text" },
                    "reasoning":"Reasoning is not a terminal answer"},"finish_reason":if case == "length" { "length" } else { "stop" }}]})
            };
            axum::Json(response)
        }
    ));
    let (model_address, _model_server) = serve(model).await;
    let db = Arc::new(Database::open_in_memory().unwrap());
    let inference = Arc::new(HttpInferenceBackend::new(Some(format!("http://{model_address}/v1/chat/completions")), None));
    let skills = Arc::new(SkillRegistry::new());
    let dispatch = Arc::new(Dispatch { attempts: AtomicUsize::new(0), recover: case == "retry_success" });
    let agent = AgentEngine::new(inference.clone(), skills.clone(), Some(db.clone()))
        .with_dispatcher(dispatch.clone()).with_failure_policy(aegis::agent::FailurePolicy {
            retry: aegis::RetryPolicy { max_attempts: 2 }, tool_timeout: None,
        });
    let state = GatewayState {
        start_time: Instant::now(), db: db.clone(), inference: inference.clone(), skills,
        heartbeat: Arc::new(HeartbeatEngine::with_inference(60, db, inference)), agent: Arc::new(agent),
        operator: aegis::OperatorAuth::with_token(TOKEN).unwrap(),
    };
    let app = create_router(state);
    let body = if websocket {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let (address, _gateway_server) = serve(app).await;
        let mut request = format!("ws://{address}/ws").into_client_request().unwrap();
        request.headers_mut().insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
        let (mut ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        ws.send(tokio_tungstenite::tungstenite::Message::Text(json!({"type":"agent_run","prompt":"read then answer","max_turns":3}).to_string())).await.unwrap();
        let body = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let message = ws.next().await.unwrap().unwrap();
                if let Ok(text) = message.to_text() {
                    let body: Value = serde_json::from_str(text).unwrap();
                    if matches!(body["type"].as_str(), Some("agent_result" | "agent_error")) { break body; }
                }
            }
        }).await.unwrap();
        ws.close(None).await.unwrap();
        body
    } else {
        let response = app.oneshot(Request::builder().method("POST").uri("/api/v1/agent/run")
            .header("Authorization", format!("Bearer {TOKEN}")).header("Content-Type","application/json")
            .body(Body::from(r#"{"prompt":"read then answer","max_turns":3}"#)).unwrap()).await.unwrap();
        assert_eq!(response.status(), if case == "empty_choices" { StatusCode::INTERNAL_SERVER_ERROR } else { StatusCode::OK });
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        if case == "empty_choices" { json!({"error": String::from_utf8(bytes.to_vec()).unwrap()}) }
        else { serde_json::from_slice(&bytes).unwrap() }
    };
    if case == "empty_choices" {
        if websocket { assert_eq!(body["type"], "agent_error"); }
        assert!(body.to_string().contains("empty choices"), "{body}");
    } else {
        let result = if websocket { &body["result"] } else { &body };
        let completed = case == "retry_success";
        assert_eq!(result["completed"], completed, "{body}");
        assert_eq!(body["status"], if completed { "completed" } else { "incomplete" });
        if case == "tool_failure" {
            assert_eq!(result["steps"][0]["tool_calls"][0]["success"], false);
            assert_eq!(result["final_response"], "final text");
        }
        if case == "length_tools" {
            assert_eq!(dispatch.attempts.load(Ordering::SeqCst), 0, "truncated tool call must not dispatch");
        }
        if completed {
            assert_eq!(dispatch.attempts.load(Ordering::SeqCst), 2);
            assert_eq!(result["steps"][0]["tool_calls"][0]["retry_failures"].as_array().unwrap().len(), 1);
        }
    }
}
macro_rules! transport_test {
    ($name:ident, $ws:expr, $case:expr) => { #[tokio::test] async fn $name() { transport_case($ws, $case).await; } };
}
transport_test!(round2_http_failed_tool_final_text, false, "tool_failure");
transport_test!(round2_ws_failed_tool_final_text, true, "tool_failure");
transport_test!(round2_http_retry_success, false, "retry_success");
transport_test!(round2_ws_retry_success, true, "retry_success");
transport_test!(round2_http_length, false, "length");
transport_test!(round2_ws_length, true, "length");
transport_test!(round2_http_empty_text, false, "empty_text");
transport_test!(round2_ws_empty_text, true, "empty_text");
transport_test!(round2_http_empty_choices, false, "empty_choices");
transport_test!(round2_ws_empty_choices, true, "empty_choices");
transport_test!(round2_http_length_tools, false, "length_tools");
transport_test!(round2_ws_length_tools, true, "length_tools");
