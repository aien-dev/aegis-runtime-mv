use aegis::{
    create_router, AgentEngine, Database, GatewayState, HeartbeatEngine, HttpInferenceBackend,
    SkillRegistry, VaultResolver, VectorMath, REDACTED_MARKER,
};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;
use tower::util::ServiceExt;

/// Operator token used by the test gateway.
pub const TEST_OPERATOR_TOKEN: &str = "test-operator-token-0123456789abcdef";

fn auth_header() -> String {
    format!("Bearer {}", TEST_OPERATOR_TOKEN)
}

/// A catalogued local command that the always-on probe gate admits.
/// The deterministic reference probe refuses `bash_eval` with `ls` (hash verdict:
/// safety < 0.5, scope "unauthorized_escalation"), so tests that must see a
/// successful run use `git status` (safety >= 0.5, scope "contained"). Tests
/// run with the crate root as working directory, which is a git checkout.
pub const PROBE_ADMITTED_COMMAND: &str = "git status";

fn authed_ws_request(url: &str) -> tokio_tungstenite::tungstenite::handshake::client::Request {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = url.into_client_request().unwrap();
    req.headers_mut()
        .insert("authorization", auth_header().parse().unwrap());
    req
}

async fn spawn_mock_inference_server() -> String {
    let app = axum::Router::new()
        .route(
            "/v1/chat/completions",
            axum::routing::post(
                |axum::Json(body): axum::Json<serde_json::Value>| async move {
                    use axum::response::IntoResponse;
                    let is_stream = body
                        .get("stream")
                        .and_then(|s| s.as_bool())
                        .unwrap_or(false);
                    if is_stream {
                        let chunk = serde_json::json!({
                            "id": "chatcmpl-mock-chunk",
                            "object": "chat.completion.chunk",
                            "choices": [{
                                "delta": {"content": "mock streaming chunk"},
                                "index": 0,
                                "finish_reason": "stop"
                            }]
                        });
                        let sse = format!("data: {}\n\ndata: [DONE]\n\n", chunk);
                        axum::response::Response::builder()
                            .header("Content-Type", "text/event-stream")
                            .body(axum::body::Body::from(sse))
                            .unwrap()
                    } else {
                        let resp = serde_json::json!({
                            "id": "chatcmpl-mock",
                            "object": "chat.completion",
                            "created": 1789783086,
                            "model": "atlas-lightning-omni",
                            "choices": [{
                                "index": 0,
                                "message": {
                                    "role": "assistant",
                                    "content": "Mock inference response."
                                },
                                "finish_reason": "stop"
                            }],
                            "usage": {
                                "prompt_tokens": 5,
                                "completion_tokens": 6,
                                "total_tokens": 11
                            }
                        });
                        axum::response::Json(resp).into_response()
                    }
                },
            ),
        )
        .route(
            "/v1/models",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({
                    "object": "list",
                    "data": [{
                        "id": "atlas-lightning-omni",
                        "object": "model"
                    }]
                }))
            }),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://127.0.0.1:{}/v1/chat/completions", port)
}

async fn create_test_state() -> GatewayState {
    let mock_endpoint = spawn_mock_inference_server().await;
    let db = Arc::new(Database::open_in_memory().unwrap());
    let inference = Arc::new(HttpInferenceBackend::new(Some(mock_endpoint), None));
    let heartbeat = Arc::new(HeartbeatEngine::with_inference(
        60,
        db.clone(),
        inference.clone(),
    ));
    let skills = Arc::new(SkillRegistry::new());
    let agent = Arc::new(AgentEngine::new(
        inference.clone(),
        skills.clone(),
        Some(db.clone()),
    ));

    GatewayState {
        start_time: Instant::now(),
        db,
        inference,
        heartbeat,
        skills,
        agent,
        operator: aegis::OperatorAuth::with_token(TEST_OPERATOR_TOKEN).unwrap(),
    }
}

#[tokio::test]
async fn test_gateway_health_endpoint() {
    let state = create_test_state().await;
    let app = create_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["status"], "healthy");
    assert_eq!(body["engine"], "aegis-runtime/0.2.0");
    assert_eq!(body["database_mode"], "sqlite-wal");
    assert_eq!(body["zero_disk_secrets"], true);
}

#[tokio::test]
async fn test_gateway_openai_completions_endpoint() {
    let state = create_test_state().await;
    let app = create_router(state);

    let req_payload = serde_json::json!({
        "model": "atlas-lightning-omni",
        "messages": [
            {"role": "system", "content": "You are AEGIS."},
            {"role": "user", "content": "ping"}
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("Authorization", auth_header())
                .header("Content-Type", "application/json")
                .body(Body::from(req_payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["object"], "chat.completion");
    assert_eq!(body["choices"][0]["message"]["role"], "assistant");
}

#[tokio::test]
async fn test_gateway_openai_multiturn_completions() {
    let state = create_test_state().await;
    let app = create_router(state);

    let req_payload = serde_json::json!({
        "model": "atlas-lightning-omni",
        "messages": [
            {"role": "system", "content": "You are AEGIS systems agent."},
            {"role": "user", "content": "List current memory status."},
            {"role": "assistant", "content": "All memory buffers nominal."},
            {"role": "user", "content": "Execute verification sweep."}
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("Authorization", auth_header())
                .header("Content-Type", "application/json")
                .body(Body::from(req_payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["object"], "chat.completion");
    assert_eq!(body["choices"][0]["finish_reason"], "stop");
}

#[tokio::test]
async fn test_gateway_openai_streaming_endpoint() {
    let state = create_test_state().await;
    let app = create_router(state);

    let req_payload = serde_json::json!({
        "model": "atlas-lightning-omni",
        "messages": [
            {"role": "user", "content": "stream tokens"}
        ],
        "stream": true
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("Authorization", auth_header())
                .header("Content-Type", "application/json")
                .body(Body::from(req_payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&bytes);
    assert!(text.contains("data:"));
}

#[tokio::test]
async fn test_gateway_skills_endpoints() {
    let state = create_test_state().await;
    let app = create_router(state.clone());

    // 1. List skills
    let list_res = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/skills")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(list_res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(list_res.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: Value = serde_json::from_slice(&bytes).unwrap();
    let skills = list.as_array().unwrap();
    assert!(skills.iter().any(|s| s["name"] == "bash_eval"));
    assert!(skills.iter().all(|s| s["name"] != "telemetry_ping")); // no telemetry

    // 2. Execute skill
    let app2 = create_router(state);
    let exec_payload = serde_json::json!({
        "skill_name": "bash_eval",
        "arguments": {"command": PROBE_ADMITTED_COMMAND}
    });

    let exec_res = app2
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/skills/execute")
                .header("Authorization", auth_header())
                .header("Content-Type", "application/json")
                .body(Body::from(exec_payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(exec_res.status(), StatusCode::OK);
    let bytes2 = axum::body::to_bytes(exec_res.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: Value = serde_json::from_slice(&bytes2).unwrap();
    assert_eq!(result["success"], true);
}

#[tokio::test]
async fn test_gateway_shell_execution() {
    let state = create_test_state().await;
    let app = create_router(state);

    let req_payload = serde_json::json!({
        "command": PROBE_ADMITTED_COMMAND
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/shell")
                .header("Authorization", auth_header())
                .header("Content-Type", "application/json")
                .body(Body::from(req_payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["success"], true);
}

#[tokio::test]
async fn test_gateway_task_creation_and_listing() {
    let state = create_test_state().await;
    let app = create_router(state.clone());

    let req_payload = serde_json::json!({
        "task_type": "build_monitor",
        "payload": "echo 'build nominal'"
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/tasks")
                .header("Authorization", auth_header())
                .header("Content-Type", "application/json")
                .body(Body::from(req_payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let app2 = create_router(state);
    let list_response = app2
        .oneshot(
            Request::builder()
                .uri("/api/v1/tasks")
                .header("Authorization", auth_header())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(list_response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(list_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let tasks: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(tasks.as_array().unwrap().len(), 1);
    assert_eq!(tasks[0]["task_type"], "build_monitor");
}

#[tokio::test]
async fn test_gateway_websocket_lifecycle() {
    let state = create_test_state().await;
    let app = create_router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let ws_url = format!("ws://127.0.0.1:{}/ws", port);
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(authed_ws_request(&ws_url))
        .await
        .unwrap();

    // 1. Welcome message
    let msg = ws_stream.next().await.unwrap().unwrap();
    let val: Value = serde_json::from_str(&msg.to_string()).unwrap();
    assert_eq!(val["type"], "welcome");
    assert_eq!(val["gateway"], "aegis-runtime");

    // 2. Ping / Pong
    ws_stream
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::json!({"type": "ping"}).to_string(),
        ))
        .await
        .unwrap();

    let msg = ws_stream.next().await.unwrap().unwrap();
    let val: Value = serde_json::from_str(&msg.to_string()).unwrap();
    assert_eq!(val["type"], "pong");

    // 3. Skills list over WebSocket
    ws_stream
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::json!({"type": "skills_list"}).to_string(),
        ))
        .await
        .unwrap();

    let msg = ws_stream.next().await.unwrap().unwrap();
    let val: Value = serde_json::from_str(&msg.to_string()).unwrap();
    assert_eq!(val["type"], "skills_list");
    assert!(!val["skills"].as_array().unwrap().is_empty());

    // 4. Skill execute over WebSocket
    ws_stream
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::json!({
                "type": "skill_exec",
                "skill_name": "telemetry_ping",
                "arguments": {}
            })
            .to_string(),
        ))
        .await
        .unwrap();

    let msg = ws_stream.next().await.unwrap().unwrap();
    let val: Value = serde_json::from_str(&msg.to_string()).unwrap();
    assert_eq!(val["type"], "skill_result");
    assert_eq!(val["response"]["success"], false);
}

#[tokio::test]
async fn test_vector_math_operations() {
    let v1 = [1.0f32, 0.0, 0.0, 0.0];
    let v2 = [1.0f32, 0.0, 0.0, 0.0];
    let sim = VectorMath::cosine_similarity_4d(v1, v2);
    assert!((sim - 1.0).abs() < 1e-4);

    let tokens = [2.0f32, 4.0, 6.0, 8.0];
    let weights = [0.25f32, 0.25, 0.25, 0.25];
    let proj = VectorMath::token_projection(tokens, weights, 2.0);
    assert!((proj - 7.0).abs() < 1e-4);

    let entropy = VectorMath::token_entropy([0.5, 0.5, 0.0, 0.0]);
    assert!((entropy - std::f32::consts::LN_2).abs() < 1e-4);
    assert!(entropy > 0.0);
}

#[tokio::test]
async fn test_gateway_agent_run_endpoint() {
    let state = create_test_state().await;
    let app = create_router(state);

    let req_payload = serde_json::json!({
        "prompt": "Status and telemetry check",
        "max_turns": 2
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/agent/run")
                .header("Authorization", auth_header())
                .header("Content-Type", "application/json")
                .body(Body::from(req_payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["status"], "completed");
    assert_eq!(body["prompt"], "Status and telemetry check");
    assert!(body["steps"].is_array());
}

#[tokio::test]
async fn test_gateway_concurrent_requests_under_tokio_spawns() {
    let state = create_test_state().await;
    let app = create_router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{}", port);

    let mut handles = Vec::new();
    for i in 0..30 {
        let client_clone = client.clone();
        let base = base_url.clone();
        handles.push(tokio::spawn(async move {
            match i % 4 {
                0 => {
                    let res = client_clone
                        .get(format!("{}/health", base))
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(res.status(), 200);
                }
                1 => {
                    let res = client_clone
                        .get(format!("{}/v1/models", base))
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(res.status(), 200);
                }
                2 => {
                    let res = client_clone
                        .get(format!("{}/api/v1/skills", base))
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(res.status(), 200);
                }
                _ => {
                    let payload = serde_json::json!({
                        "model": "atlas-lightning-omni",
                        "messages": [{"role": "user", "content": "concurrent ping"}]
                    });
                    let res = client_clone
                        .post(format!("{}/v1/chat/completions", base))
                        .header("Authorization", auth_header())
                        .json(&payload)
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(res.status(), 200);
                }
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_gateway_completions_error_handling_and_validation() {
    let state = create_test_state().await;

    // 1. Malformed JSON syntax
    let app1 = create_router(state.clone());
    let malformed_res = app1
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("Authorization", auth_header())
                .header("Content-Type", "application/json")
                .body(Body::from("{ \"model\": invalid_syntax"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        malformed_res.status() == StatusCode::BAD_REQUEST
            || malformed_res.status() == StatusCode::UNPROCESSABLE_ENTITY
    );

    // 2. Missing required field 'messages'
    let app2 = create_router(state.clone());
    let missing_field_payload = serde_json::json!({
        "model": "atlas-lightning-omni"
    });
    let missing_field_res = app2
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("Authorization", auth_header())
                .header("Content-Type", "application/json")
                .body(Body::from(missing_field_payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing_field_res.status(), StatusCode::UNPROCESSABLE_ENTITY);

    // 3. Empty messages array
    let app3 = create_router(state.clone());
    let empty_messages_payload = serde_json::json!({
        "model": "atlas-lightning-omni",
        "messages": []
    });
    let empty_res = app3
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("Authorization", auth_header())
                .header("Content-Type", "application/json")
                .body(Body::from(empty_messages_payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(empty_res.status(), StatusCode::BAD_REQUEST);
    let bytes = axum::body::to_bytes(empty_res.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["error"]["code"], "missing_required_parameter");

    // 4. Unsupported model
    let app4 = create_router(state.clone());
    let unsupported_model_payload = serde_json::json!({
        "model": "unsupported-model-v999",
        "messages": [{"role": "user", "content": "ping"}]
    });
    let unsupported_res = app4
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("Authorization", auth_header())
                .header("Content-Type", "application/json")
                .body(Body::from(unsupported_model_payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unsupported_res.status(), StatusCode::NOT_FOUND);
    let bytes = axum::body::to_bytes(unsupported_res.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["error"]["code"], "model_not_found");
}

#[tokio::test]
async fn test_gateway_openai_streaming_formatting_and_terminal_chunk() {
    let state = create_test_state().await;
    let app = create_router(state);

    let payload = serde_json::json!({
        "model": "atlas-lightning-omni",
        "messages": [{"role": "user", "content": "stream token check"}],
        "stream": true
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("Authorization", auth_header())
                .header("Content-Type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let sse_text = String::from_utf8_lossy(&bytes);

    // Split by double newline into SSE events
    let events: Vec<&str> = sse_text
        .split("\n\n")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    assert!(events.len() >= 2);

    // Verify first event is formatted chunk
    let first_event = events[0];
    assert!(first_event.starts_with("data: "));
    let json_str = &first_event["data: ".len()..];
    let chunk_json: Value = serde_json::from_str(json_str).unwrap();
    assert_eq!(chunk_json["object"], "chat.completion.chunk");
    assert!(chunk_json["choices"][0]["delta"]["content"].is_string());

    // Verify last event is [DONE]
    let last_event = events.last().unwrap();
    assert_eq!(*last_event, "data: [DONE]");
}

#[tokio::test]
async fn test_gateway_agent_run_complex_payloads_and_edge_paths() {
    let state = create_test_state().await;
    let app = create_router(state);

    let complex_goal = "Execute sovereign diagnostics:\n1. Check local environment with \"quotes\" and { \"nested\": \"json\", \"value\": 42 }\n2. Inspect filesystem safety: prohibited 'find /' checks\n3. Emit unslop confirmation report";

    let payload = serde_json::json!({
        "prompt": complex_goal,
        "system": "You are AEGIS sovereign integration verifier.",
        "max_turns": 3
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/agent/run")
                .header("Authorization", auth_header())
                .header("Content-Type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["status"], "completed");
    assert_eq!(body["prompt"], complex_goal);
    assert!(body["turns_taken"].as_u64().unwrap() <= 3);
    assert!(body["completed"].as_bool().unwrap());
    assert!(body["steps"].is_array());
}

#[tokio::test]
async fn test_gateway_websocket_lifecycle_ping_pong_and_close() {
    let state = create_test_state().await;
    let app = create_router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let ws_url = format!("ws://127.0.0.1:{}/ws", port);
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(authed_ws_request(&ws_url))
        .await
        .unwrap();

    // 1. Welcome frame
    let welcome_msg = ws_stream.next().await.unwrap().unwrap();
    let val: Value = serde_json::from_str(&welcome_msg.to_string()).unwrap();
    assert_eq!(val["type"], "welcome");

    // 2. Application Ping -> Pong
    ws_stream
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::json!({"type": "ping"}).to_string(),
        ))
        .await
        .unwrap();
    let pong_msg = ws_stream.next().await.unwrap().unwrap();
    let pong_val: Value = serde_json::from_str(&pong_msg.to_string()).unwrap();
    assert_eq!(pong_val["type"], "pong");

    // 3. Native Tungstenite Ping frame -> expect native Pong frame
    ws_stream
        .send(tokio_tungstenite::tungstenite::Message::Ping(vec![9, 8, 7]))
        .await
        .unwrap();

    let pong_frame = ws_stream.next().await.unwrap().unwrap();
    assert!(pong_frame.is_pong());
    if let tokio_tungstenite::tungstenite::Message::Pong(data) = pong_frame {
        assert_eq!(data, vec![9, 8, 7]);
    }

    // 4. Shell execution via WebSocket
    ws_stream
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::json!({
                "type": "shell",
                "command": PROBE_ADMITTED_COMMAND
            })
            .to_string(),
        ))
        .await
        .unwrap();
    let shell_msg = ws_stream.next().await.unwrap().unwrap();
    let shell_val: Value = serde_json::from_str(&shell_msg.to_string()).unwrap();
    assert_eq!(shell_val["type"], "shell_output");
    assert!(shell_val["stdout"].as_str().is_some());

    // 5. Clean connection close
    ws_stream
        .send(tokio_tungstenite::tungstenite::Message::Close(None))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gateway_submillisecond_health_latency_benchmark() {
    let state = create_test_state().await;

    let app = create_router(state);

    // Warm-up
    for _ in 0..5 {
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    let iterations = 100;
    let mut total_duration_micros: u128 = 0;

    for _ in 0..iterations {
        let start = Instant::now();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let elapsed = start.elapsed().as_micros();
        assert_eq!(response.status(), StatusCode::OK);
        total_duration_micros += elapsed;
    }

    let avg_micros = total_duration_micros / iterations;
    println!(
        "Health endpoint latency benchmark: {} iterations, average = {} microseconds",
        iterations, avg_micros
    );
    assert!(
        avg_micros < 1000,
        "Health endpoint average latency must be sub-millisecond, got {} us",
        avg_micros
    );
}

#[tokio::test]
async fn test_agent_multiturn_accumulation_and_error_recovery() {
    let mock_url = spawn_mock_inference_server().await;
    let db = Arc::new(Database::open_in_memory().unwrap());
    let inference = Arc::new(HttpInferenceBackend::new(Some(mock_url), None));
    let skills = Arc::new(SkillRegistry::new());
    let agent = AgentEngine::new(inference, skills.clone(), Some(db.clone()));

    // Test execution with max_turns = 3 under mock inference
    let result = agent
        .execute_task("Run sequential diagnostic checks", None, 3)
        .await
        .unwrap();

    assert!(result.completed);
    assert!(result.turns_taken >= 1);
    assert!(!result.final_response.is_empty());

    // Verify DB recorded turn
    let turns = db.list_recent_turns(5).unwrap();
    assert!(!turns.is_empty());
    assert_eq!(turns[0].prompt, "Run sequential diagnostic checks");
}

#[test]
fn test_agent_token_budget_boundary_conditions() {
    let mut msgs = vec![
        serde_json::json!({"role": "system", "content": "System prompt"}),
        serde_json::json!({"role": "user", "content": "User initial request"}),
        serde_json::json!({"role": "assistant", "content": "Step 1"}),
        serde_json::json!({"role": "tool", "content": "Tool output 1"}),
        serde_json::json!({"role": "assistant", "content": "Step 2"}),
        serde_json::json!({"role": "tool", "content": "Tool output 2"}),
    ];

    // Budget higher than tokens: no pruning
    let p1 = AgentEngine::prune_context_window(&mut msgs, 5000);
    assert!(!p1);
    assert_eq!(msgs.len(), 6);

    // Budget lower than tokens: prunes oldest intermediate turns first
    let p2 = AgentEngine::prune_context_window(&mut msgs, 15);
    assert!(p2);
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0]["role"], "system");
    assert_eq!(msgs[1]["role"], "user");

    // Boundary: length <= 2 cannot be pruned further
    let p3 = AgentEngine::prune_context_window(&mut msgs, 0);
    assert!(!p3);
    assert_eq!(msgs.len(), 2);
}

#[test]
fn test_vault_streaming_redaction_and_zero_disk_keys() {
    let vault = VaultResolver::new();
    vault.insert_cached("API_SECRET_KEY", "aegis_sk_live_alpha9922");
    vault.insert_cached("DB_PASS", "postgres_master_secret_123");

    // Test streaming token chunks
    let stream_chunks = vec![
        "data: {\"token\": \"Authentication header: Bearer \"}\n\n",
        "data: {\"token\": \"aegis_sk_live_alpha9922\"}\n\n",
        "data: {\"token\": \" connected with postgres_master_secret_123\"}\n\n",
        "data: [DONE]\n\n",
    ];

    let mut redacted_chunks = Vec::new();
    for chunk in stream_chunks {
        redacted_chunks.push(vault.redact_sensitive_text(chunk));
    }

    assert!(!redacted_chunks[1].contains("aegis_sk_live_alpha9922"));
    assert!(redacted_chunks[1].contains(REDACTED_MARKER));
    assert!(!redacted_chunks[2].contains("postgres_master_secret_123"));
    assert!(redacted_chunks[2].contains(REDACTED_MARKER));

    // Multi-line response redaction
    let multiline = "Error log:\nAuth: aegis_sk_live_alpha9922\nDB: postgres_master_secret_123\nStatus: nominal";
    let redacted_multi = vault.redact_sensitive_text(multiline);
    assert!(!redacted_multi.contains("aegis_sk_live_alpha9922"));
    assert!(!redacted_multi.contains("postgres_master_secret_123"));

    // Verify zero disk secrets written
    assert!(!std::path::Path::new(".env").exists());
    assert!(!std::path::Path::new(".env.local").exists());
    assert!(!std::path::Path::new(".env.production").exists());
}

#[test]
fn test_vector_math_invariants_and_edge_cases() {
    // Zero vectors
    let zero = [0.0f32, 0.0, 0.0, 0.0];
    let non_zero = [1.0f32, 2.0, 3.0, 4.0];
    assert_eq!(VectorMath::cosine_similarity_4d(zero, non_zero), 0.0);
    assert_eq!(VectorMath::cosine_similarity_4d(zero, zero), 0.0);

    // Identical vectors
    let v = [0.5f32, -0.5, 0.5, -0.5];
    let sim_ident = VectorMath::cosine_similarity_4d(v, v);
    assert!((sim_ident - 1.0).abs() < 1e-4);

    // Orthogonal vectors
    let o1 = [1.0f32, 0.0, 0.0, 0.0];
    let o2 = [0.0f32, 1.0, 0.0, 0.0];
    assert_eq!(VectorMath::cosine_similarity_4d(o1, o2), 0.0);

    // Opposite vectors
    let opp = [-0.5f32, 0.5, -0.5, 0.5];
    let sim_opp = VectorMath::cosine_similarity_4d(v, opp);
    assert!((sim_opp - (-1.0)).abs() < 1e-4);

    // NaN and Inf handling
    let nan_v = [f32::NAN, 1.0, 2.0, 3.0];
    let inf_v = [f32::INFINITY, 1.0, 2.0, 3.0];
    assert_eq!(VectorMath::cosine_similarity_4d(nan_v, non_zero), 0.0);
    assert_eq!(VectorMath::cosine_similarity_4d(inf_v, non_zero), 0.0);

    // Temperature scale boundaries
    assert_eq!(VectorMath::temperature_scale(4.0, 0.0), 4.0);
    assert_eq!(VectorMath::temperature_scale(4.0, f32::NAN), 0.0);
    let normal_scaled = VectorMath::temperature_scale(8.0, 2.0);
    assert!((normal_scaled - 4.0).abs() < 1e-4);
}

#[tokio::test]
async fn test_gateway_offline_inference_fail_closed() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let inference = Arc::new(HttpInferenceBackend::new(
        Some("http://127.0.0.1:9999/v1/chat/completions".to_string()),
        None,
    ));
    let heartbeat = Arc::new(HeartbeatEngine::with_inference(
        60,
        db.clone(),
        inference.clone(),
    ));
    let skills = Arc::new(SkillRegistry::new());
    let agent = Arc::new(AgentEngine::new(
        inference.clone(),
        skills.clone(),
        Some(db.clone()),
    ));
    let state = GatewayState {
        start_time: Instant::now(),
        db,
        inference,
        heartbeat,
        skills,
        agent,
        operator: aegis::OperatorAuth::with_token(TEST_OPERATOR_TOKEN).unwrap(),
    };
    let app = create_router(state);

    let req_payload = serde_json::json!({
        "model": "atlas-lightning-omni",
        "messages": [
            {"role": "user", "content": "ping"}
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("Authorization", auth_header())
                .header("Content-Type", "application/json")
                .body(Body::from(req_payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let err_str = String::from_utf8_lossy(&bytes);
    assert!(err_str.contains("Inference endpoint unreachable"));
}

#[tokio::test]
async fn test_agent_offline_inference_fail_closed() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let inference = Arc::new(HttpInferenceBackend::new(
        Some("http://127.0.0.1:9999/v1/chat/completions".to_string()),
        None,
    ));
    let skills = Arc::new(SkillRegistry::new());
    let agent = AgentEngine::new(inference, skills.clone(), Some(db.clone()));

    let result = agent
        .execute_task("Run sequential diagnostic checks", None, 3)
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Inference endpoint unreachable"));
}

#[tokio::test]
async fn test_gateway_probe_endpoint() {
    let state = create_test_state().await;
    let app = create_router(state);

    let probes = aien_probe::ProbeSet::new().with(
        "safety",
        aien_probe::Probe::Noul(aien_probe::Noul::new("Is this code safe?")),
    );

    let payload = serde_json::json!({
        "state": { "diff": "+ fn main() {}" },
        "probes": probes,
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/probe")
                .header("Authorization", auth_header())
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["model"], "aien-sovereign-gb10");
    assert!(body["answers"].as_array().is_some());
}

#[tokio::test]
async fn completion_truth_http_turn_exhaustion_is_incomplete() {
    let app = create_router(create_test_state().await);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/agent/run")
                .header("Authorization", auth_header())
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"prompt":"no turn budget","max_turns":0}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["completed"], false);
    assert!(body["final_response"]
        .as_str()
        .unwrap()
        .contains("maximum turn limit (0)"));
    assert_eq!(body["status"], "incomplete");
}

#[tokio::test]
async fn completion_truth_ws_turn_exhaustion_is_incomplete() {
    let app = create_router(create_test_state().await);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (mut ws, _) =
        tokio_tungstenite::connect_async(authed_ws_request(&format!("ws://{addr}/ws")))
            .await
            .unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"type":"agent_run","prompt":"no turn budget","max_turns":0}"#.into(),
    ))
    .await
    .unwrap();
    let body: Value = loop {
        let message = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Ok(text) = message.to_text() {
            let body: Value = serde_json::from_str(text).unwrap();
            if body["type"] == "agent_result" {
                break body;
            }
        }
    };
    ws.close(None).await.unwrap();
    server.abort();
    assert_eq!(body["result"]["completed"], false);
    assert!(body["result"]["final_response"]
        .as_str()
        .unwrap()
        .contains("maximum turn limit (0)"));
    assert_eq!(body["status"], "incomplete");
}

async fn assert_heartbeat_completion_truth(
    task_type: &str,
    payload: &str,
    reject_crumb: bool,
    successful: bool,
    expected_result: &str,
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("aegis.sqlite");
    let db = Arc::new(Database::open(&path).unwrap());
    let observer = rusqlite::Connection::open(&path).unwrap();
    if reject_crumb {
        observer
            .execute_batch(
                "CREATE TRIGGER reject_heartbeat_crumb BEFORE INSERT ON memory_crumbs
                 BEGIN SELECT RAISE(FAIL, 'injected crumb write failure'); END;",
            )
            .unwrap();
    }
    db.create_task("completion-truth", task_type, payload)
        .unwrap();
    let mut health_server = None;
    let engine = if task_type == "model_health" {
        let app = axum::Router::new().route(
            "/v1/models",
            axum::routing::get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        health_server = Some(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap()
        }));
        HeartbeatEngine::with_inference(
            60,
            db,
            Arc::new(HttpInferenceBackend::new(
                Some(format!("http://{addr}/v1/chat/completions")),
                None,
            )),
        )
    } else if task_type == "shell_exec" && !successful {
        // `git status` is admitted, but fails in a workspace with no repository.
        HeartbeatEngine::with_workspace(
            60,
            db,
            aegis::WorkspaceCapability::new(dir.path()).unwrap(),
        )
    } else {
        HeartbeatEngine::new(60, db)
    };
    let receipt = engine.pulse_once().await;
    if let Some(server) = health_server {
        server.abort();
    }
    assert_eq!(receipt.actions_dispatched, 1);
    let (status, result): (String, String) = observer
        .query_row(
            "SELECT status, result FROM tasks WHERE id = 'completion-truth'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(result.contains(expected_result), "{result}");
    assert_eq!(status, if successful { "completed" } else { "failed" });
    assert_eq!(
        receipt.status,
        if successful { "nominal" } else { "degraded" }
    );
    let stored_status: String = observer
        .query_row(
            "SELECT status FROM heartbeats WHERE tick_id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored_status, receipt.status);
    let transcript = std::fs::read_to_string(dir.path().join("transcript.jsonl")).unwrap();
    let events: Vec<Value> = transcript
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(events.iter().any(|event| event["event_type"]
        == if successful {
            "task_completed"
        } else {
            "task_failed"
        }));
    assert!(!events.iter().any(|event| event["event_type"]
        == if successful {
            "task_failed"
        } else {
            "task_completed"
        }));
    if reject_crumb {
        assert!(!result.contains("Memory crumbs recorded"));
        assert!(engine.pulse_once().await.status == "nominal");
    }
}

#[tokio::test]
async fn completion_truth_heartbeat_shell_failure() {
    assert_heartbeat_completion_truth(
        "shell_exec",
        PROBE_ADMITTED_COMMAND,
        false,
        false,
        "Execution failure",
    )
    .await;
}

#[tokio::test]
async fn completion_truth_heartbeat_crumb_failure() {
    assert_heartbeat_completion_truth(
        "consolidate_memory",
        "",
        true,
        false,
        "injected crumb write failure",
    )
    .await;
}

#[tokio::test]
async fn completion_truth_heartbeat_unknown_task_failure() {
    assert_heartbeat_completion_truth("not-a-task", "", false, false, "Unknown task type").await;
}

#[tokio::test]
async fn completion_truth_heartbeat_success() {
    assert_heartbeat_completion_truth(
        "consolidate_memory",
        "",
        false,
        true,
        "Memory crumbs recorded",
    )
    .await;
}

#[tokio::test]
async fn completion_truth_heartbeat_unhealthy_model_failure() {
    assert_heartbeat_completion_truth("model_health", "", false, false, "unreachable or offline")
        .await;
}

#[tokio::test]
async fn completion_truth_heartbeat_shell_success() {
    assert_heartbeat_completion_truth(
        "shell_exec",
        PROBE_ADMITTED_COMMAND,
        false,
        true,
        "Exit code 0",
    )
    .await;
}

#[tokio::test]
async fn completion_truth_http_executed_turn_budget_exhaustion() {
    // A real inference turn produces a successful tool call, but never terminal output.
    let app = axum::Router::new().route(
        "/v1/chat/completions",
        axum::routing::post(|| async {
            axum::Json(serde_json::json!({
                "choices": [{
                    "message": {
                        "content": null,
                        "tool_calls": [{
                            "id": "inspect-once",
                            "type": "function",
                            "function": {
                                "name": "bash_eval",
                                "arguments": "{\"command\":\"git status\"}"
                            }
                        }]
                    },
                    "finish_reason": "tool_calls"
                }]
            }))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut state = create_test_state().await;
    state.inference = Arc::new(HttpInferenceBackend::new(
        Some(format!("http://{addr}/v1/chat/completions")),
        None,
    ));
    state.agent = Arc::new(AgentEngine::new(
        state.inference.clone(),
        state.skills.clone(),
        Some(state.db.clone()),
    ));
    let response = create_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/agent/run")
                .header("Authorization", auth_header())
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"prompt":"keep using tools","max_turns":1}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    server.abort();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["turns_taken"], 1);
    assert_eq!(body["steps"][0]["tool_calls"][0]["success"], true);
    assert_eq!(body["completed"], false);
    assert!(body["final_response"]
        .as_str()
        .unwrap()
        .contains("maximum turn limit (1)"));
    assert_eq!(body["status"], "incomplete");
}
