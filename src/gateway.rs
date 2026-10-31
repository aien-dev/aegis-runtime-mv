use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{header, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio_stream::wrappers::BroadcastStream;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use crate::agent::{AgentEngine, AgentStep};
use crate::heartbeat::HeartbeatEngine;
use crate::inference::InferenceEngine;
use crate::persistence::Database;
use crate::skills::{SkillExecutionRequest, SkillRegistry};
use crate::vector_math::VectorMath;

#[derive(Clone)]
pub struct GatewayState {
    pub start_time: Instant,
    pub db: Arc<Database>,
    pub inference: Arc<dyn InferenceEngine>,
    pub heartbeat: Arc<HeartbeatEngine>,
    pub skills: Arc<SkillRegistry>,
    pub agent: Arc<AgentEngine>,
    /// Operator token checked on every non-public route.
    pub operator: crate::auth::OperatorAuth,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub engine: &'static str,
    pub uptime_seconds: u64,
    pub local_model: String,
    pub heartbeat_interval_seconds: u64,
    pub total_heartbeat_ticks: u64,
    pub database_mode: &'static str,
    pub zero_disk_secrets: bool,
}

pub async fn health_handler(State(state): State<GatewayState>) -> impl IntoResponse {
    let uptime = state.start_time.elapsed().as_secs();
    let ticks = state.heartbeat.tick_count();
    let model = state.inference.model_id();

    Json(HealthResponse {
        status: "healthy",
        engine: "aegis-runtime/0.2.0",
        uptime_seconds: uptime,
        local_model: model,
        heartbeat_interval_seconds: state.heartbeat.interval_secs(),
        total_heartbeat_ticks: ticks,
        database_mode: "sqlite-wal",
        zero_disk_secrets: true,
    })
}

#[derive(Deserialize)]
pub struct ChatRequest {
    pub prompt: String,
    pub system: Option<String>,
    pub temperature: Option<f32>,
}

#[derive(Serialize)]
pub struct ChatResponse {
    pub role: &'static str,
    pub content: String,
    pub model: String,
    pub duration_ms: u128,
}

pub async fn chat_handler(
    State(state): State<GatewayState>,
    Json(payload): Json<ChatRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let start = Instant::now();
    let response = state
        .inference
        .generate(
            &payload.prompt,
            payload.system.as_deref(),
            payload.temperature,
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let duration = start.elapsed().as_millis();
    let model = state.inference.model_id();

    let _ = state
        .db
        .record_turn("user", &payload.prompt, &response, duration as u64);

    Ok(Json(ChatResponse {
        role: "assistant",
        content: response,
        model,
        duration_ms: duration,
    }))
}

#[derive(Deserialize)]
pub struct AgentRunRequest {
    pub prompt: String,
    pub system: Option<String>,
    pub max_turns: Option<usize>,
}

#[derive(Serialize)]
pub struct AgentRunResponse {
    /// `completed` only for terminal output, otherwise `incomplete`.
    pub status: &'static str,
    pub prompt: String,
    pub final_response: String,
    pub steps: Vec<AgentStep>,
    pub turns_taken: usize,
    pub duration_ms: u128,
    pub completed: bool,
}

pub async fn agent_run_handler(
    State(state): State<GatewayState>,
    Json(payload): Json<AgentRunRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let max_turns = payload.max_turns.unwrap_or(8);
    let result = state
        .agent
        .execute_task(&payload.prompt, payload.system.as_deref(), max_turns)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(AgentRunResponse {
        status: if result.completed {
            "completed"
        } else {
            "incomplete"
        },
        prompt: payload.prompt,
        final_response: result.final_response,
        steps: result.steps,
        turns_taken: result.turns_taken,
        duration_ms: result.duration_ms,
        completed: result.completed,
    }))
}

// OpenAI-compatible Chat Completions API with streaming and multi-turn support
#[derive(Deserialize, Serialize, Clone)]
pub struct OpenAiMessage {
    pub role: String,
    pub content: String,
}

#[derive(Deserialize)]
pub struct OpenAiChatRequest {
    pub model: Option<String>,
    pub messages: Vec<OpenAiMessage>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
    pub stream: Option<bool>,
}

pub async fn openai_completions_handler(
    State(state): State<GatewayState>,
    Json(payload): Json<OpenAiChatRequest>,
) -> Response {
    if payload.messages.is_empty() {
        let err_body = json!({
            "error": {
                "message": "Missing required parameter: 'messages' must contain at least one message.",
                "type": "invalid_request_error",
                "param": "messages",
                "code": "missing_required_parameter"
            }
        });
        return (StatusCode::BAD_REQUEST, Json(err_body)).into_response();
    }

    if let Some(ref req_model) = payload.model {
        let supported_model = state.inference.model_id();
        if req_model != &supported_model
            && req_model != "atlas-lightning-omni"
            && req_model != "aegis-default"
            && req_model != "default"
        {
            let err_body = json!({
                "error": {
                    "message": format!("The model '{}' does not exist or is not supported.", req_model),
                    "type": "invalid_request_error",
                    "param": "model",
                    "code": "model_not_found"
                }
            });
            return (StatusCode::NOT_FOUND, Json(err_body)).into_response();
        }
    }

    let messages_json: Vec<serde_json::Value> = payload
        .messages
        .iter()
        .map(|m| json!({"role": m.role, "content": m.content}))
        .collect();

    let last_user_prompt = payload
        .messages
        .iter()
        .rev()
        .find(|m| m.role == "user")
        .map(|m| m.content.as_str())
        .unwrap_or("hello")
        .to_string();

    if payload.stream == Some(true) {
        match state
            .inference
            .stream_chat(&messages_json, payload.temperature, payload.max_tokens)
            .await
        {
            Ok(stream) => {
                let body = Body::from_stream(stream);
                Response::builder()
                    .header(header::CONTENT_TYPE, "text/event-stream")
                    .header(header::CACHE_CONTROL, "no-cache")
                    .header(header::CONNECTION, "keep-alive")
                    .body(body)
                    .unwrap()
            }
            Err(_e) => {
                let fallback_chunk = json!({
                    "id": format!("chatcmpl-{}", uuid::Uuid::new_v4()),
                    "object": "chat.completion.chunk",
                    "choices": [{
                        "delta": {"content": format!("[Fallback]: {}", last_user_prompt)},
                        "index": 0,
                        "finish_reason": "stop"
                    }]
                });
                let sse_body = format!("data: {}\n\ndata: [DONE]\n\n", fallback_chunk);
                Response::builder()
                    .header(header::CONTENT_TYPE, "text/event-stream")
                    .body(Body::from(sse_body))
                    .unwrap()
            }
        }
    } else {
        let start = Instant::now();
        let model = state.inference.model_id();
        let result = state
            .inference
            .generate_chat(&messages_json, payload.temperature, payload.max_tokens)
            .await;
        let duration = start.elapsed().as_millis();

        match result {
            Ok(response) => {
                let _ = state
                    .db
                    .record_turn("user", &last_user_prompt, &response, duration as u64);

                let p_tokens = last_user_prompt.split_whitespace().count();
                let c_tokens = response.split_whitespace().count();

                let resp_body = json!({
                    "id": format!("chatcmpl-{}", uuid::Uuid::new_v4()),
                    "object": "chat.completion",
                    "created": chrono::Utc::now().timestamp(),
                    "model": model,
                    "choices": [{
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": response,
                        },
                        "finish_reason": "stop"
                    }],
                    "usage": {
                        "prompt_tokens": p_tokens,
                        "completion_tokens": c_tokens,
                        "total_tokens": p_tokens + c_tokens,
                    }
                });
                Json(resp_body).into_response()
            }
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        }
    }
}

pub async fn openai_models_handler(State(state): State<GatewayState>) -> impl IntoResponse {
    let model = state.inference.model_id();
    Json(json!({
        "object": "list",
        "data": [{
            "id": model,
            "object": "model",
            "created": 1789783086,
            "owned_by": "aegis-runtime",
        }]
    }))
}

// SSE Events Endpoint
pub async fn sse_events_handler(
    State(state): State<GatewayState>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.heartbeat.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|item| async move {
        match item {
            Ok(receipt) => {
                let data = serde_json::to_string(&receipt).unwrap_or_default();
                Some(Ok(Event::default().event("heartbeat").data(data)))
            }
            Err(_) => None,
        }
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

// Discord Hub Relay
#[derive(Deserialize)]
pub struct DiscordRelayRequest {
    pub channel_id: String,
    pub author: String,
    pub content: String,
}

pub async fn discord_relay_handler(
    State(state): State<GatewayState>,
    Json(payload): Json<DiscordRelayRequest>,
) -> impl IntoResponse {
    let prompt = format!(
        "[Discord from {} in {}]: {}",
        payload.author, payload.channel_id, payload.content
    );
    let reply = state
        .inference
        .generate(&prompt, None, None)
        .await
        .unwrap_or_else(|e| format!("Error: {}", e));

    let _ = state
        .db
        .record_turn(&payload.author, &payload.content, &reply, 10);

    Json(json!({
        "status": "relayed",
        "reply": reply,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    }))
}

// Local Shell Executor
#[derive(Deserialize)]
pub struct ShellRequest {
    pub command: String,
}

#[derive(Serialize)]
pub struct ShellResponse {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub success: bool,
}

pub async fn shell_handler(
    State(state): State<GatewayState>,
    Json(payload): Json<ShellRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    match state
        .skills
        .workspace()
        .dispatch_shell_gated(&payload.command, None, 15)
        .await
    {
        Ok(stdout) => Ok(Json(ShellResponse {
            stdout,
            stderr: String::new(),
            exit_code: 0,
            success: true,
        })),
        Err(e) => Ok(Json(ShellResponse {
            stdout: String::new(),
            stderr: e.to_string(),
            exit_code: 1,
            success: false,
        })),
    }
}

// Task endpoints
#[derive(Deserialize)]
pub struct CreateTaskRequest {
    pub task_type: String,
    pub payload: String,
}

pub async fn create_task_handler(
    State(state): State<GatewayState>,
    Json(req): Json<CreateTaskRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let id = format!("task-{}", uuid::Uuid::new_v4());
    state
        .db
        .create_task(&id, &req.task_type, &req.payload)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!({
        "status": "created",
        "id": id,
        "task_type": req.task_type,
    })))
}

pub async fn list_tasks_handler(
    State(state): State<GatewayState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tasks = state
        .db
        .list_pending_tasks()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(tasks))
}

pub async fn trigger_heartbeat_handler(State(state): State<GatewayState>) -> impl IntoResponse {
    let receipt = state.heartbeat.pulse_once().await;
    Json(receipt)
}

// Skills endpoints
pub async fn list_skills_handler(State(state): State<GatewayState>) -> impl IntoResponse {
    let list = state.skills.advertised_skills();
    Json(list)
}

pub async fn execute_skill_handler(
    State(state): State<GatewayState>,
    Json(req): Json<SkillExecutionRequest>,
) -> impl IntoResponse {
    // Membrane + probe gate (always on), then the handler.
    let res = state.skills.execute_gated(&req).await;
    Json(res)
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<GatewayState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: GatewayState) {
    let welcome = json!({
        "type": "welcome",
        "gateway": "aegis-runtime",
        "engine": "AEGIS WebSocket Gateway",
        "status": "connected",
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });

    if socket
        .send(Message::Text(welcome.to_string()))
        .await
        .is_err()
    {
        return;
    }

    let mut pulse_rx = state.heartbeat.subscribe();

    loop {
        tokio::select! {
            pulse = pulse_rx.recv() => {
                if let Ok(receipt) = pulse {
                    let out = json!({
                        "type": "heartbeat_pulse",
                        "receipt": receipt,
                    });
                    if socket.send(Message::Text(out.to_string())).await.is_err() {
                        break;
                    }
                }
            }
            msg = socket.next() => {
                let Some(Ok(msg)) = msg else {
                    break;
                };
                if let Message::Text(text) = msg {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                        let msg_type = parsed.get("type").and_then(|v| v.as_str()).unwrap_or("chat");
                        match msg_type {
                            "ping" => {
                                let pong = json!({"type": "pong", "timestamp": chrono::Utc::now().to_rfc3339()});
                                let _ = socket.send(Message::Text(pong.to_string())).await;
                            }
                            "heartbeat" => {
                                let receipt = state.heartbeat.pulse_once().await;
                                let out = json!({"type": "heartbeat_receipt", "receipt": receipt});
                                let _ = socket.send(Message::Text(out.to_string())).await;
                            }
                            "skills_list" => {
                                let list = state.skills.advertised_skills();
                                let out = json!({"type": "skills_list", "skills": list});
                                let _ = socket.send(Message::Text(out.to_string())).await;
                            }
                            "skill_exec" => {
                                let skill_name = parsed.get("skill_name").and_then(|v| v.as_str()).unwrap_or("");
                                let args = parsed.get("arguments").cloned().unwrap_or_else(|| json!({}));
                                let req = SkillExecutionRequest {
                                    skill_name: skill_name.to_string(),
                                    arguments: args,
                                };
                                let res = state.skills.execute_gated(&req).await;
                                let out = json!({"type": "skill_result", "response": res});
                                let _ = socket.send(Message::Text(out.to_string())).await;
                            }
                            "agent_run" => {
                                let prompt = parsed.get("prompt").and_then(|v| v.as_str()).unwrap_or("");
                                let max_turns = parsed.get("max_turns").and_then(|v| v.as_u64()).unwrap_or(8) as usize;
                                let res = state.agent.execute_task(prompt, None, max_turns).await;
                                match res {
                                    Ok(exec_result) => {
                                        let out = json!({
                                            "type": "agent_result",
                                            "status": if exec_result.completed { "completed" } else { "incomplete" },
                                            "result": exec_result,
                                        });
                                        let _ = socket.send(Message::Text(out.to_string())).await;
                                    }
                                    Err(e) => {
                                        let out = json!({
                                            "type": "agent_error",
                                            "error": e.to_string(),
                                        });
                                        let _ = socket.send(Message::Text(out.to_string())).await;
                                    }
                                }
                            }
                            "shell" => {
                                let cmd_str = parsed.get("command").and_then(|v| v.as_str()).unwrap_or("echo shell ready");
                                let out = match state.skills.workspace().dispatch_shell_gated(cmd_str, None, 15).await {
                                    Ok(stdout) => json!({
                                        "type": "shell_output",
                                        "stdout": stdout,
                                        "stderr": "",
                                        "exit_code": 0,
                                    }),
                                    Err(e) => json!({
                                        "type": "shell_error",
                                        "error": e.to_string(),
                                    }),
                                };
                                let _ = socket.send(Message::Text(out.to_string())).await;
                            }
                            "simd_eval" => {
                                let sim = VectorMath::cosine_similarity(&[1.0, 0.0, 0.0, 0.0], &[1.0, 0.0, 0.0, 0.0]);
                                let out = json!({
                                    "type": "simd_result",
                                    "cosine_similarity": sim,
                                });
                                let _ = socket.send(Message::Text(out.to_string())).await;
                            }
                            _ => {
                                let prompt = parsed.get("content").and_then(|v| v.as_str()).unwrap_or(&text);
                                let start = Instant::now();
                                let result = state.inference.generate(prompt, None, None).await;
                                let duration = start.elapsed().as_millis();

                                match result {
                                    Ok(reply) => {
                                        let _ = state.db.record_turn("ws_user", prompt, &reply, duration as u64);
                                        let out = json!({
                                            "type": "response",
                                            "content": reply,
                                            "duration_ms": duration,
                                            "timestamp": chrono::Utc::now().to_rfc3339(),
                                        });
                                        if socket.send(Message::Text(out.to_string())).await.is_err() {
                                            break;
                                        }
                                    }
                                    Err(err) => {
                                        let err_out = json!({
                                            "type": "error",
                                            "message": err.to_string(),
                                        });
                                        if socket.send(Message::Text(err_out.to_string())).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[derive(Deserialize)]
pub struct ProbeApiRequest {
    pub state: serde_json::Value,
    pub probes: aien_probe::ProbeSet,
}

#[derive(Serialize)]
pub struct ProbeApiResponse {
    pub model: String,
    pub answers: Vec<(String, aien_probe::Answer)>,
    pub latency_micros: u64,
}

pub async fn probe_handler(
    Json(payload): Json<ProbeApiRequest>,
) -> Result<Json<ProbeApiResponse>, (StatusCode, String)> {
    let engine = aien_probe::ProbeEngine::new(aien_probe::DeterministicReferenceBackend::new());
    let response = engine
        .evaluate(&payload.state, &payload.probes)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(ProbeApiResponse {
        model: "aien-sovereign-gb10".to_string(),
        answers: response.answers,
        latency_micros: response.latency_micros,
    }))
}

/// Every route sits behind `auth::require_operator`: deny by default, a short
/// list of read-only GET routes is public, everything else needs the token.
pub fn create_router(state: GatewayState) -> Router {
    let operator = state.operator.clone();
    Router::new()
        .route("/health", get(health_handler))
        .route("/api/v1/health", get(health_handler))
        .route("/api/v1/chat", post(chat_handler))
        .route("/api/v1/agent/run", post(agent_run_handler))
        .route("/api/v1/heartbeat/tick", post(trigger_heartbeat_handler))
        .route("/api/v1/events", get(sse_events_handler))
        .route("/events", get(sse_events_handler))
        .route("/api/v1/discord/relay", post(discord_relay_handler))
        .route("/api/v1/shell", post(shell_handler))
        .route(
            "/api/v1/tasks",
            post(create_task_handler).get(list_tasks_handler),
        )
        .route("/api/v1/skills", get(list_skills_handler))
        .route("/api/v1/skills/execute", post(execute_skill_handler))
        .route("/v1/chat/completions", post(openai_completions_handler))
        .route("/v1/models", get(openai_models_handler))
        .route("/v1/probe", post(probe_handler))
        .route("/ws", get(ws_handler))
        .route("/api/v1/ws", get(ws_handler))
        // Auth wraps every route (including unknown paths), inside CORS.
        .layer(axum::middleware::from_fn_with_state(
            operator,
            crate::auth::require_operator,
        ))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state)
}

/// Listens on `addr`. Callers build it with `auth::resolve_bind_addr`, which
/// refuses non-loopback addresses unless the operator opted in.
pub async fn start_gateway(
    state: GatewayState,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    if !state.operator.is_configured() {
        tracing::warn!(
            "No operator token configured ({}); protected routes will refuse every request",
            crate::auth::OPERATOR_TOKEN_ENV
        );
    }
    let app = create_router(state);
    info!("AEGIS Gateway listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HttpInferenceBackend;

    #[tokio::test]
    async fn test_gateway_health_handler_unit() {
        let db = Arc::new(Database::open_in_memory().unwrap());
        let inference = Arc::new(HttpInferenceBackend::new(None, None));
        let heartbeat = Arc::new(HeartbeatEngine::new(60, db.clone()));
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
            operator: crate::auth::OperatorAuth::disabled(),
        };

        let res = health_handler(State(state)).await.into_response();
        assert_eq!(res.status(), StatusCode::OK);
    }
}
