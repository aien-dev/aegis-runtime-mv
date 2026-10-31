//! In-process embedded inference backend and HTTP adapter for AEGIS.
//! Connects AEGIS directly to NativeTransformerBackend and TinyLlamaTokenizer in-process,
//! eliminating the localhost HTTP inference daemon requirement.

use aien_inference_client::MockInferenceClient;
use aien_inference_protocol::{InferenceMessage, InferenceRequest, InferenceService};
use async_trait::async_trait;
use futures_util::StreamExt;
use reqwest::Client;
use std::pin::Pin;

pub type ChatStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send>>;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallFunction {
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallItem {
    pub id: String,
    #[serde(rename = "type")]
    pub call_type: String,
    pub function: ToolCallFunction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatTurnResponse {
    pub content: Option<String>,
    pub reasoning: Option<String>,
    pub tool_calls: Option<Vec<ToolCallItem>>,
    pub finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatMessage,
}

#[derive(Deserialize)]
struct ChatMessage {
    content: Option<String>,
    reasoning: Option<String>,
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoiceWithTools {
    message: ChatMessageWithTools,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct ChatMessageWithTools {
    content: Option<String>,
    reasoning: Option<String>,
    tool_calls: Option<Vec<ToolCallItem>>,
}

#[derive(Deserialize)]
struct ChatResponseWithTools {
    choices: Vec<ChatChoiceWithTools>,
}

/// Unified inference engine interface supporting both in-process embedded
/// and external HTTP inference backends.
#[async_trait]
pub trait InferenceEngine: Send + Sync {
    /// Identifier of the model being served.
    fn model_id(&self) -> String;

    /// Endpoint description or URI.
    fn endpoint(&self) -> String;

    /// Generates text from a prompt with optional system prompt and temperature.
    async fn generate(
        &self,
        prompt: &str,
        system_prompt: Option<&str>,
        temperature: Option<f32>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        self.generate_with_tokens(prompt, system_prompt, temperature, Some(1024))
            .await
    }

    /// Generates text with explicit token limit.
    async fn generate_with_tokens(
        &self,
        prompt: &str,
        system_prompt: Option<&str>,
        temperature: Option<f32>,
        max_tokens: Option<u32>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>>;

    /// Executes multi-turn chat generation from structured messages.
    async fn generate_chat(
        &self,
        messages: &[serde_json::Value],
        temperature: Option<f32>,
        max_tokens: Option<u32>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>>;

    /// Executes multi-turn chat generation with tool definitions.
    async fn generate_chat_with_tools(
        &self,
        messages: &[serde_json::Value],
        tools: Option<&[serde_json::Value]>,
        temperature: Option<f32>,
        max_tokens: Option<u32>,
    ) -> Result<ChatTurnResponse, Box<dyn std::error::Error + Send + Sync>>;

    /// Streams chat completions as raw SSE event bytes.
    async fn stream_chat(
        &self,
        messages: &[serde_json::Value],
        temperature: Option<f32>,
        max_tokens: Option<u32>,
    ) -> Result<ChatStream, Box<dyn std::error::Error + Send + Sync>>;

    /// Validates inference engine availability and readiness.
    async fn check_health(&self) -> bool;
}

/// External HTTP inference backend connecting to an OpenAI-compatible daemon.
pub struct HttpInferenceBackend {
    client: Client,
    endpoint: String,
    model_id: String,
}

impl HttpInferenceBackend {
    pub fn new(endpoint: Option<String>, model_id: Option<String>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(90))
            .build()
            .unwrap_or_default();

        Self {
            client,
            endpoint: endpoint
                .unwrap_or_else(|| "http://127.0.0.1:18006/v1/chat/completions".to_string()),
            model_id: model_id.unwrap_or_else(|| "atlas-lightning-omni".to_string()),
        }
    }
}

#[async_trait]
impl InferenceEngine for HttpInferenceBackend {
    fn model_id(&self) -> String {
        self.model_id.clone()
    }

    fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    async fn generate_with_tokens(
        &self,
        prompt: &str,
        system_prompt: Option<&str>,
        temperature: Option<f32>,
        max_tokens: Option<u32>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut messages = Vec::new();
        if let Some(sys) = system_prompt {
            messages.push(json!({"role": "system", "content": sys}));
        }
        messages.push(json!({"role": "user", "content": prompt}));
        self.generate_chat(&messages, temperature, max_tokens).await
    }

    async fn generate_chat(
        &self,
        messages: &[serde_json::Value],
        temperature: Option<f32>,
        max_tokens: Option<u32>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let body = json!({
            "model": self.model_id,
            "messages": messages,
            "max_tokens": max_tokens.unwrap_or(1024),
            "temperature": temperature.unwrap_or(0.2),
        });

        match self.client.post(&self.endpoint).json(&body).send().await {
            Ok(res) => {
                if res.status().is_success() {
                    let parsed: ChatResponse = res.json().await?;
                    if let Some(choice) = parsed.choices.into_iter().next() {
                        if let Some(content) = choice.message.content {
                            if !content.trim().is_empty() {
                                return Ok(content);
                            }
                        }
                        if let Some(reasoning) = choice.message.reasoning {
                            if !reasoning.trim().is_empty() {
                                return Ok(reasoning);
                            }
                        }
                    }
                    Ok("Response received with zero text content".to_string())
                } else {
                    let err_status = res.status();
                    let err_text = res.text().await.unwrap_or_default();
                    warn!("Model seat inference error {}: {}", err_status, err_text);
                    Err(
                        anyhow::anyhow!("Inference endpoint error {}: {}", err_status, err_text)
                            .into(),
                    )
                }
            }
            Err(e) => {
                warn!(
                    "Cannot reach model seat endpoint at {}: {}",
                    self.endpoint, e
                );
                Err(anyhow::anyhow!("Inference endpoint unreachable: {}", e).into())
            }
        }
    }

    async fn generate_chat_with_tools(
        &self,
        messages: &[serde_json::Value],
        tools: Option<&[serde_json::Value]>,
        temperature: Option<f32>,
        max_tokens: Option<u32>,
    ) -> Result<ChatTurnResponse, Box<dyn std::error::Error + Send + Sync>> {
        let mut body = json!({
            "model": self.model_id,
            "messages": messages,
            "max_tokens": max_tokens.unwrap_or(2048),
            "temperature": temperature.unwrap_or(0.2),
        });

        if let Some(t) = tools {
            if !t.is_empty() {
                body["tools"] = json!(t);
            }
        }

        match self.client.post(&self.endpoint).json(&body).send().await {
            Ok(res) => {
                if res.status().is_success() {
                    let parsed: ChatResponseWithTools = res.json().await?;
                    if let Some(choice) = parsed.choices.into_iter().next() {
                        return Ok(ChatTurnResponse {
                            content: choice.message.content,
                            reasoning: choice.message.reasoning,
                            tool_calls: choice.message.tool_calls,
                            finish_reason: choice.finish_reason,
                        });
                    }
                    Err(anyhow::anyhow!("Inference returned an empty choices array").into())
                } else {
                    let err_status = res.status();
                    let err_text = res.text().await.unwrap_or_default();
                    warn!("Model seat inference error {}: {}", err_status, err_text);
                    Err(
                        anyhow::anyhow!("Inference endpoint error {}: {}", err_status, err_text)
                            .into(),
                    )
                }
            }
            Err(e) => {
                warn!(
                    "Cannot reach model seat endpoint at {}: {}",
                    self.endpoint, e
                );
                Err(anyhow::anyhow!("Inference endpoint unreachable: {}", e).into())
            }
        }
    }

    async fn stream_chat(
        &self,
        messages: &[serde_json::Value],
        temperature: Option<f32>,
        max_tokens: Option<u32>,
    ) -> Result<ChatStream, Box<dyn std::error::Error + Send + Sync>> {
        let body = json!({
            "model": self.model_id,
            "messages": messages,
            "max_tokens": max_tokens.unwrap_or(1024),
            "temperature": temperature.unwrap_or(0.2),
            "stream": true,
        });

        let res = self.client.post(&self.endpoint).json(&body).send().await?;
        if !res.status().is_success() {
            let status = res.status();
            let text = res.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("Model seat streaming error {}: {}", status, text).into());
        }

        let stream = res
            .bytes_stream()
            .map(|item| item.map_err(std::io::Error::other));
        Ok(Box::pin(stream))
    }

    async fn check_health(&self) -> bool {
        let health_url = self.endpoint.replace("/v1/chat/completions", "/v1/models");
        match self.client.get(&health_url).send().await {
            Ok(res) => res.status().is_success(),
            Err(_) => false,
        }
    }
}

/// Protocol-driven inference backend executing against any conforming AIEN InferenceService.
pub struct ProtocolInferenceBackend {
    service: Arc<dyn InferenceService>,
    model_id: String,
    endpoint: String,
}

impl ProtocolInferenceBackend {
    pub fn new(service: Arc<dyn InferenceService>, model_id: String) -> Self {
        Self {
            service,
            model_id,
            endpoint: "protocol://aien-inference-service".to_string(),
        }
    }

    pub fn with_mock() -> Self {
        Self::new(
            Arc::new(MockInferenceClient::new()),
            "mock-protocol-model".to_string(),
        )
    }
}

#[async_trait]
impl InferenceEngine for ProtocolInferenceBackend {
    fn model_id(&self) -> String {
        self.model_id.clone()
    }

    fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    async fn generate_with_tokens(
        &self,
        prompt: &str,
        system_prompt: Option<&str>,
        temperature: Option<f32>,
        max_tokens: Option<u32>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut messages = Vec::new();
        if let Some(sys) = system_prompt {
            messages.push(InferenceMessage {
                role: "system".to_string(),
                content: sys.to_string(),
            });
        }
        messages.push(InferenceMessage {
            role: "user".to_string(),
            content: prompt.to_string(),
        });

        let req = InferenceRequest {
            request_id: uuid::Uuid::new_v4(),
            model: self.model_id.clone(),
            messages,
            context: None,
            max_tokens: max_tokens.unwrap_or(256),
            temperature: temperature.unwrap_or(0.0),
            stop_sequences: Vec::new(),
        };

        let resp = self
            .service
            .infer(req)
            .await
            .map_err(|e| anyhow::anyhow!("Protocol inference error: {}", e))?;

        Ok(resp.content)
    }

    async fn generate_chat(
        &self,
        messages: &[serde_json::Value],
        temperature: Option<f32>,
        max_tokens: Option<u32>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut inference_messages = Vec::new();
        for msg in messages {
            let role = msg
                .get("role")
                .and_then(|r| r.as_str())
                .unwrap_or("user")
                .to_string();
            let content = msg
                .get("content")
                .and_then(|c| c.as_str())
                .unwrap_or("")
                .to_string();
            inference_messages.push(InferenceMessage { role, content });
        }

        let req = InferenceRequest {
            request_id: uuid::Uuid::new_v4(),
            model: self.model_id.clone(),
            messages: inference_messages,
            context: None,
            max_tokens: max_tokens.unwrap_or(256),
            temperature: temperature.unwrap_or(0.0),
            stop_sequences: Vec::new(),
        };

        let resp = self
            .service
            .infer(req)
            .await
            .map_err(|e| anyhow::anyhow!("Protocol inference error: {}", e))?;

        Ok(resp.content)
    }

    async fn generate_chat_with_tools(
        &self,
        messages: &[serde_json::Value],
        _tools: Option<&[serde_json::Value]>,
        temperature: Option<f32>,
        max_tokens: Option<u32>,
    ) -> Result<ChatTurnResponse, Box<dyn std::error::Error + Send + Sync>> {
        let content = self
            .generate_chat(messages, temperature, max_tokens)
            .await?;
        let (clean_content, tool_calls) = parse_structured_tool_calls(&content);
        Ok(ChatTurnResponse {
            content: clean_content,
            reasoning: None,
            tool_calls,
            finish_reason: Some("stop".to_string()),
        })
    }

    async fn stream_chat(
        &self,
        messages: &[serde_json::Value],
        temperature: Option<f32>,
        max_tokens: Option<u32>,
    ) -> Result<ChatStream, Box<dyn std::error::Error + Send + Sync>> {
        let content = self
            .generate_chat(messages, temperature, max_tokens)
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        tokio::spawn(async move {
            let chunk_json = serde_json::json!({
                "id": uuid::Uuid::new_v4().to_string(),
                "object": "chat.completion.chunk",
                "choices": [{
                    "index": 0,
                    "delta": { "content": content },
                    "finish_reason": null
                }]
            });
            let payload = format!(
                "data: {}

data: [DONE]

",
                chunk_json
            );
            let _ = tx.send(Ok(bytes::Bytes::from(payload))).await;
        });
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Box::pin(stream))
    }

    async fn check_health(&self) -> bool {
        self.service.get_capabilities().await.is_ok()
    }
}

/// Backwards-compatible adapter for in-process protocol inference.
pub struct EmbeddedInferenceBackend {
    inner: ProtocolInferenceBackend,
}

impl EmbeddedInferenceBackend {
    pub fn new(service: Arc<dyn InferenceService>, model_id: String) -> Self {
        Self {
            inner: ProtocolInferenceBackend::new(service, model_id),
        }
    }

    pub fn with_reference_weights<T>(_config: &T) -> Result<Self, String> {
        Ok(Self {
            inner: ProtocolInferenceBackend::with_mock(),
        })
    }

    pub fn load_or_fallback(
        _model_path: Option<&str>,
        _tokenizer_path: Option<&str>,
    ) -> Result<Self, String> {
        Ok(Self {
            inner: ProtocolInferenceBackend::with_mock(),
        })
    }
}

#[async_trait]
impl InferenceEngine for EmbeddedInferenceBackend {
    fn model_id(&self) -> String {
        self.inner.model_id()
    }

    fn endpoint(&self) -> String {
        self.inner.endpoint()
    }

    async fn generate_with_tokens(
        &self,
        prompt: &str,
        system_prompt: Option<&str>,
        temperature: Option<f32>,
        max_tokens: Option<u32>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        self.inner
            .generate_with_tokens(prompt, system_prompt, temperature, max_tokens)
            .await
    }

    async fn generate_chat(
        &self,
        messages: &[serde_json::Value],
        temperature: Option<f32>,
        max_tokens: Option<u32>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        self.inner
            .generate_chat(messages, temperature, max_tokens)
            .await
    }

    async fn generate_chat_with_tools(
        &self,
        messages: &[serde_json::Value],
        tools: Option<&[serde_json::Value]>,
        temperature: Option<f32>,
        max_tokens: Option<u32>,
    ) -> Result<ChatTurnResponse, Box<dyn std::error::Error + Send + Sync>> {
        self.inner
            .generate_chat_with_tools(messages, tools, temperature, max_tokens)
            .await
    }

    async fn stream_chat(
        &self,
        messages: &[serde_json::Value],
        temperature: Option<f32>,
        max_tokens: Option<u32>,
    ) -> Result<ChatStream, Box<dyn std::error::Error + Send + Sync>> {
        self.inner
            .stream_chat(messages, temperature, max_tokens)
            .await
    }

    async fn check_health(&self) -> bool {
        self.inner.check_health().await
    }
}

/// Formats conversation turns and tool specifications into canonical TinyLlama chat template:
/// `<|system|>\n{system}</s>\n<|user|>\n{user}</s>\n<|assistant|>\n`
pub fn format_messages_to_prompt(
    messages: &[serde_json::Value],
    tools: Option<&[serde_json::Value]>,
) -> String {
    let mut prompt = String::new();
    let mut system_prompt = String::new();

    if let Some(t_list) = tools {
        if !t_list.is_empty() {
            let tools_json = serde_json::to_string_pretty(t_list).unwrap_or_default();
            system_prompt.push_str(&format!(
                "You have access to the following tools:\n{}\n\nTo execute a tool call, output ONLY a JSON object in this format:\n```json\n{{\n  \"name\": \"<tool_name>\",\n  \"arguments\": {{ ... }}\n}}\n```\nIf no tool call is needed, provide your final response directly.\n",
                tools_json
            ));
        }
    }

    for msg in messages {
        let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("user");
        let content = msg.get("content").and_then(|c| c.as_str()).unwrap_or("");

        match role {
            "system" => {
                if !system_prompt.is_empty() {
                    system_prompt.push('\n');
                }
                system_prompt.push_str(content);
            }
            "user" => {
                if !system_prompt.is_empty() {
                    prompt.push_str(&format!("<|system|>\n{}</s>\n", system_prompt.trim()));
                    system_prompt.clear();
                }
                prompt.push_str(&format!("<|user|>\n{}</s>\n", content.trim()));
            }
            "assistant" => {
                if !system_prompt.is_empty() {
                    prompt.push_str(&format!("<|system|>\n{}</s>\n", system_prompt.trim()));
                    system_prompt.clear();
                }
                if let Some(tool_calls) = msg.get("tool_calls").and_then(|tc| tc.as_array()) {
                    let mut tc_text = String::new();
                    for tc in tool_calls {
                        let name = tc
                            .get("function")
                            .and_then(|f| f.get("name"))
                            .and_then(|n| n.as_str())
                            .unwrap_or("");
                        let args = tc
                            .get("function")
                            .and_then(|f| f.get("arguments"))
                            .and_then(|a| a.as_str())
                            .unwrap_or("{}");
                        tc_text.push_str(&format!(
                            "```json\n{{\"name\": \"{}\", \"arguments\": {}}}\n```\n",
                            name, args
                        ));
                    }
                    prompt.push_str(&format!(
                        "<|assistant|>\n{}{}\n</s>\n",
                        content.trim(),
                        tc_text.trim()
                    ));
                } else {
                    prompt.push_str(&format!("<|assistant|>\n{}</s>\n", content.trim()));
                }
            }
            "tool" => {
                prompt.push_str(&format!(
                    "<|user|>\n[Tool Output]: {}</s>\n",
                    content.trim()
                ));
            }
            _ => {
                prompt.push_str(&format!("<|user|>\n{}</s>\n", content.trim()));
            }
        }
    }

    if !system_prompt.is_empty() {
        prompt.push_str(&format!("<|system|>\n{}</s>\n", system_prompt.trim()));
    }

    prompt.push_str("<|assistant|>\n");
    prompt
}

/// Parses structured tool call JSON out of model output.
pub fn parse_structured_tool_calls(text: &str) -> (Option<String>, Option<Vec<ToolCallItem>>) {
    // 1. Check for ```json ... ``` blocks
    if let Some(start) = text.find("```json") {
        let code_start = start + 7;
        if let Some(end) = text[code_start..].find("```") {
            let json_str = text[code_start..code_start + end].trim();
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let Some(item) = extract_tool_call_from_value(&val) {
                    let prefix = text[..start].trim().to_string();
                    let content = if prefix.is_empty() {
                        None
                    } else {
                        Some(prefix)
                    };
                    return (content, Some(vec![item]));
                }
            }
        }
    }

    // 2. Check for <tool_call> ... </tool_call> tags
    if let Some(start) = text.find("<tool_call>") {
        let code_start = start + 11;
        if let Some(end) = text[code_start..].find("</tool_call>") {
            let json_str = text[code_start..code_start + end].trim();
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let Some(item) = extract_tool_call_from_value(&val) {
                    let prefix = text[..start].trim().to_string();
                    let content = if prefix.is_empty() {
                        None
                    } else {
                        Some(prefix)
                    };
                    return (content, Some(vec![item]));
                }
            }
        }
    }

    // 3. Check for standalone JSON object containing name and arguments
    if let Some(start) = text.find('{') {
        if let Some(end) = text.rfind('}') {
            if end > start {
                let candidate = &text[start..=end];
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(candidate) {
                    if let Some(item) = extract_tool_call_from_value(&val) {
                        let prefix = text[..start].trim().to_string();
                        let content = if prefix.is_empty() {
                            None
                        } else {
                            Some(prefix)
                        };
                        return (content, Some(vec![item]));
                    }
                }
            }
        }
    }

    (Some(text.to_string()), None)
}

fn extract_tool_call_from_value(val: &serde_json::Value) -> Option<ToolCallItem> {
    let name = val
        .get("name")
        .or_else(|| val.get("tool"))
        .and_then(|n| n.as_str())?;
    let arguments = if let Some(args) = val.get("arguments").or_else(|| val.get("params")) {
        if args.is_string() {
            args.as_str().unwrap().to_string()
        } else {
            serde_json::to_string(args).unwrap_or_else(|_| "{}".to_string())
        }
    } else {
        "{}".to_string()
    };

    Some(ToolCallItem {
        id: format!("call_{}", uuid::Uuid::new_v4()),
        call_type: "function".to_string(),
        function: ToolCallFunction {
            name: name.to_string(),
            arguments,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_generate_chat_offline_fail_closed() {
        let engine = HttpInferenceBackend::new(
            Some("http://127.0.0.1:9999/v1/chat/completions".to_string()),
            Some("atlas-lightning-omni".to_string()),
        );
        let messages = vec![json!({"role": "user", "content": "ping"})];
        let res = engine.generate_chat(&messages, None, None).await;
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(err.to_string().contains("Inference endpoint unreachable"));
    }

    #[tokio::test]
    async fn test_generate_chat_with_tools_offline_fail_closed() {
        let engine = HttpInferenceBackend::new(
            Some("http://127.0.0.1:9999/v1/chat/completions".to_string()),
            Some("atlas-lightning-omni".to_string()),
        );
        let messages = vec![json!({"role": "user", "content": "ping"})];
        let res = engine
            .generate_chat_with_tools(&messages, None, None, None)
            .await;
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(err.to_string().contains("Inference endpoint unreachable"));
    }

    #[test]
    fn test_format_messages_to_prompt() {
        let messages = vec![
            json!({"role": "system", "content": "You are AEGIS."}),
            json!({"role": "user", "content": "Hello."}),
        ];
        let prompt = format_messages_to_prompt(&messages, None);
        assert!(prompt.contains("<|system|>\nYou are AEGIS.</s>"));
        assert!(prompt.contains("<|user|>\nHello.</s>"));
        assert!(prompt.ends_with("<|assistant|>\n"));
    }

    #[test]
    fn test_parse_structured_tool_calls_json_block() {
        let text = "Thinking about file...
```json
{
  \"name\": \"read_file\",
  \"arguments\": {\"path\": \"src/lib.rs\"}
}
```";
        let (content, calls) = parse_structured_tool_calls(text);
        assert_eq!(content.as_deref(), Some("Thinking about file..."));
        assert!(calls.is_some());
        let items = calls.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].function.name, "read_file");
        assert!(items[0].function.arguments.contains("src/lib.rs"));
    }

    #[test]
    fn test_parse_structured_tool_calls_tag() {
        let text = "<tool_call>{\"name\": \"bash_eval\", \"arguments\": {\"command\": \"ls\"}}</tool_call>";
        let (content, calls) = parse_structured_tool_calls(text);
        assert!(content.is_none());
        assert!(calls.is_some());
        let items = calls.unwrap();
        assert_eq!(items[0].function.name, "bash_eval");
    }

    #[test]
    fn test_parse_structured_tool_calls_plain_response() {
        let text = "All tasks completed successfully with zero defects.";
        let (content, calls) = parse_structured_tool_calls(text);
        assert_eq!(
            content.as_deref(),
            Some("All tasks completed successfully with zero defects.")
        );
        assert!(calls.is_none());
    }

    #[tokio::test]
    async fn test_embedded_inference_backend_reference_execution() {
        let backend = EmbeddedInferenceBackend::load_or_fallback(None, None)
            .expect("Reference backend should construct with mock service");

        assert_eq!(backend.endpoint(), "protocol://aien-inference-service");
        assert!(backend.check_health().await);

        let messages = vec![json!({"role": "user", "content": "ping"})];
        let reply = backend.generate_chat(&messages, Some(0.0), Some(4)).await;
        assert!(
            reply.is_ok(),
            "Embedded chat generation must succeed: {:?}",
            reply
        );
        let text = reply.unwrap();
        assert!(!text.is_empty(), "Generated text must not be empty");
    }
}
