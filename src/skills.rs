use crate::failure::{
    skill_effect_profile, EffectCertainty, FailureClass, ToolDispatcher, ToolFault,
};
use crate::security::WorkspaceCapability;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::process::Command;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillDefinition {
    pub name: String,
    pub description: String,
    pub parameters_schema: serde_json::Value,
    /// False when the handler does not yet perform the contract. Those names
    /// stay callable and return unavailable. They are not offered to the model.
    #[serde(default = "default_advertised")]
    pub advertised: bool,
}

fn default_advertised() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillExecutionRequest {
    pub skill_name: String,
    pub arguments: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillExecutionResponse {
    pub success: bool,
    pub output: String,
    pub error: Option<String>,
}

pub type SkillHandler = Arc<dyn Fn(serde_json::Value) -> Result<String, String> + Send + Sync>;

#[derive(Clone)]
pub struct SkillRegistry {
    skills: Arc<RwLock<HashMap<String, (SkillDefinition, SkillHandler)>>>,
    workspace: Arc<WorkspaceCapability>,
}

impl Default for SkillRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SkillRegistry {
    pub fn new() -> Self {
        Self::with_workspace(WorkspaceCapability::detect())
    }

    pub fn with_workspace(workspace: WorkspaceCapability) -> Self {
        let registry = Self {
            skills: Arc::new(RwLock::new(HashMap::new())),
            workspace: Arc::new(workspace),
        };
        registry.register_builtin_skills();
        registry
    }

    pub fn workspace(&self) -> &WorkspaceCapability {
        &self.workspace
    }

    pub fn register<F>(&self, def: SkillDefinition, handler: F)
    where
        F: Fn(serde_json::Value) -> Result<String, String> + Send + Sync + 'static,
    {
        let mut map = self.skills.write().unwrap();
        map.insert(def.name.clone(), (def, Arc::new(handler)));
    }

    pub fn list_skills(&self) -> Vec<SkillDefinition> {
        let map = self.skills.read().unwrap();
        map.values().map(|(def, _)| def.clone()).collect()
    }

    pub fn advertised_skills(&self) -> Vec<SkillDefinition> {
        self.list_skills()
            .into_iter()
            .filter(|def| def.advertised)
            .collect()
    }

    pub fn to_openai_tools(&self) -> Vec<serde_json::Value> {
        let map = self.skills.read().unwrap();
        let mut tools = Vec::new();
        for (def, _) in map.values() {
            if !def.advertised {
                continue;
            }
            tools.push(serde_json::json!({
                "type": "function",
                "function": {
                    "name": def.name,
                    "description": def.description,
                    "parameters": def.parameters_schema
                }
            }));
        }
        tools.sort_by(|a, b| {
            let na = a["function"]["name"].as_str().unwrap_or("");
            let nb = b["function"]["name"].as_str().unwrap_or("");
            na.cmp(nb)
        });
        tools
    }

    /// Membrane, then probe gate, then the handler. Every outside caller
    /// (HTTP, WebSocket, agent tool calls) uses this path.
    pub async fn execute_gated(&self, req: &SkillExecutionRequest) -> SkillExecutionResponse {
        if let Err(reason) = crate::enforcement::probe_gate(&req.skill_name, &req.arguments).await {
            return SkillExecutionResponse {
                success: false,
                output: String::new(),
                error: Some(reason),
            };
        }
        self.execute(req)
    }

    /// Same gates as `execute_gated` (probe gate, then membrane, then handler)
    /// but returns a typed fault instead of a bare error string. Each call is a
    /// fresh dispatch; nothing is cached between calls.
    pub async fn dispatch_classified(
        &self,
        skill_name: &str,
        arguments: &serde_json::Value,
    ) -> Result<String, ToolFault> {
        crate::enforcement::probe_gate(skill_name, arguments)
            .await
            .map_err(ToolFault::rejected)?;
        crate::enforcement::pre_dispatch_check(skill_name, arguments)
            .map_err(ToolFault::rejected)?;
        let handler = {
            let map = self.skills.read().unwrap();
            map.get(skill_name).map(|(_, h)| h.clone())
        };
        let Some(h) = handler else {
            return Err(ToolFault::rejected(format!(
                "Skill '{}' not found in registry",
                skill_name
            )));
        };
        h(arguments.clone()).map_err(|err| {
            if err.starts_with("Unavailable") {
                ToolFault::unavailable(err)
            } else if skill_effect_profile(skill_name).idempotent {
                ToolFault::new(FailureClass::Failed, EffectCertainty::NoEffect, err)
            } else {
                ToolFault::new(FailureClass::Failed, EffectCertainty::EffectOccurred, err)
            }
        })
    }

    pub fn execute(&self, req: &SkillExecutionRequest) -> SkillExecutionResponse {
        if let Err(reason) = crate::enforcement::pre_dispatch_check(&req.skill_name, &req.arguments)
        {
            return SkillExecutionResponse {
                success: false,
                output: String::new(),
                error: Some(reason),
            };
        }
        let handler = {
            let map = self.skills.read().unwrap();
            map.get(&req.skill_name).map(|(_, h)| h.clone())
        };

        match handler {
            Some(h) => match h(req.arguments.clone()) {
                Ok(output) => SkillExecutionResponse {
                    success: true,
                    output,
                    error: None,
                },
                Err(err) => SkillExecutionResponse {
                    success: false,
                    output: String::new(),
                    error: Some(err),
                },
            },
            None => SkillExecutionResponse {
                success: false,
                output: String::new(),
                error: Some(format!("Skill '{}' not found in registry", req.skill_name)),
            },
        }
    }

    fn register_builtin_skills(&self) {
        // Builtin 1: bash_eval
        let ws_bash = self.workspace.clone();
        let bash_def = SkillDefinition {
            name: "bash_eval".to_string(),
            description: format!(
                "Run one catalogued local command inside {}. Allowed forms are exactly: git status, git diff, git log -1 --oneline, and ls. This is not an OS sandbox.",
                ws_bash.root().display()
            ),
            parameters_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string", "description": "The command line string to run" },
                    "cwd": { "type": "string", "description": format!("Optional working directory inside authorized workspace {}", ws_bash.root().display()) }
                },
                "required": ["command"]
            }),
            advertised: true,
        };
        self.register(bash_def, move |args| {
            let cmd = args.get("command").and_then(|c| c.as_str()).unwrap_or("");
            let cwd = args.get("cwd").and_then(|c| c.as_str());
            ws_bash
                .dispatch_shell(cmd, cwd, 15)
                .map_err(|e| e.to_string())
        });

        // Builtin 2: read_file
        let ws_read = self.workspace.clone();
        let read_def = SkillDefinition {
            name: "read_file".to_string(),
            description:
                "Read the contents of a text file strictly within authorized workspace roots"
                    .to_string(),
            parameters_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Relative or absolute path inside the workspace root" }
                },
                "required": ["path"]
            }),
            advertised: true,
        };
        self.register(read_def, move |args| {
            let path_str = args.get("path").and_then(|p| p.as_str()).unwrap_or("");
            let resolved = ws_read
                .resolve_read_path(path_str)
                .map_err(|e| e.to_string())?;
            std::fs::read_to_string(&resolved)
                .map_err(|e| format!("Failed to read file {}: {}", resolved.display(), e))
        });

        // Builtin 3: write_file
        let ws_write = self.workspace.clone();
        let write_def = SkillDefinition {
            name: "write_file".to_string(),
            description: "Write text contents to a file strictly within authorized workspace roots"
                .to_string(),
            parameters_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Target file path inside the workspace root" },
                    "content": { "type": "string", "description": "Text content to write" }
                },
                "required": ["path", "content"]
            }),
            advertised: true,
        };
        self.register(write_def, move |args| {
            let path_str = args.get("path").and_then(|p| p.as_str()).unwrap_or("");
            let content = args.get("content").and_then(|c| c.as_str()).unwrap_or("");
            let resolved = ws_write
                .resolve_write_path(path_str)
                .map_err(|e| e.to_string())?;
            if let Some(parent) = resolved.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            std::fs::write(&resolved, content)
                .map(|_| format!("Wrote {} bytes to {}", content.len(), resolved.display()))
                .map_err(|e| format!("Failed to write file {}: {}", resolved.display(), e))
        });

        // Builtin 4: list_dir
        let ws_list = self.workspace.clone();
        let list_def = SkillDefinition {
            name: "list_dir".to_string(),
            description: "List directory contents strictly within authorized workspace roots"
                .to_string(),
            parameters_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Directory path inside workspace root" }
                }
            }),
            advertised: true,
        };
        self.register(list_def, move |args| {
            let path_str = args.get("path").and_then(|p| p.as_str());
            let resolved = ws_list
                .resolve_dir_path(path_str)
                .map_err(|e| e.to_string())?;
            let entries = std::fs::read_dir(&resolved)
                .map_err(|e| format!("Failed to read directory {}: {}", resolved.display(), e))?;
            let mut items = Vec::new();
            for entry in entries.flatten() {
                let fname = entry.file_name().to_string_lossy().to_string();
                let ftype = if entry.path().is_dir() { "dir" } else { "file" };
                items.push(format!("{} ({})", fname, ftype));
            }
            items.sort();
            Ok(items.join("\n"))
        });

        // Builtin 5: git_status
        let ws_git = self.workspace.clone();
        let git_def = SkillDefinition {
            name: "git_status".to_string(),
            description: "Check git status and commit in repository strictly within authorized workspace root".to_string(),
            parameters_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Repository path inside workspace root" }
                }
            }),
            advertised: true,
        };
        self.register(git_def, move |args| {
            let path_str = args.get("path").and_then(|p| p.as_str());
            let resolved = ws_git
                .resolve_dir_path(path_str)
                .map_err(|e| e.to_string())?;
            let output = Command::new("git")
                .arg("-C")
                .arg(&resolved)
                .arg("status")
                .arg("--short")
                .output()
                .map_err(|e| format!("Failed to run git status: {}", e))?;
            let log_output = Command::new("git")
                .arg("-C")
                .arg(&resolved)
                .arg("log")
                .arg("-1")
                .arg("--oneline")
                .output()
                .map_err(|e| format!("Failed to run git log: {}", e))?;
            let status_str = String::from_utf8_lossy(&output.stdout);
            let log_str = String::from_utf8_lossy(&log_output.stdout);
            Ok(format!(
                "HEAD: {}\nStatus:\n{}",
                log_str.trim(),
                if status_str.is_empty() {
                    "clean"
                } else {
                    status_str.trim()
                }
            ))
        });

        // cortex.search is the canonical tool. It is not connected, so it is
        // not advertised. cortex_recall is the legacy alias and reports the same fact.
        let search_def = SkillDefinition {
            name: "cortex.search".to_string(),
            description: "Search Cortex. Not connected.".to_string(),
            parameters_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Memory query" }
                },
                "required": ["query"]
            }),
            advertised: false,
        };
        self.register(search_def, |_| {
            Err("Unavailable: cortex.search is not connected to a Cortex backend.".to_string())
        });
        let recall_def = SkillDefinition {
            name: "cortex_recall".to_string(),
            description: "Legacy alias of cortex.search. Not connected.".to_string(),
            parameters_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Memory query" }
                },
                "required": ["query"]
            }),
            advertised: false,
        };
        self.register(recall_def, |_| {
            Err("Unavailable: cortex.search is not connected to a Cortex backend.".to_string())
        });

        let ping_def = SkillDefinition {
            name: "telemetry_ping".to_string(),
            description: "Legacy name. Not connected.".to_string(),
            parameters_schema: serde_json::json!({ "type": "object" }),
            advertised: false,
        };
        self.register(ping_def, |_| {
            Err("Unavailable: telemetry.read is not connected.".to_string()) // no telemetry
        });
    }
}

#[async_trait::async_trait]
impl ToolDispatcher for SkillRegistry {
    async fn dispatch(
        &self,
        skill_name: &str,
        arguments: &serde_json::Value,
    ) -> Result<String, ToolFault> {
        self.dispatch_classified(skill_name, arguments).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skill_registry_registration_and_execution() {
        let registry = SkillRegistry::new();
        let list = registry.list_skills();
        assert!(!list.is_empty());

        let req = SkillExecutionRequest {
            skill_name: "telemetry_ping".to_string(),
            arguments: serde_json::json!({}),
        };
        let res = registry.execute(&req);
        assert!(!res.success);
        let err = res.error.unwrap_or_default();
        assert!(err.contains("Unavailable"));
        assert!(!registry.to_openai_tools().iter().any(
            |tool| tool["function"]["name"] == "telemetry_ping" // no telemetry
                || tool["function"]["name"] == "cortex_recall"
                || tool["function"]["name"] == "cortex.search"
        ));
    }

    #[test]
    fn test_to_openai_tools_formatting() {
        let registry = SkillRegistry::new();
        let tools = registry.to_openai_tools();
        assert!(tools.len() >= 5);
        let first = &tools[0];
        assert_eq!(first["type"], "function");
        assert!(first["function"]["name"].is_string());
    }

    #[test]
    fn test_filesystem_skills_with_workspace_containment() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace = tmp.path().join("workspace");
        let outside = tmp.path().join("outside.txt");
        std::fs::write(&outside, "outside capability boundary").unwrap();
        assert!(outside.is_file());
        let cap = WorkspaceCapability::new(&workspace).unwrap();
        let registry = SkillRegistry::with_workspace(cap);

        let w_req = SkillExecutionRequest {
            skill_name: "write_file".to_string(),
            arguments: serde_json::json!({
                "path": "test_write.txt",
                "content": "Sovereign native AIEN test content"
            }),
        };
        let w_res = registry.execute(&w_req);
        assert!(w_res.success);

        let r_req = SkillExecutionRequest {
            skill_name: "read_file".to_string(),
            arguments: serde_json::json!({
                "path": "test_write.txt",
            }),
        };
        let r_res = registry.execute(&r_req);
        assert!(r_res.success);
        assert_eq!(r_res.output, "Sovereign native AIEN test content");

        let l_req = SkillExecutionRequest {
            skill_name: "list_dir".to_string(),
            arguments: serde_json::json!({
                "path": ".",
            }),
        };
        let l_res = registry.execute(&l_req);
        assert!(l_res.success);
        assert!(l_res.output.contains("test_write.txt"));

        // The outside target must exist, so this exercises containment rather
        // than depending on the host temporary directory's depth and /etc.
        let escape_req = SkillExecutionRequest {
            skill_name: "read_file".to_string(),
            arguments: serde_json::json!({
                "path": "../outside.txt",
            }),
        };
        let escape_res = registry.execute(&escape_req);
        assert!(!escape_res.success);
        let error = escape_res.error.unwrap();
        assert!(error.contains("Security rejection"), "{error}");
        assert_eq!(
            std::fs::read_to_string(&outside).unwrap(),
            "outside capability boundary"
        );
    }
}
