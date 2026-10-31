//! Capability-bound workspace security and canonical path resolution.
//! Enforces containment for filesystem access and shell execution within authorized workspace roots.

use std::ffi::CString;
use std::fs::File;
use std::io::Write;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecurityError {
    AccessDenied(String),
    PathNotFound(String),
    InvalidPath(String),
    IoError(String),
    CommandFailed(String),
}

impl std::fmt::Display for SecurityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AccessDenied(msg) => write!(f, "Security rejection: {}", msg),
            Self::PathNotFound(msg) => write!(f, "Path not found: {}", msg),
            Self::InvalidPath(msg) => write!(f, "{}", msg),
            Self::IoError(msg) => write!(f, "I/O error: {}", msg),
            Self::CommandFailed(msg) => write!(f, "Command execution failed: {}", msg),
        }
    }
}

impl std::error::Error for SecurityError {}

#[derive(Debug, Clone)]
pub struct WorkspaceCapability {
    root: PathBuf,
    root_dir: std::sync::Arc<File>,
}

impl Default for WorkspaceCapability {
    fn default() -> Self {
        Self::detect()
    }
}

impl WorkspaceCapability {
    /// Constructs a capability strictly bound to the specified root directory.
    pub fn new<P: AsRef<Path>>(root: P) -> Result<Self, SecurityError> {
        let p = root.as_ref();
        if !p.exists() {
            std::fs::create_dir_all(p).map_err(|e| {
                SecurityError::IoError(format!(
                    "Failed to create workspace directory {}: {}",
                    p.display(),
                    e
                ))
            })?;
        }
        let canonical = p.canonicalize().map_err(|e| {
            SecurityError::IoError(format!(
                "Failed to canonicalize workspace root {}: {}",
                p.display(),
                e
            ))
        })?;
        // Walk the canonical root from / with no-follow directory opens too:
        // canonicalize followed by File::open would race a replaced ancestor.
        let mut root_dir = File::open("/")
            .map_err(|e| SecurityError::IoError(format!("Failed to open filesystem root: {e}")))?;
        for component in canonical.components() {
            if let Component::Normal(name) = component {
                let name = CString::new(name.as_bytes())
                    .map_err(|_| SecurityError::InvalidPath("NUL in workspace root".into()))?;
                let fd = unsafe {
                    libc::openat(
                        root_dir.as_raw_fd(),
                        name.as_ptr(),
                        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                    )
                };
                if fd < 0 {
                    return Err(SecurityError::AccessDenied(format!(
                        "Unsafe workspace root: {}",
                        std::io::Error::last_os_error()
                    )));
                }
                root_dir = unsafe { File::from_raw_fd(fd) };
            }
        }
        Ok(Self {
            root: canonical,
            root_dir: std::sync::Arc::new(root_dir),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Automatically detects the active workspace root.
    pub fn detect() -> Self {
        if let Ok(ws) = std::env::var("AEGIS_WORKSPACE") {
            if let Ok(cap) = Self::new(&ws) {
                return cap;
            }
        }
        if let Ok(cwd) = std::env::current_dir() {
            if let Ok(cap) = Self::new(&cwd) {
                return cap;
            }
        }
        if let Ok(home) = std::env::var("HOME") {
            let candidate = PathBuf::from(home).join("workspace");
            if candidate.exists() {
                if let Ok(cap) = Self::new(&candidate) {
                    return cap;
                }
            }
        }
        Self::new(".").expect("Aegis requires a readable workspace directory")
    }

    /// Resolves an existing file path for reading, strictly verifying that it resides within the workspace root.
    /// Canonicalizes the path and rejects traversal escaping the root.
    pub fn resolve_read_path(&self, requested: &str) -> Result<PathBuf, SecurityError> {
        let trimmed = requested.trim();
        if trimmed.is_empty() {
            return Err(SecurityError::InvalidPath(
                "Path cannot be empty".to_string(),
            ));
        }

        let candidate = if Path::new(trimmed).is_absolute() {
            PathBuf::from(trimmed)
        } else {
            self.root.join(trimmed)
        };

        if !candidate.exists() {
            return Err(SecurityError::PathNotFound(format!(
                "File does not exist: {}",
                trimmed
            )));
        }

        let canonical = candidate.canonicalize().map_err(|e| {
            SecurityError::IoError(format!("Failed to canonicalize path {}: {}", trimmed, e))
        })?;

        if !canonical.starts_with(&self.root) {
            return Err(SecurityError::AccessDenied(format!(
                "Path {} escapes authorized workspace root {}",
                canonical.display(),
                self.root.display()
            )));
        }

        Ok(canonical)
    }

    /// Open each component relative to the held workspace directory. No path-based
    /// containment check is reused for the final open, and no symlink is followed.
    pub fn write_file(&self, requested: &str, content: &str) -> Result<PathBuf, SecurityError> {
        let trimmed = requested.trim();
        if trimmed.is_empty() {
            return Err(SecurityError::InvalidPath("Path cannot be empty".into()));
        }
        let target = if Path::new(trimmed).is_absolute() {
            normalize_path(Path::new(trimmed))
        } else {
            normalize_path(&self.root.join(trimmed))
        };
        let relative = target.strip_prefix(&self.root).map_err(|_| {
            SecurityError::AccessDenied(format!("Path {} escapes workspace", target.display()))
        })?;
        let mut names = relative.components().map(|c| c.as_os_str()).peekable();
        if names.peek().is_none() {
            return Err(SecurityError::InvalidPath("Path must name a file".into()));
        }
        let mut dir = self
            .root_dir
            .try_clone()
            .map_err(|e| SecurityError::IoError(e.to_string()))?;
        while let Some(name) = names.next() {
            let name = CString::new(name.as_bytes())
                .map_err(|_| SecurityError::InvalidPath("NUL in path".into()))?;
            if names.peek().is_some() {
                let mut fd = unsafe {
                    libc::openat(
                        dir.as_raw_fd(),
                        name.as_ptr(),
                        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                    )
                };
                if fd < 0 && std::io::Error::last_os_error().kind() == std::io::ErrorKind::NotFound
                {
                    let rc = unsafe { libc::mkdirat(dir.as_raw_fd(), name.as_ptr(), 0o777) };
                    if rc < 0
                        && std::io::Error::last_os_error().kind()
                            != std::io::ErrorKind::AlreadyExists
                    {
                        return Err(SecurityError::IoError(
                            std::io::Error::last_os_error().to_string(),
                        ));
                    }
                    fd = unsafe {
                        libc::openat(
                            dir.as_raw_fd(),
                            name.as_ptr(),
                            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                        )
                    };
                }
                if fd < 0 {
                    return Err(SecurityError::AccessDenied(format!(
                        "Unsafe workspace directory: {}",
                        std::io::Error::last_os_error()
                    )));
                }
                dir = unsafe { File::from_raw_fd(fd) };
            } else {
                let fd = unsafe {
                    libc::openat(
                        dir.as_raw_fd(),
                        name.as_ptr(),
                        libc::O_WRONLY
                            | libc::O_CREAT
                            | libc::O_NOFOLLOW
                            | libc::O_CLOEXEC
                            | libc::O_NONBLOCK,
                        0o666,
                    )
                };
                if fd < 0 {
                    return Err(SecurityError::AccessDenied(format!(
                        "Unsafe workspace file: {}",
                        std::io::Error::last_os_error()
                    )));
                }
                let mut file = unsafe { File::from_raw_fd(fd) };
                let metadata = file
                    .metadata()
                    .map_err(|e| SecurityError::IoError(e.to_string()))?;
                if !metadata.is_file() || metadata.nlink() != 1 {
                    return Err(SecurityError::AccessDenied(
                        "Target is not a private regular file".into(),
                    ));
                }
                file.set_len(0)
                    .map_err(|e| SecurityError::IoError(e.to_string()))?;
                file.write_all(content.as_bytes())
                    .map_err(|e| SecurityError::IoError(e.to_string()))?;
            }
        }
        Ok(target)
    }
    /// Resolves an existing directory path (for listing or working directory), ensuring it stays within root.
    pub fn resolve_dir_path(&self, requested: Option<&str>) -> Result<PathBuf, SecurityError> {
        let target = match requested {
            Some(d) if !d.trim().is_empty() && d.trim() != "." => {
                let p = Path::new(d.trim());
                if p.is_absolute() {
                    p.to_path_buf()
                } else {
                    self.root.join(p)
                }
            }
            _ => self.root.clone(),
        };

        if !target.exists() {
            return Err(SecurityError::PathNotFound(format!(
                "Directory does not exist: {}",
                target.display()
            )));
        }

        let canonical = target.canonicalize().map_err(|e| {
            SecurityError::IoError(format!(
                "Failed to canonicalize directory {}: {}",
                target.display(),
                e
            ))
        })?;

        if !canonical.is_dir() {
            return Err(SecurityError::InvalidPath(format!(
                "Path is not a directory: {}",
                canonical.display()
            )));
        }

        if !canonical.starts_with(&self.root) {
            return Err(SecurityError::AccessDenied(format!(
                "Directory {} escapes authorized workspace root {}",
                canonical.display(),
                self.root.display()
            )));
        }

        Ok(canonical)
    }

    /// The only shell entry. The membrane runs first. A command then has to
    /// match a whole local form before the workspace-bound executor runs.
    pub fn dispatch_shell(
        &self,
        command: &str,
        cwd: Option<&str>,
        timeout_secs: u64,
    ) -> Result<String, SecurityError> {
        let args = serde_json::json!({ "command": command });
        crate::enforcement::pre_dispatch_check("bash_eval", &args)
            .map_err(SecurityError::AccessDenied)?;
        admit_local_command(command)?;
        self.execute_shell(command, cwd, timeout_secs)
    }

    /// `dispatch_shell` behind the probe gate. HTTP, WebSocket and heartbeat
    /// shell paths use this.
    pub async fn dispatch_shell_gated(
        &self,
        command: &str,
        cwd: Option<&str>,
        timeout_secs: u64,
    ) -> Result<String, SecurityError> {
        let args = serde_json::json!({ "command": command });
        crate::enforcement::probe_gate("bash_eval", &args)
            .await
            .map_err(SecurityError::AccessDenied)?;
        self.dispatch_shell(command, cwd, timeout_secs)
    }

    /// Low-level executor. Callers use `dispatch_shell`.
    pub(crate) fn execute_shell(
        &self,
        command: &str,
        cwd: Option<&str>,
        timeout_secs: u64,
    ) -> Result<String, SecurityError> {
        let trimmed_cmd = command.trim();
        if trimmed_cmd.is_empty() {
            return Err(SecurityError::InvalidPath(
                "Command cannot be empty".to_string(),
            ));
        }

        let working_dir = self.resolve_dir_path(cwd)?;

        let escaped_cmd = trimmed_cmd.replace('\'', "'\\''");
        let wrapped_cmd = format!("timeout {}s bash -c '{}'", timeout_secs, escaped_cmd);

        let output = Command::new("sh")
            .arg("-c")
            .arg(&wrapped_cmd)
            .current_dir(&working_dir)
            .output()
            .map_err(|e| {
                SecurityError::CommandFailed(format!("Failed to execute command: {}", e))
            })?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        if !output.status.success() {
            let code = output.status.code().unwrap_or(-1);
            if code == 124 {
                return Err(SecurityError::CommandFailed(format!(
                    "Command timed out after {} seconds: {}",
                    timeout_secs, trimmed_cmd
                )));
            }
            return Err(SecurityError::CommandFailed(format!(
                "Exit code {}: {}",
                code,
                if !stderr.trim().is_empty() {
                    stderr.trim()
                } else {
                    stdout.trim()
                }
            )));
        }

        Ok(stdout)
    }
}

/// A local command is one exact form from the catalog. The first word is not enough.
pub fn admit_local_command(command: &str) -> Result<(), SecurityError> {
    if command.chars().any(|c| {
        matches!(
            c,
            ';' | '|'
                | '&'
                | '$'
                | '<'
                | '>'
                | '`'
                | '\\'
                | '\n'
                | '\r'
                | '('
                | ')'
                | '{'
                | '}'
                | '!'
                | '*'
                | '?'
                | '['
                | ']'
                | '\''
                | '"'
        )
    }) {
        return Err(SecurityError::AccessDenied(
            "Command is not a local form. Shell joining, substitution, and quoting are not local execution.".to_string(),
        ));
    }
    let argv: Vec<&str> = command.split_whitespace().collect();
    let allowed = [
        ["git", "status"].as_slice(),
        ["git", "diff"].as_slice(),
        ["git", "log", "-1", "--oneline"].as_slice(),
        ["ls"].as_slice(),
    ];
    if allowed.iter().any(|form| form == &argv) {
        return Ok(());
    }
    Err(SecurityError::AccessDenied(
        "Command is not eligible for local shell. Local execution is only the catalogued forms: git status, git diff, git log -1 --oneline, and ls. Anything else needs a typed tool.".to_string(),
    ))
}

pub fn normalize_path(path: &Path) -> PathBuf {
    let mut stack = Vec::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                stack.pop();
            }
            c => {
                stack.push(c.as_os_str());
            }
        }
    }
    stack.iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_workspace_capability_read_write_containment() {
        let dir = tempdir().unwrap();
        let cap = WorkspaceCapability::new(dir.path()).unwrap();

        let write_target = cap.write_file("nested/test.txt", "secure data").unwrap();
        assert!(write_target.starts_with(cap.root()));

        let read_target = cap.resolve_read_path("nested/test.txt").unwrap();
        assert_eq!(read_target, write_target.canonicalize().unwrap());

        let escape_read = cap.resolve_read_path("../../etc/passwd");
        assert!(escape_read.is_err());

        let escape_write = cap.write_file("../../../evil.sh", "not written");
        assert!(escape_write.is_err());
    }

    #[test]
    fn test_workspace_capability_shell_execution() {
        let dir = tempdir().unwrap();
        let cap = WorkspaceCapability::new(dir.path()).unwrap();

        let out = cap
            .execute_shell("echo 'contained shell'", None, 5)
            .unwrap();
        assert!(out.contains("contained shell"));

        let res = cap.execute_shell("ls", Some("/etc"), 5);
        assert!(res.is_err());
    }

    #[test]
    fn local_catalog_requires_the_whole_command() {
        assert!(admit_local_command("git status").is_ok());
        assert!(admit_local_command("git diff").is_ok());
        assert!(admit_local_command("ls").is_ok());
        assert!(admit_local_command("git").is_err());
        assert!(admit_local_command("git push").is_err());
        assert!(admit_local_command("git status --porcelain").is_err());
        assert!(admit_local_command("curl example.invalid").is_err());
        assert!(admit_local_command("git status; curl example.invalid").is_err());
        assert!(admit_local_command("echo $(curl example.invalid)").is_err());
    }

    #[test]
    fn dispatch_shell_refuses_unclassified_text() {
        let dir = tempdir().unwrap();
        let cap = WorkspaceCapability::new(dir.path()).unwrap();
        let refused = cap.dispatch_shell("echo hello", None, 5);
        assert!(refused.is_err());
        let allowed = cap.dispatch_shell("ls", None, 5);
        assert!(allowed.is_ok());
    }
}
