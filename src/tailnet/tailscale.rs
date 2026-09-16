//! Thin wrappers over the `tailscale` / `tailscaled` binaries.
//!
//! The crate never links Tailscale. It runs the open-source daemon that ships
//! with the Homebrew formula, Linux packages, or the Windows MSI, one
//! userspace instance per tailnet. This module resolves those binaries and
//! parses the JSON the CLI emits so the rest of the module stays typed.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use url::Url;

use super::proxy::RouteTable;
use super::TailnetError;

/// Resolved Tailscale binaries.
///
/// Both come from the same install so their versions match. On macOS the
/// GUI app's daemon is a different build without a usable userspace mode
/// and is never used here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TailscaleBinaries {
    /// `tailscaled` daemon executable.
    pub tailscaled: PathBuf,
    /// `tailscale` CLI executable.
    pub tailscale: PathBuf,
}

impl TailscaleBinaries {
    /// Search `PATH` and common package-manager prefixes for `tailscaled`,
    /// then prefer the `tailscale` CLI next to it.
    pub fn discover() -> Result<Self, TailnetError> {
        let tailscaled =
            find_executable("tailscaled").ok_or_else(|| TailnetError::Unavailable {
                hint: install_hint(),
            })?;
        let sibling = tailscaled
            .parent()
            .map(|directory| directory.join(executable_name("tailscale")))
            .filter(|path| path.is_file());
        let Some(tailscale) = sibling.or_else(|| find_executable("tailscale")) else {
            return Err(TailnetError::Unavailable {
                hint: install_hint(),
            });
        };
        Ok(Self {
            tailscaled,
            tailscale,
        })
    }

    /// Use explicit executables, for example from application configuration.
    pub fn new(
        tailscaled: impl Into<PathBuf>,
        tailscale: impl Into<PathBuf>,
    ) -> Result<Self, TailnetError> {
        let binaries = Self {
            tailscaled: tailscaled.into(),
            tailscale: tailscale.into(),
        };
        for (field, path) in [
            ("tailscaled", &binaries.tailscaled),
            ("tailscale", &binaries.tailscale),
        ] {
            if !path.is_file() {
                return Err(TailnetError::Invalid {
                    field,
                    message: format!("{} is not an executable file", path.display()),
                });
            }
        }
        Ok(binaries)
    }
}

fn executable_name(name: &str) -> String {
    if cfg!(windows) {
        format!("{name}.exe")
    } else {
        name.to_string()
    }
}

fn find_executable(name: &str) -> Option<PathBuf> {
    let mut candidates = std::env::var_os("PATH")
        .map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
        .unwrap_or_default();
    candidates.extend([
        PathBuf::from("/opt/homebrew/bin"),
        PathBuf::from("/usr/local/bin"),
        PathBuf::from("/usr/sbin"),
        PathBuf::from("/usr/bin"),
    ]);
    #[cfg(windows)]
    if let Some(program_files) = std::env::var_os("ProgramFiles") {
        candidates.push(PathBuf::from(program_files).join("Tailscale"));
    }
    let file_name = executable_name(name);
    candidates
        .into_iter()
        .map(|directory| directory.join(&file_name))
        .find(|path| path.is_file())
}

/// Human-readable remedy shown when the binaries are missing.
pub fn install_hint() -> &'static str {
    if cfg!(target_os = "macos") {
        "Install the open-source daemon with `brew install tailscale`; the App Store app does not ship tailscaled."
    } else if cfg!(windows) {
        "Install Tailscale from tailscale.com/download; the MSI includes tailscaled.exe."
    } else {
        "Install the tailscale package for your distribution; it includes tailscaled."
    }
}

/// Subset of `tailscale status --json` the daemon relies on.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct StatusJson {
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub backend_state: String,
    #[serde(default, rename = "AuthURL")]
    pub auth_url: String,
    #[serde(default, rename = "MagicDNSSuffix")]
    pub magic_dns_suffix: String,
    #[serde(default)]
    pub current_tailnet: Option<CurrentTailnetJson>,
    #[serde(default, rename = "Self")]
    pub self_node: Option<NodeJson>,
    /// `Peer` is `null` (not `{}`) before login.
    #[serde(default, deserialize_with = "null_to_default")]
    pub peer: std::collections::BTreeMap<String, NodeJson>,
}

/// `#[serde(default)]` only covers a missing key; Tailscale emits explicit
/// `null` for empty collections.
fn null_to_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct CurrentTailnetJson {
    #[serde(default)]
    pub name: String,
    #[serde(default, rename = "MagicDNSSuffix")]
    pub magic_dns_suffix: String,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct NodeJson {
    #[serde(default)]
    pub host_name: String,
    #[serde(default, rename = "DNSName")]
    pub dns_name: String,
    #[serde(default, rename = "TailscaleIPs", deserialize_with = "null_to_default")]
    pub tailscale_ips: Vec<String>,
    #[serde(default)]
    pub online: bool,
}

impl StatusJson {
    pub(crate) fn parse(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    pub(crate) fn needs_login(&self) -> bool {
        self.backend_state == "NeedsLogin"
    }

    pub(crate) fn is_running(&self) -> bool {
        self.backend_state == "Running"
    }

    pub(crate) fn magic_dns_suffix(&self) -> Option<String> {
        let suffix = self
            .current_tailnet
            .as_ref()
            .map(|tailnet| tailnet.magic_dns_suffix.as_str())
            .filter(|suffix| !suffix.is_empty())
            .unwrap_or(self.magic_dns_suffix.as_str());
        (!suffix.is_empty()).then(|| suffix.trim_end_matches('.').to_ascii_lowercase())
    }

    pub(crate) fn tailnet_name(&self) -> Option<String> {
        self.current_tailnet
            .as_ref()
            .map(|tailnet| tailnet.name.clone())
            .filter(|name| !name.is_empty())
    }

    pub(crate) fn self_dns_name(&self) -> Option<String> {
        self.self_node
            .as_ref()
            .map(|node| node.dns_name.trim_end_matches('.').to_string())
            .filter(|name| !name.is_empty())
    }

    pub(crate) fn self_ips(&self) -> Vec<String> {
        self.self_node
            .as_ref()
            .map(|node| node.tailscale_ips.clone())
            .unwrap_or_default()
    }

    /// The names the split proxy must send through tailscaled.
    pub(crate) fn route_table(&self) -> RouteTable {
        let mut dns_names = Vec::new();
        let mut host_names = Vec::new();
        // Before login the daemon reports the OS hostname (e.g.
        // `my-mac.local`) as "self"; that is not a tailnet name.
        if !self.is_running() {
            return RouteTable {
                magic_dns_suffix: self.magic_dns_suffix(),
                dns_names,
                host_names,
            };
        }
        for node in self.self_node.iter().chain(self.peer.values()) {
            let dns = node.dns_name.trim_end_matches('.').to_ascii_lowercase();
            if !dns.is_empty() {
                dns_names.push(dns);
            }
            let host = node.host_name.trim().to_ascii_lowercase();
            if !host.is_empty() && !host.contains(' ') {
                host_names.push(host);
            }
        }
        dns_names.sort();
        dns_names.dedup();
        host_names.sort();
        host_names.dedup();
        RouteTable {
            magic_dns_suffix: self.magic_dns_suffix(),
            dns_names,
            host_names,
        }
    }

    pub(crate) fn online_peer_count(&self) -> usize {
        self.peer.values().filter(|peer| peer.online).count()
    }
}

/// One line of `tailscale up --json` output. The CLI prints an `AuthURL`
/// object while it waits for the browser login, then a final object once
/// the backend reaches `Running`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct UpJsonLine {
    #[serde(default, rename = "AuthURL")]
    pub auth_url: String,
    #[serde(default)]
    pub backend_state: String,
}

impl UpJsonLine {
    pub(crate) fn parse(line: &str) -> Option<Self> {
        let line = line.trim();
        if !line.starts_with('{') {
            return None;
        }
        serde_json::from_str(line).ok()
    }
}

/// Fallback for CLIs that print the login URL as text.
pub(crate) fn extract_auth_url(line: &str, login_server: Option<&Url>) -> Option<String> {
    line.split_whitespace().find_map(|token| {
        let candidate = token.trim_end_matches(['.', ',', ')', ']', '}']);
        let parsed = Url::parse(candidate).ok()?;
        let is_tailscale =
            parsed.scheme() == "https" && parsed.host_str() == Some("login.tailscale.com");
        let is_headscale = login_server.is_some_and(|server| same_origin(server, &parsed));
        (is_tailscale || is_headscale).then(|| candidate.to_string())
    })
}

fn same_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

/// Run `tailscale --socket <socket> <args>` with a deadline. Returns stdout
/// on success; the error carries stderr so a host can show it verbatim.
pub(crate) async fn run_cli(
    tailscale: &Path,
    socket: &Path,
    args: &[&str],
    timeout: Duration,
) -> Result<String, TailnetError> {
    let command_name = format!("tailscale {}", args.join(" "));
    let mut command = tokio::process::Command::new(tailscale);
    command
        .arg(format!("--socket={}", socket.display()))
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let output = tokio::time::timeout(timeout, command.output())
        .await
        .map_err(|_| TailnetError::Cli {
            command: command_name.clone(),
            message: format!("timed out after {timeout:?}"),
        })?
        .map_err(|error| TailnetError::Cli {
            command: command_name.clone(),
            message: error.to_string(),
        })?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr = stderr.trim();
        Err(TailnetError::Cli {
            command: command_name,
            message: if stderr.is_empty() {
                format!("exited with {}", output.status)
            } else {
                redact_auth_urls(stderr).chars().take(1024).collect()
            },
        })
    }
}

fn redact_auth_urls(text: &str) -> String {
    const PREFIX: &str = "https://login.tailscale.com/";
    let mut redacted = String::with_capacity(text.len());
    let mut remaining = text;
    while let Some(start) = remaining.find(PREFIX) {
        redacted.push_str(&remaining[..start]);
        let after = &remaining[start + PREFIX.len()..];
        let token_len = after
            .find(|character: char| {
                character.is_whitespace() || matches!(character, '"' | '\'' | ',' | ']' | '}' | ')')
            })
            .unwrap_or(after.len());
        redacted.push_str("[REDACTED LOGIN URL]");
        remaining = &after[token_len..];
    }
    redacted.push_str(remaining);
    redacted
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"{
      "Version": "1.94.2",
      "BackendState": "Running",
      "AuthURL": "",
      "MagicDNSSuffix": "jerboa-altered.ts.net",
      "CurrentTailnet": {"Name": "gala.games", "MagicDNSSuffix": "jerboa-altered.ts.net", "MagicDNSEnabled": true},
      "Self": {"HostName": "David’s Mac Studio (2)", "DNSName": "davids-mac-studio-2-2.jerboa-altered.ts.net.", "TailscaleIPs": ["100.76.250.19"], "Online": true},
      "Peer": {
        "nodekey:abc": {"HostName": "stage-node-dcdn-cache", "DNSName": "stage-node-dcdn-cache-7.jerboa-altered.ts.net.", "TailscaleIPs": ["100.111.159.111"], "Online": false},
        "nodekey:def": {"HostName": "grafana", "DNSName": "grafana.jerboa-altered.ts.net.", "TailscaleIPs": ["100.100.1.1"], "Online": true}
      }
    }"#;

    #[test]
    fn redacts_login_urls_from_cli_failures() {
        let redacted =
            redact_auth_urls("login at https://login.tailscale.com/a/sensitive-token, then retry");
        assert_eq!(redacted, "login at [REDACTED LOGIN URL], then retry");
        assert!(!redacted.contains("sensitive-token"));
    }

    #[test]
    fn parses_status_and_builds_route_table() {
        let status = StatusJson::parse(SAMPLE).unwrap();
        assert!(status.is_running());
        assert_eq!(status.tailnet_name().as_deref(), Some("gala.games"));
        assert_eq!(
            status.magic_dns_suffix().as_deref(),
            Some("jerboa-altered.ts.net")
        );
        assert_eq!(
            status.self_dns_name().as_deref(),
            Some("davids-mac-studio-2-2.jerboa-altered.ts.net")
        );
        assert_eq!(status.online_peer_count(), 1);
        let routes = status.route_table();
        assert_eq!(
            routes.dns_names,
            vec![
                "davids-mac-studio-2-2.jerboa-altered.ts.net".to_string(),
                "grafana.jerboa-altered.ts.net".to_string(),
                "stage-node-dcdn-cache-7.jerboa-altered.ts.net".to_string(),
            ]
        );
        // Hostnames with spaces are never valid DNS labels and are skipped.
        assert_eq!(
            routes.host_names,
            vec!["grafana".to_string(), "stage-node-dcdn-cache".to_string()]
        );
        assert!(routes.is_tailnet_host("grafana"));
    }

    #[test]
    fn route_table_ignores_os_hostname_before_login() {
        let status = StatusJson::parse(
            r#"{"BackendState":"NeedsLogin","Self":{"HostName":"my-mac.local","DNSName":"","TailscaleIPs":null},"Peer":null}"#,
        )
        .unwrap();
        assert!(status.route_table().host_names.is_empty());
        assert!(status.route_table().dns_names.is_empty());
    }

    #[test]
    fn parses_needs_login_status_with_auth_url() {
        let status = StatusJson::parse(
            r#"{"BackendState":"NeedsLogin","AuthURL":"https://login.tailscale.com/a/abc123","Peer":null}"#,
        )
        .unwrap();
        assert!(status.needs_login());
        assert_eq!(status.auth_url, "https://login.tailscale.com/a/abc123");
        assert_eq!(status.magic_dns_suffix(), None);
    }

    #[test]
    fn parses_up_json_lines_and_text_fallback() {
        let line = UpJsonLine::parse(r#"{"AuthURL":"https://login.tailscale.com/a/xyz","QR":""}"#)
            .unwrap();
        assert_eq!(line.auth_url, "https://login.tailscale.com/a/xyz");
        assert!(UpJsonLine::parse("To authenticate, visit:").is_none());
        assert_eq!(
            extract_auth_url(
                "To authenticate, visit:\n\thttps://login.tailscale.com/a/xyz",
                None,
            ),
            Some("https://login.tailscale.com/a/xyz".to_string())
        );
        let headscale = Url::parse("https://vpn.example.test").unwrap();
        assert_eq!(
            extract_auth_url(
                "Register at https://vpn.example.test/register/sensitive-auth-id.",
                Some(&headscale),
            ),
            Some("https://vpn.example.test/register/sensitive-auth-id".to_string())
        );
        assert_eq!(
            extract_auth_url(
                "Ignore https://vpn.example.test.attacker.invalid/register/token",
                Some(&headscale),
            ),
            None
        );
        assert_eq!(extract_auth_url("Success.", None), None);
    }

    #[test]
    fn explicit_binaries_must_exist() {
        let temp = tempfile::tempdir().unwrap();
        let tailscaled = temp.path().join("tailscaled");
        let tailscale = temp.path().join("tailscale");
        std::fs::write(&tailscaled, "stub").unwrap();
        std::fs::write(&tailscale, "stub").unwrap();
        assert!(TailscaleBinaries::new(&tailscaled, &tailscale).is_ok());
        let missing = TailscaleBinaries::new(&tailscaled, temp.path().join("nope"));
        assert!(matches!(
            missing,
            Err(TailnetError::Invalid {
                field: "tailscale",
                ..
            })
        ));
    }
}
